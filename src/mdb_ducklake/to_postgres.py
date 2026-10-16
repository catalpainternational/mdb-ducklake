"""Set-based sync: DuckLake mirror -> partisipa Postgres (Option B).

Upsert-only and idempotent. For each mapped table, DuckDB streams the mirror's
Parquet into a Postgres staging table (mapping Access column names -> Postgres
names, TRY_CAST to the Postgres target type so a bad value becomes NULL rather
than dropping a row, and localizing naive timestamps to UTC). Then a single
psycopg2 transaction runs every `INSERT ... ON CONFLICT (pk) DO UPDATE`; Django
foreign keys are DEFERRABLE INITIALLY DEFERRED, so tables can be upserted in any
order and constraints are checked once at COMMIT.

Deletion of rows no longer present in the mirror is a SEPARATE, guarded command
(see prune_postgres.py) — never a side effect of this sync.

Mapping artifact: produced by partisipa's `manage.py dump_ducklake_mapping`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional

import duckdb

from mdb_ducklake.sync import _sql_str, open_lake

STAGING_SCHEMA = "mdb_stg"


@dataclass
class LakeConfig:
    """Where the DuckLake mirror lives and how to reach it.

    Pass both S3 credentials, or neither to read them from the environment
    (see mdb_ducklake.sync.S3_KEY_ENV / S3_SECRET_ENV).
    """

    catalog: str
    data_path: str
    s3_endpoint: Optional[str] = None
    s3_region: Optional[str] = None
    s3_key_id: Optional[str] = field(default=None, repr=False)
    s3_secret: Optional[str] = field(default=None, repr=False)


def add_lake_args(ap: argparse.ArgumentParser) -> None:
    """The --catalog/--data-path/--s3-* options shared by the Postgres CLIs."""
    ap.add_argument("--catalog", default=os.environ.get("DUCKLAKE_CATALOG", "s3://ducklake/pnds/catalog.ducklake"))
    ap.add_argument("--data-path", default=os.environ.get("DUCKLAKE_DATA", "s3://ducklake/pnds/data"))
    ap.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT", "sgp1.digitaloceanspaces.com"))
    ap.add_argument("--s3-region", default=os.environ.get("S3_REGION", "sgp1"))


def lake_from_args(args) -> LakeConfig:
    """A LakeConfig from CLI options; S3 credentials come from the environment."""
    return LakeConfig(args.catalog, args.data_path, args.s3_endpoint, args.s3_region)


@contextmanager
def mirror_and_postgres(lake: LakeConfig, dsn: str, *, read_only: bool, staging: bool = False):
    """A DuckDB connection with the mirror attached as `lake` and Postgres as `pg`.

    With staging=True, the mdb_stg schema is created first and dropped on the
    way out. The connection is always closed, and DuckDB errors are re-raised
    with the DSN and any password removed, because DuckDB quotes the whole
    connection string when it cannot reach Postgres. (The DSN still sits in
    the callers' local variables, which some error trackers record.)
    """
    try:
        con = open_lake(
            lake.catalog, lake.data_path,
            s3_endpoint=lake.s3_endpoint, s3_region=lake.s3_region,
            s3_key_id=lake.s3_key_id, s3_secret=lake.s3_secret,
        ).con
        try:
            con.execute("SET enable_progress_bar=false")  # keep stdout clean for the report
            con.execute("INSTALL postgres")
            con.execute("LOAD postgres")
            opts = "TYPE postgres, READ_ONLY" if read_only else "TYPE postgres"
            con.execute(f"ATTACH {_sql_str(dsn)} AS pg ({opts})")
            if staging:
                con.execute(f"CREATE SCHEMA IF NOT EXISTS pg.{q(STAGING_SCHEMA)}")
            try:
                yield con
            except BaseException:
                if staging:
                    # Best effort: never let a failed cleanup replace the real error.
                    try:
                        con.execute(f"DROP SCHEMA IF EXISTS pg.{q(STAGING_SCHEMA)} CASCADE")
                    except duckdb.Error:
                        pass
                raise
            if staging:
                con.execute(f"DROP SCHEMA IF EXISTS pg.{q(STAGING_SCHEMA)} CASCADE")
        finally:
            con.close()
    except duckdb.Error as exc:
        # Scrub in place and re-raise the same object: no unscrubbed copy is
        # left behind in __context__ for a logger or error tracker to find.
        exc.args = (without_dsn(str(exc), dsn),)
        raise


_PASSWORD = re.compile(r"(password\s*=\s*)('(?:[^'\\]|\\.)*'|\S+)|(://[^:/@\s]*:)[^@\s]*@", re.I)


def without_dsn(msg: str, dsn: str) -> str:
    """`msg` with the DSN and any password in it replaced."""
    msg = msg.replace(dsn, "<dsn>")
    return _PASSWORD.sub(lambda m: f"{m.group(1)}***" if m.group(1) else f"{m.group(3)}***@", msg)


def selected_tables(mapping, include):
    """syncable() narrowed to `include` (all tables when include is None)."""
    tables = list(syncable(mapping))
    if include is not None:
        want = set(include)
        tables = [(t, s) for t, s in tables if t in want]
    return tables


def dsn_from_env() -> str:
    """The CLIs' Postgres connection string, from the PG* environment variables."""
    parts = dict(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "partisipa_db"),
        user=os.environ.get("PGUSER", "partisipa"),
        password=os.environ["PGPASSWORD"],
    )
    return " ".join(f"{k}={v}" for k, v in parts.items())


def q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


_NUM = re.compile(r"numeric\((\d+),\s*(\d+)\)", re.I)


def pg_type_to_duckdb(pg_type: str) -> str:
    """Map a Postgres type (from the mapping) to a DuckDB cast target.

    Precision is preserved for numeric so TRY_CAST matches the target exactly
    (and nulls out overflows). varchar length is dropped (DuckDB VARCHAR is
    unbounded; Access text is short).
    """
    t = pg_type.strip().lower()
    m = _NUM.match(t)
    if m:
        return f"DECIMAL({m.group(1)},{m.group(2)})"
    if t.startswith(("varchar", "character varying", "char", "text", "uuid", "citext")):
        return "VARCHAR"
    return {
        "integer": "INTEGER",
        "bigint": "BIGINT",
        "smallint": "SMALLINT",
        "boolean": "BOOLEAN",
        "double precision": "DOUBLE",
        "real": "FLOAT",
        "numeric": "DECIMAL(19,4)",
        "date": "DATE",
        "timestamp with time zone": "TIMESTAMPTZ",
        "timestamp without time zone": "TIMESTAMP",
    }.get(t, "VARCHAR")


@dataclass
class TableResult:
    table: str
    source_rows: int
    target_rows: int
    mapped_cols: int
    ok: bool
    note: str = ""


def _mirror_columns(con):
    """{lake_table: {col: duckdb_type}} for every table in the attached mirror."""
    rows = con.execute(
        "SELECT table_name, column_name, data_type FROM information_schema.columns "
        "WHERE table_catalog='lake'"
    ).fetchall()
    out: dict[str, dict[str, str]] = {}
    for tbl, col, typ in rows:
        out.setdefault(tbl, {})[col] = typ
    return out


def syncable(mapping):
    """Yield (pg_table, spec) for real, mirror-backed tables (skip views)."""
    for pg_table, spec in mapping["tables"].items():
        if not spec.get("access_table") or not spec.get("pk_access_col"):
            continue  # a view / not mirror-sourced
        yield pg_table, spec


INTEGER_DUCK_TYPES = {"SMALLINT", "INTEGER", "BIGINT", "UTINYINT"}


def mapped_projection(spec, src_types):
    """The mirror -> Postgres column projection, shared by sync and validate.

    Returns [(pg_col, cast_expr, is_pk)] — one DuckDB expression per column that
    casts the mirror value to the Postgres target type (TRY_CAST -> NULL on bad
    values) and UTC-localizes naive timestamps. Columns absent from the mirror
    are dropped. Guarantees validate compares exactly what sync would write.
    """
    out = []
    for c in spec["columns"]:
        acc, pgc = c["access_col"], c["pg_col"]
        if not acc or acc not in src_types:
            continue
        src_type = src_types[acc].upper()
        duck_type = pg_type_to_duckdb(c["pg_type"])
        base = q(acc)
        if src_type == "TIMESTAMP" and duck_type == "TIMESTAMPTZ":
            base = f"({base} AT TIME ZONE 'UTC')"
        fractional_src = any(k in src_type for k in ("DECIMAL", "DOUBLE", "FLOAT", "NUMERIC"))
        if duck_type in INTEGER_DUCK_TYPES and fractional_src:
            # Access Currency/Double landing in an INTEGER column: truncate toward
            # zero to match the ORM importer's Python int() coercion — DuckDB's
            # cast would round-to-nearest, giving off-by-one vs partisipa today.
            cast = f"TRY_CAST(trunc({base}) AS {duck_type})"
        else:
            cast = f"TRY_CAST({base} AS {duck_type})"
        out.append((pgc, cast, c["is_pk"]))
    return out


def stage_table(con, pg_schema, pg_table, spec, mirror_cols):
    """DuckDB: build a Postgres staging table from the mirror. Returns column pairs."""
    proj = mapped_projection(spec, mirror_cols[spec["access_table"]])
    exprs = [f"{cast} AS {q(pgc)}" for pgc, cast, _ in proj]
    stg = f"pg.{q(STAGING_SCHEMA)}.{q(pg_table)}"
    con.execute(f"DROP TABLE IF EXISTS {stg}")
    con.execute(f"CREATE TABLE {stg} AS SELECT {', '.join(exprs)} FROM lake.{q(spec['access_table'])}")
    return [(pgc, is_pk) for pgc, _, is_pk in proj]


def load_order(specs) -> list:
    """Parents before children (a table after all its fk_targets). DFS topo sort.

    With FK checks in IMMEDIATE mode this is the order that lets upserts satisfy
    constraints statement-by-statement; reverse it for deletes.
    """
    order, seen, stack = [], set(), set()

    def visit(t):
        if t in seen or t not in specs:
            return
        if t in stack:  # cycle: best effort, don't recurse
            return
        stack.add(t)
        for parent in specs[t].get("fk_targets", []):
            visit(parent)
        stack.discard(t)
        seen.add(t)
        order.append(t)

    for t in specs:
        visit(t)
    return order


def upsert_sql(pg_schema, pg_table, pk_col, cols):
    collist = ", ".join(q(c) for c in cols)
    non_pk = [c for c in cols if c != pk_col]
    setlist = ", ".join(f"{q(c)}=EXCLUDED.{q(c)}" for c in non_pk) or f"{q(pk_col)}={q(pk_col)}"
    sql = (
        f"INSERT INTO {q(pg_schema)}.{q(pg_table)} ({collist}) "
        f"SELECT {collist} FROM {q(STAGING_SCHEMA)}.{q(pg_table)} "
        f"ON CONFLICT ({q(pk_col)}) DO UPDATE SET {setlist}"
    )
    # Change-filter: only rewrite a matched row when some value actually differs,
    # so re-syncs don't churn every row (less WAL, fewer dead tuples). rowcount
    # then reflects inserts + genuine updates.
    if non_pk:
        pred = " OR ".join(
            f"{q(pg_table)}.{q(c)} IS DISTINCT FROM EXCLUDED.{q(c)}" for c in non_pk
        )
        sql += f" WHERE {pred}"
    return sql


def upsert(mapping: dict, *, dsn: str, lake: LakeConfig, include: Optional[list] = None,
           pg_schema: str = "public") -> dict:
    """Insert or update every mapped Postgres table from the mirror. Never deletes.

    Returns the report the CLI prints with --json. `rows_written` counts rows
    inserted or actually changed; `mismatches` lists tables where Postgres has
    fewer rows than the mirror after the run (empty means all good).
    """
    import psycopg2

    with mirror_and_postgres(lake, dsn, read_only=False, staging=True) as con:
        return _upsert(con, mapping, dsn, include, pg_schema, psycopg2)


def _upsert(con, mapping, dsn, include, pg_schema, psycopg2) -> dict:
    mirror_cols = _mirror_columns(con)

    # --- Phase A: DuckDB streams mirror -> Postgres staging (order-independent) ---
    staged = []  # (pg_table, spec, pairs)
    skipped = []
    t0 = time.time()
    for pg_table, spec in selected_tables(mapping, include):
        if spec["access_table"] not in mirror_cols:
            skipped.append((pg_table, f"no mirror table {spec['access_table']!r}"))
            continue
        pairs = stage_table(con, pg_schema, pg_table, spec, mirror_cols)
        staged.append((pg_table, spec, pairs))
    stage_secs = time.time() - t0

    # --- Phase B: one transaction, all upserts ---
    # FK enforcement stays ON, deferred to COMMIT so tables can be upserted in
    # any order. This tool does NOT touch matview-refresh triggers: partisipa
    # brackets the run with `dbsamizdat nuke` (drop managed matviews + their
    # refresh triggers) beforehand and recreates/refreshes them after, so no
    # trigger fires during the bulk load. See docs/postgres-sync-handoff.md.
    t1 = time.time()
    pgconn = psycopg2.connect(dsn)
    rows_written = 0
    try:
        with pgconn:
            with pgconn.cursor() as cur:
                cur.execute("SET CONSTRAINTS ALL DEFERRED")
                for pg_table, spec, pairs in staged:
                    cols = [c for c, _ in pairs]
                    cur.execute(upsert_sql(pg_schema, pg_table, spec["pk_pg_col"], cols))
                    rows_written += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    finally:
        pgconn.close()
    upsert_secs = time.time() - t1

    # --- verify counts ---
    results = []
    for pg_table, spec, pairs in staged:
        src = con.execute(f'SELECT count(*) FROM lake.{q(spec["access_table"])}').fetchone()[0]
        tgt = con.execute(f"SELECT count(*) FROM pg.{q(pg_schema)}.{q(pg_table)}").fetchone()[0]
        results.append(TableResult(pg_table, src, tgt, len(pairs), ok=(src <= tgt)))

    return {
        "command": "to_postgres",
        "schema": pg_schema,
        "stage_secs": round(stage_secs, 1),
        "upsert_secs": round(upsert_secs, 1),
        "rows_written": rows_written,
        "load_mode": "FK-enforced (deferred to commit)",
        "tables_synced": len(results),
        "tables": [
            {"table": r.table, "mirror_rows": r.source_rows, "pg_rows": r.target_rows,
             "cols": r.mapped_cols, "ok": r.ok} for r in results
        ],
        "skipped": [{"table": t, "reason": why} for t, why in skipped],
        "mismatches": [r.table for r in results if not r.ok],
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="mdb-ducklake-to-postgres", description=__doc__)
    ap.add_argument("--mapping", required=True, help="ducklake_pg_mapping.json")
    ap.add_argument("--pg-schema", default="public", help="target Postgres schema")
    add_lake_args(ap)
    ap.add_argument("--include", nargs="*", help="only these pg tables")
    ap.add_argument("--json", action="store_true", help="emit a structured JSON run report")
    args = ap.parse_args(argv)

    with open(args.mapping) as f:
        mapping = json.load(f)
    report = upsert(mapping, dsn=dsn_from_env(), lake=lake_from_args(args),
                    include=args.include, pg_schema=args.pg_schema)

    if args.json:
        print(json.dumps(report))
    else:
        print(
            f"Upserted {report['tables_synced']} tables into schema '{report['schema']}' "
            f"(stage {report['stage_secs']:.1f}s, upsert {report['upsert_secs']:.1f}s; "
            f"load mode: {report['load_mode']}). rows_written={report['rows_written']} "
            f"skipped={len(report['skipped'])} mismatches={len(report['mismatches'])}"
        )
        for s in report["skipped"]:
            print(f"  SKIP  {s['table']}: {s['reason']}")
        for t in report["tables"]:
            if not t["ok"]:
                print(f"  CHECK {t['table']}: mirror={t['mirror_rows']} pg>={t['pg_rows']} cols={t['cols']}")
    return 1 if report["mismatches"] else 0


if __name__ == "__main__":
    sys.exit(main())
