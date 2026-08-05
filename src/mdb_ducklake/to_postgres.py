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
from dataclasses import dataclass

from mdb_ducklake.sync import open_lake

STAGING_SCHEMA = "mdb_stg"


def _dsn() -> str:
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
        duck_type = pg_type_to_duckdb(c["pg_type"])
        base = q(acc)
        if src_types[acc].upper() == "TIMESTAMP" and duck_type == "TIMESTAMPTZ":
            base = f"({base} AT TIME ZONE 'UTC')"
        out.append((pgc, f"TRY_CAST({base} AS {duck_type})", c["is_pk"]))
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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="mdb-ducklake-to-postgres", description=__doc__)
    ap.add_argument("--mapping", required=True, help="ducklake_pg_mapping.json")
    ap.add_argument("--pg-schema", default="public", help="target Postgres schema")
    ap.add_argument("--catalog", default=os.environ.get("DUCKLAKE_CATALOG", "s3://ducklake/pnds/catalog.ducklake"))
    ap.add_argument("--data-path", default=os.environ.get("DUCKLAKE_DATA", "s3://ducklake/pnds/data"))
    ap.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT", "sgp1.digitaloceanspaces.com"))
    ap.add_argument("--s3-region", default=os.environ.get("S3_REGION", "sgp1"))
    ap.add_argument("--include", nargs="*", help="only these pg tables")
    args = ap.parse_args(argv)

    import psycopg2

    mapping = json.load(open(args.mapping))
    lake = open_lake(args.catalog, args.data_path, s3_endpoint=args.s3_endpoint, s3_region=args.s3_region)
    con = lake.con
    con.execute("SET enable_progress_bar=false")  # keep stdout clean for the report
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{_dsn()}' AS pg (TYPE postgres)")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS pg.{q(STAGING_SCHEMA)}")
    mirror_cols = _mirror_columns(con)

    tables = [(t, s) for t, s in syncable(mapping)]
    if args.include:
        want = set(args.include)
        tables = [(t, s) for t, s in tables if t in want]

    # --- Phase A: DuckDB streams mirror -> Postgres staging (order-independent) ---
    staged = []  # (pg_table, spec, pairs)
    skipped = []
    t0 = time.time()
    for pg_table, spec in tables:
        if spec["access_table"] not in mirror_cols:
            skipped.append((pg_table, f"no mirror table {spec['access_table']!r}"))
            continue
        pairs = stage_table(con, args.pg_schema, pg_table, spec, mirror_cols)
        staged.append((pg_table, spec, pairs))
    stage_secs = time.time() - t0

    # --- Phase B: one transaction, all upserts ---
    # FK enforcement stays ON, deferred to COMMIT so tables can be upserted in
    # any order. This tool does NOT touch matview-refresh triggers: partisipa
    # brackets the run with `dbsamizdat nuke` (drop managed matviews + their
    # refresh triggers) beforehand and recreates/refreshes them after, so no
    # trigger fires during the bulk load. See docs/postgres-sync-handoff.md.
    t1 = time.time()
    pgconn = psycopg2.connect(_dsn())
    rows_written = 0
    try:
        with pgconn:
            with pgconn.cursor() as cur:
                cur.execute("SET CONSTRAINTS ALL DEFERRED")
                for pg_table, spec, pairs in staged:
                    cols = [c for c, _ in pairs]
                    cur.execute(upsert_sql(args.pg_schema, pg_table, spec["pk_pg_col"], cols))
                    rows_written += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    finally:
        pgconn.close()
    upsert_secs = time.time() - t1
    load_mode = "FK-enforced (deferred to commit)"

    # --- report + verify counts, then drop staging ---
    results = []
    for pg_table, spec, pairs in staged:
        src = con.execute(f'SELECT count(*) FROM lake.{q(spec["access_table"])}').fetchone()[0]
        tgt = con.execute(f"SELECT count(*) FROM pg.{q(args.pg_schema)}.{q(pg_table)}").fetchone()[0]
        results.append(TableResult(pg_table, src, tgt, len(pairs), ok=(src <= tgt)))
    con.execute(f"DROP SCHEMA IF EXISTS pg.{q(STAGING_SCHEMA)} CASCADE")

    bad = [r for r in results if not r.ok]
    print(
        f"Upserted {len(results)} tables into schema '{args.pg_schema}' "
        f"(stage {stage_secs:.1f}s, upsert {upsert_secs:.1f}s; load mode: {load_mode}). "
        f"rows_written={rows_written} skipped={len(skipped)} mismatches={len(bad)}"
    )
    for pg_table, why in skipped:
        print(f"  SKIP  {pg_table}: {why}")
    for r in bad:
        print(f"  CHECK {r.table}: mirror={r.source_rows} pg>={r.target_rows} cols={r.mapped_cols}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
