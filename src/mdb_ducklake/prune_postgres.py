"""Guarded delete: remove Postgres rows no longer present in the DuckLake mirror.

DELIBERATELY SEPARATE from the sync (never a side effect). Dry-run by default;
pass --apply to delete. Guards against wiping real data when the mirror under-
reads (e.g. mdbtools returns 0 rows for a table it can't parse):

  - empty-mirror guard: never prune a table whose mirror row count is 0;
  - fraction guard: skip a table whose deletion would exceed --max-delete-fraction
    of its current Postgres rows (override per-run with --force);
  - deletes in reverse-FK order (children before parents) inside ONE transaction
    with constraints deferred, so it's all-or-nothing.

Reuses the mirror<->Postgres projection from to_postgres so the PK comparison
matches the sync exactly.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Optional

from mdb_ducklake.to_postgres import (
    STAGING_SCHEMA,
    LakeConfig,
    _mirror_columns,
    add_lake_args,
    dsn_from_env,
    lake_from_args,
    load_order,
    mapped_projection,
    mirror_and_postgres,
    q,
    selected_tables,
    syncable,
)


@dataclass
class Prune:
    table: str
    mirror_rows: int
    pg_rows: int
    to_delete: int
    action: str  # "delete" | "skip:empty-mirror" | "skip:fraction" | "nothing"


def prune(mapping: dict, *, dsn: str, lake: LakeConfig, apply: bool = False,
          max_delete_fraction: float = 0.10, force: bool = False,
          include: Optional[list] = None, pg_schema: str = "public") -> dict:
    """Delete Postgres rows that are gone from the mirror. Dry run unless apply=True.

    Never prunes a table whose mirror is empty; skips a table whose deletions
    would exceed max_delete_fraction of its Postgres rows unless force=True.
    Returns the report the CLI prints with --json: `plans` lists every table
    with rows to delete and the action taken (including skipped ones); `rows_deleted` is the number
    deleted (None on a dry run).
    """
    import psycopg2

    with mirror_and_postgres(lake, dsn, read_only=False, staging=True) as con:
        return _prune(con, mapping, dsn, apply, max_delete_fraction, force, include, pg_schema, psycopg2)


def _prune(con, mapping, dsn, apply, max_delete_fraction, force, include, pg_schema, psycopg2) -> dict:
    mirror_cols = _mirror_columns(con)
    specs = {t: s for t, s in syncable(mapping)}

    # Assess every table (stage mirror PKs, count deletions), applying guards.
    plans, to_apply = [], {}   # to_apply: pg_table -> pk_pg_col
    for pg_table, spec in selected_tables(mapping, include):
        if spec["access_table"] not in mirror_cols:
            continue
        src_types = mirror_cols[spec["access_table"]]
        pk_pg = spec["pk_pg_col"]
        pk_cast = next(cast for pgc, cast, is_pk in mapped_projection(spec, src_types) if is_pk)
        lake_t = f"lake.{q(spec['access_table'])}"
        pg_t = f"pg.{q(pg_schema)}.{q(pg_table)}"

        mirror_rows = con.execute(f"SELECT count(*) FROM {lake_t}").fetchone()[0]
        pg_rows = con.execute(f"SELECT count(*) FROM {pg_t}").fetchone()[0]

        # Stage the mirror's PKs into Postgres for a set-based NOT IN delete.
        stg = f"pg.{q(STAGING_SCHEMA)}.{q(pg_table + '_pk')}"
        con.execute(f"DROP TABLE IF EXISTS {stg}")
        con.execute(f"CREATE TABLE {stg} AS SELECT {pk_cast} AS {q(pk_pg)} FROM {lake_t}")
        to_delete = con.execute(
            f"SELECT count(*) FROM {pg_t} WHERE {q(pk_pg)} NOT IN (SELECT {q(pk_pg)} FROM {stg})"
        ).fetchone()[0]

        if to_delete == 0:
            action = "nothing"
        elif mirror_rows == 0:
            action = "skip:empty-mirror"
        elif not force and pg_rows and (to_delete / pg_rows) > max_delete_fraction:
            action = "skip:fraction"
        else:
            action = "delete"
            to_apply[pg_table] = pk_pg
        plans.append(Prune(pg_table, mirror_rows, pg_rows, to_delete, action))

    # Apply deletes in reverse-FK order (children first), atomically.
    deleted_total = 0
    if apply and to_apply:
        # Reverse-FK order (children before parents) is defensive; FK checks are
        # deferred to commit anyway. Matview-refresh triggers are handled by
        # partisipa's dbsamizdat nuke/recreate around the run, not here.
        order = [t for t in reversed(load_order(specs)) if t in to_apply]
        pgconn = psycopg2.connect(dsn)
        try:
            with pgconn:
                with pgconn.cursor() as cur:
                    cur.execute("SET CONSTRAINTS ALL DEFERRED")
                    for pg_table in order:
                        pk_pg = to_apply[pg_table]
                        cur.execute(
                            f"DELETE FROM {q(pg_schema)}.{q(pg_table)} "
                            f"WHERE {q(pk_pg)} NOT IN (SELECT {q(pk_pg)} FROM {STAGING_SCHEMA}.{q(pg_table + '_pk')})"
                        )
                        deleted_total += cur.rowcount
        finally:
            pgconn.close()

    return {
        "command": "prune",
        "schema": pg_schema,
        "applied": apply,
        "rows_deleted": deleted_total if apply else None,
        "tables_with_deletions": len([p for p in plans if p.action == "delete"]),
        "plans": [
            {"table": p.table, "action": p.action, "mirror_rows": p.mirror_rows,
             "pg_rows": p.pg_rows, "would_delete": p.to_delete}
            for p in plans if p.action != "nothing"
        ],
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="mdb-ducklake-prune", description=__doc__)
    ap.add_argument("--mapping", required=True)
    ap.add_argument("--pg-schema", default="public")
    ap.add_argument("--apply", action="store_true", help="actually delete (default: dry-run)")
    ap.add_argument("--max-delete-fraction", type=float, default=0.10,
                    help="skip a table if deletes exceed this fraction of its PG rows")
    ap.add_argument("--force", action="store_true", help="ignore the fraction guard (empty-mirror guard still applies)")
    add_lake_args(ap)
    ap.add_argument("--include", nargs="*")
    ap.add_argument("--json", action="store_true", help="emit a structured JSON prune report")
    args = ap.parse_args(argv)

    with open(args.mapping) as f:
        mapping = json.load(f)
    report = prune(mapping, dsn=dsn_from_env(), lake=lake_from_args(args), apply=args.apply,
                   max_delete_fraction=args.max_delete_fraction, force=args.force,
                   include=args.include, pg_schema=args.pg_schema)

    if args.json:
        print(json.dumps(report))
    else:
        mode = "APPLIED" if args.apply else "DRY-RUN (use --apply to delete)"
        print(
            f"Prune [{mode}] schema '{report['schema']}': "
            f"{report['tables_with_deletions']} table(s) with deletions"
            + (f", {report['rows_deleted']} rows deleted" if args.apply else "")
        )
        for p in report["plans"]:
            print(f"  {p['action']:18} {p['table']}: mirror={p['mirror_rows']} pg={p['pg_rows']} would_delete={p['would_delete']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
