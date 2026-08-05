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
import os
import sys
import time
from dataclasses import dataclass

from mdb_ducklake.sync import open_lake
from mdb_ducklake.to_postgres import (
    STAGING_SCHEMA,
    _dsn,
    _mirror_columns,
    mapped_projection,
    q,
    syncable,
)


def load_order(specs: dict) -> list[str]:
    """Parents before children (a table after all its fk_targets). DFS topo sort."""
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


@dataclass
class Prune:
    table: str
    mirror_rows: int
    pg_rows: int
    to_delete: int
    action: str  # "delete" | "skip:empty-mirror" | "skip:fraction" | "nothing"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="mdb-ducklake-prune", description=__doc__)
    ap.add_argument("--mapping", required=True)
    ap.add_argument("--pg-schema", default="public")
    ap.add_argument("--apply", action="store_true", help="actually delete (default: dry-run)")
    ap.add_argument("--max-delete-fraction", type=float, default=0.10,
                    help="skip a table if deletes exceed this fraction of its PG rows")
    ap.add_argument("--force", action="store_true", help="ignore the fraction guard (empty-mirror guard still applies)")
    ap.add_argument("--catalog", default=os.environ.get("DUCKLAKE_CATALOG", "s3://ducklake/pnds/catalog.ducklake"))
    ap.add_argument("--data-path", default=os.environ.get("DUCKLAKE_DATA", "s3://ducklake/pnds/data"))
    ap.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT", "sgp1.digitaloceanspaces.com"))
    ap.add_argument("--s3-region", default=os.environ.get("S3_REGION", "sgp1"))
    ap.add_argument("--include", nargs="*")
    args = ap.parse_args(argv)

    import psycopg2

    mapping = json.load(open(args.mapping))
    lake = open_lake(args.catalog, args.data_path, s3_endpoint=args.s3_endpoint, s3_region=args.s3_region)
    con = lake.con
    con.execute("SET enable_progress_bar=false")
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{_dsn()}' AS pg (TYPE postgres)")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS pg.{q(STAGING_SCHEMA)}")
    mirror_cols = _mirror_columns(con)

    specs = {t: s for t, s in syncable(mapping)}
    tables = list(specs)
    if args.include:
        want = set(args.include)
        tables = [t for t in tables if t in want]

    # Assess every table (stage mirror PKs, count deletions), applying guards.
    plans, to_apply = [], {}   # to_apply: pg_table -> pk_pg_col
    for pg_table in tables:
        spec = specs[pg_table]
        if spec["access_table"] not in mirror_cols:
            continue
        src_types = mirror_cols[spec["access_table"]]
        pk_pg = spec["pk_pg_col"]
        pk_cast = next(cast for pgc, cast, is_pk in mapped_projection(spec, src_types) if is_pk)
        lake_t = f"lake.{q(spec['access_table'])}"
        pg_t = f"pg.{q(args.pg_schema)}.{q(pg_table)}"

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
        elif not args.force and pg_rows and (to_delete / pg_rows) > args.max_delete_fraction:
            action = "skip:fraction"
        else:
            action = "delete"
            to_apply[pg_table] = pk_pg
        plans.append(Prune(pg_table, mirror_rows, pg_rows, to_delete, action))

    # Apply deletes in reverse-FK order (children first), atomically.
    deleted_total = 0
    if args.apply and to_apply:
        order = [t for t in reversed(load_order(specs)) if t in to_apply]
        pgconn = psycopg2.connect(_dsn())
        try:
            with pgconn:
                with pgconn.cursor() as cur:
                    cur.execute("SET CONSTRAINTS ALL DEFERRED")
                    for pg_table in order:
                        pk_pg = to_apply[pg_table]
                        cur.execute(
                            f"DELETE FROM {q(args.pg_schema)}.{q(pg_table)} "
                            f"WHERE {q(pk_pg)} NOT IN (SELECT {q(pk_pg)} FROM {STAGING_SCHEMA}.{q(pg_table + '_pk')})"
                        )
                        deleted_total += cur.rowcount
        finally:
            pgconn.close()

    con.execute(f"DROP SCHEMA IF EXISTS pg.{q(STAGING_SCHEMA)} CASCADE")

    mode = "APPLIED" if args.apply else "DRY-RUN (use --apply to delete)"
    would = [p for p in plans if p.action == "delete"]
    print(
        f"Prune [{mode}] schema '{args.pg_schema}': "
        f"{len(would)} table(s) with deletions"
        + (f", {deleted_total} rows deleted" if args.apply else "")
    )
    for p in plans:
        if p.action != "nothing":
            print(f"  {p.action:18} {p.table}: mirror={p.mirror_rows} pg={p.pg_rows} would_delete={p.to_delete}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
