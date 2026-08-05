"""Read-only reconciliation: DuckLake mirror vs partisipa Postgres.

Attaches both the mirror (S3) and Postgres in one DuckDB engine and, per table,
compares WITHOUT writing anything:
  - row counts (mirror vs Postgres);
  - PK-set differences (rows only in the mirror / only in Postgres);
  - with --deep, the number of matched-PK rows whose mapped column values differ.

Uses the exact same column projection as the sync (see to_postgres.mapped_
projection), so a "clean" validate means a sync would be a no-op. Note that
`--deep` differences legitimately include the known semantic gaps (mirror
`1900-01-00` -> NULL vs the old importer's `1900-01-01`, and TRY_CAST rejects).
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
    _dsn,
    _mirror_columns,
    mapped_projection,
    q,
    syncable,
)


@dataclass
class Diff:
    table: str
    mirror_rows: int
    pg_rows: int
    only_in_mirror: int   # PKs present in mirror, missing from Postgres
    only_in_pg: int       # PKs present in Postgres, gone from the mirror (prune candidates)
    changed: int | None   # matched-PK rows with differing values (None unless --deep)

    @property
    def clean(self) -> bool:
        return (
            self.only_in_mirror == 0
            and self.only_in_pg == 0
            and (self.changed in (0, None))
        )


def _one_val(con, sql):
    return con.execute(sql).fetchone()[0]


def validate_table(con, pg_schema, pg_table, spec, src_types, deep):
    access = spec["access_table"]
    proj = mapped_projection(spec, src_types)
    pk_pg = spec["pk_pg_col"]
    pk_cast = next(cast for pgc, cast, is_pk in proj if is_pk)
    lake_t = f"lake.{q(access)}"
    pg_t = f"pg.{q(pg_schema)}.{q(pg_table)}"

    mirror_rows = _one_val(con, f"SELECT count(*) FROM {lake_t}")
    pg_rows = _one_val(con, f"SELECT count(*) FROM {pg_t}")

    # PK-set differences (cast mirror PK to the PG type so EXCEPT aligns).
    m_pk = f"SELECT {pk_cast} AS pk FROM {lake_t}"
    p_pk = f"SELECT {q(pk_pg)} AS pk FROM {pg_t}"
    only_in_mirror = _one_val(con, f"SELECT count(*) FROM ({m_pk} EXCEPT {p_pk})")
    only_in_pg = _one_val(con, f"SELECT count(*) FROM ({p_pk} EXCEPT {m_pk})")

    changed = None
    if deep:
        m_cols = ", ".join(f"{cast} AS {q(pgc)}" for pgc, cast, _ in proj)
        p_cols = ", ".join(q(pgc) for pgc, _, _ in proj)
        non_pk = [pgc for pgc, _, is_pk in proj if not is_pk]
        distinct = " OR ".join(f"m.{q(c)} IS DISTINCT FROM p.{q(c)}" for c in non_pk) or "false"
        changed = _one_val(
            con,
            f"SELECT count(*) FROM (SELECT {m_cols} FROM {lake_t}) m "
            f"JOIN (SELECT {p_cols} FROM {pg_t}) p ON m.{q(pk_pg)}=p.{q(pk_pg)} "
            f"WHERE {distinct}",
        )
    return Diff(pg_table, mirror_rows, pg_rows, only_in_mirror, only_in_pg, changed)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="mdb-ducklake-validate", description=__doc__)
    ap.add_argument("--mapping", required=True)
    ap.add_argument("--pg-schema", default="public")
    ap.add_argument("--deep", action="store_true", help="also count changed rows (slower; includes expected semantic diffs)")
    ap.add_argument("--catalog", default=os.environ.get("DUCKLAKE_CATALOG", "s3://ducklake/pnds/catalog.ducklake"))
    ap.add_argument("--data-path", default=os.environ.get("DUCKLAKE_DATA", "s3://ducklake/pnds/data"))
    ap.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT", "sgp1.digitaloceanspaces.com"))
    ap.add_argument("--s3-region", default=os.environ.get("S3_REGION", "sgp1"))
    ap.add_argument("--include", nargs="*")
    args = ap.parse_args(argv)

    mapping = json.load(open(args.mapping))
    lake = open_lake(args.catalog, args.data_path, s3_endpoint=args.s3_endpoint, s3_region=args.s3_region)
    con = lake.con
    con.execute("SET enable_progress_bar=false")
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{_dsn()}' AS pg (TYPE postgres, READ_ONLY)")
    mirror_cols = _mirror_columns(con)

    tables = [(t, s) for t, s in syncable(mapping)]
    if args.include:
        want = set(args.include)
        tables = [(t, s) for t, s in tables if t in want]

    t0 = time.time()
    diffs, skipped = [], []
    for pg_table, spec in tables:
        if spec["access_table"] not in mirror_cols:
            skipped.append(pg_table)
            continue
        diffs.append(validate_table(con, args.pg_schema, pg_table, spec, mirror_cols[spec["access_table"]], args.deep))
    secs = time.time() - t0

    dirty = [d for d in diffs if not d.clean]
    print(
        f"Validated {len(diffs)} tables vs schema '{args.pg_schema}' in {secs:.1f}s: "
        f"{len(diffs) - len(dirty)} clean, {len(dirty)} with differences, {len(skipped)} skipped."
    )
    for d in dirty:
        chg = "" if d.changed is None else f" changed={d.changed}"
        print(
            f"  DIFF {d.table}: mirror={d.mirror_rows} pg={d.pg_rows} "
            f"only_in_mirror={d.only_in_mirror} only_in_pg={d.only_in_pg}{chg}"
        )
    for t in skipped:
        print(f"  SKIP {t}: not in mirror")
    return 1 if dirty else 0


if __name__ == "__main__":
    sys.exit(main())
