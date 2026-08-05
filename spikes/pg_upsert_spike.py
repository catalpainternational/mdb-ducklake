"""SPIKE — set-based DuckLake -> partisipa Postgres upsert (Option B).

Reads two tables from the DuckLake S3 mirror and upserts them into a Postgres
schema via `INSERT ... ON CONFLICT`, in one DuckDB engine (postgres extension),
with a psycopg2 fallback for the merge. Throwaway proof-of-concept — see
spikes/README.md.

Run (from mdb-ducklake, with the partisipa dev PG reachable):
    sops exec-env s3.enc.env 'PGPASSWORD=... PGPORT=5458 PG_SCHEMA=spike \
        uv run python spikes/pg_upsert_spike.py'
"""

from __future__ import annotations

import os
import sys
import time

from mdb_ducklake.sync import open_lake

# --- config -----------------------------------------------------------------
CATALOG = os.environ.get("DUCKLAKE_CATALOG", "s3://ducklake/pnds/catalog.ducklake")
DATA = os.environ.get("DUCKLAKE_DATA", "s3://ducklake/pnds/data")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "sgp1.digitaloceanspaces.com")
S3_REGION = os.environ.get("S3_REGION", "sgp1")

PG_SCHEMA = os.environ.get("PG_SCHEMA", "spike")
PG = dict(
    host=os.environ.get("PGHOST", "localhost"),
    port=os.environ.get("PGPORT", "5458"),
    dbname=os.environ.get("PGDATABASE", "partisipa_db"),
    user=os.environ.get("PGUSER", "partisipa"),
    password=os.environ["PGPASSWORD"],
)
DSN = " ".join(f"{k}={v}" for k, v in PG.items())

# DuckLake (verbatim Access) name -> Postgres table name -> natural PK column.
TABLES = [
    ("zSuco", "zsuco", "suco_id"),
    ("Suco_Operational_Report", "suco_operational_report", "operational_report_id"),
]


def q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def lake_columns(con, lake_table):
    rows = con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_catalog='lake' AND table_name=?",
        [lake_table],
    ).fetchall()
    return rows  # [(name, type), ...]


def pg_columns(con, pg_table):
    # DESCRIBE the attached PG table -> {column_name: duckdb_type_string}.
    rows = con.execute(f"DESCRIBE pg.{q(PG_SCHEMA)}.{q(pg_table)}").fetchall()
    return {r[0]: r[1] for r in rows}


def build_mapping(lake_cols, pg_types):
    """Match DuckLake column -> Postgres column by lower(name).

    Returns (pairs, skipped) where
    pairs = [(lake_col, pg_col, target_type, is_timestamp_source)].
    """
    pairs, skipped = [], []
    for name, dtype in lake_cols:
        target = name.lower()
        if target in pg_types:
            pairs.append((name, target, pg_types[target], "TIMESTAMP" in dtype.upper()))
        else:
            skipped.append(name)
    return pairs, skipped


def merge_via_duckdb(con, sql):
    con.execute("CALL postgres_execute('pg', $mdb$" + sql + "$mdb$)")


def merge_via_psycopg2(sql):
    import psycopg2

    conn = psycopg2.connect(DSN)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()


def main() -> None:
    lake = open_lake(CATALOG, DATA, s3_endpoint=S3_ENDPOINT, s3_region=S3_REGION)
    con = lake.con
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{DSN}' AS pg (TYPE postgres)")
    print(f"Attached DuckLake ({DATA}) and Postgres ({PG['host']}:{PG['port']}/{PG['dbname']})")

    merge_path = None
    for lake_table, pg_table, pk in TABLES:
        t0 = time.time()
        lcols = lake_columns(con, lake_table)
        ptypes = pg_columns(con, pg_table)
        pairs, skipped = build_mapping(lcols, ptypes)
        target = f"pg.{q(PG_SCHEMA)}.{q(pg_table)}"
        staging = f"pg.{q(PG_SCHEMA)}.{q('_stg_' + pg_table)}"

        # 1) DuckDB streams S3 parquet -> a Postgres staging table. Each column is
        #    cast to its Postgres target type (TRY_CAST -> NULL on bad values, so a
        #    row is never lost), and naive DuckLake timestamps are localized to UTC
        #    (matching the old importer). Casting to the target type is also what
        #    fixes varchar-in-mirror vs integer-in-Postgres mismatches.
        select_exprs = []
        for lname, pname, ptype, is_ts in pairs:
            base = f'({q(lname)} AT TIME ZONE \'UTC\')' if is_ts else q(lname)
            select_exprs.append(f"TRY_CAST({base} AS {ptype}) AS {q(pname)}")
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        con.execute(
            f"CREATE TABLE {staging} AS SELECT {', '.join(select_exprs)} "
            f'FROM lake.{q(lake_table)}'
        )

        # 2) native Postgres upsert: INSERT ... ON CONFLICT (pk) DO UPDATE
        cols = [p for _, p, _, _ in pairs]
        non_pk = [c for c in cols if c != pk]
        collist = ", ".join(q(c) for c in cols)
        setlist = ", ".join(f"{q(c)}=EXCLUDED.{q(c)}" for c in non_pk)
        stg_plain = f"{PG_SCHEMA}._stg_{pg_table}"
        tgt_plain = f"{PG_SCHEMA}.{pg_table}"
        upsert = (
            f"INSERT INTO {tgt_plain} ({collist}) SELECT {collist} FROM {stg_plain} "
            f"ON CONFLICT ({q(pk)}) DO UPDATE SET {setlist}"
        )
        try:
            merge_via_duckdb(con, upsert)
            merge_path = "duckdb postgres_execute"
        except Exception as exc:  # noqa: BLE001 - spike: record and fall back
            print(f"  (postgres_execute unavailable: {type(exc).__name__}: {str(exc)[:80]}; using psycopg2)")
            merge_via_psycopg2(upsert)
            merge_path = "psycopg2"
        con.execute(f"DROP TABLE IF EXISTS {staging}")

        src_n = con.execute(f'SELECT count(*) FROM lake.{q(lake_table)}').fetchone()[0]
        tgt_n = con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]
        dt = time.time() - t0
        flag = "OK" if src_n == tgt_n else "MISMATCH"
        print(
            f"[{flag}] {pg_table}: mapped {len(pairs)} cols"
            + (f" (skipped {skipped})" if skipped else "")
            + f"; source={src_n} -> pg={tgt_n}; {dt:.1f}s via {merge_path}"
        )

    print(f"\nMerge mechanism used: {merge_path}")


if __name__ == "__main__":
    sys.exit(main())
