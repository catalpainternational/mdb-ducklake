# Spike: set-based DuckLake → partisipa Postgres upsert

Throwaway proof-of-concept for **Option B** — pushing the DuckLake S3 mirror
into partisipa's Postgres with a set-based `INSERT … ON CONFLICT`, bypassing the
row-by-row Django ORM importer. `pg_upsert_spike.py` syncs two representative
tables (`zSuco`, `Suco_Operational_Report`).

## How it works

One DuckDB connection does everything:
1. `open_lake()` attaches the DuckLake mirror (S3) + configures the S3 secret.
2. `INSTALL/LOAD postgres; ATTACH … (TYPE postgres)` attaches partisipa's PG.
3. Per table: DuckDB streams S3 Parquet → a PG **staging** table, mapping
   DuckLake (verbatim Access) column names → Postgres column names by
   `lower(name)`, and **`TRY_CAST`ing each column to its Postgres target type**
   (naive timestamps localized to UTC). Bad values → NULL, never a dropped row.
4. Native Postgres upsert via `CALL postgres_execute('pg', 'INSERT … SELECT …
   FROM staging ON CONFLICT (pk) DO UPDATE SET …')`. Drops staging after.

A psycopg2 fallback for the merge exists but was **not needed** —
`postgres_execute` drove `ON CONFLICT` fine.

## Prerequisites

- A partisipa Postgres reachable on the host with the `pnds_data` schema. The
  spike reuses a **throwaway `spike` schema** cloned from an existing running
  partisipa pod, so it never touches real `public` data:
  ```sql
  CREATE SCHEMA spike;
  CREATE TABLE spike.zsuco (LIKE public.zsuco INCLUDING ALL);
  CREATE TABLE spike.suco_operational_report (LIKE public.suco_operational_report INCLUDING ALL);
  ```
  (`LIKE … INCLUDING ALL` copies columns + PK, not the FK constraints.)
- `uv sync` (adds `psycopg2-binary` via the dev group).

## Run

```bash
PW=$(cat ../partisipa-import/.devcontainer/secrets/postgres-password)
sops exec-env s3.enc.env "PGPASSWORD='$PW' PGPORT=5458 PG_SCHEMA=spike \
    uv run python spikes/pg_upsert_spike.py"
```
Env knobs: `PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD`, `PG_SCHEMA`,
`DUCKLAKE_CATALOG`, `DUCKLAKE_DATA`, `S3_ENDPOINT`, `S3_REGION`.

## Result (validated 2026-08-05)

- One-engine path works: `zsuco` 472→472 (0.5s), `suco_operational_report`
  47,369→47,369 (~2.4s), via `postgres_execute` (no fallback). Counts match the
  mirror exactly (spike schema starts empty ⇒ PK-set parity).
- Types land correctly (`numeric` / `timestamptz` / `boolean`); UTC localization
  correct (midnight-UTC → `09:00+09` in the +09 server tz).
- Idempotent: a second run is a no-op on counts. Update path proven: mutating a
  row to a sentinel and re-running restores it via `ON CONFLICT DO UPDATE`.
- **Key learning:** cast every column to its **Postgres target type** — mirror
  types don't line up 1:1 with the Django schema (e.g. mirror `BIGINT` vs PG
  `integer`), and Postgres won't implicitly coerce across type families.

## Not covered (deliberately — spike scope)

Automated column-mapping generation from partisipa's models; all 86 tables; FK
load-order (spike stripped FKs via `LIKE`); delete-of-rows-missing-from-source;
performance vs the ORM at full scale; packaging/scheduling. See
`~/.claude/plans/refactored-dancing-crane.md`.
