# mdb-ducklake

Mirror an MS Access database into an **updatable [DuckLake](https://ducklake.select/)
lakehouse on S3**, and sync that mirror **set-based into Postgres**.

One embedded DuckDB engine does the whole job — reading Parquet from S3,
maintaining the DuckLake catalog, and attaching Postgres to stream data
table-to-table. No Spark, no Airflow, no warehouse; a CLI binary plus a few
hundred lines of Python. (For the story, see
[docs/duckdb-as-glue.md](docs/duckdb-as-glue.md).)

Extraction shells out to the [mdbtools](https://github.com/mdbtools/mdbtools)
CLI; everything after that is DuckDB. Dependencies: `duckdb`, `pytz`, `boto3`,
plus `psycopg2` for the Postgres sync (`pip install mdb-ducklake[postgres]`, or
bring your own) — no pydantic.

## Two halves

**1. Mirror** — `.accdb` → typed, incremental DuckLake on S3.
Types come from the Access schema and are applied with `TRY_CAST` (lossless: bad
cells become NULL and are kept verbatim in a `_mdb_cast_rejects` sidecar).
Incremental by sha256 fingerprint; each sync is a snapshot you can time-travel.

**2. Postgres sync** ("Option B" for partisipa) — three commands driven by a
mapping artifact:

| Command | Role | Safety |
|---|---|---|
| `mdb-ducklake-validate` | read-only reconcile (row counts, PK-set diffs, `--deep` changed rows) | writes nothing |
| `mdb-ducklake-to-postgres` | set-based upsert (stage + `INSERT … ON CONFLICT`, change-filtered) | never deletes |
| `mdb-ducklake-prune` | guarded delete of rows gone from the mirror | dry-run default |

All three take `--json` for a structured run report; exit codes signal drift.

The same three are importable, taking the connection details as arguments rather
than environment variables; each returns the dict its CLI prints with `--json`:

```python
from mdb_ducklake import LakeConfig, prune, upsert, validate

lake = LakeConfig(
    catalog="s3://bucket/pnds/catalog.ducklake", data_path="s3://bucket/pnds/data",
    s3_endpoint="sgp1.digitaloceanspaces.com", s3_region="sgp1",
    s3_key_id=..., s3_secret=...,
)
dsn = "host=localhost dbname=app user=app password=..."
report = validate(mapping, dsn=dsn, lake=lake)          # report["differences"] == [] when clean
upsert(mapping, dsn=dsn, lake=lake)                     # ["rows_written"], ["mismatches"]
prune(mapping, dsn=dsn, lake=lake, apply=False)         # dry run; ["plans"] lists tables with rows to delete
```

## Quick start

```bash
uv sync                            # duckdb + boto3 + pytz
uv run python examples/smoke.py    # proves the merge/snapshot loop locally, no .accdb needed

# Sync a real Access DB into a local mirror (needs mdbtools: brew/apt install mdbtools):
uv run mdb-ducklake --catalog local.ducklake --data-path data sync path/to/db.accdb
```

Mirror flags: `--full-refresh` (reload every table), `--merge --pks '{"Suco":"Suco_ID"}'`
(row-level MERGE instead of full replace).

### S3 + Postgres (with SOPS-encrypted creds)

Credentials live encrypted in `s3.enc.env` (SOPS + the org's SSH-key recipients)
and are injected only into the child process via `sops exec-env` — never in code,
logs, or shell history. Typical reconcile loop:

```bash
sops exec-env s3.enc.env "PGPASSWORD=… uv run mdb-ducklake-validate --mapping m.json"        # see drift
# partisipa: manage.py dbsamizdat nuke     # drop managed matviews + refresh triggers
sops exec-env s3.enc.env "PGPASSWORD=… uv run mdb-ducklake-to-postgres --mapping m.json"      # upsert
sops exec-env s3.enc.env "PGPASSWORD=… uv run mdb-ducklake-prune --mapping m.json --apply"    # delete gone rows
# partisipa: manage.py dbsamizdat sync     # recreate + refresh matviews in dependency order
sops exec-env s3.enc.env "PGPASSWORD=… uv run mdb-ducklake-validate --mapping m.json --deep"  # confirm clean
```

Env: `PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD`, target `--pg-schema`, S3 via
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (+ `--s3-endpoint`/`--s3-region`).

## Status

Working, validated against the PNDS database (86 tables, ~180 MB):

- full copy mirrored to DigitalOcean Spaces, catalog + data in the bucket,
  cold-readable from S3 alone; incremental; snapshots + time-travel;
- Postgres sync of all 85 real tables in ~30s; read-only `validate` in ~20s;
  guarded `prune`;
- parity checked against the old row-by-row ORM importer on a representative
  subset — identical row counts, differences limited to the intentional
  `1900-01-00`→NULL cells (a parity check caught + fixed a currency-rounding bug
  along the way).

Before production: full-table-set parity confirmation; regenerate the mapping
from partisipa `main`; wire the `dbsamizdat nuke → sync` bracket into the job.
See the handoff doc.

## Documentation

- [docs/duckdb-as-glue.md](docs/duckdb-as-glue.md) — the narrative: why this is a
  neat DuckDB pattern, and the honest sharp edges.
- [docs/architecture.md](docs/architecture.md) — how the two halves work
  (storage model, typing, incremental, the Postgres-sync mechanics, module map).
- [docs/postgres-sync-handoff.md](docs/postgres-sync-handoff.md) — ops run
  sequence + the pre-production checklist.
