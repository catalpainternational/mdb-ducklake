# mdb-ducklake

Sync an MS Access database into an **updatable DuckLake mirror**.

Each run reads the current state of an `.accdb` and MERGEs it into a
[DuckLake](https://ducklake.select/) lakehouse — inserting new rows, updating
changed rows, and deleting rows that have vanished from the source. The result
is a queryable, always-current clone of the Access DB, with per-sync snapshots
you can time-travel through.

Extraction shells out to the [mdbtools](https://github.com/mdbtools/mdbtools)
CLI (see `access.py`); this repo adds only the DuckLake side. No Python
dependency on mdb-codegen (and thus none on pydantic v1).

## Storage model

Both the catalog and the data live in the **same S3 bucket** — git stays
code-only (the catalog is a multi-MB binary that grows every sync).

- **Catalog** (metadata) — a DuckDB file, e.g. `s3://ducklake/pnds/catalog.ducklake`.
  It's the source of truth for which snapshots/tables exist. A DuckLake catalog
  can't be attached read-write over S3, so the CLI **downloads it before a run
  and uploads it after** (`boto3`, single-writer). Cold readers pull it too.
- **Data** (parquet) — under `DATA_PATH`, e.g. `s3://ducklake/pnds/data`.

For local prototyping, pass a local path for either (a `.ducklake` file and a
`data/` dir); nothing then touches S3.

## Quick start

```bash
uv sync                           # duckdb + boto3
uv run python examples/smoke.py   # proves the merge/snapshot loop locally, no .accdb needed
```

### Sync a real Access DB (local)

```bash
# mdbtools CLI must be installed: brew install mdbtools  (or apt install mdbtools)
uv run mdb-ducklake --catalog local.ducklake --data-path data \
  sync path/to/db.accdb
```

Syncs are **incremental by default**: each table is exported and fingerprinted
(sha256), and a table whose fingerprint matches the last sync is skipped — no
load, no parquet write, no S3 upload (fingerprints are kept in a hidden
`_mdb_sync_state` table in the lake). Changed/new tables are fully replaced (no
primary keys needed). Flags:

- `--full-refresh` — reload every table even if unchanged.
- `--merge --pks '{"Suco":"Suco_ID"}'` — row-level MERGE (only changed rows
  rewritten) for the listed tables, instead of full replace.

### S3 DATA_PATH (DigitalOcean Spaces)

Credentials live encrypted in `s3.enc.env` (SOPS + the org's SSH-key
recipients) and are injected only into the child process at runtime — they
never touch code, logs, or the shell history:

```bash
# Full PNDS copy — catalog and data both in the bucket:
sops exec-env s3.enc.env 'uv run mdb-ducklake \
  --catalog s3://ducklake/pnds/catalog.ducklake \
  --data-path s3://ducklake/pnds/data \
  --s3-endpoint sgp1.digitaloceanspaces.com --s3-region sgp1 \
  sync ~/Downloads/PNDS_Interim_MIS-Data.accdb'

# Prove the S3 round-trip end-to-end (no .accdb needed):
sops exec-env s3.enc.env 'uv run python examples/s3_smoke.py'
```

`open_lake` reads the key/secret from `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` (also accepts `MDB_DUCKLAKE_S3_*` or the pgbackrest
names) and registers a DuckDB S3 secret. To edit the encrypted creds:
`sops s3.enc.env`.

## Status

Working: full PNDS copy (86 tables) mirrored to DigitalOcean Spaces with the
catalog also in the bucket; local and S3 `DATA_PATH`; full-replace and MERGE
loads; snapshots + time-travel; SOPS-encrypted creds via `sops exec-env`;
cold readers reconstruct the mirror from S3 alone.

## Completeness

`mdb-count` is an unreliable oracle on Access-2007+ (`ACE12`) files, so the
per-table row-count check prints a **REVIEW** list, not a pass/fail — most
deltas are mdb-count noise (verified against a second independent reader), not
data loss. Two known mdbtools gaps on the PNDS file, accepted as out-of-scope:
`syncImportLog` (mdbtools reads 0 rows; not needed) and
`zFreebalance_Budget_extract` (genuinely empty). See the memory note
`mdbtools-ace-fidelity` for the full audit.

Not yet: automatic primary-key detection (supplied via `--pks` for `--merge`),
explicit Access→DuckDB type mapping (currently inferred from CSV/JSON).
