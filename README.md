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

## Typing (lossless)

Columns are typed from the Access schema (`mdb-schema`), not guessed from the
CSV: `Long Integer`→`BIGINT`, `DateTime`→`TIMESTAMP`, `Currency`→`DECIMAL(19,4)`,
`Boolean`→`BOOLEAN`, `Text`/`Memo`→`VARCHAR`, etc. Each table is loaded as raw
text and then `TRY_CAST` into the typed schema, so:

- every row is kept (a bad value never drops a row);
- a *successful* cast is lossless (reversible), so the only information typing
  can't hold is a cell that fails to cast — and each of those is preserved
  verbatim in the hidden **`_mdb_cast_rejects`** table
  `(table_name, row_ordinal, column_name, access_type, raw_value)`, and counted
  in the sync report.

On PNDS this leaves exactly 10 rejects, all Access's invalid `1900-01-00` zero
date — typed cell becomes NULL, original kept in the sidecar. No polars/pydantic
dependency; typing is pure DuckDB. Re-typing an existing mirror after a loader
change needs one `--full-refresh` (unchanged sources are otherwise skipped).

## Completeness

`mdb-count` is an unreliable oracle on Access-2007+ (`ACE12`) files, so the
per-table row-count check prints a **REVIEW** list, not a pass/fail — most
deltas are mdb-count noise (verified against a second independent reader), not
data loss. Two known mdbtools gaps on the PNDS file, accepted as out-of-scope:
`syncImportLog` (mdbtools reads 0 rows; not needed) and
`zFreebalance_Budget_extract` (genuinely empty). See the memory note
`mdbtools-ace-fidelity` for the full audit.

Not yet: automatic primary-key detection (supplied via `--pks` for `--merge`).

## Postgres sync — partisipa (Option B)

Push the mirror into partisipa's Postgres set-based, bypassing the row-by-row
Django ORM importer. Three commands, all reading the mapping artifact produced by
partisipa's `manage.py dump_ducklake_mapping` (Access col → Postgres col/type,
PK, FK graph):

| Command | What it does |
|---|---|
| `mdb-ducklake-validate` | **read-only** reconcile: per-table row counts, PK-set diffs, and (`--deep`) changed-row counts. Clean ⇒ a sync would no-op. |
| `mdb-ducklake-to-postgres` | upsert: DuckDB streams Parquet → PG staging (`TRY_CAST` to target type, UTC timestamps), then one psycopg2 transaction of `INSERT … ON CONFLICT DO UPDATE` (deferred FKs). Never deletes. |
| `mdb-ducklake-prune` | guarded delete of rows gone from the mirror. **Dry-run by default**; `--apply` to delete. Skips empty-mirror tables and tables exceeding `--max-delete-fraction`; deletes children-before-parents in one transaction. |

Typical reconcile loop (creds via env + `sops exec-env`):

```bash
sops exec-env s3.enc.env "PGPASSWORD=… uv run mdb-ducklake-validate --mapping m.json"       # see drift
sops exec-env s3.enc.env "PGPASSWORD=… uv run mdb-ducklake-to-postgres --mapping m.json"     # upsert
sops exec-env s3.enc.env "PGPASSWORD=… uv run mdb-ducklake-prune --mapping m.json --apply"   # delete gone rows
sops exec-env s3.enc.env "PGPASSWORD=… uv run mdb-ducklake-validate --mapping m.json --deep" # confirm clean
```

Validated on a throwaway schema cloned from a partisipa pod: all 85 tables sync
in ~30s; validate runs read-only in ~19s and reports clean after a sync;
prune's empty-mirror guard refuses to wipe rows for tables the mirror reads as 0
(e.g. `syncimportlog`). PG connection via `PGHOST/PGPORT/PGDATABASE/PGUSER/
PGPASSWORD`, target `--pg-schema`. Views (no source alias) are skipped.

> Still to validate before production: a run against the **real schema with FK
> constraints** (the test clones use `LIKE`, which drops FKs), and a mapping
> generated from partisipa `main` (this one is from a feature-branch pod).
