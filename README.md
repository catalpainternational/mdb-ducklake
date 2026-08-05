# mdb-ducklake

Sync an MS Access database into an **updatable DuckLake mirror**.

Each run reads the current state of an `.accdb` and MERGEs it into a
[DuckLake](https://ducklake.select/) lakehouse — inserting new rows, updating
changed rows, and deleting rows that have vanished from the source. The result
is a queryable, always-current clone of the Access DB, with per-sync snapshots
you can time-travel through.

Extraction reuses [`mdb-codegen`](https://github.com/catalpainternational/mdb-codegen)'s
mdbtools wrapper; this repo adds only the DuckLake side.

## Storage model

- **Catalog** (metadata) — a DuckDB file at `catalog/catalog.ducklake`, **checked
  into this repo**. It is the source of truth for which snapshots/tables exist.
  Commit it after every sync.
- **Data** (parquet) — under `DATA_PATH`. A local `data/` dir for the prototype
  (git-ignored); an `s3://…` prefix in production.

> Because the catalog references data-file paths, the catalog and its data are a
> matched pair. Single-writer + manual CLI keeps this safe: **sync, then commit
> the catalog**.

## Quick start

```bash
uv sync                      # installs duckdb
uv run python examples/smoke.py   # proves the merge/snapshot loop, no .accdb needed
```

### Sync a real Access DB

```bash
uv sync --extra access       # adds the mdbtools Python wrapper (mdb-codegen)
# mdbtools' CLI must also be installed (mdb-export etc.)

uv run mdb-ducklake sync path/to/db.accdb \
  --pks '{"Village": "VillageID", "Suco": "SucoID"}'
git add catalog/catalog.ducklake && git commit -m "sync $(date -I)"
```

Primary keys are supplied explicitly for now — mdbtools' schema output doesn't
surface them. See **Status** below.

### S3 DATA_PATH (DigitalOcean Spaces)

Credentials live encrypted in `s3.enc.env` (SOPS + the org's SSH-key
recipients) and are injected only into the child process at runtime — they
never touch code, logs, or the shell history:

```bash
sops exec-env s3.enc.env 'uv run mdb-ducklake \
  --data-path s3://ducklake/mirror \
  --s3-endpoint sgp1.digitaloceanspaces.com --s3-region sgp1 \
  sync path/to/db.accdb --pks '"'"'{"Village": "VillageID"}'"'"''

# Prove the S3 round-trip end-to-end (no .accdb needed):
sops exec-env s3.enc.env 'uv run python examples/s3_smoke.py'
```

`open_lake` reads the key/secret from `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` (also accepts `MDB_DUCKLAKE_S3_*` or the pgbackrest
names) and registers a DuckDB S3 secret. To edit the encrypted creds:
`sops s3.enc.env`.

## Status — prototype

Working: local **and S3** (`s3://`, DigitalOcean Spaces) `DATA_PATH`,
MERGE-based mirror (insert/update/delete-missing), snapshots + time-travel,
CSV and `.accdb` ingestion, SOPS-encrypted credentials via `sops exec-env`.

Not yet: automatic primary-key detection (supplied via `--pks` for now),
explicit Access→DuckDB type mapping (currently inferred from CSV).
