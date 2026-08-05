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

## Status — prototype

Working: local `DATA_PATH`, MERGE-based mirror (insert/update/delete-missing),
snapshots + time-travel, CSV and `.accdb` ingestion.

Not yet: S3 `DATA_PATH` (design supports it — just pass an `s3://` prefix +
DuckDB `httpfs`/secret config), automatic primary-key detection, explicit
Access→DuckDB type mapping (currently inferred from CSV).
