# DuckDB as glue: mirroring an Access database to S3 and syncing it into Postgres

*An engineering narrative behind `mdb-ducklake`. The interesting part isn't any
single feature — it's that one embedded query engine holds three worlds together
with no ETL platform underneath.*

## The problem

PNDS (Timor-Leste's national village-development programme) runs on an MS Access
database — `PNDS_Interim_MIS-Data.accdb`, ~180 MB, 86 tables, the `ACE12`
(Access 2007+) format. Two things need it:

1. **Analytics.** A single-file, Windows-shaped `.accdb` is a poor substrate for
   analysis — no columnar scans, no cheap "what did this look like last month",
   and you need Access to open it.
2. **partisipa**, a Django/Postgres app, which ingests the Access data into its
   `pnds_data` schema. Historically via a row-by-row Django ORM importer:
   `update_or_create` per row, every existing row loaded into memory first.
   Correct, but slow and coupled to a legacy pydantic-v1 stack.

We wanted one pipeline that produces a fast, queryable, always-current clone of
the Access DB **and** feeds partisipa's Postgres — without standing up Spark,
Airflow, or a warehouse.

## The idea: one embedded engine as connective tissue

DuckDB turned out to be the whole spine. In a single process — a CLI binary, no
server — it:

- reads Parquet from S3 (the `httpfs` extension),
- maintains a **DuckLake** lakehouse (a catalog + Parquet files, with snapshots
  and time-travel),
- and attaches **Postgres** (the `postgres` extension) to stream data
  table-to-table.

So the entire flow — `s3://…/parquet → staging table in Postgres → INSERT …
ON CONFLICT` — happens *inside one embedded engine*, orchestrated by ~a few
hundred lines of Python. That "federate object storage and an OLTP database from
a CLI" move is the thing worth showing: most people still file DuckDB under
"SQLite for analytics" and don't realise it can be the glue.

## Part 1 — Access → an updatable DuckLake mirror on S3

Extraction is the one thing DuckDB *doesn't* do: `mdbtools` (a C CLI) reads the
`.accdb`. From there it's all DuckDB.

**Typing, losslessly.** Rather than let DuckDB's CSV sniffer guess, we parse the
Access schema (`mdb-schema`) and map each column to a DuckDB type
(`Long Integer`→`BIGINT`, `Currency`→`DECIMAL(19,4)`, `DateTime`→`TIMESTAMP`, …).
Each table is loaded as raw text and then `TRY_CAST` into the typed schema:

- every row is kept — a bad value never drops a row;
- a *successful* cast is reversible, so the only information typing can't carry
  is a cell that fails to cast — and each of those is preserved verbatim in a
  hidden `_mdb_cast_rejects` sidecar.

On PNDS this leaves exactly **10** rejects across 86 tables — all Access's
invalid `1900-01-00` "zero date". The typed cell becomes NULL; the original text
is kept. No pydantic, no polars — the typing is pure `TRY_CAST`.

**Incremental by fingerprint.** Each table is exported and hashed; if the hash
matches the last sync, the table is skipped entirely — no Parquet write, no S3
upload. Fingerprints live in a hidden `_mdb_sync_state` table in the lake.

**DuckLake** gives the "updatable clone" its shape: the catalog + Parquet live in
the same S3 bucket, each sync is a snapshot, and a cold reader with nothing local
reconstructs the whole mirror from S3 alone (`ATTACH 'ducklake:…' … AT
(VERSION => n)` for time-travel).

Result: 86 tables mirrored to DigitalOcean Spaces, typed and queryable, in tens
of seconds.

## Part 2 — DuckLake → Postgres, set-based

The same engine then talks to partisipa's Postgres. Both the lake and Postgres
are attached in one DuckDB connection:

```sql
ATTACH 'ducklake:…catalog.ducklake' AS lake (DATA_PATH 's3://…/data');
INSTALL postgres; LOAD postgres;
ATTACH 'host=… dbname=… …' AS pg (TYPE postgres);
```

For each table, DuckDB streams the mirror's Parquet into a Postgres **staging**
table — mapping Access column names to the Django column names (the authoritative
map comes from partisipa's own models, not a guess), `TRY_CAST`ing to each
column's Postgres type, and UTC-localising naive timestamps. Then a single
transaction runs `INSERT … ON CONFLICT (pk) DO UPDATE`, with a change-filter
(`WHERE target IS DISTINCT FROM EXCLUDED`) so a re-sync of unchanged data writes
**zero** rows.

Around the upsert sit two companions that reuse the exact same projection, so
they can't disagree with it:

- **`validate`** — read-only reconcile: per-table row counts, primary-key set
  differences, and (deep) changed-row counts. A clean result *means* a sync
  would be a no-op. It answers "does Postgres match the mirror?" across all
  tables in ~20 seconds, touching nothing.
- **`prune`** — guarded delete of rows gone from the mirror. Dry-run by default;
  refuses to touch a table the mirror reads as empty (so an extraction gap can't
  wipe real data).

Matview lifecycle stays with partisipa: it brackets the run with `dbsamizdat
nuke` / `sync` to drop and recreate its materialized views, so the sync tool
never touches a trigger.

**The payoff vs the ORM importer:** 471k rows typed and moved in ~30s (vs
row-by-row minutes), and a read-only "are we in sync?" check that didn't exist
before.

## Why DuckDB specifically

- **It's the glue, not just a query engine.** One process bridges S3 Parquet, a
  DuckLake lakehouse, and Postgres. No cluster, no scheduler, no warehouse.
- **Embedded / zero-infra.** It's a library in a CLI. Credentials come from the
  environment; there's nothing to run.
- **Set-based and fast.** Columnar + vectorised makes "cast and move half a
  million rows" a non-event, which is the whole reason this beats the row-by-row
  importer.
- **`TRY_CAST` as a typing strategy.** Lossless-by-construction typing falls out
  of ordinary SQL cast semantics + a rejects sidecar.
- **DuckLake.** A genuinely new (2025) lakehouse format doing real work: an
  incremental, snapshotted mirror that's cold-readable from object storage.

## The honest part: sharp edges

It's tidy, not frictionless. Things that bit us:

- **`REFRESH MATERIALIZED VIEW CONCURRENTLY` can't run inside a transaction** —
  so partisipa's matview-refresh triggers conflicted with our single bulk
  transaction. (We resolved it by having partisipa drop/recreate its matviews
  around the run, rather than the ETL touching triggers.)
- **`pytz` is needed** for DuckDB to hand back DuckLake's `TIMESTAMPTZ` snapshot
  columns — a non-obvious dependency.
- **`mdb-count` is an unreliable oracle** on ACE files (disagrees with the actual
  exported rows in both directions), so completeness had to be verified by
  cross-checking two independent readers, not trusted.
- DuckDB's **progress bar prints to stdout**, which muddied machine-readable
  output until disabled.
- Postgres-extension transaction scoping and `postgres_execute` semantics took
  some care.

And the extraction floor is `mdbtools`, which has real gaps on ACE12 (it silently
returns 0 rows for some tables) — DuckDB only picks up once data is out of Access.

## What wasn't DuckDB

Worth being clear: DuckDB was the clean engine *underneath* the work, but the
hard parts were domain understanding, not SQL —

- partisipa's authoritative Access→Postgres column map (irregular, lives in its
  Django models);
- the dbsamizdat-managed materialized-view lifecycle (11 of 13 read tables we
  sync);
- mdbtools' ACE fidelity gaps;
- and a **currency-rounding bug** a parity check against the old importer caught:
  Access `Currency` values landing in `INTEGER` columns were *rounded* by
  DuckDB's cast but *truncated* by the ORM's `int()`, an off-by-one on ~360
  financial cells. We changed the sync to truncate, matching partisipa's existing
  values.

That parity check — load the same `.accdb` through both paths and diff — is what
turns "looks right" into "provably equivalent." It found a real financial bug.

## The reusable pattern

Strip out PNDS and partisipa and you're left with something general:

> **DuckDB as a single-binary ETL spine** — attach object storage and one or more
> databases in the same process, use `TRY_CAST` for lossless typing, DuckLake for
> a snapshotted incremental mirror, and `INSERT … ON CONFLICT` for a set-based
> upsert — with a read-only `validate` that reuses the exact write projection so
> "are we in sync?" is a cheap, honest question.

No ETL platform required. That's the neat part.
