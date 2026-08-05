"""Command-line entry point for syncing an Access DB into a DuckLake mirror."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mdb_ducklake.sync import open_lake, sync_access_database, sync_csv


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mdb-ducklake", description=__doc__)
    parser.add_argument("--catalog", default="catalog/catalog.ducklake", help="DuckLake catalog file (checked into the repo)")
    parser.add_argument("--data-path", default="data", help="DATA_PATH for parquet: a local dir, or an s3:// prefix")
    parser.add_argument("--s3-endpoint", default=None, help="S3 endpoint host for an s3:// DATA_PATH, e.g. sgp1.digitaloceanspaces.com")
    parser.add_argument("--s3-region", default=None, help="S3 region for an s3:// DATA_PATH, e.g. sgp1")
    parser.add_argument("--s3-url-style", default="vhost", choices=["vhost", "path"], help="S3 URL style (DigitalOcean Spaces uses vhost)")
    parser.add_argument("--no-delete-missing", action="store_true", help="Keep rows that have vanished from the source (default: mirror, so delete them)")

    sub = parser.add_subparsers(dest="command", required=True)

    p_access = sub.add_parser("sync", help="Copy an .accdb into the lake")
    p_access.add_argument("access_file", type=Path)
    p_access.add_argument("--pks", default=None, help='JSON mapping of table -> pk column for incremental MERGE, e.g. \'{"Village": "VillageID"}\'. Only used with --merge.')
    p_access.add_argument("--merge", action="store_true", help="Row-level MERGE for tables with a pk in --pks (default: full replace of changed tables)")
    p_access.add_argument("--full-refresh", action="store_true", help="Reload every table even if unchanged (disables fingerprint skipping)")
    p_access.add_argument("--include", nargs="*", help="Only sync these tables")

    p_csv = sub.add_parser("sync-csv", help="Mirror a single CSV into the lake (no mdbtools needed)")
    p_csv.add_argument("table")
    p_csv.add_argument("csv_file", type=Path)
    p_csv.add_argument("--pk", required=True)

    p_info = sub.add_parser("info", help="List tables and snapshots in the lake")

    args = parser.parse_args(argv)
    lake = open_lake(
        args.catalog,
        args.data_path,
        s3_endpoint=args.s3_endpoint,
        s3_region=args.s3_region,
        s3_url_style=args.s3_url_style,
    )
    delete_missing = not args.no_delete_missing

    if args.command == "sync":
        pks = json.loads(args.pks) if args.pks else None
        results = sync_access_database(
            lake,
            args.access_file,
            pks=pks,
            include=args.include,
            delete_missing=delete_missing,
            full=not args.merge,
            incremental=not args.full_refresh,
        )
        by = lambda s: [r for r in results if r.status == s]  # noqa: E731
        new, changed, unchanged, errored = by("new"), by("changed"), by("unchanged"), by("error")
        wrote = bool(new or changed)
        print(
            f"{len(results)} tables: {len(new)} new, {len(changed)} changed, "
            f"{len(unchanged)} unchanged, {len(errored)} errored."
        )
        # NB: source_rows is mdb-count, unreliable on ACE — see README "Completeness".
        review = [r for r in (new + changed) if not r.ok]
        for r in review:
            delta = r.loaded_rows - r.source_rows
            print(f"  REVIEW {r.table}: mdb-count={r.source_rows} loaded={r.loaded_rows} ({delta:+d})")
        for r in errored:
            print(f"  ERROR  {r.table}: {r.error}")
        _finish(lake, wrote=wrote)
        return 0
    elif args.command == "sync-csv":
        sync_csv(lake, args.table, args.csv_file, args.pk, delete_missing=delete_missing)
        print("Tables:", lake.tables())
        print("Snapshots:", len(lake.snapshots()))
        _finish(lake, wrote=True)
        return 0

    # info (read-only)
    print("Tables:", lake.tables())
    print("Snapshots:", len(lake.snapshots()))
    return 0


def _finish(lake, wrote: bool) -> None:
    """Persist an S3-backed catalog after a write (no-op locally)."""
    if wrote and lake.catalog_uri:
        lake.push_catalog()
        print(f"Catalog uploaded to {lake.catalog_uri}")


if __name__ == "__main__":
    raise SystemExit(main())
