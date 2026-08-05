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
    parser.add_argument("--no-delete-missing", action="store_true", help="Keep rows that have vanished from the source (default: mirror, so delete them)")

    sub = parser.add_subparsers(dest="command", required=True)

    p_access = sub.add_parser("sync", help="Mirror an .accdb into the lake")
    p_access.add_argument("access_file", type=Path)
    p_access.add_argument("--pks", required=True, help='JSON mapping of table name -> pk column, e.g. \'{"Village": "VillageID"}\'')
    p_access.add_argument("--include", nargs="*", help="Only sync these tables")

    p_csv = sub.add_parser("sync-csv", help="Mirror a single CSV into the lake (no mdbtools needed)")
    p_csv.add_argument("table")
    p_csv.add_argument("csv_file", type=Path)
    p_csv.add_argument("--pk", required=True)

    p_info = sub.add_parser("info", help="List tables and snapshots in the lake")

    args = parser.parse_args(argv)
    lake = open_lake(args.catalog, args.data_path)
    delete_missing = not args.no_delete_missing

    if args.command == "sync":
        sync_access_database(lake, args.access_file, json.loads(args.pks), include=args.include, delete_missing=delete_missing)
    elif args.command == "sync-csv":
        sync_csv(lake, args.table, args.csv_file, args.pk, delete_missing=delete_missing)
    elif args.command == "info":
        pass

    print("Tables:", lake.tables())
    print("Snapshots:", len(lake.snapshots()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
