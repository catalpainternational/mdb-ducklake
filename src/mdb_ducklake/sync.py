"""Core sync logic: mirror source tables into an updatable DuckLake.

The DuckLake *catalog* (metadata) is a DuckDB file that is checked into the
repo; the *data* (parquet) lives under DATA_PATH — a local directory for the
prototype, an ``s3://`` prefix in production.

Sync is "latest mirror only": each run MERGEs the source's current state into
the lake, touching only rows that actually changed, and (optionally) deleting
rows that have vanished from the source. DuckLake records one snapshot per
committing statement, so history is available via time-travel even though we
don't model it explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

import duckdb


@dataclass
class Lake:
    """An attached DuckLake, exposed through a live DuckDB connection."""

    con: duckdb.DuckDBPyConnection
    alias: str
    catalog_path: Path
    data_path: Path

    def snapshots(self) -> list[tuple]:
        return self.con.execute(
            f"SELECT * FROM ducklake_snapshots('{self.alias}') ORDER BY snapshot_id"
        ).fetchall()

    def tables(self) -> list[str]:
        rows = self.con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = ? ORDER BY table_name",
            [self.alias],
        ).fetchall()
        return [r[0] for r in rows]


def open_lake(
    catalog_path: str | Path,
    data_path: str | Path,
    alias: str = "lake",
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> Lake:
    """Attach (creating if needed) a DuckLake and return a :class:`Lake`.

    ``catalog_path`` is a DuckDB file (checked into the repo). ``data_path`` is
    a directory for local prototyping or an ``s3://`` prefix in production.
    """
    catalog_path = Path(catalog_path)
    catalog_path.parent.mkdir(parents=True, exist_ok=True)

    # Local prototype: ensure the data dir exists. For s3:// leave it alone.
    data_str = str(data_path)
    if "://" not in data_str:
        Path(data_path).mkdir(parents=True, exist_ok=True)
        data_str = str(Path(data_path).resolve()) + "/"

    con = con or duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute(
        f"ATTACH IF NOT EXISTS 'ducklake:{catalog_path}' AS {alias} "
        f"(DATA_PATH '{data_str}')"
    )
    return Lake(con=con, alias=alias, catalog_path=catalog_path, data_path=Path(data_str))


def _describe(con: duckdb.DuckDBPyConnection, relation: str) -> list[str]:
    return [row[0] for row in con.execute(f"DESCRIBE {relation}").fetchall()]


def merge_staging(
    lake: Lake,
    table: str,
    pk: str | Sequence[str],
    staging: str = "staging",
    delete_missing: bool = True,
) -> None:
    """MERGE the ``staging`` relation into ``lake.<table>`` keyed on ``pk``.

    Creates the target (inferring types from staging) on first run. Updates
    only rows whose non-key columns differ; inserts new rows; and, when
    ``delete_missing`` is set, deletes rows absent from staging so the lake is a
    true mirror of the source.
    """
    con = lake.con
    alias = lake.alias
    pks = [pk] if isinstance(pk, str) else list(pk)

    columns = _describe(con, staging)
    missing_pks = [k for k in pks if k not in columns]
    if missing_pks:
        raise KeyError(f"PK column(s) {missing_pks} not found in {staging} ({columns})")
    non_pk = [c for c in columns if c not in pks]

    qualified = f"{alias}.{_quote(table)}"

    # First run: materialise an empty target with staging's inferred schema.
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {qualified} AS "
        f"SELECT * FROM {staging} WHERE false"
    )

    on = " AND ".join(f"tgt.{_quote(k)} = src.{_quote(k)}" for k in pks)
    change_pred = " OR ".join(
        f"tgt.{_quote(c)} IS DISTINCT FROM src.{_quote(c)}" for c in non_pk
    ) or "false"
    set_clause = ", ".join(f"{_quote(c)} = src.{_quote(c)}" for c in non_pk)
    col_list = ", ".join(_quote(c) for c in columns)
    val_list = ", ".join(f"src.{_quote(c)}" for c in columns)

    # DuckLake's MERGE supports only a single UPDATE/DELETE action per
    # statement, so upsert (update + insert) and delete-missing are two steps.
    upsert = [
        f"MERGE INTO {qualified} AS tgt",
        f"USING {staging} AS src",
        f"ON {on}",
    ]
    if non_pk:
        upsert.append(f"WHEN MATCHED AND ({change_pred}) THEN UPDATE SET {set_clause}")
    upsert.append(f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})")
    con.execute("\n".join(upsert))

    if delete_missing:
        key_tuple = ", ".join(_quote(k) for k in pks)
        con.execute(
            f"DELETE FROM {qualified} "
            f"WHERE ({key_tuple}) NOT IN (SELECT {key_tuple} FROM {staging})"
        )


def sync_csv(
    lake: Lake,
    table: str,
    csv_path: str | Path,
    pk: str | Sequence[str],
    delete_missing: bool = True,
) -> None:
    """Load a CSV file into a staging table and MERGE it into the lake."""
    con = lake.con
    con.execute("DROP TABLE IF EXISTS staging")
    con.execute(
        "CREATE TEMP TABLE staging AS "
        "SELECT * FROM read_csv(?, header=true, all_varchar=false)",
        [str(csv_path)],
    )
    merge_staging(lake, table, pk, delete_missing=delete_missing)


def sync_access_database(
    lake: Lake,
    access_file: str | Path,
    pks: dict[str, str | Sequence[str]],
    include: Optional[Iterable[str]] = None,
    delete_missing: bool = True,
) -> None:
    """Mirror every table of an .accdb into the lake.

    ``pks`` maps Access table name -> primary key column(s). mdbtools' schema
    output does not currently surface primary keys, so they are supplied
    explicitly (a future refinement can parse ALTER TABLE ... PRIMARY KEY).

    Requires the ``access`` extra (``pip install 'mdb-ducklake[access]'``).
    """
    import tempfile

    from mdb_codegen.mdbtools import AccessDatabase  # lazy: only needs the extra

    db = AccessDatabase(Path(access_file))
    include_set = set(include) if include is not None else None
    for name, atable in db.tables.items():
        if include_set is not None and name not in include_set:
            continue
        if name not in pks:
            raise KeyError(f"No primary key configured for Access table {name!r}")
        # mdb-export emits CSV (with a header row) on stdout; DuckDB reads CSV
        # from a path, so stage it through a temp file.
        with tempfile.NamedTemporaryFile(suffix=".csv") as tmp:
            tmp.write(atable.export())
            tmp.flush()
            sync_csv(lake, name, tmp.name, pks[name], delete_missing=delete_missing)


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'
