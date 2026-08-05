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

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

import duckdb

# Credentials are read from the environment so they can be injected at runtime
# (e.g. `sops exec-env pgbackrest.env ...`) and never live in code or logs.
# First match wins; the pgbackrest names let a DO Spaces backup secret be
# reused as-is.
S3_KEY_ENV = ("MDB_DUCKLAKE_S3_KEY_ID", "PGBACKREST_REPO2_S3_KEY", "AWS_ACCESS_KEY_ID")
S3_SECRET_ENV = ("MDB_DUCKLAKE_S3_SECRET", "PGBACKREST_REPO2_S3_KEY_SECRET", "AWS_SECRET_ACCESS_KEY")


def _first_env(names: Sequence[str]) -> Optional[str]:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


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
    s3_endpoint: Optional[str] = None,
    s3_region: Optional[str] = None,
    s3_url_style: str = "vhost",
) -> Lake:
    """Attach (creating if needed) a DuckLake and return a :class:`Lake`.

    ``catalog_path`` is a DuckDB file (checked into the repo). ``data_path`` is
    a directory for local prototyping or an ``s3://`` prefix in production. For
    an ``s3://`` path, credentials are read from the environment (see
    :data:`S3_KEY_ENV` / :data:`S3_SECRET_ENV`) and ``s3_endpoint``/``s3_region``
    identify the S3-compatible provider (e.g. DigitalOcean Spaces).
    """
    catalog_path = Path(catalog_path)
    catalog_path.parent.mkdir(parents=True, exist_ok=True)

    data_str = str(data_path)
    is_s3 = data_str.startswith("s3://")
    if is_s3:
        if not data_str.endswith("/"):
            data_str += "/"
    elif "://" not in data_str:
        # Local prototype: ensure the data dir exists.
        Path(data_path).mkdir(parents=True, exist_ok=True)
        data_str = str(Path(data_path).resolve()) + "/"

    con = con or duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    if is_s3:
        _configure_s3(con, s3_endpoint, s3_region, s3_url_style)
    con.execute(
        f"ATTACH IF NOT EXISTS 'ducklake:{catalog_path}' AS {alias} "
        f"(DATA_PATH '{data_str}')"
    )
    return Lake(con=con, alias=alias, catalog_path=catalog_path, data_path=Path(data_str))


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _configure_s3(
    con: duckdb.DuckDBPyConnection,
    endpoint: Optional[str],
    region: Optional[str],
    url_style: str,
    secret_name: str = "mdb_ducklake_s3",
) -> None:
    """Register a DuckDB S3 secret from environment credentials.

    The secret value is only ever inlined into the CREATE SECRET statement,
    which is never logged; on failure we raise a scrubbed error so the key
    cannot leak through an exception message.
    """
    key_id = _first_env(S3_KEY_ENV)
    secret = _first_env(S3_SECRET_ENV)
    if not key_id or not secret:
        raise RuntimeError(
            "s3:// DATA_PATH needs credentials in the environment "
            f"(one of {S3_KEY_ENV} and {S3_SECRET_ENV}). "
            "Inject them at runtime, e.g. `sops exec-env s3.enc.env <command>`."
        )

    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    parts = [
        "TYPE s3",
        f"KEY_ID {_sql_str(key_id)}",
        f"SECRET {_sql_str(secret)}",
        f"URL_STYLE {_sql_str(url_style)}",
    ]
    if endpoint:
        parts.append(f"ENDPOINT {_sql_str(endpoint)}")
    if region:
        parts.append(f"REGION {_sql_str(region)}")
    try:
        con.execute(f"CREATE OR REPLACE SECRET {secret_name} ({', '.join(parts)})")
    except duckdb.Error as exc:  # scrub: never surface the inlined secret
        raise RuntimeError(f"Failed to create DuckDB S3 secret: {type(exc).__name__}") from None


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
