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
import tempfile
from dataclasses import dataclass, field
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
    # Set when the catalog is backed by an S3 object (download-before/upload-after).
    catalog_uri: Optional[str] = None
    s3_endpoint: Optional[str] = None
    s3_region: Optional[str] = None
    s3_key_id: Optional[str] = field(default=None, repr=False)
    s3_secret: Optional[str] = field(default=None, repr=False)

    def push_catalog(self) -> None:
        """Flush and upload the catalog back to S3 (no-op for a local catalog).

        DETACH first so DuckDB merges the WAL into the single catalog file,
        making the on-disk bytes consistent before upload. The Lake must not be
        used for further queries after this — call it once, at the end of a run.
        """
        if not self.catalog_uri:
            return
        from mdb_ducklake import s3store

        self.con.execute(f"DETACH {self.alias}")
        s3store.upload(
            self.catalog_path, self.catalog_uri, self.s3_endpoint, self.s3_region,
            self.s3_key_id, self.s3_secret,
        )

    def snapshots(self) -> list[tuple]:
        return self.con.execute(
            f"SELECT * FROM ducklake_snapshots('{self.alias}') ORDER BY snapshot_id"
        ).fetchall()

    def tables(self) -> list[str]:
        rows = self.con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_name NOT LIKE '\\_mdb\\_%' ESCAPE '\\' "
            "ORDER BY table_name",
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
    s3_key_id: Optional[str] = None,
    s3_secret: Optional[str] = None,
) -> Lake:
    """Attach (creating if needed) a DuckLake and return a :class:`Lake`.

    ``catalog_path`` is a DuckDB file (checked into the repo). ``data_path`` is
    a directory for local prototyping or an ``s3://`` prefix in production. For
    an ``s3://`` path, credentials are ``s3_key_id``/``s3_secret`` when both are
    given, else read from the environment (see :data:`S3_KEY_ENV` /
    :data:`S3_SECRET_ENV`), and ``s3_endpoint``/``s3_region``
    identify the S3-compatible provider (e.g. DigitalOcean Spaces).
    """
    from mdb_ducklake import s3store

    data_str = str(data_path)
    data_is_s3 = data_str.startswith("s3://")
    if data_is_s3:
        if not data_str.endswith("/"):
            data_str += "/"
    elif "://" not in data_str:
        # Local prototype: ensure the data dir exists.
        Path(data_path).mkdir(parents=True, exist_ok=True)
        data_str = str(Path(data_path).resolve()) + "/"

    if not (s3_key_id and s3_secret):
        # One pair for every S3 client (DuckDB and boto3), never half from each.
        s3_key_id, s3_secret = _first_env(S3_KEY_ENV), _first_env(S3_SECRET_ENV)

    # The catalog is either a local file or an S3 object mirrored to a local
    # temp file (S3 is the source of truth; we download it before use).
    catalog_uri: Optional[str] = None
    catalog_str = str(catalog_path)
    if s3store.is_s3_uri(catalog_str):
        catalog_uri = catalog_str
        local = Path(tempfile.gettempdir()) / f"ducklake_{Path(s3store.parse_s3_uri(catalog_str)[1]).name}"
        s3store.download(catalog_uri, local, s3_endpoint, s3_region, s3_key_id, s3_secret)  # False if new -> fresh catalog
        catalog_path = local
    else:
        catalog_path = Path(catalog_path)
        catalog_path.parent.mkdir(parents=True, exist_ok=True)

    con = con or duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    if data_is_s3 or catalog_uri:
        _configure_s3(con, s3_endpoint, s3_region, s3_url_style, s3_key_id, s3_secret)
    con.execute(
        f"ATTACH IF NOT EXISTS 'ducklake:{catalog_path}' AS {alias} "
        f"(DATA_PATH '{data_str}')"
    )
    return Lake(
        con=con,
        alias=alias,
        catalog_path=catalog_path,
        data_path=Path(data_str),
        catalog_uri=catalog_uri,
        s3_endpoint=s3_endpoint,
        s3_region=s3_region,
        s3_key_id=s3_key_id,
        s3_secret=s3_secret,
    )


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _configure_s3(
    con: duckdb.DuckDBPyConnection,
    endpoint: Optional[str],
    region: Optional[str],
    url_style: str,
    key_id: Optional[str] = None,
    secret: Optional[str] = None,
    secret_name: str = "mdb_ducklake_s3",
) -> None:
    """Register a DuckDB S3 secret from the given credentials, else the environment.

    The secret value is only ever inlined into the CREATE SECRET statement,
    which is never logged; on failure we raise a scrubbed error so the key
    cannot leak through an exception message.
    """
    key_id = key_id or _first_env(S3_KEY_ENV)
    secret = secret or _first_env(S3_SECRET_ENV)
    if not key_id or not secret:
        raise RuntimeError(
            "s3:// DATA_PATH needs credentials, passed in or in the environment "
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


# Access type token -> DuckDB storage type. Unknown/absent -> VARCHAR (kept as
# text, never a cast failure). A *successful* TRY_CAST is lossless, so only
# failed casts can lose information — those are captured in REJECTS_TABLE.
ACCESS_TO_DUCKDB = {
    "Long Integer": "BIGINT",
    "Integer": "INTEGER",
    "Byte": "UTINYINT",
    "Single": "FLOAT",
    "Double": "DOUBLE",
    "Currency": "DECIMAL(19,4)",
    "Decimal": "DECIMAL(19,4)",
    "DateTime": "TIMESTAMP",
    "Boolean": "BOOLEAN",
    "Text": "VARCHAR",
    "Memo": "VARCHAR",
    "Memo/Hyperlink": "VARCHAR",
    "GUID": "VARCHAR",
    "OLE": "VARCHAR",
}

REJECTS_TABLE = "_mdb_cast_rejects"


def _duckdb_type(access_type: str) -> str:
    return ACCESS_TO_DUCKDB.get(access_type, "VARCHAR")


def _cast_sql(col_quoted: str, duck_type: str) -> str:
    if duck_type == "BOOLEAN":
        # mdb-export emits 0/1 (Access true is -1); anything non-zero is true.
        return f"(TRY_CAST({col_quoted} AS INTEGER) <> 0)"
    return f"TRY_CAST({col_quoted} AS {duck_type})"


def _ensure_rejects_table(lake: Lake) -> None:
    lake.con.execute(
        f"CREATE TABLE IF NOT EXISTS {lake.alias}.{REJECTS_TABLE} "
        "(table_name VARCHAR, row_ordinal BIGINT, column_name VARCHAR, "
        "access_type VARCHAR, raw_value VARCHAR)"
    )


def load_full(
    lake: Lake,
    table: str,
    csv_path: str | Path,
    column_types: Optional[Sequence[tuple[str, str]]] = None,
) -> dict[str, int]:
    """Replace ``lake.<table>`` entirely with the CSV contents.

    With ``column_types`` (from :func:`access.column_types`), columns are typed
    via ``TRY_CAST`` off an all-VARCHAR raw load: every row is kept, and any
    cell that can't be typed becomes NULL but is preserved verbatim in
    ``_mdb_cast_rejects`` (table, row ordinal, column, raw value). Without
    ``column_types``, falls back to CSV type inference.

    Returns ``{column: failed_cast_count}`` for the report.
    """
    con = lake.con
    qualified = f"{lake.alias}.{_quote(table)}"

    if not column_types:
        con.execute(f"DROP TABLE IF EXISTS {qualified}")
        typed = "SELECT * FROM read_csv(?, header=true, sample_size=-1)"
        untyped = "SELECT * FROM read_csv(?, header=true, all_varchar=true)"
        try:
            con.execute(f"CREATE TABLE {qualified} AS {typed}", [str(csv_path)])
        except duckdb.Error:
            con.execute(f"CREATE TABLE {qualified} AS {untyped}", [str(csv_path)])
        return {}

    # Raw all-VARCHAR load with a synthetic ordinal to locate any rejects.
    con.execute("DROP TABLE IF EXISTS _mdb_raw")
    con.execute(
        "CREATE TEMP TABLE _mdb_raw AS "
        "SELECT *, row_number() OVER () AS _mdb_rn "
        "FROM read_csv(?, header=true, all_varchar=true)",
        [str(csv_path)],
    )
    raw_cols = [c for c in _describe(con, "_mdb_raw") if c != "_mdb_rn"]
    typemap = {name: (_duckdb_type(t), t) for name, t in column_types}

    select_parts: list[str] = []
    typed_cols: list[tuple[str, str, str]] = []  # (col, duck_type, access_type)
    for c in raw_cols:
        cq = _quote(c)
        duck_type, access_type = typemap.get(c, ("VARCHAR", "Text"))
        if duck_type == "VARCHAR":
            select_parts.append(cq)  # keep raw text; never a reject
        else:
            select_parts.append(f"{_cast_sql(cq, duck_type)} AS {cq}")
            typed_cols.append((c, duck_type, access_type))

    con.execute(f"DROP TABLE IF EXISTS {qualified}")
    con.execute(f"CREATE TABLE {qualified} AS SELECT {', '.join(select_parts)} FROM _mdb_raw")

    return _capture_rejects(lake, table, typed_cols)


def _capture_rejects(
    lake: Lake, table: str, typed_cols: Sequence[tuple[str, str, str]]
) -> dict[str, int]:
    """Record cells that failed to type (raw non-empty but TRY_CAST -> NULL)."""
    con = lake.con
    _ensure_rejects_table(lake)
    rq = f"{lake.alias}.{_quote(REJECTS_TABLE)}"

    # Clear this table's prior rejects only if it had any (avoid empty snapshots).
    had = con.execute(f"SELECT count(*) FROM {rq} WHERE table_name = ?", [table]).fetchone()[0]
    if had:
        con.execute(f"DELETE FROM {rq} WHERE table_name = ?", [table])
    if not typed_cols:
        return {}

    # Gather into a temp table (cheap, no snapshots), then one insert into the lake.
    con.execute("DROP TABLE IF EXISTS _mdb_rej")
    con.execute(
        "CREATE TEMP TABLE _mdb_rej "
        "(table_name VARCHAR, row_ordinal BIGINT, column_name VARCHAR, "
        "access_type VARCHAR, raw_value VARCHAR)"
    )
    for col, duck_type, access_type in typed_cols:
        cq = _quote(col)
        con.execute(
            f"INSERT INTO _mdb_rej SELECT ?, _mdb_rn, ?, ?, {cq} FROM _mdb_raw "
            f"WHERE {cq} IS NOT NULL AND {cq} <> '' AND ({_cast_sql(cq, duck_type)}) IS NULL",
            [table, col, access_type],
        )
    counts = dict(
        con.execute("SELECT column_name, count(*) FROM _mdb_rej GROUP BY column_name").fetchall()
    )
    if counts:
        con.execute(f"INSERT INTO {rq} SELECT * FROM _mdb_rej")
    return counts


STATE_TABLE = "_mdb_sync_state"


def _ensure_state_table(lake: Lake) -> None:
    lake.con.execute(
        f"CREATE TABLE IF NOT EXISTS {lake.alias}.{STATE_TABLE} "
        "(table_name VARCHAR, source_sha256 VARCHAR, loaded_rows BIGINT)"
    )


def _load_state(lake: Lake) -> dict[str, tuple[str, int]]:
    rows = lake.con.execute(
        f"SELECT table_name, source_sha256, loaded_rows FROM {lake.alias}.{STATE_TABLE}"
    ).fetchall()
    return {r[0]: (r[1], r[2]) for r in rows}


def _record_state(lake: Lake, table: str, digest: str, loaded_rows: int) -> None:
    q = f"{lake.alias}.{STATE_TABLE}"
    lake.con.execute(f"DELETE FROM {q} WHERE table_name = ?", [table])
    lake.con.execute(f"INSERT INTO {q} VALUES (?, ?, ?)", [table, digest, loaded_rows])


def _sha256(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class TableResult:
    table: str
    source_rows: int
    loaded_rows: int
    ok: bool
    # "new" | "changed" | "unchanged" | "error"
    status: str = "changed"
    error: Optional[str] = None
    # {column: count} of cells that failed to type (preserved in _mdb_cast_rejects)
    rejects: Optional[dict[str, int]] = None

    @property
    def reject_count(self) -> int:
        return sum(self.rejects.values()) if self.rejects else 0


def sync_access_database(
    lake: Lake,
    access_file: str | Path,
    pks: Optional[dict[str, str | Sequence[str]]] = None,
    include: Optional[Iterable[str]] = None,
    delete_missing: bool = True,
    full: bool = True,
    incremental: bool = True,
) -> list[TableResult]:
    """Copy every table of an .accdb into the lake.

    Each table is exported and fingerprinted (sha256 of the export). With
    ``incremental=True`` (default), a table whose fingerprint matches the last
    sync is **skipped** — no load, no parquet write, no S3 upload — which is
    where the cost is. Changed/new tables are loaded: fully replaced
    (``full=True`` or no pk known) or MERGE-mirrored when a primary key is
    given in ``pks`` and ``full=False``.

    Returns a :class:`TableResult` per table (status: new/changed/unchanged/
    error). ``source_rows`` comes from ``mdb-count``, an unreliable oracle on
    ACE files — treat count mismatches as "review", not proof of data loss.
    """
    from mdb_ducklake import access

    access.ensure_installed()
    pks = pks or {}
    include_set = set(include) if include is not None else None
    results: list[TableResult] = []

    _ensure_state_table(lake)
    prior = _load_state(lake)

    with tempfile.TemporaryDirectory() as tmpdir:
        for name in access.list_tables(access_file):
            if include_set is not None and name not in include_set:
                continue
            try:
                csv_path = Path(tmpdir) / "t.csv"
                access.export_csv(access_file, name, csv_path)
                digest = _sha256(csv_path)

                if incremental and name in prior and prior[name][0] == digest:
                    results.append(
                        TableResult(name, prior[name][1], prior[name][1], ok=True, status="unchanged")
                    )
                    continue

                status = "changed" if name in prior else "new"
                rejects: dict[str, int] = {}
                if full or name not in pks:
                    rejects = load_full(lake, name, csv_path, column_types=access.column_types(access_file, name))
                else:
                    sync_csv(lake, name, csv_path, pks[name], delete_missing=delete_missing)

                source_rows = access.count(access_file, name)
                loaded_rows = lake.con.execute(
                    f"SELECT count(*) FROM {lake.alias}.{_quote(name)}"
                ).fetchone()[0]
                _record_state(lake, name, digest, loaded_rows)
                results.append(
                    TableResult(
                        name, source_rows, loaded_rows, ok=(source_rows == loaded_rows),
                        status=status, rejects=rejects or None,
                    )
                )
            except Exception as exc:  # keep going; report per-table
                results.append(TableResult(name, -1, -1, ok=False, status="error", error=f"{type(exc).__name__}: {exc}"))
    return results


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'
