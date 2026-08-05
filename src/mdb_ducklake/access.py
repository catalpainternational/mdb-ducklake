"""Thin, dependency-free wrappers around the mdbtools CLI.

Only the handful of commands the sync needs. Requires the mdbtools binaries
(``mdb-tables``, ``mdb-export``, ``mdb-count``, ``mdb-schema``) on ``PATH`` —
install them with e.g. ``brew install mdbtools`` or ``apt install mdbtools``.
No Python dependency, so this repo stays off mdb-codegen's pydantic-v1 stack.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

# Match mdb-codegen's export formats so DuckDB can parse dates/datetimes.
DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class MdbToolsError(RuntimeError):
    pass


def ensure_installed() -> None:
    missing = [b for b in ("mdb-tables", "mdb-export", "mdb-count") if shutil.which(b) is None]
    if missing:
        raise MdbToolsError(
            f"mdbtools not found on PATH ({', '.join(missing)}). "
            "Install it, e.g. `brew install mdbtools`."
        )


def _run(args: list[str]) -> bytes:
    proc = subprocess.run(args, capture_output=True)
    if proc.returncode != 0:
        raise MdbToolsError(f"{args[0]} failed: {proc.stderr.decode(errors='replace').strip()}")
    return proc.stdout


def list_tables(access_file: str | Path) -> list[str]:
    out = _run(["mdb-tables", "-1", str(access_file)]).decode()
    return [t for t in out.splitlines() if t.strip()]


def count(access_file: str | Path, table: str) -> int:
    return int(_run(["mdb-count", str(access_file), table]).decode().strip())


def schema(access_file: str | Path, table: str) -> str:
    return _run(["mdb-schema", "-T", table, str(access_file)]).decode(errors="replace")


# A column line in mdb-schema DDL, e.g. "\t[Report_Month]\t\t\tDateTime, "
# The CREATE TABLE line doesn't start with "[", so it won't match.
_COL_RE = re.compile(r"^\s*\[(?P<name>.+?)\]\s+(?P<type>[A-Za-z][A-Za-z /]*)")


def column_types(access_file: str | Path, table: str) -> list[tuple[str, str]]:
    """Parse mdb-schema DDL into ``[(column_name, access_type), ...]``.

    ``access_type`` is the raw Access type token, e.g. "Long Integer",
    "DateTime", "Currency", "Text" (the "(255)" length is dropped). Callers map
    it to a storage type.
    """
    cols: list[tuple[str, str]] = []
    for line in schema(access_file, table).splitlines():
        m = _COL_RE.match(line)
        if m:
            cols.append((m.group("name"), m.group("type").strip()))
    return cols


def export_csv(access_file: str | Path, table: str, dest: str | Path) -> Path:
    """Export ``table`` to a CSV file at ``dest`` (with header row).

    Binary/OLE columns are stripped — Access OLE blobs don't round-trip through
    CSV. Dates/datetimes use ISO formats DuckDB parses natively.
    """
    csv_bytes = _run(
        [
            "mdb-export",
            "-D", DATE_FORMAT,
            "-T", DATETIME_FORMAT,
            "-b", "strip",
            str(access_file),
            table,
        ]
    )
    dest = Path(dest)
    dest.write_bytes(csv_bytes)
    return dest
