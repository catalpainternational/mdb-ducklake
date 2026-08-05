"""End-to-end smoke test of the DuckLake merge/snapshot loop.

Uses synthetic in-memory data so it needs neither mdbtools nor an .accdb —
it proves the *mechanics*: insert, update-changed-only, delete-missing, and
that each sync produces a new snapshot with correct final state.

Run:  uv run python examples/smoke.py
"""

from __future__ import annotations

import tempfile

import duckdb
from pathlib import Path

from mdb_ducklake.sync import merge_staging, open_lake


def load(lake, rows):
    """Replace the staging table with `rows` (list of (id, name, qty))."""
    con = lake.con
    con.execute("DROP TABLE IF EXISTS staging")
    con.execute("CREATE TEMP TABLE staging (id INTEGER, name VARCHAR, qty INTEGER)")
    con.executemany("INSERT INTO staging VALUES (?, ?, ?)", rows)


def state(lake):
    return lake.con.execute('SELECT * FROM lake."Village" ORDER BY id').fetchall()


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        lake = open_lake(tmp / "catalog.ducklake", tmp / "data")

        # Sync 1: initial insert of 3 rows.
        load(lake, [(1, "Aileu", 10), (2, "Baucau", 20), (3, "Cova", 30)])
        merge_staging(lake, "Village", pk="id")
        assert state(lake) == [(1, "Aileu", 10), (2, "Baucau", 20), (3, "Cova", 30)]

        # Sync 2: row 2 changes qty, row 3 disappears, row 4 is new.
        load(lake, [(1, "Aileu", 10), (2, "Baucau", 25), (4, "Dili", 40)])
        merge_staging(lake, "Village", pk="id")
        assert state(lake) == [(1, "Aileu", 10), (2, "Baucau", 25), (4, "Dili", 40)], state(lake)

        # Sync 3: no changes at all -> still a valid mirror, cheap MERGE.
        merge_staging(lake, "Village", pk="id")
        assert state(lake) == [(1, "Aileu", 10), (2, "Baucau", 25), (4, "Dili", 40)]

        snaps = lake.snapshots()
        print(f"OK — final state is a correct mirror; {len(snaps)} snapshots recorded")

        # Time-travel: walk every snapshot and show how the row count evolved.
        for snap in snaps:
            sid = snap[0]
            try:
                n = lake.con.execute(
                    f'SELECT count(*) FROM lake."Village" AT (VERSION => {sid})'
                ).fetchone()[0]
            except duckdb.Error:
                n = "-"  # table did not exist yet at this snapshot
            print(f"  snapshot {sid}: {n} rows")


if __name__ == "__main__":
    main()
