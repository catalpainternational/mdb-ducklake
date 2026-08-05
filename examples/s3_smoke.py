"""Same merge/snapshot proof as smoke.py, but with DATA_PATH on real S3.

Credentials come from the environment (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY),
so run it under sops:

    sops exec-env s3.enc.env 'uv run python examples/s3_smoke.py'

Endpoint/region/bucket default to the DigitalOcean Spaces target but can be
overridden via S3_ENDPOINT / S3_REGION / DUCKLAKE_DATA_PATH.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from mdb_ducklake.sync import merge_staging, open_lake

ENDPOINT = os.environ.get("S3_ENDPOINT", "sgp1.digitaloceanspaces.com")
REGION = os.environ.get("S3_REGION", "sgp1")
DATA_PATH = os.environ.get("DUCKLAKE_DATA_PATH", "s3://ducklake/smoke-test")


def load(lake, rows):
    con = lake.con
    con.execute("DROP TABLE IF EXISTS staging")
    con.execute("CREATE TEMP TABLE staging (id INTEGER, name VARCHAR, qty INTEGER)")
    con.executemany("INSERT INTO staging VALUES (?, ?, ?)", rows)


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # Catalog kept local+temporary; only the parquet DATA lands on S3.
        lake = open_lake(
            Path(tmp) / "catalog.ducklake",
            DATA_PATH,
            s3_endpoint=ENDPOINT,
            s3_region=REGION,
        )
        print(f"Attached DuckLake with DATA_PATH={DATA_PATH} via {ENDPOINT}")

        load(lake, [(1, "Aileu", 10), (2, "Baucau", 20), (3, "Cova", 30)])
        merge_staging(lake, "Village", pk="id")

        load(lake, [(1, "Aileu", 10), (2, "Baucau", 25), (4, "Dili", 40)])
        merge_staging(lake, "Village", pk="id")

        final = lake.con.execute('SELECT * FROM lake."Village" ORDER BY id').fetchall()
        assert final == [(1, "Aileu", 10), (2, "Baucau", 25), (4, "Dili", 40)], final
        print(f"OK — mirror correct on S3; {len(lake.snapshots())} snapshots")
        print("Sample parquet written under:", DATA_PATH)


if __name__ == "__main__":
    main()
