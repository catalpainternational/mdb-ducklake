"""End-to-end: validate / upsert / prune against a real Postgres and a local mirror.

Needs a disposable Postgres; set MDB_DUCKLAKE_TEST_DSN to run, e.g.
  podman run -d --rm -p 55432:5432 -e POSTGRES_PASSWORD=pw postgres:16
  MDB_DUCKLAKE_TEST_DSN="host=localhost port=55432 dbname=postgres user=postgres password=pw"
"""

import os

import pytest

from mdb_ducklake import LakeConfig, open_lake, prune, sync_csv, upsert, validate

DSN = os.environ.get("MDB_DUCKLAKE_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="MDB_DUCKLAKE_TEST_DSN not set")

MAPPING = {
    "tables": {
        "village": {
            "model": "Village",
            "access_table": "Village",
            "pk_access_col": "VillageID",
            "pk_pg_col": "id",
            "columns": [
                {"access_col": "VillageID", "pg_col": "id", "pg_type": "integer", "is_pk": True, "nullable": False},
                {"access_col": "Name", "pg_col": "name", "pg_type": "varchar(100)", "is_pk": False, "nullable": True},
            ],
            "fk_targets": [],
        },
        "a_view": {"model": "AView", "access_table": None, "pk_access_col": None,
                   "pk_pg_col": "id", "columns": [], "fk_targets": []},
    },
    "warnings": [],
}


@pytest.fixture
def pg():
    psycopg2 = pytest.importorskip("psycopg2")
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS village")
    cur.execute("CREATE TABLE village (id integer PRIMARY KEY, name varchar(100))")
    yield cur
    cur.execute("DROP TABLE IF EXISTS village")
    cur.execute("DROP FUNCTION IF EXISTS skip_village_2() CASCADE")
    conn.close()


def write_mirror(tmp_path, rows):
    csv = tmp_path / "village.csv"
    csv.write_text("VillageID,Name\n" + "".join(f"{i},{n}\n" for i, n in rows))
    lake = open_lake(tmp_path / "catalog.ducklake", tmp_path / "data")
    sync_csv(lake, "Village", csv, "VillageID")
    lake.con.close()
    return LakeConfig(str(tmp_path / "catalog.ducklake"), str(tmp_path / "data"))


def test_validate_upsert_prune_round_trip(tmp_path, pg):
    lake = write_mirror(tmp_path, [(i, f"v{i}") for i in range(1, 21)])
    pg.execute("INSERT INTO village VALUES (1, 'old'), (99, 'gone')")

    before = validate(MAPPING, dsn=DSN, lake=lake, deep=True)
    assert [d["table"] for d in before["differences"]] == ["village"]
    d = before["differences"][0]
    assert (d["only_in_mirror"], d["only_in_pg"], d["changed"]) == (19, 1, 1)

    up = upsert(MAPPING, dsn=DSN, lake=lake)
    assert up["rows_written"] == 20  # 19 inserted + 1 changed
    assert up["mismatches"] == []
    pg.execute("SELECT name FROM village WHERE id = 1")
    assert pg.fetchone() == ("v1",)

    # Dry run reports the stale row but keeps it.
    dry = prune(MAPPING, dsn=DSN, lake=lake)
    assert dry["rows_deleted"] is None
    assert dry["plans"] == [{"table": "village", "action": "delete", "mirror_rows": 20,
                             "pg_rows": 21, "would_delete": 1}]
    pg.execute("SELECT count(*) FROM village")
    assert pg.fetchone() == (21,)

    done = prune(MAPPING, dsn=DSN, lake=lake, apply=True)
    assert done["rows_deleted"] == 1

    after = validate(MAPPING, dsn=DSN, lake=lake, deep=True)
    assert after["differences"] == []
    assert upsert(MAPPING, dsn=DSN, lake=lake)["rows_written"] == 0


def test_prune_fraction_guard(tmp_path, pg):
    lake = write_mirror(tmp_path, [(1, "a")])
    pg.execute("INSERT INTO village VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    guarded = prune(MAPPING, dsn=DSN, lake=lake, apply=True)
    assert guarded["plans"][0]["action"] == "skip:fraction"
    assert guarded["rows_deleted"] == 0

    loosened = prune(MAPPING, dsn=DSN, lake=lake, max_delete_fraction=0.9)
    assert loosened["plans"][0]["action"] == "delete"

    forced = prune(MAPPING, dsn=DSN, lake=lake, apply=True, force=True)
    assert forced["rows_deleted"] == 2


def test_prune_never_empties_a_table_from_an_empty_mirror(tmp_path, pg):
    lake = write_mirror(tmp_path, [])
    pg.execute("INSERT INTO village VALUES (1, 'a')")

    report = prune(MAPPING, dsn=DSN, lake=lake, apply=True, force=True)
    assert report["plans"][0]["action"] == "skip:empty-mirror"
    pg.execute("SELECT count(*) FROM village")
    assert pg.fetchone() == (1,)


def test_upsert_reports_rows_postgres_did_not_keep(tmp_path, pg):
    lake = write_mirror(tmp_path, [(1, "a"), (2, "b")])
    pg.execute(
        "CREATE FUNCTION skip_village_2() RETURNS trigger LANGUAGE plpgsql AS "
        "$$ BEGIN IF NEW.id = 2 THEN RETURN NULL; END IF; RETURN NEW; END $$"
    )
    pg.execute("CREATE TRIGGER skip2 BEFORE INSERT ON village FOR EACH ROW EXECUTE FUNCTION skip_village_2()")
    assert upsert(MAPPING, dsn=DSN, lake=lake)["mismatches"] == ["village"]


def staging_left(pg):
    pg.execute("SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'mdb_stg'")
    return pg.fetchone()[0]


def test_staging_schema_is_dropped_after_success_and_failure(tmp_path, pg):
    lake = write_mirror(tmp_path, [(1, "a")])
    upsert(MAPPING, dsn=DSN, lake=lake)
    assert staging_left(pg) == 0
    prune(MAPPING, dsn=DSN, lake=lake)
    assert staging_left(pg) == 0
    pg.execute("DROP TABLE village")
    with pytest.raises(Exception):
        upsert(MAPPING, dsn=DSN, lake=lake)
    assert staging_left(pg) == 0
    pg.execute("CREATE TABLE village (id integer PRIMARY KEY, name varchar(100))")


def test_include_limits_tables(tmp_path, pg):
    lake = write_mirror(tmp_path, [(1, "a")])
    assert upsert(MAPPING, dsn=DSN, lake=lake, include=[])["tables_synced"] == 0
    assert validate(MAPPING, dsn=DSN, lake=lake, include=["village"])["tables_checked"] == 1
