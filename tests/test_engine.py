"""Unit tests for the pure logic in the Postgres-sync engine (no DB/S3)."""

from mdb_ducklake.prune_postgres import load_order
from mdb_ducklake.to_postgres import mapped_projection, pg_type_to_duckdb, syncable


def test_pg_type_to_duckdb():
    cases = {
        "integer": "INTEGER",
        "bigint": "BIGINT",
        "smallint": "SMALLINT",
        "boolean": "BOOLEAN",
        "double precision": "DOUBLE",
        "date": "DATE",
        "numeric(19, 4)": "DECIMAL(19,4)",
        "numeric(12, 2)": "DECIMAL(12,2)",
        "numeric": "DECIMAL(19,4)",
        "timestamp with time zone": "TIMESTAMPTZ",
        "timestamp without time zone": "TIMESTAMP",
        "character varying(255)": "VARCHAR",
        "varchar(1024)": "VARCHAR",
        "text": "VARCHAR",
        "uuid": "VARCHAR",
        "some_unknown_type": "VARCHAR",  # safe fallback
    }
    for pg, expected in cases.items():
        assert pg_type_to_duckdb(pg) == expected, pg


def test_mapped_projection_casts_skips_and_utc():
    spec = {
        "columns": [
            {"access_col": "Suco_ID", "pg_col": "suco_id", "pg_type": "integer", "is_pk": True},
            {"access_col": "Report_Month", "pg_col": "report_month",
             "pg_type": "timestamp with time zone", "is_pk": False},
            {"access_col": "Absent", "pg_col": "absent", "pg_type": "integer", "is_pk": False},
        ]
    }
    src_types = {"Suco_ID": "BIGINT", "Report_Month": "TIMESTAMP"}  # 'Absent' not in mirror
    proj = mapped_projection(spec, src_types)

    cols = [pgc for pgc, _, _ in proj]
    assert cols == ["suco_id", "report_month"]        # 'absent' dropped
    assert proj[0] == ("suco_id", 'TRY_CAST("Suco_ID" AS INTEGER)', True)
    # naive mirror TIMESTAMP -> UTC before cast to timestamptz
    assert proj[1][1] == 'TRY_CAST(("Report_Month" AT TIME ZONE \'UTC\') AS TIMESTAMPTZ)'
    assert proj[1][2] is False


def test_mapped_projection_truncates_fractional_to_integer():
    # Access Currency/Double -> INTEGER column must truncate (match ORM int()),
    # not round; an integer source casts plainly (no DOUBLE precision loss).
    spec = {"columns": [
        {"access_col": "Amt", "pg_col": "amt", "pg_type": "integer", "is_pk": False},
        {"access_col": "Sid", "pg_col": "sid", "pg_type": "integer", "is_pk": True},
    ]}
    proj = {pgc: cast for pgc, cast, _ in mapped_projection(spec, {"Amt": "DECIMAL(19,4)", "Sid": "BIGINT"})}
    assert proj["amt"] == 'TRY_CAST(trunc("Amt") AS INTEGER)'
    assert proj["sid"] == 'TRY_CAST("Sid" AS INTEGER)'


def test_mapped_projection_no_utc_when_source_not_timestamp():
    # A varchar-in-mirror date column must NOT get AT TIME ZONE (would error).
    spec = {"columns": [{"access_col": "D", "pg_col": "d",
                         "pg_type": "timestamp with time zone", "is_pk": False}]}
    proj = mapped_projection(spec, {"D": "VARCHAR"})
    assert proj[0][1] == 'TRY_CAST("D" AS TIMESTAMPTZ)'


def test_load_order_parents_before_children():
    specs = {
        "grandchild": {"fk_targets": ["child"]},
        "child": {"fk_targets": ["parent"]},
        "parent": {"fk_targets": []},
        "lonely": {"fk_targets": []},
    }
    order = load_order(specs)
    assert set(order) == set(specs)
    assert order.index("parent") < order.index("child") < order.index("grandchild")


def test_load_order_tolerates_cycle():
    specs = {"a": {"fk_targets": ["b"]}, "b": {"fk_targets": ["a"]}}
    order = load_order(specs)  # must not hang / recurse forever
    assert set(order) == {"a", "b"}


def test_syncable_skips_views():
    mapping = {
        "tables": {
            "real_tbl": {"access_table": "Real", "pk_access_col": "ID", "columns": []},
            "a_view": {"access_table": None, "pk_access_col": None, "columns": []},
        }
    }
    got = {t for t, _ in syncable(mapping)}
    assert got == {"real_tbl"}


def test_s3_credentials_passed_in_need_no_environment(monkeypatch):
    import duckdb

    from mdb_ducklake.sync import S3_KEY_ENV, S3_SECRET_ENV, _configure_s3

    for name in S3_KEY_ENV + S3_SECRET_ENV:
        monkeypatch.delenv(name, raising=False)
    con = duckdb.connect()
    _configure_s3(con, "sgp1.digitaloceanspaces.com", "sgp1", "vhost", "KEY123", "SECRET456")
    (secret_string,) = con.execute(
        "SELECT secret_string FROM duckdb_secrets() WHERE name = 'mdb_ducklake_s3'"
    ).fetchone()
    assert "key_id=KEY123" in secret_string


def test_unreachable_postgres_error_hides_password(tmp_path):
    import pytest

    from mdb_ducklake import LakeConfig, validate

    lake = LakeConfig(str(tmp_path / "c.ducklake"), str(tmp_path / "data"))
    dsn = "host=localhost port=1 dbname=x user=u password=REALPW77 connect_timeout=2"
    with pytest.raises(Exception) as caught:
        validate({"tables": {}}, dsn=dsn, lake=lake)
    exc = caught.value
    while exc is not None:  # nothing in the chain may still carry it
        assert "REALPW77" not in str(exc)
        exc = exc.__cause__ or exc.__context__


def test_without_dsn_redacts_key_value_and_uri_passwords():
    from mdb_ducklake.to_postgres import without_dsn

    msg = without_dsn(
        r"at 'host=h password='it\'s secret' user=u' and postgresql://u:hunter2@h/db", "unused"
    )
    assert "secret" not in msg and "hunter2" not in msg
    assert "postgresql://u:***@h/db" in msg


def test_open_lake_hands_passed_credentials_to_s3_setup(tmp_path, monkeypatch):
    import pytest

    from mdb_ducklake import sync

    seen = {}

    def fake_configure(con, endpoint, region, url_style, key_id=None, secret=None):
        seen.update(key_id=key_id, secret=secret)
        raise RuntimeError("stop here")

    monkeypatch.setattr(sync, "_configure_s3", fake_configure)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENVKEY")
    with pytest.raises(RuntimeError, match="stop here"):
        sync.open_lake(tmp_path / "c.ducklake", "s3://bucket/data", s3_key_id="KEY1", s3_secret="SEC1")
    assert seen == {"key_id": "KEY1", "secret": "SEC1"}


def test_lake_config_credentials_reach_open_lake(monkeypatch):
    import pytest

    from mdb_ducklake import LakeConfig, to_postgres

    seen = {}

    def fake_open_lake(catalog, data_path, **kwargs):
        seen.update(kwargs)
        raise RuntimeError("stop here")

    monkeypatch.setattr(to_postgres, "open_lake", fake_open_lake)
    lake = LakeConfig("s3://b/c.ducklake", "s3://b/data", s3_key_id="KEY2", s3_secret="SEC2")
    with pytest.raises(RuntimeError, match="stop here"):
        to_postgres.upsert({"tables": {}}, dsn="x", lake=lake)
    assert (seen["s3_key_id"], seen["s3_secret"]) == ("KEY2", "SEC2")
