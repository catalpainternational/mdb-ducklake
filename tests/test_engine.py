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
