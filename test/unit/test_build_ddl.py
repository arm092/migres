"""Unit tests for build_table_ddl."""

import pytest

from migres.schema.ddl import build_table_ddl


pytestmark = pytest.mark.unit


def _col(name, data_type="int", column_type=None, nullable="NO", default=None):
    d = {
        "COLUMN_NAME": name,
        "DATA_TYPE": data_type,
        "COLUMN_TYPE": column_type or data_type,
        "IS_NULLABLE": nullable,
        "COLUMN_DEFAULT": default,
    }
    return d


MIG = {"ddl_engine": "ReplacingMergeTree"}


def test_order_by_id_when_id_column_present():
    cols = [_col("id"), _col("name", "varchar", "varchar(50)")]
    ddl, insertable = build_table_ddl("users", cols, ["id"], MIG)

    assert "ORDER BY (`id`)" in ddl
    assert "ENGINE = ReplacingMergeTree(__data_transfer_commit_time)" in ddl
    assert "`id`" in ddl
    assert "id" in insertable


def test_order_by_pk_without_id():
    cols = [_col("pk"), _col("val", "varchar", "varchar(20)")]
    ddl, _ = build_table_ddl("items", cols, ["pk"], MIG)

    assert "ORDER BY (`pk`)" in ddl
    assert "__migres_pk" not in ddl


def test_no_pk_synthesizes_migres_pk():
    cols = [_col("a"), _col("b", "varchar", "varchar(10)")]
    ddl, insertable = build_table_ddl("plain", cols, [], MIG)

    assert "`__migres_pk` UInt64 MATERIALIZED cityHash64(" in ddl
    assert "ORDER BY (`__migres_pk`)" in ddl
    assert "__migres_pk" not in insertable  # materialized, not insertable


def test_metadata_columns_present():
    cols = [_col("id")]
    ddl, insertable = build_table_ddl("t", cols, ["id"], MIG)

    assert "`__data_transfer_commit_time` UInt64" in ddl
    assert "`__data_transfer_delete_time` UInt64 DEFAULT 0" in ddl
    assert "`__data_transfer_is_deleted` UInt8 MATERIALIZED" in ddl
    assert "__data_transfer_commit_time" in insertable
    assert "__data_transfer_delete_time" in insertable


def test_replacing_mergetree_engine():
    cols = [_col("id")]
    ddl, _ = build_table_ddl("t", cols, ["id"], MIG)

    assert "CREATE TABLE IF NOT EXISTS `t`" in ddl
    assert "ENGINE = ReplacingMergeTree(__data_transfer_commit_time)" in ddl
    assert "ENGINE = MergeTree()" not in ddl
