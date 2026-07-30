"""Unit tests for DDL parsing helpers."""

import pytest

from schema_and_ddl import (
    strip_sql_leading_comments,
    parse_ddl_table_name,
    parse_drop_table_names,
    parse_rename_table_pairs,
)


pytestmark = pytest.mark.unit


def test_strip_sql_leading_comments_line_and_block():
    assert strip_sql_leading_comments("-- note\nCREATE TABLE t (id INT)") == "CREATE TABLE t (id INT)"
    assert strip_sql_leading_comments("# note\nDROP TABLE t") == "DROP TABLE t"
    assert strip_sql_leading_comments("/* block */\nALTER TABLE t ADD c INT") == "ALTER TABLE t ADD c INT"
    assert strip_sql_leading_comments("  -- a\n  /* b */\n  TRUNCATE TABLE t") == "TRUNCATE TABLE t"


def test_strip_sql_leading_comments_empty():
    assert strip_sql_leading_comments("") == ""
    assert strip_sql_leading_comments(None) == ""
    assert strip_sql_leading_comments("-- only comment") == ""


def test_parse_ddl_table_name_create_alter_truncate():
    assert parse_ddl_table_name("CREATE TABLE users (id INT)", "create") == "users"
    assert parse_ddl_table_name("CREATE TABLE IF NOT EXISTS `db`.`users` (id INT)", "create") == "users"
    assert parse_ddl_table_name("ALTER TABLE orders ADD COLUMN x INT", "alter") == "orders"
    assert parse_ddl_table_name("TRUNCATE TABLE items", "truncate") == "items"
    assert parse_ddl_table_name("TRUNCATE items", "truncate") == "items"


def test_parse_ddl_table_name_with_leading_comments():
    q = "/* cdc */\n-- skip\nCREATE TABLE IF NOT EXISTS mydb.events (id INT)"
    assert parse_ddl_table_name(q, "create") == "events"


def test_parse_drop_table_names_multi_table():
    assert parse_drop_table_names("DROP TABLE a, b, c") == ["a", "b", "c"]
    assert parse_drop_table_names("DROP TABLE IF EXISTS `db`.`t1`, t2") == ["t1", "t2"]


def test_parse_drop_table_names_with_comments():
    assert parse_drop_table_names("-- drop\nDROP TABLE IF EXISTS old_table") == ["old_table"]


def test_parse_rename_table_pairs():
    assert parse_rename_table_pairs("RENAME TABLE old TO new") == [("old", "new")]
    assert parse_rename_table_pairs(
        "RENAME TABLE a TO b, `db`.`c` TO `db`.`d`"
    ) == [("a", "b"), ("c", "d")]


def test_parse_rename_with_leading_comments():
    q = "/* x */\nRENAME TABLE t1 TO t2"
    assert parse_rename_table_pairs(q) == [("t1", "t2")]


def test_if_exists_if_not_exists():
    assert parse_ddl_table_name("CREATE TABLE IF NOT EXISTS foo (id INT)", "create") == "foo"
    assert parse_drop_table_names("DROP TABLE IF EXISTS foo") == ["foo"]
