"""Unit tests for binlog_position_key ordering."""

import pytest

from schema_and_ddl import binlog_position_key


pytestmark = pytest.mark.unit


def test_numeric_suffix_ordering_across_files():
    # Lexicographic string compare would put 000999 > 001000; key must reverse that
    assert binlog_position_key("mysql-bin.000999", 100) < binlog_position_key("mysql-bin.001000", 1)


def test_same_file_higher_pos_wins():
    a = binlog_position_key("mysql-bin.000001", 100)
    b = binlog_position_key("mysql-bin.000001", 200)
    assert a < b
    assert binlog_position_key("mysql-bin.000001", 200) == binlog_position_key("mysql-bin.000001", 200)


def test_different_basenames():
    mysql_key = binlog_position_key("mysql-bin.000001", 50)
    binlog_key = binlog_position_key("binlog.000001", 50)
    assert mysql_key != binlog_key
    # Prefix differs; ordering is by (prefix, suffix, pos)
    assert binlog_key[0] == "binlog."
    assert mysql_key[0] == "mysql-bin."
    assert binlog_key < mysql_key


def test_empty_file_name():
    assert binlog_position_key("", 10) == ("", 0, 0)
    assert binlog_position_key(None, 10) == ("", 0, 0)
