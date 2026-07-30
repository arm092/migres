"""Unit tests for MySQL -> ClickHouse type mapping."""

import pytest

from schema_and_ddl import map_mysql_to_ch_type, map_with_low_cardinality


pytestmark = pytest.mark.unit


def _col(data_type, column_type=None, nullable="NO", name="c"):
    return {
        "COLUMN_NAME": name,
        "DATA_TYPE": data_type,
        "COLUMN_TYPE": column_type if column_type is not None else data_type,
        "IS_NULLABLE": nullable,
    }


@pytest.mark.parametrize(
    "data_type,column_type,expected",
    [
        ("tinyint", "tinyint(4)", "Int8"),
        ("tinyint", "tinyint(3) unsigned", "UInt8"),
        ("smallint", "smallint(6)", "Int16"),
        ("smallint", "smallint(5) unsigned", "UInt16"),
        ("int", "int(11)", "Int32"),
        ("int", "int(10) unsigned", "UInt32"),
        ("integer", "integer", "Int32"),
        ("bigint", "bigint(20)", "Int64"),
        ("bigint", "bigint(20) unsigned", "UInt64"),
    ],
)
def test_integer_signed_and_unsigned(data_type, column_type, expected):
    assert map_mysql_to_ch_type(_col(data_type, column_type)) == expected


@pytest.mark.parametrize(
    "data_type,column_type,expected",
    [
        ("float", "float", "Float32"),
        ("double", "double", "Float64"),
        ("decimal", "decimal(10,2)", "Decimal(10,2)"),
        ("decimal", "decimal", "Decimal(38,10)"),
        ("numeric", "numeric(18,4)", "Decimal(18,4)"),
    ],
)
def test_float_double_decimal(data_type, column_type, expected):
    assert map_mysql_to_ch_type(_col(data_type, column_type)) == expected


def test_date():
    assert map_mysql_to_ch_type(_col("date", "date")) == "Date"


def test_datetime_without_timezone():
    assert map_mysql_to_ch_type(_col("datetime", "datetime")) == "DateTime64(3)"
    assert map_mysql_to_ch_type(_col("timestamp", "timestamp")) == "DateTime64(3)"


def test_datetime_with_timezone():
    mig = {"clickhouse_timezone": "UTC"}
    assert map_mysql_to_ch_type(_col("datetime", "datetime"), mig) == "DateTime64(3, 'UTC')"
    assert map_mysql_to_ch_type(_col("timestamp", "timestamp"), mig) == "DateTime64(3, 'UTC')"


@pytest.mark.parametrize(
    "data_type,column_type",
    [
        ("time", "time"),
        ("enum", "enum('a','b')"),
        ("set", "set('x','y')"),
        ("json", "json"),
        ("blob", "blob"),
        ("longblob", "longblob"),
        ("varchar", "varchar(255)"),
        ("char", "char(10)"),
        ("text", "text"),
    ],
)
def test_string_like_types(data_type, column_type):
    assert map_mysql_to_ch_type(_col(data_type, column_type)) == "String"


def test_nullable_wrapping():
    assert map_mysql_to_ch_type(_col("int", "int(11)", nullable="YES")) == "Nullable(Int32)"
    assert map_mysql_to_ch_type(_col("varchar", "varchar(50)", nullable="YES")) == "Nullable(String)"
    assert map_mysql_to_ch_type(_col("date", "date", nullable="YES")) == "Nullable(Date)"


def test_low_cardinality_for_strings_when_enabled():
    mig = {"low_cardinality_strings": True}
    assert map_with_low_cardinality(_col("varchar", "varchar(100)"), mig) == "LowCardinality(String)"
    assert (
        map_with_low_cardinality(_col("varchar", "varchar(100)", nullable="YES"), mig)
        == "Nullable(LowCardinality(String))"
    )


def test_low_cardinality_disabled():
    mig = {"low_cardinality_strings": False}
    assert map_with_low_cardinality(_col("varchar", "varchar(100)"), mig) == "String"
    assert map_with_low_cardinality(_col("int", "int(11)"), mig) == "Int32"
