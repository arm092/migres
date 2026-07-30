"""Unit tests for ClickHouse DEFAULT expression mapping."""

import pytest

from migres.schema.ddl import _default_expr_for_column


pytestmark = pytest.mark.unit


def _col(data_type, default, name="c"):
    return {
        "COLUMN_NAME": name,
        "DATA_TYPE": data_type,
        "COLUMN_DEFAULT": default,
    }


def test_numeric_valid_defaults():
    assert _default_expr_for_column(_col("int", "42"), "Int32") == "42"
    assert _default_expr_for_column(_col("bigint", "-7"), "Int64") == "-7"
    assert _default_expr_for_column(_col("float", "1.5"), "Float32") == "1.5"
    assert _default_expr_for_column(_col("decimal", "0.00"), "Decimal(10,2)") == "0.00"


def test_numeric_invalid_defaults_fall_to_quoted_string():
    assert _default_expr_for_column(_col("int", "abc"), "Int32") == "'abc'"
    assert _default_expr_for_column(_col("int", "1e3"), "Int32") == "'1e3'"


def test_string_backslash_and_quote_escaping_sec2():
    # SEC-2 regression: backslash must be escaped before single quotes
    assert _default_expr_for_column(_col("varchar", "O'Reilly\\"), "String") == "'O\\'Reilly\\\\'"
    assert _default_expr_for_column(_col("varchar", "foo\\bar"), "String") == "'foo\\\\bar'"


def test_datetime_defaults_with_datetime64():
    expr = _default_expr_for_column(_col("datetime", "2024-01-15 12:00:00"), "DateTime64(3)")
    assert expr == "parseDateTimeBestEffort('2024-01-15 12:00:00')"

    expr_tz = _default_expr_for_column(
        _col("timestamp", "2024-01-15 12:00:00"),
        "DateTime64(3, 'UTC')",
    )
    assert expr_tz == "parseDateTimeBestEffort('2024-01-15 12:00:00')"


def test_none_default_returns_none():
    assert _default_expr_for_column(_col("int", None), "Int32") is None
