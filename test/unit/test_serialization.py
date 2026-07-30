"""Unit tests for value serialization / ClickHouse conversion."""

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from pipeline_consumer import _convert_for_clickhouse
from pipeline_producer import PipelineProducer


pytestmark = pytest.mark.unit


@pytest.fixture
def producer_serialize():
    p = object.__new__(PipelineProducer)
    return p._serialize_value


def test_serialize_decimal(producer_serialize):
    assert producer_serialize(Decimal("1.5")) == "1.5"
    assert producer_serialize(Decimal("0")) == "0"


def test_serialize_set(producer_serialize):
    assert producer_serialize({"b", "a"}) == "a,b"


def test_serialize_timedelta(producer_serialize):
    assert producer_serialize(timedelta(hours=1, minutes=2, seconds=3)) == "01:02:03"
    assert producer_serialize(timedelta(seconds=-90)) == "-00:01:30"


def test_serialize_bytes(producer_serialize):
    assert producer_serialize(b"hello") == "hello"
    # non-utf8 falls back to base64
    assert isinstance(producer_serialize(b"\xff\xfe"), str)


def test_serialize_datetime(producer_serialize):
    assert producer_serialize(datetime(2024, 6, 1, 12, 30, 0)) == "2024-06-01T12:30:00"
    assert producer_serialize(date(2024, 6, 1)) == "2024-06-01"


def test_convert_for_clickhouse_decimal():
    assert _convert_for_clickhouse("1.5", "Decimal(10,2)") == Decimal("1.5")
    assert _convert_for_clickhouse(Decimal("2.0"), "Decimal(10,2)") == Decimal("2.0")
    assert _convert_for_clickhouse("3.25", "Nullable(Decimal(10,2))") == Decimal("3.25")


def test_convert_for_clickhouse_date():
    assert _convert_for_clickhouse("2024-01-15", "Date") == date(2024, 1, 15)
    assert _convert_for_clickhouse("2024-01-15", "Nullable(Date)") == date(2024, 1, 15)
    assert _convert_for_clickhouse(date(2024, 1, 15), "Date") == date(2024, 1, 15)


def test_convert_for_clickhouse_datetime():
    dt = _convert_for_clickhouse("2024-01-15T12:30:00", "DateTime64(3)")
    assert isinstance(dt, datetime)
    assert dt.year == 2024 and dt.month == 1 and dt.day == 15

    dt2 = _convert_for_clickhouse("2024-01-15 12:30:00", "Nullable(DateTime64(3))")
    assert isinstance(dt2, datetime)
    assert dt2.hour == 12
