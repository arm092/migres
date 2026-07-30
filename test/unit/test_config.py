"""Unit tests for load_config defaults and env overrides."""

import os
import tempfile
import textwrap

import pytest

from migres.config import load_config


pytestmark = pytest.mark.unit


MINIMAL_YAML = textwrap.dedent(
    """\
    mysql:
      host: yaml-host
      port: 3306
      user: root
      password: secret
      database: testdb
    clickhouse:
      host: ch-host
      port: 9000
      user: default
      password: ""
      database: testdb
    migration:
      mode: cdc
    """
)


@pytest.fixture
def config_path():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "config.yml")
        with open(path, "w", encoding="utf-8") as f:
            f.write(MINIMAL_YAML)
        yield path


def test_load_config_env_overrides(config_path, monkeypatch):
    monkeypatch.setenv("BUFFER_FILE", "/tmp/unit-buffer.db")
    monkeypatch.setenv("MYSQL_HOST", "env-mysql-host")
    monkeypatch.setenv("MIGRATION_DEBUG", "true")

    cfg = load_config(config_path)

    assert cfg["buffer_file"] == "/tmp/unit-buffer.db"
    assert cfg["mysql"]["host"] == "env-mysql-host"
    assert cfg["migration"]["debug"] is True


def test_prepared_queries_merge_rows_limit_not_in_defaults(config_path, monkeypatch):
    # Ensure env does not inject the removed key
    monkeypatch.delenv("CDC_PREPARED_QUERIES_MERGE_ROWS_LIMIT", raising=False)

    cfg = load_config(config_path)
    cdc = cfg["migration"]["cdc"]

    assert "prepared_queries_merge_rows_limit" not in cdc
    # Sanity: other CDC defaults are still applied
    assert "prepared_queries_batch_limit" in cdc


def test_poll_interval_defaults_from_batch_delay(config_path, monkeypatch):
    monkeypatch.delenv("CDC_PRODUCER_FLUSH_INTERVAL", raising=False)
    cfg = load_config(config_path)
    cdc = cfg["migration"]["cdc"]
    # batch_delay default 0 → resolved intervals use fallbacks
    assert cdc.resolved_producer_flush_interval() == 0.0
    assert cdc.resolved_transformer_poll_interval() == 0.5
    assert cdc.resolved_consumer_poll_interval() == 0.1
    assert cdc.get("use_gtid") is False
    assert cdc.get("raw_events_max") == 50000
