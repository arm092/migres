"""Unit tests for load_config defaults and env overrides."""

import os
import tempfile
import textwrap

import pytest

from config import load_config


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
