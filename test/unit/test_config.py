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


def test_poll_interval_defaults(config_path, monkeypatch):
    for var in (
        "CDC_PRODUCER_FLUSH_INTERVAL",
        "CDC_TRANSFORMER_POLL_INTERVAL",
        "CDC_CONSUMER_POLL_INTERVAL",
    ):
        monkeypatch.delenv(var, raising=False)
    cfg = load_config(config_path)
    cdc = cfg["migration"]["cdc"]
    assert cdc.producer_flush_interval == 5.0
    assert cdc.transformer_poll_interval == 0.5
    assert cdc.consumer_poll_interval == 0.5
    # legacy knob is gone entirely
    assert "batch_delay_seconds" not in cdc
    assert cdc.get("use_gtid") is False
    assert cdc.get("raw_events_max") == 50000


def test_legacy_webhook_url_maps_to_teams(config_path, monkeypatch):
    monkeypatch.delenv("NOTIFICATIONS_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("NOTIFICATIONS_TEAMS_WEBHOOK_URL", raising=False)
    with open(config_path, "a", encoding="utf-8") as f:
        f.write(
            "\nnotifications:\n  enabled: true\n"
            "  webhook_url: https://example.webhook.office.com/abc\n"
        )
    cfg = load_config(config_path)
    assert cfg.notifications.webhook_url.endswith("/abc")
    assert cfg.notifications.teams.webhook_url.endswith("/abc")


def test_notification_env_overrides(config_path, monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_ENABLED", "true")
    monkeypatch.setenv("NOTIFICATIONS_SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/X")
    monkeypatch.setenv("NOTIFICATIONS_TELEGRAM_BOT_TOKEN", "123:secret")
    monkeypatch.setenv("NOTIFICATIONS_TELEGRAM_CHAT_ID", "-1001")
    cfg = load_config(config_path)
    assert cfg.notifications.enabled is True
    assert cfg.notifications.slack.webhook_url.endswith("/X")
    assert cfg.notifications.telegram.bot_token == "123:secret"
    assert cfg.notifications.telegram.chat_id == "-1001"
