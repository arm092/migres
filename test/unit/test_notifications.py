"""Unit tests for multi-channel notifications (mocked HTTP)."""

from unittest.mock import MagicMock, patch

import pytest

from migres.notifications import (
    NotificationLevel,
    build_slack_payload,
    build_teams_payload,
    build_telegram_payload,
    create_notification_handler,
    html_escape,
    initialize_notifications,
    notify_cdc_error,
    notify_cdc_info,
)
from migres.notifications.telegram import telegram_api_url


pytestmark = pytest.mark.unit

TEAMS_URL = "https://example.webhook.office.com/webhookb2/abc"
SLACK_URL = "https://hooks.slack.com/services/T00/B00/XXX"
TG_TOKEN = "123456:ABC-TOKEN"
TG_CHAT = "-100123"


def _resp(status=200, text="ok", json_body=None):
    m = MagicMock()
    m.status_code = status
    m.text = text
    m.json.return_value = json_body if json_body is not None else {}
    return m


def _ok_for_url(url, payload, timeout=10):
    if "api.telegram.org" in url:
        return _resp(200, '{"ok": true}', {"ok": True})
    if "hooks.slack.com" in url:
        return _resp(200, "ok")
    return _resp(200, "1")


ALL_CHANNELS = {
    "enabled": True,
    "rate_limit_seconds": 0,
    "teams": {"enabled": True, "webhook_url": TEAMS_URL},
    "slack": {"enabled": True, "webhook_url": SLACK_URL},
    "telegram": {"enabled": True, "bot_token": TG_TOKEN, "chat_id": TG_CHAT},
}


def test_teams_payload_shape():
    payload = build_teams_payload(
        "Title [PROD]",
        "hello",
        NotificationLevel.ERROR,
        {"Table": "users"},
        environment="prod",
    )
    assert payload["type"] == "message"
    card = payload["attachments"][0]["content"]
    assert card["type"] == "AdaptiveCard"
    texts = [b.get("text") for b in card["body"] if "text" in b]
    assert "Title [PROD]" in texts
    assert any("**Table:**" in t for t in texts)
    assert card["body"][0]["color"] == "Attention"


def test_slack_payload_shape():
    payload = build_slack_payload(
        "Title [DEV]",
        "*Table:* users",
        NotificationLevel.INFO,
        {"Info Type": "test"},
        environment="dev",
    )
    att = payload["attachments"][0]
    assert payload["text"] == "Title [DEV]"
    assert att["fallback"] == "Title [DEV]"
    assert att["color"] == "#2EB67D"
    types = [b["type"] for b in att["blocks"]]
    assert "header" in types
    assert "section" in types


def test_telegram_html_escape():
    payload = build_telegram_payload(
        "Error <script>",
        "x < y & z",
        NotificationLevel.ERROR,
        {"Table": "users<script>"},
        chat_id=TG_CHAT,
    )
    assert payload["parse_mode"] == "HTML"
    assert payload["chat_id"] == TG_CHAT
    assert payload["link_preview_options"]["is_disabled"] is True
    assert "<script>" not in payload["text"]
    assert "&lt;script&gt;" in payload["text"]
    assert "&amp;" in payload["text"]
    assert html_escape("<x>") == "&lt;x&gt;"


def test_legacy_webhook_url_starts_teams_only():
    handler = create_notification_handler(
        {"enabled": True, "rate_limit_seconds": 0, "webhook_url": TEAMS_URL},
        environment="prod",
    )
    assert handler is not None
    assert [p.name for p in handler.providers] == ["teams"]


def test_slack_only_initialize():
    handler = create_notification_handler(
        {
            "enabled": True,
            "rate_limit_seconds": 0,
            "slack": {"webhook_url": SLACK_URL},
        }
    )
    assert handler is not None
    assert [p.name for p in handler.providers] == ["slack"]


def test_telegram_only_initialize():
    handler = create_notification_handler(
        {
            "enabled": True,
            "rate_limit_seconds": 0,
            "telegram": {"bot_token": TG_TOKEN, "chat_id": TG_CHAT},
        }
    )
    assert handler is not None
    assert [p.name for p in handler.providers] == ["telegram"]
    assert telegram_api_url(TG_TOKEN).endswith("/sendMessage")


def test_disabled_or_empty_returns_none():
    assert create_notification_handler({"enabled": False, "webhook_url": TEAMS_URL}) is None
    assert create_notification_handler({"enabled": True}) is None


@patch("migres.notifications.base.http_post", side_effect=_ok_for_url)
def test_fan_out_all_three_providers(mock_post):
    handler = create_notification_handler(ALL_CHANNELS, environment="dev")
    ok = handler.send_cdc_info("Ping", "hello")
    assert ok is True
    assert mock_post.call_count == 3
    urls = [c.args[0] for c in mock_post.call_args_list]
    assert TEAMS_URL in urls
    assert SLACK_URL in urls
    assert any(u == f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage" for u in urls)


@patch("migres.notifications.base.http_post")
def test_one_provider_failure_does_not_block_others(mock_post):
    def side_effect(url, payload, timeout=10):
        if "hooks.slack.com" in url:
            raise ConnectionError("slack down")
        return _ok_for_url(url, payload, timeout)

    mock_post.side_effect = side_effect
    handler = create_notification_handler(ALL_CHANNELS)
    assert handler.send_cdc_info("Ping", "hello") is True
    assert mock_post.call_count == 3


@patch("migres.notifications.base.http_post", side_effect=_ok_for_url)
def test_dispatcher_rate_limit(mock_post):
    cfg = dict(ALL_CHANNELS)
    cfg["rate_limit_seconds"] = 60
    handler = create_notification_handler(cfg)
    assert handler.send_cdc_info("A", "one") is True
    assert mock_post.call_count == 3
    assert handler.send_cdc_info("B", "two") is False
    assert mock_post.call_count == 3


@patch("migres.notifications.base.http_post", return_value=_resp(202, ""))
def test_teams_accepts_power_automate_202(mock_post):
    handler = create_notification_handler(
        {"enabled": True, "rate_limit_seconds": 0, "webhook_url": TEAMS_URL}
    )
    assert handler.send_cdc_info("Ping", "hello") is True
    assert mock_post.call_count == 1


@patch("migres.notifications.base.http_post", side_effect=_ok_for_url)
def test_initialize_and_notify_cdc_error(mock_post):
    assert initialize_notifications(ALL_CHANNELS, environment="prod") is True
    ok = notify_cdc_error(
        "Test Error",
        "users",
        "boom <oops>",
        operation_details={"Code": "X"},
    )
    assert ok is True
    assert mock_post.call_count == 3
    tg_call = next(c for c in mock_post.call_args_list if "api.telegram.org" in c.args[0])
    assert "&lt;oops&gt;" in tg_call.args[1]["text"]
    assert TG_TOKEN in tg_call.args[0]
    assert "ABC-TOKEN" not in str(tg_call.args[1])
