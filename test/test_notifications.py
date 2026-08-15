#!/usr/bin/env python3
"""Opt-in live notification test. Skips unless at least one provider is configured."""

import os
import time

import pytest
import yaml

from migres.notifications import (
    initialize_notifications,
    notify_cdc_error,
    notify_cdc_info,
    notify_cdc_warning,
)


def _has_placeholder(value: str) -> bool:
    if not value:
        return True
    lowered = value.lower()
    return any(p in lowered for p in ("your-webhook", "example.com", "xxx", "abc...", "123456:abc"))


@pytest.fixture(scope="module")
def notification_config():
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yml")
    if not os.path.exists(config_path):
        pytest.skip("config.yml not found")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    ncfg = config.get("notifications") or {}
    if not ncfg.get("enabled", False):
        pytest.skip("Notifications are disabled in config.yml")

    teams = (ncfg.get("teams") or {})
    slack = (ncfg.get("slack") or {})
    telegram = (ncfg.get("telegram") or {})
    teams_url = teams.get("webhook_url") or ncfg.get("webhook_url") or ""
    slack_url = slack.get("webhook_url") or ""
    tg_token = telegram.get("bot_token") or ""
    tg_chat = str(telegram.get("chat_id") or "")

    active = []
    if teams.get("enabled", True) and teams_url and not _has_placeholder(teams_url):
        active.append("teams")
    if slack.get("enabled", True) and slack_url and not _has_placeholder(slack_url):
        active.append("slack")
    if telegram.get("enabled", True) and tg_token and tg_chat and not _has_placeholder(tg_token):
        active.append("telegram")

    if not active:
        pytest.skip("No live notification providers configured")

    ncfg["_active_providers"] = active
    return ncfg


@pytest.mark.notifications
def test_notification_initialization(notification_config):
    success = initialize_notifications(notification_config)
    assert success, "Failed to initialize notifications"
    print("Active providers:", notification_config["_active_providers"])


@pytest.mark.notifications
def test_cdc_error_notification(notification_config):
    initialize_notifications(notification_config)
    success = notify_cdc_error(
        error_type="Test Error",
        table="test_table",
        error_message="This is a test error notification",
        operation_details={"Test": True, "Error Code": "TEST_001"},
    )
    assert success, "Failed to send error notification"
    time.sleep(2)


@pytest.mark.notifications
def test_cdc_warning_notification(notification_config):
    initialize_notifications(notification_config)
    success = notify_cdc_warning(
        warning_type="Test Warning",
        table="test_table",
        warning_message="This is a test warning notification",
        details={"Test": True, "Warning Code": "TEST_002"},
    )
    assert success, "Failed to send warning notification"
    time.sleep(2)


@pytest.mark.notifications
def test_cdc_info_notification(notification_config):
    initialize_notifications(notification_config)
    success = notify_cdc_info(
        info_type="Test Info",
        message="This is a test info notification",
        details={"Test": True, "Info Code": "TEST_003"},
    )
    assert success, "Failed to send info notification"
    time.sleep(2)


@pytest.mark.notifications
def test_rate_limiting(notification_config):
    cfg = dict(notification_config)
    cfg["rate_limit_seconds"] = 60
    initialize_notifications(cfg)
    sent_count = 0
    for i in range(3):
        success = notify_cdc_info(
            info_type=f"Rate Limit Test {i+1}",
            message=f"This is rate limit test notification {i+1}",
            details={"Test": True, "Iteration": i + 1},
        )
        if success:
            sent_count += 1
        time.sleep(1)
    assert sent_count >= 1, "No notifications were sent (rate limiting may be too strict)"
    assert sent_count < 3, "Rate limiting did not suppress follow-up notifications"
