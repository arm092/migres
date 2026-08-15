"""Slack incoming webhook (Block Kit + colored attachment)."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from . import base
from .base import NotificationLevel, format_detail_value, utc_now_str

log = logging.getLogger(__name__)

_ENV_HEX = {
    "dev": "#2EB67D",
    "stage": "#ECB22E",
    "prod": "#E01E5A",
    "production": "#E01E5A",
}


def _env_hex(environment: str) -> str:
    return _ENV_HEX.get(environment.lower(), "#2EB67D")


def build_slack_payload(
    title: str,
    message: str,
    level: NotificationLevel,
    details: Optional[Dict[str, Any]] = None,
    environment: str = "prod",
) -> Dict[str, Any]:
    header = title if len(title) <= 150 else title[:147] + "..."
    blocks: list = [
        {"type": "header", "text": {"type": "plain_text", "text": header, "emoji": True}},
        {"type": "section", "text": {"type": "mrkdwn", "text": message}},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"*Level:* {level.value.upper()}  •  {utc_now_str()}",
                }
            ],
        },
    ]
    if details:
        fields = []
        for key, value in details.items():
            fields.append({
                "type": "mrkdwn",
                "text": f"*{key}:*\n{format_detail_value(value, max_len=200)}",
            })
            if len(fields) >= 10:
                break
        if fields:
            blocks.append({"type": "section", "fields": fields})
    return {
        "text": title,
        "attachments": [
            {
                "color": _env_hex(environment),
                "fallback": title,
                "blocks": blocks,
            }
        ],
    }


class SlackProvider:
    name = "slack"

    def __init__(self, webhook_url: str, environment: str = "prod"):
        self.webhook_url = webhook_url
        self.environment = environment

    def send(
        self,
        title: str,
        message: str,
        level: NotificationLevel,
        details: Optional[Dict[str, Any]] = None,
    ) -> bool:
        payload = build_slack_payload(title, message, level, details, self.environment)
        try:
            response = base.http_post(self.webhook_url, payload)
        except Exception as e:
            log.error("Error sending Slack notification: %s", e)
            return False
        text = (response.text or "").strip()
        if response.status_code == 200 and text.lower() == "ok":
            log.info("Slack notification sent: %s", title)
            return True
        log.error(
            "Slack notification failed. Status: %s body: %s",
            response.status_code,
            text[:200],
        )
        return False
