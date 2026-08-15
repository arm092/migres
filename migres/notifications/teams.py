"""MS Teams incoming webhook (Adaptive Card)."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from . import base
from .base import NotificationLevel, format_detail_value, utc_now_str

log = logging.getLogger(__name__)


def _env_color(environment: str) -> str:
    env = environment.lower()
    if env == "dev":
        return "Good"
    if env == "stage":
        return "Warning"
    if env in ("prod", "production"):
        return "Attention"
    return "Good"


def build_teams_payload(
    title: str,
    message: str,
    level: NotificationLevel,
    details: Optional[Dict[str, Any]] = None,
    environment: str = "prod",
) -> Dict[str, Any]:
    color = _env_color(environment)
    body: list = [
        {
            "type": "TextBlock",
            "text": title,
            "weight": "Bolder",
            "size": "Medium",
            "color": color,
            "wrap": True,
        },
        {"type": "TextBlock", "text": message, "wrap": True, "spacing": "Medium"},
        {
            "type": "TextBlock",
            "text": f"Level: {level.value.upper()}",
            "spacing": "Small",
            "isSubtle": True,
        },
        {
            "type": "TextBlock",
            "text": f"Timestamp: {utc_now_str()}",
            "spacing": "Small",
            "isSubtle": True,
        },
    ]
    if details:
        body.append({
            "type": "TextBlock",
            "text": "---",
            "spacing": "Medium",
            "separator": True,
        })
        for key, value in details.items():
            body.append({
                "type": "TextBlock",
                "text": f"**{key}:** {format_detail_value(value)}",
                "wrap": True,
                "spacing": "Small",
            })
    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "version": "1.2",
                    "body": body,
                },
            }
        ],
    }


class TeamsProvider:
    name = "teams"

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
        payload = build_teams_payload(title, message, level, details, self.environment)
        try:
            response = base.http_post(self.webhook_url, payload)
        except Exception as e:
            log.error("Error sending Teams notification: %s", e)
            return False
        text = (response.text or "").strip()
        if response.status_code == 200 and text == "1":
            log.info("Teams notification sent: %s", title)
            return True
        # Classic Office connector returns body "1". Power Automate workflow
        # webhooks often return 202 or 200 with an empty/JSON body.
        if response.status_code in (200, 202):
            log.info("Teams notification sent: %s", title)
            return True
        log.error("Teams notification failed. Status: %s", response.status_code)
        return False
