"""Telegram Bot API sendMessage (HTML)."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from . import base
from .base import NotificationLevel, format_detail_value, utc_now_str

log = logging.getLogger(__name__)


def html_escape(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def build_telegram_payload(
    title: str,
    message: str,
    level: NotificationLevel,
    details: Optional[Dict[str, Any]] = None,
    chat_id: str = "",
) -> Dict[str, Any]:
    parts = [
        f"<b>{html_escape(title)}</b>",
        "",
        html_escape(message).replace("**", ""),
        "",
        f"<i>Level: {html_escape(level.value.upper())}</i>",
        f"<i>{html_escape(utc_now_str())}</i>",
    ]
    if details:
        parts.append("")
        budget = 3500
        used = sum(len(p) for p in parts)
        for key, value in details.items():
            val = html_escape(format_detail_value(value, max_len=300))
            line = f"<b>{html_escape(str(key))}:</b> <code>{val}</code>"
            if used + len(line) + 1 > budget:
                parts.append("<i>…truncated</i>")
                break
            parts.append(line)
            used += len(line) + 1
    return {
        "chat_id": chat_id,
        "text": "\n".join(parts),
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "link_preview_options": {"is_disabled": True},
    }


def telegram_api_url(bot_token: str) -> str:
    return f"https://api.telegram.org/bot{bot_token}/sendMessage"


class TelegramProvider:
    name = "telegram"

    def __init__(self, bot_token: str, chat_id: str, environment: str = "prod"):
        self.bot_token = bot_token
        self.chat_id = str(chat_id)
        self.environment = environment

    def send(
        self,
        title: str,
        message: str,
        level: NotificationLevel,
        details: Optional[Dict[str, Any]] = None,
    ) -> bool:
        url = telegram_api_url(self.bot_token)
        payload = build_telegram_payload(title, message, level, details, self.chat_id)
        try:
            response = base.http_post(url, payload)
        except Exception as e:
            log.error("Error sending Telegram notification: %s", e)
            return False
        try:
            body = response.json()
        except ValueError:
            body = {}
        if response.status_code == 200 and body.get("ok") is True:
            log.info("Telegram notification sent: %s", title)
            return True
        log.error(
            "Telegram notification failed. Status: %s description: %s",
            response.status_code,
            body.get("description", (response.text or "")[:200]),
        )
        return False
