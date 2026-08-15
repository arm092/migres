"""Multi-channel CDC notifications (Teams, Slack, Telegram).

Public API is unchanged: initialize_notifications + notify_cdc_*.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Optional

from .base import NotificationLevel, RateLimiter, extract_exception_info, utc_now_str
from .slack import SlackProvider, build_slack_payload
from .teams import TeamsProvider, build_teams_payload
from .telegram import TelegramProvider, build_telegram_payload, html_escape

log = logging.getLogger(__name__)

__all__ = [
    "NotificationLevel",
    "NotificationDispatcher",
    "initialize_notifications",
    "get_notification_handler",
    "create_notification_handler",
    "notify_cdc_error",
    "notify_cdc_warning",
    "notify_cdc_info",
    "notify_cdc_startup",
    "notify_cdc_shutdown",
    "build_teams_payload",
    "build_slack_payload",
    "build_telegram_payload",
    "html_escape",
]


def _cfg_map(value: Any) -> Mapping[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return value
    return {}


def _truthy(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    return bool(value)


class NotificationDispatcher:
    """Rate-limits once, then fans out to every enabled provider."""

    def __init__(
        self,
        providers: List[Any],
        rate_limit_seconds: int = 60,
        environment: str = "prod",
    ):
        self.providers = providers
        self.environment = (environment or "prod").upper()
        self._limiter = RateLimiter(rate_limit_seconds)

    def _format_title(self, title: str) -> str:
        return f"{title} [{self.environment}]"

    def send_notification(
        self,
        title: str,
        message: str,
        level: NotificationLevel = NotificationLevel.INFO,
        details: Optional[Dict[str, Any]] = None,
        notification_type: str = "general",
    ) -> bool:
        if not self.providers:
            return False
        if not self._limiter.allow(notification_type):
            log.debug("Notification rate limited for type: %s", notification_type)
            return False

        any_ok = False
        for provider in self.providers:
            try:
                if provider.send(title, message, level, details):
                    any_ok = True
            except Exception as e:
                log.error("%s provider failed: %s", getattr(provider, "name", provider), e)
        return any_ok

    def send_cdc_error(
        self,
        error_type: str,
        table: str,
        error_message: str,
        operation_details: Optional[Dict] = None,
        exc: Optional[Exception] = None,
    ) -> bool:
        title = self._format_title(f"🚨 CDC Error: {error_type}")
        message = f"**Table:** {table}\n**Error:** {error_message}"
        details: Dict[str, Any] = {
            "Error Type": error_type,
            "Table": table,
            "Error Message": error_message,
        }
        location = extract_exception_info(exc) if exc is not None else extract_exception_info()
        if location:
            details.update(location)
        if operation_details:
            details.update(operation_details)
        return self.send_notification(
            title, message, NotificationLevel.ERROR, details, "cdc_error"
        )

    def send_cdc_warning(
        self,
        warning_type: str,
        table: str,
        warning_message: str,
        details: Optional[Dict] = None,
    ) -> bool:
        title = self._format_title(f"⚠️ CDC Warning: {warning_type}")
        message = f"**Table:** {table}\n**Warning:** {warning_message}"
        payload = {
            "Warning Type": warning_type,
            "Table": table,
            "Warning Message": warning_message,
        }
        if details:
            payload.update(details)
        return self.send_notification(
            title, message, NotificationLevel.WARNING, payload, "cdc_warning"
        )

    def send_cdc_info(
        self,
        info_type: str,
        message: str,
        details: Optional[Dict] = None,
    ) -> bool:
        title = self._format_title(f"ℹ️ CDC Info: {info_type}")
        payload = {"Info Type": info_type, "Message": message}
        if details:
            payload.update(details)
        return self.send_notification(
            title, message, NotificationLevel.INFO, payload, "cdc_info"
        )

    def send_cdc_startup(self, config_summary: Dict, mode: str = "cdc") -> bool:
        if (mode or "").lower() == "snapshot":
            title = self._format_title("📸 Snapshot Started")
            message = "Snapshot migration process has started successfully"
        else:
            title = self._format_title("🚀 CDC Started")
            message = "CDC (Change Data Capture) process has started successfully"
        details = {
            "Startup Time": utc_now_str(),
            "Configuration": config_summary,
        }
        return self.send_notification(
            title, message, NotificationLevel.INFO, details, "cdc_startup"
        )

    def send_cdc_shutdown(self, reason: str = "Normal shutdown") -> bool:
        title = self._format_title("🛑 CDC Process Stopped")
        message = f"CDC process has stopped. Reason: {reason}"
        details = {
            "Shutdown Time": utc_now_str(),
            "Reason": reason,
        }
        return self.send_notification(
            title, message, NotificationLevel.WARNING, details, "cdc_shutdown"
        )


def _build_providers(config: Mapping[str, Any], environment: str) -> List[Any]:
    providers: List[Any] = []
    teams_cfg = _cfg_map(config.get("teams"))
    slack_cfg = _cfg_map(config.get("slack"))
    tg_cfg = _cfg_map(config.get("telegram"))

    teams_url = (teams_cfg.get("webhook_url") or config.get("webhook_url") or "").strip()
    if _truthy(teams_cfg.get("enabled"), True) and teams_url:
        providers.append(TeamsProvider(teams_url, environment))

    slack_url = (slack_cfg.get("webhook_url") or "").strip()
    if _truthy(slack_cfg.get("enabled"), True) and slack_url:
        providers.append(SlackProvider(slack_url, environment))

    bot_token = (tg_cfg.get("bot_token") or "").strip()
    chat_id = str(tg_cfg.get("chat_id") or "").strip()
    if _truthy(tg_cfg.get("enabled"), True) and bot_token and chat_id:
        providers.append(TelegramProvider(bot_token, chat_id, environment))

    return providers


def create_notification_handler(
    config: Mapping[str, Any],
    environment: str = "prod",
) -> Optional[NotificationDispatcher]:
    if not _truthy(config.get("enabled"), False):
        log.info("Notifications are disabled in configuration")
        return None

    providers = _build_providers(config, environment)
    if not providers:
        log.info("Notifications enabled but no providers configured")
        return None

    names = ", ".join(p.name for p in providers)
    log.info("Notifications initialized: %s", names)
    rate_limit = int(config.get("rate_limit_seconds") or 60)
    return NotificationDispatcher(providers, rate_limit, environment)


_notification_handler: Optional[NotificationDispatcher] = None


def initialize_notifications(config: Mapping[str, Any], environment: str = "prod") -> bool:
    global _notification_handler
    try:
        _notification_handler = create_notification_handler(config, environment)
        return _notification_handler is not None
    except Exception as e:
        log.error("Failed to initialize notifications: %s", e)
        _notification_handler = None
        return False


def get_notification_handler() -> Optional[NotificationDispatcher]:
    return _notification_handler


def notify_cdc_error(
    error_type: str,
    table: str,
    error_message: str,
    operation_details: Optional[Dict] = None,
    exc: Optional[Exception] = None,
) -> bool:
    handler = get_notification_handler()
    if handler:
        return handler.send_cdc_error(error_type, table, error_message, operation_details, exc)
    return False


def notify_cdc_warning(
    warning_type: str,
    table: str,
    warning_message: str,
    details: Optional[Dict] = None,
) -> bool:
    handler = get_notification_handler()
    if handler:
        return handler.send_cdc_warning(warning_type, table, warning_message, details)
    return False


def notify_cdc_info(info_type: str, message: str, details: Optional[Dict] = None) -> bool:
    handler = get_notification_handler()
    if handler:
        return handler.send_cdc_info(info_type, message, details)
    return False


def notify_cdc_startup(config_summary: Dict, mode: str = "cdc") -> bool:
    handler = get_notification_handler()
    if handler:
        return handler.send_cdc_startup(config_summary, mode)
    return False


def notify_cdc_shutdown(reason: str = "Normal shutdown") -> bool:
    handler = get_notification_handler()
    if handler:
        return handler.send_cdc_shutdown(reason)
    return False
