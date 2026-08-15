"""Shared notification types, rate limiting, and HTTP helper."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

import requests

log = logging.getLogger(__name__)


class NotificationLevel(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


def http_post(url: str, payload: Dict[str, Any], timeout: int = 10):
    return requests.post(url, json=payload, timeout=timeout)


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def format_detail_value(value: Any, max_len: int = 500) -> str:
    if isinstance(value, (dict, list)):
        text = json.dumps(value, indent=2, default=str)
    else:
        text = str(value)
    if len(text) > max_len:
        return text[:max_len] + "... (truncated)"
    return text


def extract_exception_info(exc: Optional[Exception] = None, tb=None) -> Dict[str, str]:
    """First user-code frame (skips site-packages / Cython)."""
    location_info: Dict[str, str] = {}
    try:
        if tb is None:
            _exc_type, _exc_value, exc_tb = sys.exc_info()
            if exc_tb is not None:
                tb = exc_tb
            elif exc is not None:
                tb = exc.__traceback__

        if tb is None:
            return location_info

        user_frames = []
        frame = tb
        while frame is not None:
            filename = frame.tb_frame.f_code.co_filename
            skip = ("site-packages", "dist-packages", "clickhouse_driver", ".pyx")
            if not any(s in filename for s in skip) and filename.endswith(".py"):
                user_frames.append((
                    os.path.basename(filename),
                    frame.tb_lineno,
                    frame.tb_frame.f_code.co_name,
                ))
            frame = frame.tb_next

        if user_frames:
            primary = user_frames[0]
            location_info["Exception Location"] = f"{primary[0]}:{primary[1]} in {primary[2]}()"
            if len(user_frames) > 1:
                extra = [f"{f[0]}:{f[1]} in {f[2]}()" for f in user_frames[1:3]]
                location_info["Call Chain"] = " → ".join(extra)
    except Exception as e:
        log.debug("Failed to extract exception info: %s", e)
    return location_info


class RateLimiter:
    """Per-notification-type cooldown. Shared across all providers."""

    def __init__(self, rate_limit_seconds: int = 60):
        self.rate_limit_seconds = rate_limit_seconds
        self._last: Dict[str, datetime] = {}

    def allow(self, notification_type: str) -> bool:
        if self.rate_limit_seconds <= 0:
            return True
        now = datetime.now(timezone.utc)
        last = self._last.get(notification_type)
        if last is None or (now - last).total_seconds() >= self.rate_limit_seconds:
            self._last[notification_type] = now
            return True
        return False


class BaseProvider:
    name = "provider"

    def send(
        self,
        title: str,
        message: str,
        level: NotificationLevel,
        details: Optional[Dict[str, Any]] = None,
    ) -> bool:
        raise NotImplementedError
