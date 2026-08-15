"""Typed configuration for migres (dataclass-based, Mapping-compatible)."""

from __future__ import annotations

import logging
import os
from dataclasses import asdict, dataclass, field, fields
from typing import Any, Dict, Iterator, List, Mapping, Optional

import yaml

log = logging.getLogger(__name__)


class _MapMixin(Mapping[str, Any]):
    """Allow dict-style access alongside attribute access."""

    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError as e:
            raise KeyError(key) from e

    def __iter__(self) -> Iterator[str]:
        return (f.name for f in fields(self))

    def __len__(self) -> int:
        return len(fields(self))

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def setdefault(self, key: str, default: Any = None) -> Any:
        if hasattr(self, key):
            val = getattr(self, key)
            if val is None:
                setattr(self, key, default)
                return default
            return val
        setattr(self, key, default)
        return default

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MySQLConfig(_MapMixin):
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = ""
    include_tables: List[str] = field(default_factory=list)
    exclude_tables: List[str] = field(default_factory=list)
    ssl_ca: Optional[str] = None
    ssl_disabled: bool = False


@dataclass
class ClickHouseConfig(_MapMixin):
    host: str = "127.0.0.1"
    port: int = 9000
    user: str = "default"
    password: str = ""
    database: str = ""
    secure: bool = False
    verify: bool = True
    ca_certs: Optional[str] = None


@dataclass
class CdcConfig(_MapMixin):
    heartbeat_seconds: int = 5
    checkpoint_interval_rows: int = 5000
    prepared_queries_batch_limit: int = 100
    batch_max_wait_seconds: int = 60
    producer_batch_size: int = 100
    producer_flush_interval: float = 5.0
    transformer_poll_interval: float = 0.5
    consumer_poll_interval: float = 0.5
    snapshot_before: bool = True
    server_id: int = 4379
    force_binlog_position: Optional[str] = None
    db_debug: bool = False
    use_gtid: bool = False
    raw_events_max: int = 50000
    raw_events_resume_ratio: float = 0.8


@dataclass
class MigrationConfig(_MapMixin):
    mode: str = "snapshot"
    debug: bool = False
    batch_rows: int = 5000
    ddl_engine: str = "ReplacingMergeTree"
    low_cardinality_strings: bool = False
    workers: int = 4
    clickhouse_timezone: Optional[str] = None
    clickhouse_send_receive_timeout: int = 60
    cdc: CdcConfig = field(default_factory=CdcConfig)


@dataclass
class NotificationsConfig(_MapMixin):
    enabled: bool = False
    webhook_url: str = ""
    rate_limit_seconds: int = 60


@dataclass
class MigresConfig(_MapMixin):
    mysql: MySQLConfig = field(default_factory=MySQLConfig)
    clickhouse: ClickHouseConfig = field(default_factory=ClickHouseConfig)
    migration: MigrationConfig = field(default_factory=MigrationConfig)
    notifications: NotificationsConfig = field(default_factory=NotificationsConfig)
    state_file: str = "/app/state.json"
    buffer_file: str = "data/buffer.db"
    environment: str = "prod"


def _merge_dataclass(cls, data: Optional[Dict[str, Any]], nested: Optional[Dict[str, type]] = None):
    data = data or {}
    kwargs: Dict[str, Any] = {}
    nested = nested or {}
    for f in fields(cls):
        if f.name in nested and f.name in data:
            kwargs[f.name] = _merge_dataclass(nested[f.name], data.get(f.name) or {})
        elif f.name in data and data[f.name] is not None:
            kwargs[f.name] = data[f.name]
    return cls(**kwargs)


def _apply_env_overrides(cfg: MigresConfig) -> MigresConfig:
    if "MIGRATION_DEBUG" in os.environ:
        cfg.migration.debug = os.environ["MIGRATION_DEBUG"].lower() in ("true", "1", "yes", "on")
        log.info("Override: migration.debug = %s", cfg.migration.debug)

    mysql_map = {
        "MYSQL_HOST": ("host", str),
        "MYSQL_PORT": ("port", int),
        "MYSQL_USER": ("user", str),
        "MYSQL_PASSWORD": ("password", str),
        "MYSQL_DATABASE": ("database", str),
        "MYSQL_SSL_CA": ("ssl_ca", str),
        "MYSQL_SSL_DISABLED": ("ssl_disabled", bool),
    }
    for env_var, (key, typ) in mysql_map.items():
        if env_var not in os.environ:
            continue
        raw = os.environ[env_var]
        try:
            val: Any = raw.lower() in ("true", "1", "yes", "on") if typ is bool else typ(raw)
        except ValueError:
            log.warning("Invalid value for %s: %s", env_var, raw)
            continue
        setattr(cfg.mysql, key, val)
        log.info("Override: mysql.%s = %s", key, "***" if key == "password" else val)

    ch_map = {
        "CLICKHOUSE_HOST": ("host", str),
        "CLICKHOUSE_PORT": ("port", int),
        "CLICKHOUSE_USER": ("user", str),
        "CLICKHOUSE_PASSWORD": ("password", str),
        "CLICKHOUSE_DATABASE": ("database", str),
        "CLICKHOUSE_SECURE": ("secure", bool),
        "CLICKHOUSE_VERIFY": ("verify", bool),
        "CLICKHOUSE_CA_CERTS": ("ca_certs", str),
    }
    for env_var, (key, typ) in ch_map.items():
        if env_var not in os.environ:
            continue
        raw = os.environ[env_var]
        try:
            val = raw.lower() in ("true", "1", "yes", "on") if typ is bool else typ(raw)
        except ValueError:
            log.warning("Invalid value for %s: %s", env_var, raw)
            continue
        setattr(cfg.clickhouse, key, val)
        log.info("Override: clickhouse.%s = %s", key, "***" if key == "password" else val)

    mig_map = {
        "MIGRATION_MODE": ("mode", str),
        "MIGRATION_BATCH_ROWS": ("batch_rows", int),
        "MIGRATION_WORKERS": ("workers", int),
        "MIGRATION_CLICKHOUSE_TIMEZONE": ("clickhouse_timezone", str),
    }
    for env_var, (key, typ) in mig_map.items():
        if env_var not in os.environ:
            continue
        raw = os.environ[env_var]
        try:
            val = typ(raw)
        except ValueError:
            log.warning("Invalid value for %s: %s", env_var, raw)
            continue
        setattr(cfg.migration, key, val)
        log.info("Override: migration.%s = %s", key, val)

    cdc_map = {
        "CDC_HEARTBEAT_SECONDS": ("heartbeat_seconds", int),
        "CDC_CHECKPOINT_INTERVAL_ROWS": ("checkpoint_interval_rows", int),
        "CDC_BATCH_MAX_WAIT_SECONDS": ("batch_max_wait_seconds", int),
        "CDC_PRODUCER_BATCH_SIZE": ("producer_batch_size", int),
        "CDC_FORCE_BINLOG_POSITION": ("force_binlog_position", str),
        "CDC_DB_DEBUG": ("db_debug", bool),
        "CDC_PREPARED_QUERIES_BATCH_LIMIT": ("prepared_queries_batch_limit", int),
        "CDC_PRODUCER_FLUSH_INTERVAL": ("producer_flush_interval", float),
        "CDC_TRANSFORMER_POLL_INTERVAL": ("transformer_poll_interval", float),
        "CDC_CONSUMER_POLL_INTERVAL": ("consumer_poll_interval", float),
        "CDC_USE_GTID": ("use_gtid", bool),
        "CDC_RAW_EVENTS_MAX": ("raw_events_max", int),
    }
    for env_var, (key, typ) in cdc_map.items():
        if env_var not in os.environ:
            continue
        raw = os.environ[env_var]
        if typ is bool:
            val = raw.lower() in ("true", "1", "yes", "on")
        elif typ is str:
            val = None if raw == "" else raw
        else:
            try:
                val = typ(raw)
            except ValueError:
                log.warning("Invalid value for %s: %s", env_var, raw)
                continue
        setattr(cfg.migration.cdc, key, val)
        log.info("Override: migration.cdc.%s = %s", key, val)

    notif_map = {
        "NOTIFICATIONS_ENABLED": ("enabled", bool),
        "NOTIFICATIONS_WEBHOOK_URL": ("webhook_url", str),
        "NOTIFICATIONS_RATE_LIMIT_SECONDS": ("rate_limit_seconds", int),
    }
    for env_var, (key, typ) in notif_map.items():
        if env_var not in os.environ:
            continue
        raw = os.environ[env_var]
        try:
            val = raw.lower() in ("true", "1", "yes", "on") if typ is bool else typ(raw)
        except ValueError:
            log.warning("Invalid value for %s: %s", env_var, raw)
            continue
        setattr(cfg.notifications, key, val)
        log.info("Override: notifications.%s = %s", key, val)

    if "STATE_FILE" in os.environ:
        cfg.state_file = os.environ["STATE_FILE"]
        log.info("Override: state_file = %s", cfg.state_file)
    if "BUFFER_FILE" in os.environ:
        cfg.buffer_file = os.environ["BUFFER_FILE"]
        log.info("Override: buffer_file = %s", cfg.buffer_file)
    if "ENVIRONMENT" in os.environ:
        cfg.environment = os.environ["ENVIRONMENT"]
        log.info("Override: environment = %s", cfg.environment)

    return cfg


def _warn_removed_keys(raw: Dict[str, Any]) -> None:
    """Warn about config keys removed in 3.0.0 (they are ignored, not honored)."""
    cdc_raw = (raw.get("migration") or {}).get("cdc") or {}
    if "batch_delay_seconds" in cdc_raw:
        log.warning(
            "migration.cdc.batch_delay_seconds was removed in 3.0.0 and is ignored; "
            "use producer_flush_interval / transformer_poll_interval / consumer_poll_interval"
        )
    if "CDC_BATCH_DELAY_SECONDS" in os.environ:
        log.warning(
            "CDC_BATCH_DELAY_SECONDS was removed in 3.0.0 and is ignored; "
            "use CDC_PRODUCER_FLUSH_INTERVAL / CDC_TRANSFORMER_POLL_INTERVAL / CDC_CONSUMER_POLL_INTERVAL"
        )


def load_config(path: str) -> MigresConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    _warn_removed_keys(raw)
    cfg = MigresConfig(
        mysql=_merge_dataclass(MySQLConfig, raw.get("mysql")),
        clickhouse=_merge_dataclass(ClickHouseConfig, raw.get("clickhouse")),
        migration=_merge_dataclass(
            MigrationConfig,
            raw.get("migration"),
            nested={"cdc": CdcConfig},
        ),
        notifications=_merge_dataclass(NotificationsConfig, raw.get("notifications")),
        state_file=raw.get("state_file") or "/app/state.json",
        buffer_file=raw.get("buffer_file") or "data/buffer.db",
        environment=raw.get("environment") or "prod",
    )
    if cfg.mysql.include_tables is None:
        cfg.mysql.include_tables = []
    return _apply_env_overrides(cfg)
