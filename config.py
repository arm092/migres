import yaml
import os
import logging

log = logging.getLogger(__name__)

def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # Apply environment variable overrides
    cfg = _apply_env_overrides(cfg)

    # defaults
    cfg.setdefault("migration", {})
    cfg["migration"].setdefault("batch_rows", 5000)
    cfg["migration"].setdefault("mode", "snapshot")
    cfg.setdefault("checkpoint_file", "/app/binlog_checkpoint.json")
    cfg.setdefault("state_file", "/app/state.json")
    
    # CDC defaults
    if "cdc" not in cfg["migration"]:
        cfg["migration"]["cdc"] = {}
    cfg["migration"]["cdc"].setdefault("batch_delay_seconds", 0)
    # include_tables empty => all tables
    if cfg["mysql"].get("include_tables") is None:
        cfg["mysql"]["include_tables"] = []
    return cfg

def _apply_env_overrides(cfg):
    """Apply environment variable overrides to configuration"""
    
    # MySQL configuration overrides
    if "mysql" not in cfg:
        cfg["mysql"] = {}
    
    mysql_overrides = {
        "MYSQL_HOST": "host",
        "MYSQL_PORT": "port", 
        "MYSQL_USER": "user",
        "MYSQL_PASSWORD": "password",
        "MYSQL_DATABASE": "database"
    }
    
    for env_var, config_key in mysql_overrides.items():
        if env_var in os.environ:
            value = os.environ[env_var]
            # Convert port to integer if it's the port field
            if config_key == "port":
                try:
                    value = int(value)
                except ValueError:
                    log.warning(f"Invalid port value for {env_var}: {value}")
                    continue
            cfg["mysql"][config_key] = value
            log.info(f"Override: mysql.{config_key} = {value}")
    
    # ClickHouse configuration overrides
    if "clickhouse" not in cfg:
        cfg["clickhouse"] = {}
    
    clickhouse_overrides = {
        "CLICKHOUSE_HOST": "host",
        "CLICKHOUSE_PORT": "port",
        "CLICKHOUSE_USER": "user", 
        "CLICKHOUSE_PASSWORD": "password",
        "CLICKHOUSE_DATABASE": "database"
    }
    
    for env_var, config_key in clickhouse_overrides.items():
        if env_var in os.environ:
            value = os.environ[env_var]
            # Convert port to integer if it's the port field
            if config_key == "port":
                try:
                    value = int(value)
                except ValueError:
                    log.warning(f"Invalid port value for {env_var}: {value}")
                    continue
            cfg["clickhouse"][config_key] = value
            log.info(f"Override: clickhouse.{config_key} = {value}")
    
    # Migration configuration overrides
    if "migration" not in cfg:
        cfg["migration"] = {}
    
    migration_overrides = {
        "MIGRATION_MODE": "mode",
        "MIGRATION_BATCH_ROWS": "batch_rows",
        "MIGRATION_WORKERS": "workers",
        "MIGRATION_MYSQL_TIMEZONE": "mysql_timezone",
        "MIGRATION_CLICKHOUSE_TIMEZONE": "clickhouse_timezone"
    }
    
    for env_var, config_key in migration_overrides.items():
        if env_var in os.environ:
            value = os.environ[env_var]
            # Convert numeric fields
            if config_key in ["batch_rows", "workers"]:
                try:
                    value = int(value)
                except ValueError:
                    log.warning(f"Invalid numeric value for {env_var}: {value}")
                    continue
            cfg["migration"][config_key] = value
            log.info(f"Override: migration.{config_key} = {value}")
    
    # CDC configuration overrides
    if "cdc" not in cfg["migration"]:
        cfg["migration"]["cdc"] = {}
    
    cdc_overrides = {
        "CDC_BATCH_DELAY_SECONDS": "batch_delay_seconds",
        "CDC_HEARTBEAT_SECONDS": "heartbeat_seconds",
        "CDC_CHECKPOINT_INTERVAL_ROWS": "checkpoint_interval_rows",
        "CDC_CHECKPOINT_INTERVAL_SECONDS": "checkpoint_interval_seconds"
    }
    
    for env_var, config_key in cdc_overrides.items():
        if env_var in os.environ:
            value = os.environ[env_var]
            try:
                value = int(value)
            except ValueError:
                log.warning(f"Invalid numeric value for {env_var}: {value}")
                continue
            cfg["migration"]["cdc"][config_key] = value
            log.info(f"Override: migration.cdc.{config_key} = {value}")
    
    # Notifications configuration overrides
    if "notifications" not in cfg:
        cfg["notifications"] = {}
    
    notification_overrides = {
        "NOTIFICATIONS_ENABLED": "enabled",
        "NOTIFICATIONS_WEBHOOK_URL": "webhook_url",
        "NOTIFICATIONS_RATE_LIMIT_SECONDS": "rate_limit_seconds"
    }
    
    for env_var, config_key in notification_overrides.items():
        if env_var in os.environ:
            value = os.environ[env_var]
            # Convert boolean for enabled field
            if config_key == "enabled":
                value = value.lower() in ("true", "1", "yes", "on")
            # Convert numeric for rate_limit_seconds
            elif config_key == "rate_limit_seconds":
                try:
                    value = int(value)
                except ValueError:
                    log.warning(f"Invalid numeric value for {env_var}: {value}")
                    continue
            cfg["notifications"][config_key] = value
            log.info(f"Override: notifications.{config_key} = {value}")
    
    # File path overrides
    file_overrides = {
        "CHECKPOINT_FILE": "checkpoint_file",
        "STATE_FILE": "state_file"
    }
    
    for env_var, config_key in file_overrides.items():
        if env_var in os.environ:
            cfg[config_key] = os.environ[env_var]
            log.info(f"Override: {config_key} = {os.environ[env_var]}")
    
    return cfg
