import logging
import threading
from clickhouse_driver import Client

log = logging.getLogger(__name__)

class CHClient:
    # Track which configs have logged initialization (shared across threads)
    _initialization_logged = set()
    _log_lock = threading.Lock()
    
    def __init__(self, cfg, mig_cfg=None):
        self.db = cfg["database"]
        self._config_key = (
            cfg["host"], 
            cfg.get("port", 9000), 
            cfg.get("user", "default"),
            cfg["database"],
            mig_cfg.get("clickhouse_timezone") if mig_cfg else None
        )
        
        # Configure timezone settings
        settings = {"input_format_null_as_default": 1}
        if mig_cfg and mig_cfg.get("clickhouse_timezone"):
            timezone = mig_cfg["clickhouse_timezone"]
            settings["timezone"] = timezone
            # Only log timezone once per config
            with self._log_lock:
                timezone_key = (self._config_key[:4], timezone)
                if timezone_key not in self._initialization_logged:
                    log.info("ClickHouse timezone set to: %s", timezone)
                    self._initialization_logged.add(timezone_key)

        # Step 1: connect without database to ensure DB exists
        # Only do this once per config (use a lock to prevent race condition)
        db_created_key = ("db_created", self._config_key[:4])
        should_create_db = False
        with self._log_lock:
            if db_created_key not in self._initialization_logged:
                should_create_db = True
                self._initialization_logged.add(db_created_key)
        
        if should_create_db:
            tmp_client = Client(
                host=cfg["host"], port=cfg.get("port", 9000),
                user=cfg.get("user", "default"), password=cfg.get("password", ""),
                settings=settings
            )
            tmp_client.execute(f"CREATE DATABASE IF NOT EXISTS `{self.db}`")
            tmp_client.disconnect()

        # Step 2: connect to actual database (each instance gets its own connection)
        self.client = Client(
            host=cfg["host"], port=cfg.get("port", 9000),
            user=cfg.get("user", "default"), password=cfg.get("password", ""),
            database=self.db,
            settings=settings
        )
        
        # Only log initialization once per config
        with self._log_lock:
            init_key = ("init", self._config_key[:4])
            if init_key not in self._initialization_logged:
                log.info(
                    "ClickHouse client initialized for %s:%s/%s",
                    cfg["host"], cfg.get("port", 9000), self.db
                )
                self._initialization_logged.add(init_key)

    def execute(self, sql, params=None):
        # Track ClickHouse queries
        try:
            from cdc import _add_clickhouse_query
            _add_clickhouse_query(f"Query: {sql} | Params: {params}")
        except ImportError:
            pass  # If cdc module not available, just continue
        return self.client.execute(sql, params or None)

    def insert_rows(self, table, columns, rows):
        """
        columns: list of column names
        rows: list of tuples
        """
        if not rows:
            return
        cols = ",".join([f"`{c}`" for c in columns])
        sql = f"INSERT INTO `{self.db}`.`{table}` ({cols}) VALUES"
        
        # Track ClickHouse queries
        try:
            from cdc import _add_clickhouse_query
            _add_clickhouse_query(f"INSERT INTO {self.db}.{table} ({cols}) VALUES [with {len(rows)} rows]", table)
        except ImportError:
            pass  # If cdc module not available, just continue
        
        try:
            self.client.execute(sql, rows)
        except Exception:
            log.exception("ClickHouse insert failed for table %s", table)
            raise

    def close(self):
        self.client.disconnect()
