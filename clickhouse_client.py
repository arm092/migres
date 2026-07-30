import logging
import threading
from clickhouse_driver import Client
from schema_and_ddl import quote_ident

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
        
        settings = {"input_format_null_as_default": 1}
        if mig_cfg and mig_cfg.get("clickhouse_timezone"):
            timezone = mig_cfg["clickhouse_timezone"]
            settings["timezone"] = timezone
            with self._log_lock:
                timezone_key = (self._config_key[:4], timezone)
                if timezone_key not in self._initialization_logged:
                    log.info("ClickHouse timezone set to: %s", timezone)
                    self._initialization_logged.add(timezone_key)
        
        self.connect_timeout = 10
        # Keep query timeout bounded so poison/hanging inserts fail fast
        self.send_receive_timeout = int((mig_cfg or {}).get("clickhouse_send_receive_timeout", 60))

        secure = bool(cfg.get("secure", False))
        verify = cfg.get("verify", True)
        ca_certs = cfg.get("ca_certs")

        client_kwargs = dict(
            host=cfg["host"],
            port=cfg.get("port", 9000),
            user=cfg.get("user", "default"),
            password=cfg.get("password", ""),
            settings=settings,
            connect_timeout=self.connect_timeout,
            send_receive_timeout=self.send_receive_timeout,
            secure=secure,
        )
        if secure:
            client_kwargs["verify"] = bool(verify)
            if ca_certs:
                client_kwargs["ca_certs"] = ca_certs

        db_created_key = ("db_created", self._config_key[:4])
        should_create_db = False
        with self._log_lock:
            if db_created_key not in self._initialization_logged:
                should_create_db = True
                self._initialization_logged.add(db_created_key)
        
        if should_create_db:
            tmp_client = Client(**client_kwargs)
            tmp_client.execute(f"CREATE DATABASE IF NOT EXISTS {quote_ident(self.db)}")
            tmp_client.disconnect()

        self.client = Client(**{**client_kwargs, "database": self.db})
        
        with self._log_lock:
            init_key = ("init", self._config_key[:4])
            if init_key not in self._initialization_logged:
                log.info(
                    "ClickHouse client initialized for %s:%s/%s",
                    cfg["host"], cfg.get("port", 9000), self.db
                )
                self._initialization_logged.add(init_key)

    def execute(self, sql, params=None):
        return self.client.execute(sql, params or None)

    def insert_rows(self, table, columns, rows):
        """
        columns: list of column names
        rows: list of tuples
        """
        if not rows:
            return
        cols = ",".join([quote_ident(c) for c in columns])
        sql = f"INSERT INTO {quote_ident(self.db)}.{quote_ident(table)} ({cols}) VALUES"
        
        try:
            self.client.execute(sql, rows)
        except Exception:
            log.exception("ClickHouse insert failed for table %s", table)
            raise

    def list_migres_tables(self):
        """Return table names in this database that have migres metadata columns."""
        qdb = quote_ident(self.db)
        try:
            rows = self.execute(
                "SELECT name FROM system.tables "
                "WHERE database = %(db)s AND name IN ("
                "  SELECT table FROM system.columns "
                "  WHERE database = %(db)s AND name = '__data_transfer_commit_time'"
                ")",
                {"db": self.db},
            )
            return [r[0] for r in rows]
        except Exception:
            # Fallback: list all tables (older CH / permission issues)
            log.warning("Could not filter migres tables via system.columns; falling back to SHOW TABLES")
            tables = self.execute(f"SHOW TABLES FROM {qdb}")
            return [t[0] for t in tables]

    def close(self):
        self.client.disconnect()
