import logging
from clickhouse_driver import Client

log = logging.getLogger(__name__)

class CHClient:
    def __init__(self, cfg, mig_cfg=None):
        self.db = cfg["database"]
        
        # Configure timezone settings
        settings = {"input_format_null_as_default": 1}
        if mig_cfg and mig_cfg.get("clickhouse_timezone"):
            timezone = mig_cfg["clickhouse_timezone"]
            settings["timezone"] = timezone
            log.info("ClickHouse timezone set to: %s", timezone)

        # Step 1: connect without database to ensure DB exists
        tmp_client = Client(
            host=cfg["host"], port=cfg.get("port", 9000),
            user=cfg.get("user", "default"), password=cfg.get("password", ""),
            settings=settings
        )
        tmp_client.execute(f"CREATE DATABASE IF NOT EXISTS `{self.db}`")
        tmp_client.disconnect()

        # Step 2: connect to actual database
        self.client = Client(
            host=cfg["host"], port=cfg.get("port", 9000),
            user=cfg.get("user", "default"), password=cfg.get("password", ""),
            database=self.db,
            settings=settings
        )
        log.info(
            "ClickHouse client initialized for %s:%s/%s",
            cfg["host"], cfg.get("port", 9000), self.db
        )

    def execute(self, sql, params=None):
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
        
        
        try:
            self.client.execute(sql, rows)
        except Exception:
            log.exception("ClickHouse insert failed for table %s", table)
            raise
