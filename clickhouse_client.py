import logging
from clickhouse_driver import Client

log = logging.getLogger(__name__)

class CHClient:
    def __init__(self, cfg):
        self.db = cfg["database"]

        # Step 1: connect without database to ensure DB exists
        tmp_client = Client(
            host=cfg["host"], port=cfg.get("port", 9000),
            user=cfg.get("user", "default"), password=cfg.get("password", "")
        )
        tmp_client.execute(f"CREATE DATABASE IF NOT EXISTS `{self.db}`")
        tmp_client.disconnect()

        # Step 2: connect to actual database
        self.client = Client(
            host=cfg["host"], port=cfg.get("port", 9000),
            user=cfg.get("user", "default"), password=cfg.get("password", ""),
            database=self.db,
            settings={"input_format_null_as_default": 1}
        )
        log.info(
            "ClickHouse client initialized for %s:%s/%s",
            cfg["host"], cfg.get("port", 9000), self.db
        )

    def execute(self, sql, params=None):
        log.debug("CH SQL: %s", sql)
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
