import mysql.connector
import logging

log = logging.getLogger(__name__)

class MySQLClient:
    def __init__(self, cfg):
        self.cfg = cfg
        self.cn = None

    def connect(self):
        self.cn = mysql.connector.connect(
            host=self.cfg["host"],
            port=self.cfg.get("port", 3306),
            user=self.cfg["user"],
            password=self.cfg["password"],
            database=self.cfg["database"],
            charset="utf8mb4",
            use_unicode=True,
            autocommit=False,
        )
        log.info("MySQL connected: %s:%s/%s", self.cfg["host"], self.cfg.get("port", 3306), self.cfg["database"])
        return self.cn

    def close(self):
        if self.cn:
            try:
                self.cn.close()
            except Exception:
                pass

    def show_master_status(self):
        cur = self.cn.cursor()
        cur.execute("SHOW MASTER STATUS")
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return row[0], int(row[1])

    def start_repeatable_snapshot(self):
        try:
            self.cn.start_transaction(isolation_level='REPEATABLE READ')
            log.info("Started REPEATABLE READ transaction for consistent snapshot (this connection)")
        except Exception:
            cur = self.cn.cursor()
            cur.execute("START TRANSACTION")
            cur.close()
            log.info("Started transaction (fallback)")

    def list_tables(self, include_list, exclude_list=None):
        """
        include_list: if non-empty -> only these tables (validated).
        otherwise return all tables from schema minus exclude_list.
        """
        exclude_list = exclude_list or []
        cur = self.cn.cursor()
        if include_list:
            res = []
            for t in include_list:
                cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
                            (self.cfg["database"], t))
                if cur.fetchone()[0] > 0:
                    res.append(t)
                else:
                    log.warning("Table %s not found in database %s", t, self.cfg["database"])
            cur.close()
            return res
        else:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=%s AND table_type='BASE TABLE'",
                        (self.cfg["database"],))
            rows = [r[0] for r in cur.fetchall()]
            cur.close()
            filtered = [t for t in rows if t not in exclude_list]
            # warn about excluded tables not present? no need.
            return filtered

    def get_table_columns_and_pk(self, table):
        cur = self.cn.cursor(dictionary=True)
        cur.execute("""
            SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
            ORDER BY ORDINAL_POSITION
        """, (self.cfg["database"], table))
        cols = cur.fetchall()

        cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY'
            ORDER BY ORDINAL_POSITION
        """, (self.cfg["database"], table))
        pk_rows = cur.fetchall()
        pk = [r["COLUMN_NAME"] for r in pk_rows] if pk_rows else []

        cur.close()
        return cols, pk

    def fetch_rows_by_pk(self, table, columns, pk_col, last_pk, batch):
        cur = self.cn.cursor()
        cols_sql = ", ".join([f"`{c}`" for c in columns])
        if last_pk is None:
            sql = f"SELECT {cols_sql} FROM `{self.cfg['database']}`.`{table}` ORDER BY `{pk_col}` ASC LIMIT %s"
            cur.execute(sql, (batch,))
        else:
            sql = f"SELECT {cols_sql} FROM `{self.cfg['database']}`.`{table}` WHERE `{pk_col}` > %s ORDER BY `{pk_col}` ASC LIMIT %s"
            cur.execute(sql, (last_pk, batch))
        rows = cur.fetchall()
        cur.close()
        return rows

    def fetch_stream_with_offset(self, table, columns, offset, batch):
        cur = self.cn.cursor()
        cols_sql = ", ".join([f"`{c}`" for c in columns])
        sql = f"SELECT {cols_sql} FROM `{self.cfg['database']}`.`{table}` LIMIT %s OFFSET %s"
        cur.execute(sql, (batch, offset))
        rows = cur.fetchall()
        cur.close()
        return rows
