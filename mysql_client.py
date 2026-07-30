import mysql.connector
import logging
import threading
from schema_and_ddl import quote_ident

log = logging.getLogger(__name__)

class MySQLClient:
    # Thread-local storage for instances (each thread gets its own)
    _local = threading.local()
    _connection_logged = set()  # Track which configs have logged connection (shared across threads)
    _log_lock = threading.Lock()
    
    def __init__(self, cfg):
        self.cfg = cfg
        self.cn = None
        self._config_key = (cfg["host"], cfg.get("port", 3306), cfg["user"], cfg["database"])
        
        with self._log_lock:
            self._should_log_connection = self._config_key not in self._connection_logged
            if self._should_log_connection:
                self._connection_logged.add(self._config_key)

    def connect(self):
        if self.cn:
            try:
                self.cn.close()
            except mysql.connector.Error:
                pass
            self.cn = None
        
        if self._should_log_connection:
            log.info("MySQL connected: %s:%s/%s", self.cfg["host"], self.cfg.get("port", 3306), self.cfg["database"])
            self._should_log_connection = False
        
        connect_kwargs = dict(
            host=self.cfg["host"],
            port=self.cfg.get("port", 3306),
            user=self.cfg["user"],
            password=self.cfg["password"],
            database=self.cfg["database"],
            charset="utf8mb4",
            use_unicode=True,
            autocommit=False,
        )
        # Optional TLS
        if self.cfg.get("ssl_disabled"):
            connect_kwargs["ssl_disabled"] = True
        elif self.cfg.get("ssl_ca"):
            connect_kwargs["ssl_ca"] = self.cfg["ssl_ca"]
            connect_kwargs["ssl_verify_cert"] = True

        self.cn = mysql.connector.connect(**connect_kwargs)
        return self.cn

    def close(self):
        if self.cn:
            try:
                self.cn.close()
            except mysql.connector.Error:
                pass
            finally:
                self.cn = None

    def get_mysql_version(self):
        """Get MySQL version for compatibility checks"""
        cur = self.cn.cursor()
        try:
            cur.execute("SELECT VERSION()")
            version_str = cur.fetchone()[0]
            cur.close()
            
            log.debug("MySQL version: %s", version_str)
            
            try:
                version_parts = version_str.split('.')
                major = int(version_parts[0])
                minor = int(version_parts[1])
                return major, minor
            except (ValueError, IndexError):
                log.warning("Could not parse MySQL version '%s', assuming 8.0", version_str)
                return 8, 0
        except mysql.connector.Error as e:
            log.warning("Failed to get MySQL version: %s", e)
            cur.close()
            return 8, 0

    def show_master_status(self):
        """Get binary log status with MySQL version compatibility"""
        cur = self.cn.cursor()
        
        try:
            major, minor = self.get_mysql_version()
            
            if major >= 8 and minor >= 4:
                cur.execute("SHOW BINARY LOG STATUS")
                log.debug("Using SHOW BINARY LOG STATUS for MySQL %d.%d", major, minor)
            else:
                cur.execute("SHOW MASTER STATUS")
                log.debug("Using SHOW MASTER STATUS for MySQL %d.%d", major, minor)
            
            row = cur.fetchone()
            cur.close()
            
            if not row:
                return None
            
            return row[0], int(row[1])
            
        except mysql.connector.Error as e:
            log.warning("Failed to get binary log status: %s", e)
            cur.close()
            return None

    def get_binlog_settings(self):
        """Return dict of binlog_format, binlog_row_image, binlog_row_metadata."""
        cur = self.cn.cursor()
        try:
            cur.execute(
                "SHOW VARIABLES WHERE Variable_name IN "
                "('binlog_format', 'binlog_row_image', 'binlog_row_metadata')"
            )
            rows = {r[0].lower(): (r[1] or "").upper() for r in cur.fetchall()}
            cur.close()
            return rows
        except mysql.connector.Error as e:
            cur.close()
            raise RuntimeError(f"Failed to read binlog settings: {e}") from e

    def assert_cdc_binlog_settings(self):
        """Raise RuntimeError if binlog settings are unsuitable for CDC."""
        settings = self.get_binlog_settings()
        fmt = settings.get("binlog_format", "")
        image = settings.get("binlog_row_image", "")
        errors = []
        if fmt != "ROW":
            errors.append(f"binlog_format={fmt!r} (required: ROW)")
        if image != "FULL":
            errors.append(f"binlog_row_image={image!r} (required: FULL)")
        if errors:
            raise RuntimeError(
                "MySQL binlog settings are not suitable for CDC: " + "; ".join(errors)
            )

    def start_repeatable_snapshot(self):
        try:
            self.cn.start_transaction(isolation_level='REPEATABLE READ')
            log.info("Started REPEATABLE READ transaction for consistent snapshot (this connection)")
        except mysql.connector.Error:
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
            return filtered

    def get_table_columns_and_pk(self, table):
        cur = self.cn.cursor(dictionary=True)
        
        log.info("MySQL: Getting schema for table '%s' in database '%s'", table, self.cfg["database"])
        
        try:
            query1 = """
                SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
                ORDER BY ORDINAL_POSITION
            """
            cur.execute(query1, (self.cfg["database"], table))
            cols = cur.fetchall()
            log.info("MySQL: Found %d columns for table '%s': %s", len(cols), table, [c["COLUMN_NAME"] for c in cols])

            query2 = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY'
                ORDER BY ORDINAL_POSITION
            """
            cur.execute(query2, (self.cfg["database"], table))
            pk_rows = cur.fetchall()
            pk = [r["COLUMN_NAME"] for r in pk_rows] if pk_rows else []
            log.info("MySQL: Found %d primary key columns for table '%s': %s", len(pk), table, pk)

            cur.close()
            return cols, pk
            
        except mysql.connector.Error as e:
            log.error("MySQL: Error getting schema for table '%s': %s", table, str(e))
            cur.close()
            raise

    def fetch_rows_by_pk(self, table, columns, pk_col, last_pk, batch):
        cur = self.cn.cursor()
        cols_sql = ", ".join([quote_ident(c) for c in columns])
        qdb = quote_ident(self.cfg["database"])
        qtable = quote_ident(table)
        qpk = quote_ident(pk_col)
        if last_pk is None:
            sql = f"SELECT {cols_sql} FROM {qdb}.{qtable} ORDER BY {qpk} ASC LIMIT %s"
            cur.execute(sql, (batch,))
        else:
            sql = f"SELECT {cols_sql} FROM {qdb}.{qtable} WHERE {qpk} > %s ORDER BY {qpk} ASC LIMIT %s"
            cur.execute(sql, (last_pk, batch))
        rows = cur.fetchall()
        cur.close()
        return rows

    def fetch_stream_with_offset(self, table, columns, offset, batch, order_columns=None):
        """Fetch a page of rows with deterministic ORDER BY (required for OFFSET safety)."""
        cur = self.cn.cursor()
        cols_sql = ", ".join([quote_ident(c) for c in columns])
        qdb = quote_ident(self.cfg["database"])
        qtable = quote_ident(table)
        order_cols = order_columns or columns
        if not order_cols:
            raise ValueError(f"Cannot paginate table {table} without ORDER BY columns")
        order_sql = ", ".join([quote_ident(c) for c in order_cols])
        sql = f"SELECT {cols_sql} FROM {qdb}.{qtable} ORDER BY {order_sql} LIMIT %s OFFSET %s"
        cur.execute(sql, (batch, offset))
        rows = cur.fetchall()
        cur.close()
        return rows
