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
            except mysql.connector.Error:
                pass

    def get_mysql_version(self):
        """Get MySQL version for compatibility checks"""
        cur = self.cn.cursor()
        try:
            cur.execute("SELECT VERSION()")
            version_str = cur.fetchone()[0]
            cur.close()
            
            log.debug("MySQL version: %s", version_str)
            
            # Parse version string (e.g., "8.4.0" or "8.0.35")
            try:
                version_parts = version_str.split('.')
                major = int(version_parts[0])
                minor = int(version_parts[1])
                return major, minor
            except (ValueError, IndexError):
                # Fallback to assuming older version
                log.warning("Could not parse MySQL version '%s', assuming 8.0", version_str)
                return 8, 0
        except mysql.connector.Error as e:
            log.warning("Failed to get MySQL version: %s", e)
            cur.close()
            # Fallback to assuming older version
            return 8, 0

    def show_master_status(self):
        """Get binary log status with MySQL version compatibility"""
        cur = self.cn.cursor()
        
        try:
            # Check MySQL version to use appropriate command
            major, minor = self.get_mysql_version()
            
            if major >= 8 and minor >= 4:
                # MySQL 8.4+ uses SHOW BINARY LOG STATUS
                cur.execute("SHOW BINARY LOG STATUS")
                log.debug("Using SHOW BINARY LOG STATUS for MySQL %d.%d", major, minor)
            else:
                # MySQL 8.0 and earlier use SHOW MASTER STATUS
                cur.execute("SHOW MASTER STATUS")
                log.debug("Using SHOW MASTER STATUS for MySQL %d.%d", major, minor)
            
            row = cur.fetchone()
            cur.close()
            
            if not row:
                return None
            
            # Both commands return the same format: (file, position, ...)
            return row[0], int(row[1])
            
        except mysql.connector.Error as e:
            log.warning("Failed to get binary log status: %s", e)
            cur.close()
            return None

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
                # Use parameterized queries - handles reserved keywords correctly
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
        
        log.info("MySQL: Getting schema for table '%s' in database '%s'", table, self.cfg["database"])
        
        try:
            # Track MySQL queries
            from cdc import _add_mysql_query
            
            # Use parameterized queries - this handles reserved keywords correctly
            # MySQL automatically treats the parameterized values as strings, not identifiers
            query1 = """
                SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
                ORDER BY ORDINAL_POSITION
            """
            _add_mysql_query(f"Query: {query1.strip()} | Params: ('{self.cfg['database']}', '{table}')", table)
            cur.execute(query1, (self.cfg["database"], table))
            cols = cur.fetchall()
            log.info("MySQL: Found %d columns for table '%s': %s", len(cols), table, [c["COLUMN_NAME"] for c in cols])

            query2 = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY'
                ORDER BY ORDINAL_POSITION
            """
            _add_mysql_query(f"Query: {query2.strip()} | Params: ('{self.cfg['database']}', '{table}')", table)
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
