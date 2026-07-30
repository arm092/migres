import sqlite3
import logging
import json
import threading
import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Optional, Any

log = logging.getLogger(__name__)

# UTC+4 for debug table timestamps (e.g. Asia/Yerevan)
DEBUG_TZ = timezone(timedelta(hours=4))
CH_DEBUG_TZ_NAME = "Asia/Yerevan"  # UTC+4

class BufferDB:
    def __init__(self, db_path: str = None, db_debug: bool = False, cfg: Optional[Dict[str, Any]] = None):
        self.db_debug = db_debug
        self._cfg = cfg if db_debug else None
        self._debug_ch = None
        self._debug_ch_lock = threading.Lock()

        if db_path is None:
            if cfg and cfg.get("buffer_file"):
                db_path = cfg["buffer_file"]
            else:
                db_path = "data/buffer.db"

        self.db_path = os.path.abspath(db_path)
        dir_path = os.path.dirname(self.db_path) or "."
        try:
            os.makedirs(dir_path, exist_ok=True)
            test_file = os.path.join(dir_path, ".buffer_test")
            with open(test_file, 'w') as f:
                f.write("test")
            os.remove(test_file)
        except Exception as e:
            raise RuntimeError(
                f"Cannot write buffer database directory '{dir_path}' for {self.db_path}: {e}. "
                f"Set buffer_file / BUFFER_FILE to a writable path."
            ) from e

        self.local = threading.local()
        self._init_db()

    def _get_conn(self):
        """Get thread-local connection"""
        if not hasattr(self.local, 'conn'):
            dir_path = os.path.dirname(self.db_path) or "."
            os.makedirs(dir_path, exist_ok=True)
            connect_timeout = 30
            try:
                # isolation_level=None => autocommit; avoids stale READ snapshots where
                # one pipeline stage never sees commits from another stage's connection.
                self.local.conn = sqlite3.connect(
                    self.db_path,
                    check_same_thread=False,
                    timeout=connect_timeout,
                    isolation_level=None,
                )
            except sqlite3.OperationalError as e:
                log.error(f"Failed to connect to database at {self.db_path}: {e}")
                raise RuntimeError(
                    f"Unable to open buffer database at {self.db_path}: {e}"
                ) from e
            self.local.conn.execute("PRAGMA journal_mode=WAL")
            self.local.conn.execute("PRAGMA synchronous=NORMAL")
            self.local.conn.execute("PRAGMA busy_timeout=30000")
        return self.local.conn

    def _init_db(self):
        """Initialize database schema"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                binlog_file TEXT NOT NULL,
                binlog_pos INTEGER NOT NULL,
                schema_name TEXT,
                table_name TEXT,
                event_type TEXT,
                event_data JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_events_pos ON raw_events (binlog_file, binlog_pos)")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prepared_queries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sql_query TEXT NOT NULL,
                params JSON,
                group_id TEXT,
                schema_name TEXT,
                table_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_prepared_created ON prepared_queries (created_at)")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS failed_queries (
                id INTEGER PRIMARY KEY,
                sql_query TEXT,
                params JSON,
                schema_name TEXT,
                table_name TEXT,
                error_reason TEXT,
                failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS checkpoint (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                binlog_file TEXT,
                binlog_pos INTEGER,
                gtid TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()

    def _get_debug_ch(self):
        """Lazy-init ClickHouse client for db_debug; ensures debug tables exist."""
        if not self.db_debug or not self._cfg:
            return None
        with self._debug_ch_lock:
            if self._debug_ch is None:
                from migres.clients.clickhouse import CHClient
                ch_cfg = self._cfg["clickhouse"]
                mig_cfg = self._cfg.get("migration", {})
                self._debug_ch = CHClient(ch_cfg, mig_cfg)
                self._ensure_debug_tables_ch()
            return self._debug_ch

    def _ensure_debug_tables_ch(self):
        """Create debug_processed_events and debug_processed_queries in ClickHouse if not exist (UTC+4)."""
        ch = self._debug_ch
        if not ch:
            return
        from migres.schema.ddl import quote_ident
        db = quote_ident(ch.db)
        ch.execute(f"""
            CREATE TABLE IF NOT EXISTS {db}.`debug_processed_events` (
                id UInt64,
                binlog_file String,
                binlog_pos UInt64,
                schema_name Nullable(String),
                table_name Nullable(String),
                event_type Nullable(String),
                event_data String,
                received_at DateTime64(3, '{CH_DEBUG_TZ_NAME}'),
                processed_at DateTime64(3, '{CH_DEBUG_TZ_NAME}')
            ) ENGINE = MergeTree() ORDER BY (id)
        """)
        ch.execute(f"""
            CREATE TABLE IF NOT EXISTS {db}.`debug_processed_queries` (
                id UInt64,
                sql_query String,
                params Nullable(String),
                group_id Nullable(String),
                schema_name Nullable(String),
                table_name Nullable(String),
                received_at DateTime64(3, '{CH_DEBUG_TZ_NAME}'),
                processed_at DateTime64(3, '{CH_DEBUG_TZ_NAME}'),
                updated_at DateTime64(3, '{CH_DEBUG_TZ_NAME}')
            ) ENGINE = MergeTree() ORDER BY (id)
        """)
        log.info("DB debug mode: ClickHouse tables debug_processed_events and debug_processed_queries ensured")

    def _parse_sqlite_ts(self, s) -> Optional[datetime]:
        """Parse SQLite timestamp (UTC) to datetime; return as UTC+4 for CH."""
        if s is None:
            return None
        if isinstance(s, datetime):
            dt = s
        else:
            s = str(s).strip()
            if not s:
                return None
            try:
                dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(DEBUG_TZ)

    def _send_processed_events_to_ch(self, rows: List[Dict]):
        """Insert processed raw events into ClickHouse debug_processed_events (UTC+4)."""
        ch = self._get_debug_ch()
        if not ch or not rows:
            return
        now_ch = datetime.now(DEBUG_TZ)
        cols = ["id", "binlog_file", "binlog_pos", "schema_name", "table_name", "event_type", "event_data", "received_at", "processed_at"]
        ch_rows = []
        for r in rows:
            received = self._parse_sqlite_ts(r.get("created_at")) or now_ch
            event_data = r.get("event_data")
            if isinstance(event_data, dict):
                event_data = json.dumps(event_data)
            elif event_data is None:
                event_data = "{}"
            ch_rows.append((
                r["id"],
                r.get("binlog_file") or "",
                r.get("binlog_pos") or 0,
                r.get("schema_name"),
                r.get("table_name"),
                r.get("event_type"),
                event_data,
                received,
                now_ch,
            ))
        try:
            ch.insert_rows("debug_processed_events", cols, ch_rows)
        except Exception:
            log.exception("Failed to send processed events to ClickHouse debug table")

    def _send_processed_queries_to_ch(self, rows: List[Dict]):
        """Insert processed prepared queries into ClickHouse debug_processed_queries (UTC+4)."""
        ch = self._get_debug_ch()
        if not ch or not rows:
            return
        now_ch = datetime.now(DEBUG_TZ)
        cols = ["id", "sql_query", "params", "group_id", "schema_name", "table_name", "received_at", "processed_at", "updated_at"]
        ch_rows = []
        for r in rows:
            received = self._parse_sqlite_ts(r.get("created_at")) or now_ch
            params_val = r.get("params")
            if isinstance(params_val, (dict, list)):
                params_val = json.dumps(params_val)
            ch_rows.append((
                r["id"],
                r.get("sql_query") or "",
                params_val,
                r.get("group_id"),
                r.get("schema_name"),
                r.get("table_name"),
                received,
                now_ch,
                now_ch,
            ))
        try:
            ch.insert_rows("debug_processed_queries", cols, ch_rows)
        except Exception as e:
            log.exception("Failed to send processed queries to ClickHouse debug table: %s", e)

    def _merge_debug_query_rows(self, rows: List[Dict]) -> List[Dict]:
        """Merge contiguous compatible processed query rows for cleaner debug output."""
        if not rows:
            return []

        sorted_rows = sorted(rows, key=lambda r: r.get("id", 0))
        merged = []

        for r in sorted_rows:
            params_raw = r.get("params")
            params_obj = None
            if params_raw is not None:
                try:
                    params_obj = json.loads(params_raw) if isinstance(params_raw, str) else params_raw
                except Exception:
                    params_obj = params_raw

            if merged:
                last = merged[-1]
                can_merge = (
                    last.get("sql_query") == r.get("sql_query")
                    and last.get("schema_name") == r.get("schema_name")
                    and last.get("table_name") == r.get("table_name")
                    and last.get("group_id") == r.get("group_id")
                    and isinstance(last.get("params_obj"), list)
                    and isinstance(params_obj, list)
                )
                if can_merge:
                    last["params_obj"].extend(params_obj)
                    last["id"] = max(last.get("id", 0), r.get("id", 0))
                    continue

            merged.append({
                "id": r.get("id"),
                "sql_query": r.get("sql_query"),
                "params_obj": params_obj,
                "group_id": r.get("group_id"),
                "schema_name": r.get("schema_name"),
                "table_name": r.get("table_name"),
                "created_at": r.get("created_at"),
            })

        out = []
        for m in merged:
            out.append({
                "id": m.get("id"),
                "sql_query": m.get("sql_query"),
                "params": json.dumps(m.get("params_obj")) if isinstance(m.get("params_obj"), (dict, list)) else m.get("params_obj"),
                "group_id": m.get("group_id"),
                "schema_name": m.get("schema_name"),
                "table_name": m.get("table_name"),
                "created_at": m.get("created_at"),
            })
        return out

    def get_last_committed_pos(self) -> Tuple[Optional[str], Optional[int]]:
        """Get the last binlog position safely committed to raw_events"""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT binlog_file, binlog_pos FROM raw_events ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            return row[0], row[1]
        return None, None

    def get_checkpoint(self) -> Tuple[Optional[str], Optional[int], Optional[str]]:
        """Return (binlog_file, binlog_pos, gtid) from producer checkpoint table."""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT binlog_file, binlog_pos, gtid FROM checkpoint WHERE id = 1")
        row = cursor.fetchone()
        if not row:
            return None, None, None
        return row[0], row[1], row[2]

    def insert_raw_events(
        self,
        events: List[Dict],
        checkpoint_file: Optional[str] = None,
        checkpoint_pos: Optional[int] = None,
        gtid: Optional[str] = None,
    ):
        """Bulk insert raw events; optionally update checkpoint in the same transaction."""
        if not events and checkpoint_file is None and gtid is None:
            return

        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        try:
            if events:
                data = []
                for e in events:
                    data.append((
                        e['binlog_file'],
                        e['binlog_pos'],
                        e.get('schema'),
                        e.get('table'),
                        e.get('event_type'),
                        json.dumps(e.get('event_data', {})),
                    ))
                cursor.executemany("""
                    INSERT INTO raw_events (binlog_file, binlog_pos, schema_name, table_name, event_type, event_data)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, data)

            if events and checkpoint_file is None:
                checkpoint_file = events[-1].get("binlog_file")
                checkpoint_pos = events[-1].get("binlog_pos")
            if checkpoint_file is not None or gtid is not None:
                cursor.execute("""
                    INSERT INTO checkpoint (id, binlog_file, binlog_pos, gtid, updated_at)
                    VALUES (1, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(id) DO UPDATE SET
                        binlog_file=COALESCE(excluded.binlog_file, checkpoint.binlog_file),
                        binlog_pos=COALESCE(excluded.binlog_pos, checkpoint.binlog_pos),
                        gtid=COALESCE(excluded.gtid, checkpoint.gtid),
                        updated_at=CURRENT_TIMESTAMP
                """, (checkpoint_file, checkpoint_pos, gtid))
            cursor.execute("COMMIT")
        except Exception:
            try:
                cursor.execute("ROLLBACK")
            except Exception:
                pass
            raise

    def fetch_raw_events_batch(self, limit: int = 1000) -> List[Dict]:
        """Fetch oldest raw events for processing"""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, binlog_file, binlog_pos, schema_name, table_name, event_type, event_data 
            FROM raw_events 
            ORDER BY id ASC 
            LIMIT ?
        """, (limit,))
        
        rows = []
        for r in cursor.fetchall():
            rows.append({
                'id': r[0],
                'binlog_file': r[1],
                'binlog_pos': r[2],
                'schema': r[3],
                'table': r[4],
                'event_type': r[5],
                'event_data': json.loads(r[6]) if r[6] else {}
            })
        return rows

    def commit_prepared_queries(self, queries: List[Dict], processed_event_ids: List[int]):
        """
        Atomic transaction:
        1. Insert prepared queries
        2. Delete or archive processed raw events (depending on db_debug)
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        try:
            if queries:
                q_data = []
                for q in queries:
                    q_data.append((
                        q['sql'],
                        json.dumps(q.get('params')),
                        q.get('group_id'),
                        q.get('schema'),
                        q.get('table')
                    ))
                cursor.executemany("""
                    INSERT INTO prepared_queries (sql_query, params, group_id, schema_name, table_name)
                    VALUES (?, ?, ?, ?, ?)
                """, q_data)

            if processed_event_ids:
                placeholders = ','.join(['?'] * len(processed_event_ids))
                if self.db_debug:
                    cursor.execute(f"""
                        SELECT id, binlog_file, binlog_pos, schema_name, table_name, event_type, event_data, created_at
                        FROM raw_events WHERE id IN ({placeholders})
                    """, processed_event_ids)
                    event_rows = [
                        {"id": r[0], "binlog_file": r[1], "binlog_pos": r[2], "schema_name": r[3], "table_name": r[4],
                         "event_type": r[5], "event_data": r[6], "created_at": r[7]}
                        for r in cursor.fetchall()
                    ]
                    self._send_processed_events_to_ch(event_rows)
                cursor.execute(f"DELETE FROM raw_events WHERE id IN ({placeholders})", processed_event_ids)
            cursor.execute("COMMIT")
        except Exception:
            try:
                cursor.execute("ROLLBACK")
            except Exception:
                pass
            raise

    def fetch_prepared_queries_batch(self, limit: int = 100) -> List[Dict]:
        """Fetch oldest prepared queries"""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, sql_query, params, group_id, schema_name, table_name 
            FROM prepared_queries 
            ORDER BY id ASC 
            LIMIT ?
        """, (limit,))
        
        rows = []
        for r in cursor.fetchall():
            rows.append({
                'id': r[0],
                'sql': r[1],
                'params': json.loads(r[2]) if r[2] else None,
                'group_id': r[3],
                'schema': r[4],
                'table': r[5]
            })
        return rows

    def delete_prepared_queries(self, query_ids: List[int]):
        """Delete processed queries; when db_debug send them to ClickHouse first."""
        if not query_ids:
            return
        conn = self._get_conn()
        cursor = conn.cursor()
        placeholders = ','.join(['?'] * len(query_ids))
        if self.db_debug:
            cursor.execute(f"""
                SELECT id, sql_query, params, group_id, schema_name, table_name, created_at
                FROM prepared_queries WHERE id IN ({placeholders})
            """, query_ids)
            query_rows = [
                {"id": r[0], "sql_query": r[1], "params": r[2], "group_id": r[3], "schema_name": r[4], "table_name": r[5], "created_at": r[6]}
                for r in cursor.fetchall()
            ]
            merged_query_rows = self._merge_debug_query_rows(query_rows)
            self._send_processed_queries_to_ch(merged_query_rows)
        cursor.execute(f"DELETE FROM prepared_queries WHERE id IN ({placeholders})", query_ids)

    def move_to_failed(self, queries: List[Dict], error_reason: str):
        """
        Atomically move prepared queries to failed_queries and delete them from prepared_queries.
        queries: list of dicts with keys id, sql, params, schema, table
        """
        if not queries:
            return
        conn = self._get_conn()
        cursor = conn.cursor()
        query_ids = [q["id"] for q in queries if q.get("id") is not None]
        if not query_ids:
            return
        # Explicit immediate transaction for multi-statement atomicity under autocommit
        cursor.execute("BEGIN IMMEDIATE")
        try:
            for q in queries:
                cursor.execute("""
                    INSERT OR REPLACE INTO failed_queries
                    (id, sql_query, params, schema_name, table_name, error_reason, failed_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    q["id"],
                    q.get("sql"),
                    json.dumps(q.get("params")) if q.get("params") is not None else None,
                    q.get("schema"),
                    q.get("table"),
                    error_reason[:2000] if error_reason else None,
                ))
            placeholders = ','.join(['?'] * len(query_ids))
            cursor.execute(f"DELETE FROM prepared_queries WHERE id IN ({placeholders})", query_ids)
            cursor.execute("COMMIT")
            log.warning("Moved %d queries to failed_queries: %s", len(query_ids), error_reason[:200])
        except Exception:
            try:
                cursor.execute("ROLLBACK")
            except Exception:
                pass
            raise

    def clear_raw_events(self):
        """Clear all raw events from the table"""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM raw_events")
        log.info("Cleared all raw_events from buffer database")

    def get_queue_stats(self) -> Dict:
        """Get stats for monitoring"""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw_events")
        raw_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM prepared_queries")
        prep_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM failed_queries")
        failed_count = cursor.fetchone()[0]
        return {"raw_events": raw_count, "prepared_queries": prep_count, "failed_queries": failed_count}
