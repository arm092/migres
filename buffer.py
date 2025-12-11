import sqlite3
import logging
import json
import threading
import time
from typing import List, Dict, Tuple, Optional

log = logging.getLogger(__name__)

class BufferDB:
    def __init__(self, db_path: str = "data/buffer.db"):
        import os
        # Resolve to absolute path to avoid issues with working directory
        self.db_path = os.path.abspath(db_path)
        # Ensure directory exists before initialization
        dir_path = os.path.dirname(self.db_path) or "."
        try:
            os.makedirs(dir_path, exist_ok=True)
            # Test if we can actually write to this location
            test_file = os.path.join(dir_path, ".buffer_test")
            try:
                with open(test_file, 'w') as f:
                    f.write("test")
                os.remove(test_file)
            except Exception:
                # If we can't write to the default location, use /tmp as fallback
                # This handles Windows Docker volume mount permission issues
                log.warning(f"Cannot write to {dir_path}, using /tmp for buffer database")
                self.db_path = "/tmp/buffer.db"
                os.makedirs("/tmp", exist_ok=True)
        except Exception as e:
            log.warning(f"Failed to create directory {dir_path}, using /tmp: {e}")
            self.db_path = "/tmp/buffer.db"
            os.makedirs("/tmp", exist_ok=True)
        self.local = threading.local()
        self._init_db()

    def _get_conn(self):
        """Get thread-local connection"""
        if not hasattr(self.local, 'conn'):
            import os
            # Ensure directory exists (double-check for thread safety)
            dir_path = os.path.dirname(self.db_path) or "."
            try:
                os.makedirs(dir_path, exist_ok=True)
            except Exception as e:
                log.warning(f"Failed to create directory {dir_path}, using /tmp: {e}")
                self.db_path = "/tmp/buffer.db"
                dir_path = "/tmp"
                os.makedirs(dir_path, exist_ok=True)
            
            # Try to connect, with fallback to /tmp if it fails
            try:
                self.local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            except sqlite3.OperationalError as e:
                if "unable to open database file" in str(e) and self.db_path != "/tmp/buffer.db":
                    log.warning(f"Cannot create database at {self.db_path} (likely Windows Docker volume issue), using /tmp/buffer.db")
                    self.db_path = "/tmp/buffer.db"
                    os.makedirs("/tmp", exist_ok=True)
                    self.local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
                else:
                    log.error(f"Failed to connect to database at {self.db_path}: {e}")
                    raise
            # Enable WAL mode for better concurrency
            self.local.conn.execute("PRAGMA journal_mode=WAL")
            self.local.conn.execute("PRAGMA synchronous=NORMAL")
        return self.local.conn

    def _init_db(self):
        """Initialize database schema"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        # Table 1: Raw Events (Stage 1 Output)
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
        
        # Index for faster lookups and order
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_events_pos ON raw_events (binlog_file, binlog_pos)")
        
        # Table 2: Prepared Queries (Stage 2 Output)
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
        
        # Table 3: Failed Queries (for manual inspection)
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
        
        conn.commit()

    def get_last_committed_pos(self) -> Tuple[Optional[str], Optional[int]]:
        """Get the last binlog position safely committed to raw_events"""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT binlog_file, binlog_pos FROM raw_events ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            return row[0], row[1]
        return None, None

    def insert_raw_events(self, events: List[Dict]):
        """Bulk insert raw events"""
        if not events:
            return
        
        conn = self._get_conn()
        cursor = conn.cursor()
        
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
        conn.commit()

    def fetch_raw_events_batch(self, limit: int = 1000) -> List[Dict]:
        """Fetch oldest raw events for processing"""
        conn = self._get_conn()
        cursor = conn.cursor()
        # Use rowid/id for stable ordering
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
        2. Delete processed raw events
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        
        try:
            # 1. Insert Queries
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
            
            # 2. Delete Raw Events
            if processed_event_ids:
                # SQLite has limits on variable count, split if needed but usually batch size is small
                placeholders = ','.join(['?'] * len(processed_event_ids))
                cursor.execute(f"DELETE FROM raw_events WHERE id IN ({placeholders})", processed_event_ids)
                
            conn.commit()
        except Exception:
            conn.rollback()
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
        """Delete processed queries"""
        if not query_ids:
            return
        conn = self._get_conn()
        cursor = conn.cursor()
        placeholders = ','.join(['?'] * len(query_ids))
        cursor.execute(f"DELETE FROM prepared_queries WHERE id IN ({placeholders})", query_ids)
        conn.commit()


    def get_queue_stats(self) -> Dict:
        """Get stats for monitoring"""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw_events")
        raw_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM prepared_queries")
        prep_count = cursor.fetchone()[0]
        return {"raw_events": raw_count, "prepared_queries": prep_count}

