import logging
import sys
import time
from datetime import datetime
from mysql_client import MySQLClient
from state_json import StateJson
from buffer import BufferDB
from notifications import notify_cdc_error
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from pymysqlreplication.event import QueryEvent

log = logging.getLogger(__name__)

class PipelineProducer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.mysql_cfg = cfg["mysql"]
        self.mig_cfg = cfg.get("migration", {})
        self.cdc_cfg = self.mig_cfg.get("cdc", {})
        
        self.buffer = BufferDB()
        self.state = StateJson(cfg.get("state_file"))
        self.mysql_client = MySQLClient(self.mysql_cfg)
        
        # Use process ID to make server_id unique per process instance
        # This prevents conflicts when multiple migres instances run or restart quickly
        import os
        base_server_id = int(self.cdc_cfg.get("server_id", 4379))
        pid = os.getpid()
        # Combine base server_id with PID modulo to ensure uniqueness
        # PID can be large, so use modulo to keep it reasonable
        self.server_id = base_server_id + (pid % 1000)
        self.heartbeat_seconds = int(self.cdc_cfg.get("heartbeat_seconds", 5))
        
        # Determine start position
        # Priority: 1. Buffer's last committed position, 2. State file
        buf_file, buf_pos = self.buffer.get_last_committed_pos()
        state_binlog = self.state.get_binlog()
        
        if buf_file and buf_pos:
            self.start_file = buf_file
            self.start_pos = buf_pos
            if self.mig_cfg.get('debug'):
                log.info(f"Producer starting from Buffer position: {self.start_file}:{self.start_pos}")
        elif state_binlog:
            self.start_file = state_binlog["file"]
            self.start_pos = state_binlog["pos"]
            if self.mig_cfg.get('debug'):
                log.info(f"Producer starting from State file position: {self.start_file}:{self.start_pos}")
        else:
            self.start_file = None
            self.start_pos = None
            if self.mig_cfg.get('debug'):
                log.info("Producer starting from current master position")

    def _serialize_event(self, event):
        """Serialize binlog event for storage"""
        base = {
            "type": event.__class__.__name__,
            "timestamp": self._serialize_value(getattr(event, 'timestamp', None)),
            "log_pos": getattr(event.packet, 'log_pos', None) if hasattr(event, 'packet') else None,
        }
        
        if hasattr(event, "schema"):
            base["schema"] = self._serialize_value(event.schema)
        if hasattr(event, "table"):
            base["table"] = self._serialize_value(event.table)
            
        if hasattr(event, "rows"):
            # Serialize rows
            # Handle bytes and other non-serializable types
            serialized_rows = []
            for row in event.rows:
                serialized_row = {}
                if "values" in row:
                    serialized_row["values"] = self._serialize_dict(row["values"])
                if "before_values" in row:
                    serialized_row["before_values"] = self._serialize_dict(row["before_values"])
                if "after_values" in row:
                    serialized_row["after_values"] = self._serialize_dict(row["after_values"])
                serialized_rows.append(serialized_row)
            base["rows"] = serialized_rows
            
        if hasattr(event, "query"):
            base["query"] = self._serialize_value(event.query)
            
        return base

    def _serialize_value(self, v):
        """Recursively serialize a value to JSON-compatible types"""
        if v is None:
            return None
        elif isinstance(v, bytes):
            try:
                return v.decode('utf-8')
            except UnicodeDecodeError:
                import base64
                return base64.b64encode(v).decode('ascii')
        elif isinstance(v, (datetime,)):
            return v.isoformat()
        elif isinstance(v, dict):
            return {k: self._serialize_value(val) for k, val in v.items()}
        elif isinstance(v, (list, tuple)):
            return [self._serialize_value(item) for item in v]
        elif isinstance(v, (int, float, str, bool)):
            return v
        else:
            # Fallback for unknown types
            return str(v)

    def _serialize_dict(self, d):
        """Helper to serialize dictionary values"""
        return {k: self._serialize_value(v) for k, v in d.items()}

    def _create_stream(self, log_file=None, log_pos=None):
        """Create a new BinLogStreamReader with current settings"""
        only_schemas = [self.mysql_cfg["database"]]
        included = set(self.mysql_cfg.get("include_tables") or [])
        excluded = set(self.mysql_cfg.get("exclude_tables") or [])
        only_tables = list(included) if included else None
        ignored_tables = list(excluded) if excluded else None

        # Connection settings with timeouts to prevent connection drops
        connection_settings = {
            "host": self.mysql_cfg["host"],
            "port": self.mysql_cfg["port"] or 3306,
            "user": self.mysql_cfg["user"],
            "passwd": self.mysql_cfg["password"],
            "connect_timeout": 60,  # Time to wait for initial connection
            "read_timeout": 60,  # Time to wait for read operations
            "write_timeout": 60,  # Time to wait for write operations
            "autocommit": False,
            "charset": "utf8mb4",
        }
        
        return BinLogStreamReader(
            connection_settings=connection_settings,
            server_id=self.server_id,
            blocking=False,  # Non-blocking to allow time-based flushing
            resume_stream=True,
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent],
            only_schemas=only_schemas,
            only_tables=only_tables,
            ignored_tables=ignored_tables,
            log_file=log_file or self.start_file,
            log_pos=log_pos or self.start_pos,
            slave_heartbeat=self.heartbeat_seconds,
        )

    def run(self):
        if self.mig_cfg.get('debug'):
            log.info("Starting Pipeline Producer...")
        self.mysql_client.connect()

        stream = self._create_stream()
        
        # Track current position for reconnection
        current_file = self.start_file
        current_pos = self.start_pos

        batch_size = 100
        flush_interval = self.cdc_cfg.get("batch_delay_seconds", 5)  # Flush batch every 5 seconds even if not full
        batch = []
        last_flush_time = time.time()
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        try:
            while True:
                try:
                    event = stream.fetchone()
                    consecutive_errors = 0  # Reset on success
                except Exception as conn_err:
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        log.error(f"Too many consecutive connection errors ({consecutive_errors}), giving up")
                        raise
                    
                    # Flush any pending batch before reconnecting
                    if batch:
                        try:
                            self.buffer.insert_raw_events(batch)
                            if self.mig_cfg.get('debug'):
                                log.info(f"Producer flushed {len(batch)} events before reconnect")
                            batch = []
                        except Exception as flush_err:
                            log.warning(f"Failed to flush batch before reconnect: {flush_err}")
                    
                    wait_time = min(5 * consecutive_errors, 30)  # Exponential backoff, max 30s
                    log.warning(f"BinLog stream error: {conn_err}, reconnecting in {wait_time}s... (attempt {consecutive_errors})")
                    time.sleep(wait_time)
                    
                    # Close old stream and create new one from last known position
                    try:
                        stream.close()
                    except Exception:
                        pass
                    
                    # Brief pause to allow MySQL to release the connection
                    time.sleep(1)
                    
                    stream = self._create_stream(current_file, current_pos)
                    last_flush_time = time.time()
                    continue
                
                # No event available - check if we should flush
                if event is None:
                    now = time.time()
                    if batch and (now - last_flush_time >= flush_interval):
                        self.buffer.insert_raw_events(batch)
                        if self.mig_cfg.get('debug'):
                            log.info(f"Producer flushed {len(batch)} events (time-based)")
                        batch = []
                        last_flush_time = now
                    time.sleep(0.1)  # Small sleep to avoid busy-waiting
                    continue
                # Handle DDL and Data events
                
                # Filter DDL
                if isinstance(event, QueryEvent):
                    query = (event.query or "").lower().strip()
                    # Skip transaction control statements - they're not DDL and don't need processing
                    if query in ("begin", "commit", "rollback", "xa start", "xa end", "xa prepare", "xa commit", "xa rollback"):
                        continue
                    
                    # For CREATE TABLE DDL, check if table is in include_tables
                    # QueryEvent might be filtered by BinLogStreamReader's only_tables,
                    # but we need to ensure CREATE TABLE for included tables is captured
                    included = set(self.mysql_cfg.get("include_tables") or [])
                    if included:
                        import re
                        create_match = re.search(r'create\s+table\s+(?:if\s+not\s+exists\s+)?(?:`?\w+`?\.)?`?(\w+)`?', query, re.IGNORECASE)
                        if create_match:
                            table_name = create_match.group(1)
                            if table_name not in included:
                                log.debug(f"Skipping CREATE TABLE for {table_name} (not in include_tables)")
                                continue
                    
                # Serialize
                try:
                    event_data = self._serialize_event(event)
                except Exception as e:
                    log.error(f"Failed to serialize event: {e}")
                    continue

                # Track current position for reconnection
                current_file = stream.log_file
                current_pos = stream.log_pos

                # Add to batch
                item = {
                    "binlog_file": current_file,
                    "binlog_pos": current_pos,
                    "schema": getattr(event, "schema", None),
                    "table": getattr(event, "table", None),
                    "event_type": event.__class__.__name__,
                    "event_data": event_data
                }
                
                # Handle byte decoding for schema/table
                if isinstance(item["schema"], bytes):
                    item["schema"] = item["schema"].decode('utf-8')
                if isinstance(item["table"], bytes):
                    item["table"] = item["table"].decode('utf-8')

                batch.append(item)
                
                # Commit if batch full OR time elapsed (flush_interval seconds)
                now = time.time()
                should_flush = len(batch) >= batch_size or (now - last_flush_time >= flush_interval and batch)
                
                if should_flush:
                    self.buffer.insert_raw_events(batch)
                    if self.mig_cfg.get('debug'):
                        log.info(f"Producer committed {len(batch)} events up to {current_file}:{current_pos}")
                    batch = []
                    last_flush_time = now

        except Exception as e:
            log.exception("Pipeline Producer failed")
            notify_cdc_error("CDC Producer Failure", "N/A", str(e))
            sys.exit(1)
        finally:
            try:
                stream.close()
            except Exception:
                pass
            # mysql_client is not used by producer (BinLogStreamReader has its own connection)
