import base64
import logging
import os
import sys
import threading
import time
from datetime import datetime, date, timedelta
from decimal import Decimal

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from pymysqlreplication.event import QueryEvent

from migres.clients.mysql import MySQLClient
from migres.state import StateJson
from migres.buffer import BufferDB
from migres.notifications import notify_cdc_error
from migres.schema.ddl import parse_ddl_table_name, strip_sql_leading_comments

log = logging.getLogger(__name__)

_TX_CONTROL = frozenset({
    "begin", "commit", "rollback",
    "xa start", "xa end", "xa prepare", "xa commit", "xa rollback",
})


class PipelineProducer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.mysql_cfg = cfg["mysql"]
        self.mig_cfg = cfg.get("migration", {})
        self.cdc_cfg = self.mig_cfg.get("cdc", {})

        db_debug = self.cdc_cfg.get("db_debug", False)
        self.buffer = BufferDB(db_debug=db_debug, cfg=cfg)
        self.state = StateJson(cfg.get("state_file"))
        self.mysql_client = MySQLClient(self.mysql_cfg)
        self._shutdown_flag = threading.Event()

        base_server_id = int(self.cdc_cfg.get("server_id", 4379))
        pid = os.getpid()
        self.server_id = base_server_id + (pid % 1000)
        self.heartbeat_seconds = int(self.cdc_cfg.get("heartbeat_seconds", 5))
        self.use_gtid = bool(self.cdc_cfg.get("use_gtid", False))
        self.raw_events_max = int(self.cdc_cfg.get("raw_events_max", 50000) or 50000)
        self.raw_events_resume = int(
            self.raw_events_max * float(self.cdc_cfg.get("raw_events_resume_ratio", 0.8) or 0.8)
        )

        if hasattr(self.cdc_cfg, "resolved_producer_flush_interval"):
            self.flush_interval = float(self.cdc_cfg.resolved_producer_flush_interval())
        else:
            self.flush_interval = float(self.cdc_cfg.get("producer_flush_interval")
                                       or self.cdc_cfg.get("batch_delay_seconds", 5) or 0)

        cp_file, cp_pos, cp_gtid = self.buffer.get_checkpoint()
        buf_file, buf_pos = self.buffer.get_last_committed_pos()
        state_binlog = self.state.get_binlog()
        state_gtid = self.state.get_gtid()

        self.start_gtid = None
        self.start_file = None
        self.start_pos = None

        if self.use_gtid:
            self.start_gtid = cp_gtid or state_gtid
            if self.mig_cfg.get("debug"):
                log.info("Producer GTID mode; start_gtid=%s", self.start_gtid or "(current)")
        elif cp_file and cp_pos is not None:
            self.start_file = cp_file
            self.start_pos = cp_pos
            if self.mig_cfg.get("debug"):
                log.info("Producer starting from checkpoint: %s:%s", self.start_file, self.start_pos)
        elif buf_file and buf_pos is not None:
            self.start_file = buf_file
            self.start_pos = buf_pos
            if self.mig_cfg.get("debug"):
                log.info("Producer starting from Buffer position: %s:%s", self.start_file, self.start_pos)
        elif state_binlog:
            self.start_file = state_binlog["file"]
            self.start_pos = state_binlog["pos"]
            if self.mig_cfg.get("debug"):
                log.info("Producer starting from State file position: %s:%s", self.start_file, self.start_pos)
        else:
            if self.mig_cfg.get("debug"):
                log.info("Producer starting from current master position")

    def _serialize_value(self, v):
        """Recursively serialize a value to JSON-compatible types."""
        if v is None:
            return None
        if isinstance(v, bytes):
            try:
                return v.decode("utf-8")
            except UnicodeDecodeError:
                return base64.b64encode(v).decode("ascii")
        if isinstance(v, Decimal):
            return str(v)
        if isinstance(v, set):
            return ",".join(str(x) for x in sorted(v, key=str))
        if isinstance(v, timedelta):
            total_seconds = int(v.total_seconds())
            if total_seconds < 0:
                sign = "-"
                total_seconds = abs(total_seconds)
            else:
                sign = ""
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, dict):
            return {k: self._serialize_value(val) for k, val in v.items()}
        if isinstance(v, (list, tuple)):
            return [self._serialize_value(item) for item in v]
        if isinstance(v, (int, float, str, bool)):
            return v
        return str(v)

    def _serialize_dict(self, d):
        """Serialize a row-values dict. Nested dict/list (JSON cols) become JSON text."""
        import json
        out = {}
        for k, v in d.items():
            if isinstance(v, (dict, list)):
                out[k] = json.dumps(v, ensure_ascii=False, default=str)
            else:
                out[k] = self._serialize_value(v)
        return out

    def _serialize_event(self, event):
        base = {
            "type": event.__class__.__name__,
            "timestamp": self._serialize_value(getattr(event, "timestamp", None)),
            "log_pos": getattr(event.packet, "log_pos", None) if hasattr(event, "packet") else None,
        }

        if hasattr(event, "schema"):
            base["schema"] = self._serialize_value(event.schema)
        if hasattr(event, "table"):
            base["table"] = self._serialize_value(event.table)

        if hasattr(event, "rows"):
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

    def _create_stream(self, log_file=None, log_pos=None, gtid=None):
        only_schemas = [self.mysql_cfg["database"]]
        included = set(self.mysql_cfg.get("include_tables") or [])
        excluded = set(self.mysql_cfg.get("exclude_tables") or [])
        only_tables = list(included) if included else None
        ignored_tables = list(excluded) if excluded else None

        connection_settings = {
            "host": self.mysql_cfg["host"],
            "port": self.mysql_cfg["port"] or 3306,
            "user": self.mysql_cfg["user"],
            "passwd": self.mysql_cfg["password"],
            "connect_timeout": 60,
            "read_timeout": 60,
            "write_timeout": 60,
            "autocommit": False,
            "charset": "utf8mb4",
        }

        if self.mysql_cfg.get("ssl_disabled"):
            pass
        elif self.mysql_cfg.get("ssl_ca"):
            connection_settings["ssl"] = {
                "ca": self.mysql_cfg["ssl_ca"],
                "check_hostname": False,
            }

        kwargs = dict(
            connection_settings=connection_settings,
            server_id=self.server_id,
            blocking=False,
            resume_stream=True,
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent],
            only_schemas=only_schemas,
            only_tables=only_tables,
            ignored_tables=ignored_tables,
            slave_heartbeat=self.heartbeat_seconds,
            skip_to_timestamp=None,
        )
        if self.use_gtid:
            # mysql-replication accepts GTID set string via auto_position
            auto_pos = gtid if gtid is not None else self.start_gtid
            kwargs["auto_position"] = auto_pos if auto_pos else True
        else:
            kwargs["log_file"] = log_file or self.start_file
            kwargs["log_pos"] = log_pos or self.start_pos
        return BinLogStreamReader(**kwargs)

    def _should_skip_query_event(self, event):
        query = event.query or ""
        cleaned = strip_sql_leading_comments(query).lower().strip()
        if cleaned in _TX_CONTROL or cleaned.startswith("savepoint"):
            return True

        included = set(self.mysql_cfg.get("include_tables") or [])
        if included:
            table_name = parse_ddl_table_name(query, "create")
            if table_name and table_name not in included:
                if self.mig_cfg.get("debug"):
                    log.debug("Skipping CREATE TABLE for %s (not in include_tables)", table_name)
                return True
        return False

    def _wait_for_backpressure(self):
        """Pause binlog reading when raw_events is above the configured limit."""
        if self.raw_events_max <= 0:
            return
        while not self._shutdown_flag.is_set():
            stats = self.buffer.get_queue_stats()
            raw_count = stats.get("raw_events", 0)
            if raw_count < self.raw_events_max:
                return
            log.warning(
                "Backpressure: raw_events=%d >= %d; pausing producer until below %d",
                raw_count, self.raw_events_max, self.raw_events_resume,
            )
            while not self._shutdown_flag.is_set():
                time.sleep(0.5)
                raw_count = self.buffer.get_queue_stats().get("raw_events", 0)
                if raw_count <= self.raw_events_resume:
                    log.info("Backpressure cleared: raw_events=%d", raw_count)
                    return

    def _flush_batch(self, batch, current_file, current_pos, current_gtid=None):
        if not batch:
            return
        self.buffer.insert_raw_events(
            batch,
            checkpoint_file=current_file,
            checkpoint_pos=current_pos,
            gtid=current_gtid,
        )
        if current_gtid:
            self.state.set_gtid(current_gtid)
        if self.mig_cfg.get("debug"):
            log.info(
                "Producer committed %d events up to %s:%s gtid=%s",
                len(batch), current_file, current_pos, current_gtid,
            )

    def run(self):
        if self.mig_cfg.get("debug"):
            log.info("Starting Pipeline Producer...")

        self.mysql_client.connect()
        try:
            self.mysql_client.assert_cdc_binlog_settings()
        except RuntimeError as e:
            log.error("CDC binlog settings check failed: %s", e)
            notify_cdc_error("CDC Producer Failure", "N/A", str(e), exc=e)
            sys.exit(1)

        stream = self._create_stream()
        current_file = self.start_file
        current_pos = self.start_pos
        current_gtid = self.start_gtid

        batch_size = self.cdc_cfg.get("producer_batch_size", 100)
        flush_interval = self.flush_interval
        batch = []
        last_flush_time = time.time()
        consecutive_errors = 0
        max_consecutive_errors = 10

        try:
            while not self._shutdown_flag.is_set():
                self._wait_for_backpressure()
                if self._shutdown_flag.is_set():
                    break

                try:
                    event = stream.fetchone()
                    consecutive_errors = 0
                except Exception as conn_err:
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        log.error("Too many consecutive connection errors (%d), giving up", consecutive_errors)
                        raise

                    if batch:
                        try:
                            self._flush_batch(batch, current_file, current_pos, current_gtid)
                            batch = []
                        except Exception as flush_err:
                            log.warning("Failed to flush batch before reconnect: %s", flush_err)

                    wait_time = min(5 * consecutive_errors, 30)
                    log.warning(
                        "BinLog stream error: %s, reconnecting in %ss... (attempt %d)",
                        conn_err, wait_time, consecutive_errors,
                    )
                    time.sleep(wait_time)

                    try:
                        stream.close()
                    except Exception:
                        pass

                    time.sleep(1)
                    stream = self._create_stream(current_file, current_pos, current_gtid)
                    last_flush_time = time.time()
                    continue

                if event is None:
                    now = time.time()
                    if batch and (flush_interval <= 0 or (now - last_flush_time >= flush_interval)):
                        self._flush_batch(batch, current_file, current_pos, current_gtid)
                        batch = []
                        last_flush_time = now
                    time.sleep(0.1)
                    continue

                if isinstance(event, QueryEvent) and self._should_skip_query_event(event):
                    continue

                try:
                    event_data = self._serialize_event(event)
                except Exception as e:
                    log.error("Failed to serialize event: %s", e)
                    continue

                current_file = stream.log_file
                current_pos = stream.log_pos
                if hasattr(stream, "gtid") and stream.gtid:
                    current_gtid = stream.gtid

                item = {
                    "binlog_file": current_file,
                    "binlog_pos": current_pos,
                    "schema": getattr(event, "schema", None),
                    "table": getattr(event, "table", None),
                    "event_type": event.__class__.__name__,
                    "event_data": event_data,
                }

                if isinstance(item["schema"], bytes):
                    item["schema"] = item["schema"].decode("utf-8")
                if isinstance(item["table"], bytes):
                    item["table"] = item["table"].decode("utf-8")

                batch.append(item)

                now = time.time()
                should_flush = (
                    len(batch) >= batch_size
                    or (batch and (flush_interval <= 0 or (now - last_flush_time >= flush_interval)))
                )
                if should_flush:
                    self._flush_batch(batch, current_file, current_pos, current_gtid)
                    batch = []
                    last_flush_time = now

        except Exception as e:
            log.exception("Pipeline Producer failed")
            notify_cdc_error("CDC Producer Failure", "N/A", str(e), exc=e)
            sys.exit(1)
        finally:
            if batch:
                try:
                    log.info("Producer flushing %d pending events before shutdown", len(batch))
                    self._flush_batch(batch, current_file, current_pos, current_gtid)
                    log.info("Producer flushed pending events successfully")
                except Exception as flush_err:
                    log.error("Failed to flush pending batch during shutdown: %s", flush_err)
            try:
                stream.close()
            except Exception:
                pass
            log.info("Pipeline Producer shutdown complete")
