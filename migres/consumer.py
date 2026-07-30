import logging
import queue
import re
import threading
import time
from datetime import datetime, date
from decimal import Decimal
from collections import defaultdict

from migres.clients.clickhouse import CHClient
from migres.buffer import BufferDB
from migres.notifications import notify_cdc_error
from migres.schema.ddl import quote_ident

log = logging.getLogger(__name__)

ISO_DATETIME_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")
ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_TRANSIENT_MARKERS = (
    "timeout", "timed out", "connection", "network", "broken pipe",
    "temporary", "socket", "eof", "connection reset", "connection refused",
    "server closed", "lost connection", "cannot connect",
)


def _inner_column_type(column_type: str) -> str:
    column_type_normalized = column_type.lower().strip()
    if column_type_normalized.startswith("nullable(") and column_type_normalized.endswith(")"):
        return column_type_normalized[9:-1].strip()
    return column_type_normalized


def _default_for_non_nullable(column_type: str):
    """Best-effort default when binlog/value is NULL but CH column is non-Nullable."""
    inner = _inner_column_type(column_type)
    if inner.startswith(("int", "uint")) or inner in ("bool", "boolean"):
        return 0
    if inner.startswith("float") or inner.startswith("decimal"):
        return 0
    if inner == "date" or (inner.startswith("date") and "datetime" not in inner):
        return date(1970, 1, 1)
    if "datetime" in inner:
        return datetime(1970, 1, 1)
    return ""


def _convert_for_clickhouse(v, column_type=None):
    import json
    if v is None:
        if column_type and not column_type.lower().strip().startswith("nullable("):
            return _default_for_non_nullable(column_type)
        return None
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8")
        except UnicodeDecodeError:
            return v.decode("latin-1")
    if isinstance(v, (datetime, date)):
        return v
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False, default=str)
    if column_type:
        inner_type = _inner_column_type(column_type)
        if "decimal" in inner_type:
            if isinstance(v, Decimal):
                return v
            if isinstance(v, (str, float, int)):
                try:
                    return Decimal(str(v))
                except Exception:
                    return v
        if inner_type == "date" or (inner_type.startswith("date") and "datetime" not in inner_type):
            if isinstance(v, str) and ISO_DATE_PATTERN.match(v):
                try:
                    return date.fromisoformat(v)
                except (ValueError, TypeError):
                    return v
        elif "datetime" in inner_type:
            if isinstance(v, str) and ISO_DATETIME_PATTERN.match(v):
                try:
                    if "T" in v:
                        return datetime.fromisoformat(v.replace("Z", "+00:00"))
                    return datetime.strptime(v[:19], "%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError):
                    return v
    return v


def _deserialize_value(v):
    if v is None:
        return None
    return v


def _deserialize_row(row):
    return [_deserialize_value(v) for v in row]


def _convert_row_for_clickhouse(row, column_types=None):
    if column_types and len(column_types) == len(row):
        return [_convert_for_clickhouse(v, column_types[i]) for i, v in enumerate(row)]
    return [_convert_for_clickhouse(v) for v in row]


class PipelineConsumer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.mig_cfg = cfg.get("migration", {})
        cdc_cfg = self.mig_cfg.get("cdc", {})
        db_debug = cdc_cfg.get("db_debug", False)
        self.buffer = BufferDB(db_debug=db_debug, cfg=cfg)
        self.ch = CHClient(cfg["clickhouse"], self.mig_cfg)
        self._slow_query_threshold = 5.0
        self._column_types_cache = {}
        self._column_types_cache_lock = threading.Lock()
        self._shutdown_flag = threading.Event()
        self._ch_lock = threading.Lock()  # clickhouse-driver client is not thread-safe
        self._table_queues = {}
        self._table_threads = {}
        self._table_inflight = defaultdict(int)
        self._table_state_lock = threading.Lock()
        self._fatal_error = None

        if hasattr(cdc_cfg, "resolved_consumer_poll_interval"):
            self.poll_interval = float(cdc_cfg.resolved_consumer_poll_interval())
        else:
            self.poll_interval = float(
                cdc_cfg.get("consumer_poll_interval")
                or cdc_cfg.get("batch_delay_seconds", 5)
                or 0.1
            )

    def _ch_table_ref(self, table: str) -> str:
        if "." in table:
            parts = table.split(".", 1)
            return f"{quote_ident(parts[0])}.{quote_ident(parts[1])}"
        return f"{quote_ident(self.ch.db)}.{quote_ident(table)}"

    def _is_transient_error(self, exc: Exception) -> bool:
        msg = str(exc).lower()
        return any(marker in msg for marker in _TRANSIENT_MARKERS)

    def _is_table_not_exists_error(self, exc: Exception) -> bool:
        msg = str(exc).lower()
        return (
            "doesn't exist" in msg
            or "does not exist" in msg
            or ("table" in msg and "exist" in msg)
        )

    def _get_column_types(self, table, sql):
        with self._column_types_cache_lock:
            if table in self._column_types_cache:
                cached_cols, cached_types = self._column_types_cache[table]
                match = re.search(r"INSERT INTO[^(]*\(([^)]+)\)", sql, re.IGNORECASE)
                if match:
                    sql_columns = [col.strip().strip("`") for col in match.group(1).split(",")]
                    if sql_columns == cached_cols:
                        return cached_types

        table_match = re.search(
            r"INSERT INTO\s+`([^`]+)`\.`([^`]+)`", sql, re.IGNORECASE,
        )
        if table_match:
            db_name, table_name = table_match.groups()
            full_table_sql = f"{quote_ident(db_name)}.{quote_ident(table_name)}"
        else:
            table_match = re.search(r"INSERT INTO\s+`([^`]+)`", sql, re.IGNORECASE)
            if table_match:
                full_table_sql = self._ch_table_ref(table_match.group(1))
            else:
                full_table_sql = self._ch_table_ref(table)

        match = re.search(r"INSERT INTO[^(]*\(([^)]+)\)", sql, re.IGNORECASE)
        if not match:
            return None

        sql_columns = [col.strip().strip("`") for col in match.group(1).split(",")]

        try:
            with self._ch_lock:
                result = self.ch.execute(f"DESCRIBE TABLE {full_table_sql}")
            column_info = {row[0]: row[1] for row in result}

            missing = [col for col in sql_columns if col not in column_info]
            if missing:
                log.warning(
                    "Columns missing from DESCRIBE for table %s: %s; defaulting missing to String",
                    table, missing,
                )
                with self._column_types_cache_lock:
                    self._column_types_cache.pop(table, None)

            column_types = [column_info.get(col, "String") for col in sql_columns]

            if not missing:
                with self._column_types_cache_lock:
                    self._column_types_cache[table] = (sql_columns, column_types)

            return column_types
        except Exception as e:
            log.warning("Failed to get column types for table %s: %s", table, e)
            with self._column_types_cache_lock:
                self._column_types_cache.pop(table, None)
            return None

    def _execute_insert(self, sql, params, table):
        if params:
            deserialized_params = [_deserialize_row(row) for row in params]
            column_types = self._get_column_types(table, sql)
            if column_types is None:
                log.warning("Could not get column types for %s, converting without type info", table)
            clickhouse_params = [
                _convert_row_for_clickhouse(row, column_types) for row in deserialized_params
            ]
            with self._ch_lock:
                self.ch.client.execute(sql, clickhouse_params)
        else:
            with self._ch_lock:
                self.ch.execute(sql)

    def _execute_one(self, q):
        query_ids = [q["id"]]
        sql = q["sql"]
        params = q["params"]
        table = q.get("table")
        group_id = q.get("group_id")
        is_ddl = q.get("is_ddl", False)

        try:
            if is_ddl:
                start_exec = time.time()
                with self._ch_lock:
                    self.ch.execute(sql)
                exec_time = time.time() - start_exec
                if self.mig_cfg.get("debug"):
                    log.info(
                        "Consumer: DDL executed (group_id=%s, ids=%s): %.2fs",
                        group_id, query_ids, exec_time,
                    )
                self.buffer.delete_prepared_queries(query_ids)
                return

            start_exec = time.time()
            retry_count = 0
            max_retries = 1
            row_count = len(params) if params else 0

            while retry_count <= max_retries:
                try:
                    self._execute_insert(sql, params, table)
                    break
                except Exception as exec_err:
                    if self._is_table_not_exists_error(exec_err) and retry_count < max_retries:
                        with self._column_types_cache_lock:
                            self._column_types_cache.pop(table, None)
                        log.warning(
                            "Table %s doesn't exist, retrying after 2s (attempt %d/%d)",
                            table, retry_count + 1, max_retries + 1,
                        )
                        time.sleep(2)
                        retry_count += 1
                        continue
                    raise

            exec_time = time.time() - start_exec
            if exec_time > self._slow_query_threshold:
                log.warning(
                    "Consumer: SLOW query took %.2fs for table %s (%d rows)",
                    exec_time, table, row_count,
                )
            log.info(
                "Consumer: INSERT %d rows into %s (group_id=%s, ids=%s) (%.2fs)",
                row_count, table, group_id, query_ids, exec_time,
            )
            self.buffer.delete_prepared_queries(query_ids)

        except Exception as e:
            log.exception(
                "Consumer failed to execute query ids=%s table=%s schema=%s",
                query_ids, q.get("table"), q.get("schema"),
            )
            log.error("Failed SQL: %s", sql)
            if params:
                log.error("Failed params (first row): %s", params[0] if params else None)

            if self._is_transient_error(e):
                self._fatal_error = e
                self._shutdown_flag.set()
                raise

            error_msg = f"Execution failed: {e}"
            notify_cdc_error(
                "Consumer Error",
                f"{q.get('schema')}.{q.get('table')}",
                error_msg,
                {
                    "Query IDs": query_ids,
                    "Schema": q.get("schema"),
                    "Table": q.get("table"),
                    "Error Type": type(e).__name__,
                },
                exc=e,
            )
            self.buffer.move_to_failed([q], error_msg)

    def _table_worker(self, table_name, q: queue.Queue):
        while True:
            item = q.get()
            try:
                if item is None:
                    return
                self._execute_one(item)
            except Exception:
                # fatal transient already sets shutdown
                pass
            finally:
                with self._table_state_lock:
                    self._table_inflight[table_name] = max(0, self._table_inflight[table_name] - 1)
                q.task_done()

    def _ensure_table_worker(self, table_name):
        with self._table_state_lock:
            if table_name not in self._table_queues:
                q = queue.Queue()
                t = threading.Thread(
                    target=self._table_worker,
                    args=(table_name, q),
                    name=f"Consumer-{table_name}",
                    daemon=True,
                )
                self._table_queues[table_name] = q
                self._table_threads[table_name] = t
                t.start()
            return self._table_queues[table_name]

    def _enqueue_table(self, table_name, item):
        key = table_name or "__unknown__"
        q = self._ensure_table_worker(key)
        with self._table_state_lock:
            self._table_inflight[key] += 1
        q.put(item)

    def _wait_table_workers_idle(self, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._table_state_lock:
                inflight = sum(self._table_inflight.values())
            if inflight == 0:
                return
            if self._shutdown_flag.is_set():
                return
            time.sleep(0.05)

    def _stop_table_workers(self):
        with self._table_state_lock:
            keys = list(self._table_queues.keys())
        for key in keys:
            self._table_queues[key].put(None)
        for key in keys:
            self._table_threads[key].join(timeout=10)

    def run(self):
        log.info("Starting Pipeline Consumer (per-table workers)...")

        try:
            while not self._shutdown_flag.is_set():
                if self._fatal_error:
                    raise self._fatal_error

                try:
                    cdc_cfg = self.mig_cfg.get("cdc", {})
                    limit = cdc_cfg.get("prepared_queries_batch_limit", 100)

                    queries = self.buffer.fetch_prepared_queries_batch(limit=limit)

                    if not queries:
                        time.sleep(self.poll_interval if self.poll_interval > 0 else 0.1)
                        continue

                    for q in queries:
                        item = {
                            "id": q.get("id"),
                            "sql": q.get("sql"),
                            "params": q.get("params"),
                            "table": q.get("table"),
                            "schema": q.get("schema"),
                            "group_id": q.get("group_id"),
                            "is_ddl": (q.get("sql") or "").strip().upper().startswith(
                                ("DROP ", "CREATE ", "ALTER ", "TRUNCATE ", "RENAME ")
                            ),
                        }
                        if item["is_ddl"]:
                            # Preserve global DDL ordering relative to prior DML
                            self._wait_table_workers_idle()
                            self._execute_one(item)
                        else:
                            self._enqueue_table(item.get("table"), item)

                    if len(queries) < limit and self.poll_interval > 0:
                        time.sleep(self.poll_interval)

                except Exception as e:
                    if self._shutdown_flag.is_set():
                        break
                    log.critical("Pipeline Consumer fatal error: %s", e)
                    notify_cdc_error(
                        "Consumer Fatal Error",
                        "N/A",
                        f"Consumer crashed: {e}",
                        {"Error Type": type(e).__name__},
                        exc=e,
                    )
                    raise
        finally:
            self._wait_table_workers_idle(timeout=30)
            self._stop_table_workers()
            log.info("Pipeline Consumer shutdown complete")
