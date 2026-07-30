import logging
import threading
import time

from migres.clients.mysql import MySQLClient
from migres.clients.clickhouse import CHClient
from migres.buffer import BufferDB
from migres.state import StateJson
from migres.schema.ddl import (
    quote_ident,
    map_with_low_cardinality,
    binlog_position_key,
    strip_sql_leading_comments,
    parse_drop_table_names,
    parse_rename_table_pairs,
    parse_ddl_table_name,
    detect_ddl_kind,
    build_table_ddl,
    ensure_clickhouse_columns,
    _default_expr_for_column,
)

log = logging.getLogger(__name__)

_TX_CONTROL = frozenset({
    "begin", "commit", "rollback",
    "xa start", "xa end", "xa prepare", "xa commit", "xa rollback",
})


class PipelineTransformer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.mig_cfg = cfg.get("migration", {})
        cdc_cfg = self.mig_cfg.get("cdc", {})
        db_debug = cdc_cfg.get("db_debug", False)
        self.buffer = BufferDB(db_debug=db_debug, cfg=cfg)
        self.state = StateJson(cfg.get("state_file"))
        self.mysql_client = MySQLClient(cfg["mysql"])
        self.ch = CHClient(cfg["clickhouse"], self.mig_cfg)
        self._last_commit_ns = 0
        self._shutdown_flag = threading.Event()
        self.table_cache = {}

    def _table_allowed(self, table: str) -> bool:
        if not table:
            return False
        included = set(self.cfg.get("mysql", {}).get("include_tables") or [])
        excluded = set(self.cfg.get("mysql", {}).get("exclude_tables") or [])
        if included and table not in included:
            return False
        if table in excluded:
            return False
        return True

    def _ch_table_ref(self, table: str) -> str:
        return f"{quote_ident(self.ch.db)}.{quote_ident(table)}"

    def _detect_ddl_kind(self, query: str):
        return detect_ddl_kind(query)

    def _is_transaction_control(self, query: str) -> bool:
        cleaned = strip_sql_leading_comments(query).lower().strip()
        return cleaned in _TX_CONTROL or cleaned.startswith("savepoint")

    def _ensure_table_schema(self, table: str) -> tuple:
        if table in self.table_cache:
            try:
                exists_check = self.ch.client.execute(
                    f"EXISTS TABLE {self._ch_table_ref(table)}"
                )
                if exists_check and exists_check[0][0] == 1:
                    return self.table_cache[table]
                log.warning("Table %s no longer exists, recreating...", table)
                del self.table_cache[table]
            except Exception as e:
                log.warning("Failed to check table existence for %s: %s", table, e)
                del self.table_cache[table]

        max_retries = 3
        last_error = None

        for attempt in range(max_retries):
            try:
                self.mysql_client.connect()
                cols_meta, pk_cols = self.mysql_client.get_table_columns_and_pk(table)

                if not cols_meta:
                    if attempt < max_retries - 1:
                        wait_time = min(1 * (attempt + 1), 3)
                        log.warning(
                            "Table %s not found in MySQL (attempt %d/%d), retrying in %ss...",
                            table, attempt + 1, max_retries, wait_time,
                        )
                        time.sleep(wait_time)
                        continue
                    raise ValueError(f"No columns found for table {table} after {max_retries} attempts")

                ddl, insert_cols = build_table_ddl(table, cols_meta, pk_cols, self.mig_cfg)

                exists_check = self.ch.client.execute(
                    f"EXISTS TABLE {self._ch_table_ref(table)}"
                )
                exists = bool(exists_check and exists_check[0][0] == 1)
                if not exists:
                    if self.mig_cfg.get("debug"):
                        log.info("Creating ClickHouse table: %s", table)
                    self.ch.execute(ddl)
                    exists_check = self.ch.client.execute(
                        f"EXISTS TABLE {self._ch_table_ref(table)}"
                    )
                    if not exists_check or exists_check[0][0] != 1:
                        if attempt < max_retries - 1:
                            wait_time = min(1 * (attempt + 1), 3)
                            log.warning(
                                "Table %s creation verification failed (attempt %d/%d), retrying in %ss...",
                                table, attempt + 1, max_retries, wait_time,
                            )
                            time.sleep(wait_time)
                            continue
                        raise RuntimeError(
                            f"Table {table} was not created successfully after {max_retries} attempts"
                        )
                elif self.mig_cfg.get("debug"):
                    log.info("ClickHouse table %s already exists; syncing columns", table)

                desired = []
                for col in cols_meta:
                    name = col["COLUMN_NAME"]
                    ch_type = map_with_low_cardinality(col, self.mig_cfg)
                    try:
                        default_expr = _default_expr_for_column(col, ch_type)
                    except (ValueError, TypeError):
                        default_expr = None
                    desired.append({"name": name, "type_sql": ch_type, "default_expr": default_expr})

                desired.extend([
                    ("__data_transfer_commit_time", "UInt64"),
                    ("__data_transfer_delete_time", "UInt64"),
                ])

                ensure_clickhouse_columns(self.ch, table, desired)

                self.table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                return self.table_cache[table]
            except ValueError as e:
                last_error = e
                if attempt < max_retries - 1:
                    wait_time = min(1 * (attempt + 1), 3)
                    log.warning(
                        "Table %s schema fetch failed (attempt %d/%d): %s, retrying in %ss...",
                        table, attempt + 1, max_retries, e, wait_time,
                    )
                    time.sleep(wait_time)
                    continue
                raise
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    wait_time = min(1 * (attempt + 1), 3)
                    log.warning(
                        "Schema sync failed for %s (attempt %d/%d): %s, retrying in %ss...",
                        table, attempt + 1, max_retries, e, wait_time,
                    )
                    time.sleep(wait_time)
                    continue
                log.error("Schema sync failed for %s after %d attempts: %s", table, max_retries, e)
                raise

        if last_error:
            raise last_error
        raise RuntimeError(f"Failed to ensure table schema for {table} after {max_retries} attempts")

    def _handle_ddl_event(self, event_data: dict, queue_only: bool = False):
        """
        Handle DDL events.

        queue_only=False: sync CREATE/ALTER schema immediately; returns True if handled, False if skipped.
        queue_only=True: return list of prepared query dicts for DROP/TRUNCATE/RENAME.
        """
        query = event_data.get("query", "")
        if not query:
            return [] if queue_only else True

        if self._is_transaction_control(query):
            return [] if queue_only else True

        if self.mig_cfg.get("debug"):
            log.info("DDL: Processing query: %s...", query[:200])

        kind = self._detect_ddl_kind(query)
        schema = event_data.get("schema")

        if kind == "create":
            if queue_only:
                return []
            table_name = parse_ddl_table_name(query, "create")
            if not table_name or not self._table_allowed(table_name):
                return False
            if self.mig_cfg.get("debug"):
                log.info("DDL: Detected CREATE TABLE for %s", table_name)
            if table_name in self.table_cache:
                del self.table_cache[table_name]
            max_retries = 10
            for attempt in range(max_retries):
                try:
                    if attempt > 0:
                        try:
                            self.mysql_client.close()
                            time.sleep(0.5)
                        except Exception:
                            pass
                    self._ensure_table_schema(table_name)
                    if self.mig_cfg.get("debug"):
                        log.info("DDL: Created table %s in ClickHouse", table_name)
                    return True
                except (ValueError, RuntimeError) as e:
                    error_msg = str(e)
                    if (
                        ("No columns found" in error_msg or "not found" in error_msg.lower()
                         or "was not created successfully" in error_msg)
                        and attempt < max_retries - 1
                    ):
                        wait_time = min(2 ** attempt, 10)
                        log.warning(
                            "DDL: Table %s not visible yet, retrying in %ss... (attempt %d/%d)",
                            table_name, wait_time, attempt + 1, max_retries,
                        )
                        time.sleep(wait_time)
                    else:
                        raise
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait_time = min(2 ** attempt, 5)
                        log.warning(
                            "DDL: Error creating table %s (attempt %d/%d): %s, retrying in %ss...",
                            table_name, attempt + 1, max_retries, e, wait_time,
                        )
                        time.sleep(wait_time)
                    else:
                        log.error("DDL: Failed to create table %s after %d attempts: %s", table_name, max_retries, e)
                        raise
            return True

        if kind == "alter":
            if queue_only:
                return []
            table_name = parse_ddl_table_name(query, "alter")
            if not table_name or not self._table_allowed(table_name):
                return False
            if self.mig_cfg.get("debug"):
                log.info("DDL: Detected ALTER TABLE for %s", table_name)
            if table_name in self.table_cache:
                del self.table_cache[table_name]
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    if attempt > 0:
                        try:
                            self.mysql_client.close()
                            time.sleep(0.5)
                        except Exception:
                            pass
                    self._ensure_table_schema(table_name)
                    if self.mig_cfg.get("debug"):
                        log.info("DDL: Synchronized schema for %s after ALTER", table_name)
                    return True
                except ValueError as e:
                    if "No columns found" in str(e) and attempt < max_retries - 1:
                        wait_time = min(2 ** attempt, 5)
                        log.warning(
                            "DDL: Table %s schema not updated yet, retrying in %ss... (attempt %d/%d)",
                            table_name, wait_time, attempt + 1, max_retries,
                        )
                        time.sleep(wait_time)
                    else:
                        raise
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait_time = min(2 ** attempt, 5)
                        log.warning(
                            "DDL: Error syncing schema for %s, retrying in %ss... (attempt %d/%d): %s",
                            table_name, wait_time, attempt + 1, max_retries, e,
                        )
                        time.sleep(wait_time)
                    else:
                        log.error("DDL: Failed to sync schema for %s after %d attempts: %s", table_name, max_retries, e)
                        raise
            return True

        if kind == "drop":
            if not queue_only:
                return True
            queued = []
            for table_name in parse_drop_table_names(query):
                if not self._table_allowed(table_name):
                    continue
                if self.mig_cfg.get("debug"):
                    log.info("DDL: Queuing DROP TABLE for %s", table_name)
                if table_name in self.table_cache:
                    del self.table_cache[table_name]
                queued.append({
                    "sql": f"DROP TABLE IF EXISTS {self._ch_table_ref(table_name)}",
                    "params": None,
                    "schema": schema,
                    "table": table_name,
                    "is_ddl": True,
                })
            return queued

        if kind == "truncate":
            if not queue_only:
                return True
            table_name = parse_ddl_table_name(query, "truncate")
            if not table_name or not self._table_allowed(table_name):
                return []
            if self.mig_cfg.get("debug"):
                log.info("DDL: Queuing TRUNCATE TABLE for %s", table_name)
            return [{
                "sql": f"TRUNCATE TABLE IF EXISTS {self._ch_table_ref(table_name)}",
                "params": None,
                "schema": schema,
                "table": table_name,
                "is_ddl": True,
            }]

        if kind == "rename":
            if not queue_only:
                return True
            queued = []
            for old_name, new_name in parse_rename_table_pairs(query):
                if not self._table_allowed(old_name):
                    continue
                if self.mig_cfg.get("debug"):
                    log.info("DDL: Queuing RENAME TABLE %s TO %s", old_name, new_name)
                if old_name in self.table_cache:
                    del self.table_cache[old_name]
                if new_name in self.table_cache:
                    del self.table_cache[new_name]
                queued.append({
                    "sql": (
                        f"RENAME TABLE {self._ch_table_ref(old_name)} "
                        f"TO {self._ch_table_ref(new_name)}"
                    ),
                    "params": None,
                    "schema": schema,
                    "table": old_name,
                    "is_ddl": True,
                })
            return queued

        query_lower = strip_sql_leading_comments(query).lower()
        if any(kw in query_lower for kw in ("create ", "drop ", "alter ", "rename ", "truncate ")):
            log.warning("DDL: Unhandled DDL statement: %s...", query[:100])
        return [] if queue_only else True

    def _next_commit_ts(self) -> int:
        now = time.time_ns()
        if now <= self._last_commit_ns:
            now = self._last_commit_ns + 1
        self._last_commit_ns = now
        return now

    def _generate_sql_for_batch(self, schema: str, table: str, table_events: list):
        try:
            try:
                insert_cols, _ = self._ensure_table_schema(table)
            except (ValueError, RuntimeError) as e:
                error_msg = str(e)
                if (
                    "No columns found" in error_msg
                    or "not found" in error_msg.lower()
                    or "was not created successfully" in error_msg
                ):
                    total_rows = sum(
                        len(ev.get("event_data", {}).get("rows", [])) for ev in table_events
                    )
                    log.warning(
                        "Skipping %d table events (%d rows) for %s: %s",
                        len(table_events), total_rows, table, error_msg,
                    )
                    return None
                raise

            rows = []
            for item in table_events:
                event_type = item.get("event_type")
                event_data = item.get("event_data") or {}
                for row in event_data.get("rows", []) or []:
                    if event_type == "WriteRowsEvent":
                        values = row.get("values", {}) or {}
                        vals = [values.get(col) for col in insert_cols[:-2]]
                        commit_ns = self._next_commit_ts()
                        rows.append(vals + [commit_ns, 0])
                    elif event_type == "UpdateRowsEvent":
                        before_vals = row.get("before_values", {}) or {}
                        after_vals = row.get("after_values", {}) or row.get("values", {}) or {}
                        merged_vals = dict(before_vals)
                        merged_vals.update(after_vals)
                        vals = [merged_vals.get(col) for col in insert_cols[:-2]]
                        commit_ns = self._next_commit_ts()
                        rows.append(vals + [commit_ns, 0])
                    elif event_type == "DeleteRowsEvent":
                        values = row.get("values", {}) or {}
                        vals = [values.get(col) for col in insert_cols[:-2]]
                        commit_ns = self._next_commit_ts()
                        rows.append(vals + [commit_ns, commit_ns])

            if not rows:
                return None

            cols_sql = ", ".join(quote_ident(c) for c in insert_cols)
            sql = f"INSERT INTO {self._ch_table_ref(table)} ({cols_sql}) VALUES"
            return {"sql": sql, "params": rows, "schema": schema, "table": table}

        except Exception as e:
            log.error("Failed to generate SQL for %s: %s", table, e)
            raise

    def _flush_pending_data(self, pending_order, pending_data, prepared_queries, processed_event_ids):
        batch_group_prefix = f"batch_{int(time.time() * 1000)}"
        query_idx = len(prepared_queries) + 1

        for key in pending_order:
            group = pending_data.get(key)
            if not group or not group["events"]:
                continue
            schema, table = key
            query = self._generate_sql_for_batch(schema, table, group["events"])
            if query:
                query["group_id"] = f"{batch_group_prefix}_{query_idx}"
                query_idx += 1
                prepared_queries.append(query)
                processed_event_ids.extend(group["event_ids"])
            else:
                processed_event_ids.extend(group["event_ids"])

    def _max_binlog_from_events(self, raw_events, event_ids):
        id_set = set(event_ids)
        max_key = None
        max_file = None
        max_pos = None
        for event in raw_events:
            if event["id"] not in id_set:
                continue
            event_file = event.get("binlog_file")
            event_pos = event.get("binlog_pos")
            if not event_file or event_pos is None:
                continue
            key = binlog_position_key(event_file, event_pos)
            if max_key is None or key > max_key:
                max_key = key
                max_file = event_file
                max_pos = event_pos
        return max_file, max_pos

    def run(self):
        if self.mig_cfg.get("debug"):
            log.info("Starting Pipeline Transformer...")

        last_process_time = time.time()

        while not self._shutdown_flag.is_set():
            try:
                cdc_cfg = self.mig_cfg.get("cdc", {})
                checkpoint_rows = int(cdc_cfg.get("checkpoint_interval_rows", 5000))
                batch_max_wait = int(cdc_cfg.get("batch_max_wait_seconds", 60))
                fetch_limit = checkpoint_rows if checkpoint_rows > 0 else 5000

                if checkpoint_rows > 0:
                    while not self._shutdown_flag.is_set():
                        stats = self.buffer.get_queue_stats()
                        raw_count = stats.get("raw_events", 0)
                        if raw_count >= checkpoint_rows:
                            break
                        time_since_last = time.time() - last_process_time
                        if raw_count > 0 and time_since_last >= batch_max_wait:
                            if self.mig_cfg.get("debug"):
                                log.info(
                                    "Transformer processing due to time limit (%.1fs >= %ds), events: %d",
                                    time_since_last, batch_max_wait, raw_count,
                                )
                            break
                        if hasattr(cdc_cfg, "resolved_transformer_poll_interval"):
                            wait_seconds = float(cdc_cfg.resolved_transformer_poll_interval())
                        else:
                            wait_seconds = float(
                                cdc_cfg.get("transformer_poll_interval")
                                or cdc_cfg.get("batch_delay_seconds", 5)
                                or 0.5
                            )
                        if wait_seconds <= 0:
                            wait_seconds = 0.5
                        if self.mig_cfg.get("debug"):
                            log.info(
                                "Transformer waiting for raw_events to reach %d (current=%d)",
                                checkpoint_rows, raw_count,
                            )
                        if self._shutdown_flag.wait(timeout=wait_seconds):
                            break

                if self._shutdown_flag.is_set():
                    break

                raw_events = self.buffer.fetch_raw_events_batch(limit=fetch_limit)
                if not raw_events:
                    if self._shutdown_flag.wait(timeout=5):
                        break
                    continue

                last_process_time = time.time()

                pending_order = []
                pending_data = {}
                prepared_queries = []
                processed_event_ids = []
                skipped_event_ids = []

                for event in raw_events:
                    event_id = event["id"]

                    if event["event_type"] == "QueryEvent":
                        query = (event.get("event_data") or {}).get("query", "")
                        if self._is_transaction_control(query):
                            skipped_event_ids.append(event_id)
                            continue

                        self._flush_pending_data(
                            pending_order, pending_data, prepared_queries, processed_event_ids,
                        )
                        pending_order.clear()
                        pending_data.clear()

                        kind = self._detect_ddl_kind(query)
                        if kind in ("create", "alter"):
                            try:
                                handled = self._handle_ddl_event(event["event_data"], queue_only=False)
                                if handled:
                                    processed_event_ids.append(event_id)
                                else:
                                    skipped_event_ids.append(event_id)
                            except Exception as e:
                                log.error("DDL handling failed: %s", e)
                                raise
                        elif kind in ("drop", "truncate", "rename"):
                            queued = self._handle_ddl_event(event["event_data"], queue_only=True)
                            prepared_queries.extend(queued)
                            processed_event_ids.append(event_id)
                        else:
                            skipped_event_ids.append(event_id)
                        continue

                    table = event.get("table")
                    if not table or not self._table_allowed(table):
                        skipped_event_ids.append(event_id)
                        continue

                    key = (event.get("schema"), table)
                    if key not in pending_data:
                        pending_data[key] = {"events": [], "event_ids": []}
                        pending_order.append(key)
                    pending_data[key]["events"].append({
                        "event_type": event["event_type"],
                        "event_data": event.get("event_data") or {},
                    })
                    pending_data[key]["event_ids"].append(event_id)

                self._flush_pending_data(
                    pending_order, pending_data, prepared_queries, processed_event_ids,
                )

                events_to_commit = processed_event_ids + skipped_event_ids
                if prepared_queries or events_to_commit:
                    self.buffer.commit_prepared_queries(prepared_queries, events_to_commit)
                    if self.mig_cfg.get("debug"):
                        total_rows = sum(len(q.get("params") or []) for q in prepared_queries if q.get("params"))
                        log.info(
                            "Transformer: %d events -> %d prepared queries (%d rows), skipped=%d",
                            len(events_to_commit), len(prepared_queries), total_rows, len(skipped_event_ids),
                        )

                    max_binlog_file, max_binlog_pos = self._max_binlog_from_events(raw_events, events_to_commit)
                    if max_binlog_file and max_binlog_pos is not None:
                        self.state.set_binlog(max_binlog_file, max_binlog_pos)
                        if self.mig_cfg.get("debug"):
                            log.info(
                                "Transformer: Updated state.json binlog position to %s:%s",
                                max_binlog_file, max_binlog_pos,
                            )

            except Exception as e:
                log.exception("Pipeline Transformer failed: %s", e)
                if self._shutdown_flag.is_set():
                    log.info("Transformer shutting down due to shutdown flag")
                    break
                time.sleep(1)

        log.info("Pipeline Transformer shutdown complete")
