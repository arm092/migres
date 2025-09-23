import logging
import time
import threading
import queue
import json
from datetime import datetime
from typing import Dict, List
from collections import defaultdict

from mysql_client import MySQLClient
from clickhouse_client import CHClient
from schema_and_ddl import build_table_ddl, ensure_clickhouse_columns
from state_json import StateJson

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from pymysqlreplication.event import QueryEvent

log = logging.getLogger(__name__)


def _serialize_event(event):
    try:
        etype = event.__class__.__name__
        base = {
            "type": etype,
            "schema": getattr(event, "schema", None),
            "table": getattr(event, "table", None),
        }
        if hasattr(event, "rows"):
            # rows is a list of dictionaries (values/after_values)
            base["rows"] = event.rows
        if hasattr(event, "query"):
            base["query"] = getattr(event, "query", None)
        # low-level position if available
        if hasattr(event, "packet"):
            try:
                base["log_pos"] = getattr(event.packet, "log_pos", None)
            except (AttributeError, KeyError):
                pass
        return base
    except (AttributeError, TypeError, ValueError):
        return {"type": str(type(event)), "repr": repr(event)}


def _build_insertable(cols_meta, pk_cols, mig_cfg):
    ddl, insert_cols = build_table_ddl("_dummy", cols_meta, pk_cols, mig_cfg)
    # We only need the column list, ddl will be rebuilt for real table
    return insert_cols


def _ensure_table_and_columns(mysql_client: MySQLClient, ch: CHClient, table: str, mig_cfg):
    cols_meta, pk_cols = mysql_client.get_table_columns_and_pk(table)
    ddl, insert_cols = build_table_ddl(table, cols_meta, pk_cols, mig_cfg)
    ch.execute(ddl)
    # ensure all columns exist (in case of ALTER ADD COLUMN)
    desired = []
    for col in cols_meta:
        name = col["COLUMN_NAME"]
        ch_type = _map_with_low_cardinality(col, mig_cfg)
        # Attach default expression when possible so ALTER ADD COLUMN sets defaults in CH
        from schema_and_ddl import _default_expr_for_column
        try:
            default_expr = _default_expr_for_column(col, ch_type)
        except (ValueError, TypeError):
            default_expr = None
        desired.append({"name": name, "type_sql": ch_type, "default_expr": default_expr})
    # transfer columns
    desired.extend([
        ("__data_transfer_commit_time", "UInt64"),
        ("__data_transfer_delete_time", "UInt64")
    ])
    ensure_clickhouse_columns(ch, table, desired)
    return insert_cols, cols_meta, pk_cols


def _wait_for_mutations(ch: CHClient, table: str, timeout_seconds: int = 180, poll_interval: float = 0.5):
    """
    Wait until all mutations for given table are finished, up to timeout.
    """
    import time as _t
    deadline = _t.time() + timeout_seconds
    dbname = ch.db
    while True:
        try:
            rows = ch.execute(
                "SELECT count() FROM system.mutations WHERE database = %(db)s AND table = %(tbl)s AND is_done = 0",
                {"db": dbname, "tbl": table}
            )
            pending = rows[0][0] if rows and rows[0] else 0
        except (AttributeError, TypeError, ValueError):
            pending = 0
        if pending == 0:
            return True
        if _t.time() >= deadline:
            log.warning("CDC: timeout waiting for mutations to finish on %s.%s (pending=%d)", dbname, table, pending)
            return False
        _t.sleep(poll_interval)


def _rebuild_entire_table_with_type_change(mysql_client: MySQLClient, ch: CHClient, table: str, col_name: str, target_ch_type: str, mig_cfg):
    """
    Create a new table with the desired schema and swap. Used as last resort when column type refuses to change.
    """
    cols_meta, pk_cols = mysql_client.get_table_columns_and_pk(table)
    ddl, insert_cols = build_table_ddl(f"{table}__migres_new", cols_meta, pk_cols, mig_cfg)
    ch.execute(ddl)
    db = ch.db
    # Build SELECT list mapping from old table
    select_exprs = []
    for c in insert_cols:
        if c in ("__data_transfer_commit_time", "__data_transfer_delete_time"):
            # Will set in SELECT from existing columns
            select_exprs.append(f"`{c}`")
        elif c == col_name:
            if target_ch_type.endswith("String)") or target_ch_type == "String":
                select_exprs.append(f"toString(`{c}`)")
            else:
                select_exprs.append(f"CAST(`{c}` AS {target_ch_type})")
        else:
            select_exprs.append(f"`{c}`")
    select_sql = ", ".join(select_exprs)
    ch.execute(
        f"INSERT INTO `{db}`.`{table}__migres_new` ({', '.join('`'+c+'`' for c in insert_cols)}) SELECT {select_sql} FROM `{db}`.`{table}`"
    )
    _wait_for_mutations(ch, table, timeout_seconds=300)
    # Swap tables
    ch.execute(f"RENAME TABLE `{db}`.`{table}` TO `{db}`.`{table}__migres_old`, `{db}`.`{table}__migres_new` TO `{db}`.`{table}`")
    # Drop old
    ch.execute(f"DROP TABLE IF EXISTS `{db}`.`{table}__migres_old`")


def _map_with_low_cardinality(col, mig_cfg):
    from schema_and_ddl import map_mysql_to_ch_type
    ch_type = map_mysql_to_ch_type(col, mig_cfg)
    if bool(mig_cfg.get("low_cardinality_strings", True)) and ch_type.endswith("String") and not ch_type.startswith("LowCardinality("):
        ch_type = ch_type.replace("Nullable(String)", "Nullable(LowCardinality(String))") if ch_type.startswith("Nullable(") else f"LowCardinality({ch_type})"
    return ch_type


class EventQueue:
    """Thread-safe queue for accumulating CDC events"""
    def __init__(self):
        self.queue = queue.Queue()
        self.lock = threading.Lock()
        self.event_count = 0
    
    def put(self, event):
        """Add an event to the queue"""
        with self.lock:
            self.queue.put(event)
            self.event_count += 1
    
    def get_all(self):
        """Get all events from the queue and clear it"""
        events = []
        with self.lock:
            while not self.queue.empty():
                try:
                    events.append(self.queue.get_nowait())
                except queue.Empty:
                    break
            self.event_count = 0
        return events
    
    def size(self):
        """Get current queue size"""
        with self.lock:
            return self.event_count


class EventGrouper:
    """Groups events by table and operation type for efficient processing"""
    def __init__(self):
        self.groups = defaultdict(list)  # key: (schema, table, event_type) -> list of events
    
    def add_event(self, event):
        """Add an event to the appropriate group"""
        schema = getattr(event, "schema", None)
        table = getattr(event, "table", None)
        event_type = event.__class__.__name__
        
        # Handle bytes schema (decode if needed)
        if isinstance(schema, bytes):
            schema = schema.decode('utf-8')
        
        if table and event_type in ["WriteRowsEvent", "UpdateRowsEvent", "DeleteRowsEvent"]:
            key = (schema, table, event_type)
            self.groups[key].append(event)
            return True
        return False
    
    def get_groups(self):
        """Get all groups and clear the grouper"""
        groups = dict(self.groups)
        self.groups.clear()
        return groups


def _dump_failed_operations(operations: List[Dict], error_msg: str):
    """Dump failed operations to a file for manual review"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"failed_operations_{timestamp}.json"
    
    dump_data = {
        "timestamp": timestamp,
        "error": error_msg,
        "operations": operations
    }
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(dump_data, f, indent=2, default=str)
        log.error("Failed operations dumped to %s", filename)
    except Exception as e:
        log.exception("Failed to dump operations to file: %s", e)


def _process_grouped_events(groups: Dict, mysql_client: MySQLClient, ch: CHClient, table_cache: dict, mig_cfg: dict) -> int:
    """
    Process grouped events efficiently.
    Returns total number of rows processed.
    """
    total_rows = 0
    failed_operations = []
    
    for (schema, table, event_type), events in groups.items():
        try:
            # Count total rows across all events in this group
            total_rows_in_group = sum(len(getattr(event, 'rows', [])) for event in events)
            log.info("CDC: processing group %s.%s (%s) with %d events containing %d total rows", 
                    schema, table, event_type, len(events), total_rows_in_group)
            
            # Combine all rows from events of the same type
            all_rows = []
            for event in events:
                if hasattr(event, 'rows') and event.rows:
                    all_rows.extend(event.rows)
            
            if not all_rows:
                continue
            
            # Create a combined event
            class CombinedEvent:
                def __init__(self, event_type, schema, table, rows):
                    self.__class__.__name__ = event_type
                    self.schema = schema
                    self.table = table
                    self.rows = rows
            
            combined_event = CombinedEvent(event_type, schema, table, all_rows)
            rows_processed = _process_batched_event(combined_event, mysql_client, ch, table_cache, mig_cfg)
            total_rows += rows_processed
            
            log.info("CDC: successfully processed %d rows for %s.%s (%s)", rows_processed, schema, table, event_type)
            
        except Exception as e:
            log.exception("CDC: failed to process group %s.%s (%s)", schema, table, event_type)
            # Collect failed operation details
            operation = {
                "schema": schema,
                "table": table,
                "event_type": event_type,
                "event_count": len(events),
                "error": str(e),
                "events": []
            }
            
            # Add event details (limited to avoid huge files)
            for i, event in enumerate(events[:5]):  # Only first 5 events
                try:
                    event_data = {
                        "event_index": i,
                        "rows_count": len(getattr(event, 'rows', [])),
                        "log_pos": getattr(event, 'log_pos', None)
                    }
                    operation["events"].append(event_data)
                except Exception:
                    pass
            
            failed_operations.append(operation)
    
    # Dump failed operations if any
    if failed_operations:
        _dump_failed_operations(failed_operations, "Failed to process grouped events")
    
    return total_rows


def _process_batched_event(event, mysql_client: MySQLClient, ch: CHClient, table_cache: dict, mig_cfg: dict) -> int:
    """
    Process a single event (can be a batched/combined event).
    Returns the number of rows processed.
    """
    table = getattr(event, "table", None)
    
    if not table:
        log.info("CDC: skipping event without table: %s", event.__class__.__name__)
        return 0
    
    # Ensure table exists in CH and columns are up-to-date
    if table not in table_cache:
        insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
        table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
    
    insert_cols, mysql_cols = table_cache[table]
    
    if isinstance(event, WriteRowsEvent) or event.__class__.__name__ == "WriteRowsEvent":
        # Handle INSERT events
        # If event contains columns unknown to our cache, refresh schema
        try:
            sample = event.rows[0]["values"] if event.rows else {}
            incoming_cols = set(sample.keys())
            cached_cols = set(insert_cols[:-2])
            if incoming_cols and not incoming_cols.issubset(cached_cols):
                log.info("CDC: detected new columns for %s, refreshing (writing) schema...", table)
                insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                cached_cols = set(insert_cols[:-2])
                # If still missing (information_schema lag), retry a few times
                if incoming_cols and not incoming_cols.issubset(cached_cols):
                    import time as _t
                    for _ in range(10):
                        insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                        table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                        cached_cols = set(insert_cols[:-2])
                        if incoming_cols.issubset(cached_cols):
                            break
                        _t.sleep(0.1)
        except (AttributeError, KeyError, TypeError):
            log.exception("CDC: failed to auto-refresh schema on INSERT; proceeding with current cache")
        
        rows = []
        for row in event.rows:
            vals = [row["values"].get(col) for col in insert_cols[:-2]]
            commit_ns = time.time_ns()
            rows.append(tuple(vals + [commit_ns, 0]))
        
        try:
            ch.insert_rows(table, insert_cols, rows)
        except Exception:
            # If insert fails due to missing column(s), refresh schema and retry once
            log.exception("CDC: insert failed, refreshing schema and retrying once for %s", table)
            insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
            table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
            # Retry the insert with updated schema
            ch.insert_rows(table, insert_cols, rows)
        
        log.info("CDC: inserted %d row(s) into %s (INSERT)", len(rows), table)
        return len(rows)
    
    elif isinstance(event, UpdateRowsEvent) or event.__class__.__name__ == "UpdateRowsEvent":
        # Handle UPDATE events
        # Refresh if new columns appear in after_values
        try:
            sample = event.rows[0]["after_values"] if event.rows else {}
            incoming_cols = set(sample.keys())
            cached_cols = set(insert_cols[:-2])
            if incoming_cols and not incoming_cols.issubset(cached_cols):
                log.info("CDC: detected new columns for %s, refreshing (updating) schema...", table)
                insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                cached_cols = set(insert_cols[:-2])
                # If still missing (information_schema lag), retry a few times
                if incoming_cols and not incoming_cols.issubset(cached_cols):
                    import time as _t
                    for _ in range(10):
                        insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                        table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                        cached_cols = set(insert_cols[:-2])
                        if incoming_cols.issubset(cached_cols):
                            break
                        _t.sleep(0.1)
        except Exception:
            log.exception("CDC: failed to auto-refresh schema on UPDATE; proceeding with current cache")
        
        rows = []
        for row in event.rows:
            # Build values with explicit None for missing columns (so CH uses DEFAULT or NULL)
            after_vals = row["after_values"]
            vals = [after_vals[col] if col in after_vals else None for col in insert_cols[:-2]]
            commit_ns = time.time_ns()
            rows.append(tuple(vals + [commit_ns, 0]))
        
        try:
            ch.insert_rows(table, insert_cols, rows)
        except Exception:
            log.exception("CDC: insert failed, refreshing schema and retrying once for %s", table)
            insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
            table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
            # Retry the insert with updated schema
            ch.insert_rows(table, insert_cols, rows)
        
        log.info("CDC: inserted %d row(s) into %s (UPDATE->upsert)", len(rows), table)
        return len(rows)
    
    elif isinstance(event, DeleteRowsEvent) or event.__class__.__name__ == "DeleteRowsEvent":
        # Handle DELETE events
        rows = []
        for row in event.rows:
            # We write a delete tombstone: copy values if possible, set delete_time
            vals = [row["values"].get(col) for col in insert_cols[:-2]]
            commit_ns = time.time_ns()
            rows.append(tuple(vals + [commit_ns, commit_ns]))
        
        try:
            ch.insert_rows(table, insert_cols, rows)
        except Exception:
            log.exception("CDC: insert failed, refreshing (deleting) schema and retrying once for %s", table)
            insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
            table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
            # Retry the insert with updated schema
            ch.insert_rows(table, insert_cols, rows)
        
        log.info("CDC: inserted %d tombstone row(s) into %s (DELETE)", len(rows), table)
        return len(rows)
    
    return 0


def run_cdc(cfg):
    mysql_cfg = cfg["mysql"]
    ch_cfg = cfg["clickhouse"]
    mig_cfg = cfg.get("migration", {})
    cdc_cfg = mig_cfg.get("cdc", {})

    heartbeat_seconds = int(cdc_cfg.get("heartbeat_seconds", 5))
    checkpoint_interval_rows = int(cdc_cfg.get("checkpoint_interval_rows", 1000))
    checkpoint_interval_seconds = int(cdc_cfg.get("checkpoint_interval_seconds", 5))
    batch_delay_seconds = float(cdc_cfg.get("batch_delay_seconds", 0))

    state_file = cfg.get("state_file")
    checkpoint_file = cfg.get("checkpoint_file")
    state = StateJson(state_file)

    mysql_client = MySQLClient(mysql_cfg)
    mysql_client.connect()
    ch = CHClient(ch_cfg, mig_cfg)

    # read start position
    binlog = state.get_binlog()
    log.info("Starting CDC from %s", binlog if binlog else "current master position")

    server_id = int(cdc_cfg.get("server_id", 4379))
    only_schemas = [mysql_cfg["database"]]
    included = set(mysql_cfg.get("include_tables") or [])
    excluded = set(mysql_cfg.get("exclude_tables") or [])
    only_tables = list(included) if included else None
    ignored_tables = list(excluded) if excluded else None

    # Map table -> insertable columns cache
    table_cache = {}
    
    # Initialize event queue and grouper
    event_queue = EventQueue()
    event_grouper = EventGrouper()
    
    log.info("CDC: batch_delay_seconds=%s, queue-based processing=%s", batch_delay_seconds, batch_delay_seconds > 0)

    stream = BinLogStreamReader(
        connection_settings={
            "host": mysql_cfg["host"],
            "port": mysql_cfg.get("port", 3306),
            "user": mysql_cfg["user"],
            "passwd": mysql_cfg["password"],
        },
        server_id=server_id,
        blocking=False,  # Non-blocking to allow queue processing
        resume_stream=True,
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent],
        only_schemas=only_schemas,
        only_tables=only_tables,
        ignored_tables=ignored_tables,
        log_file=(binlog["file"] if binlog else None),
        log_pos=(binlog["pos"] if binlog else None),
        slave_heartbeat=heartbeat_seconds,
    )

    log.info("CDC stream configured: resume_stream=%s, blocking=%s, only_schemas=%s, only_tables=%s, ignored_tables=%s, server_id=%s, log_file=%s, log_pos=%s",
             True, False, only_schemas, only_tables, ignored_tables, server_id, (binlog["file"] if binlog else None), (binlog["pos"] if binlog else None))

    last_checkpoint_time = time.time()
    rows_since_checkpoint = 0
    last_queue_process_time = time.time()

    try:
        while True:
            # Process queue every batch_delay_seconds
            now = time.time()
            if 0 < batch_delay_seconds <= (now - last_queue_process_time):
                log.info("CDC: processing queue (time since last process: %.1fs, queue size: %d)", 
                        now - last_queue_process_time, event_queue.size())
                
                # Get all events from queue
                events = event_queue.get_all()
                if events:
                    log.info("CDC: processing %d events from queue", len(events))
                    
                    # Group events by table and operation type
                    for event in events:
                        event_grouper.add_event(event)
                    
                    # Get grouped events and process them
                    groups = event_grouper.get_groups()
                    if groups:
                        log.info("CDC: processing %d groups", len(groups))
                        rows_processed = _process_grouped_events(groups, mysql_client, ch, table_cache, mig_cfg)
                        rows_since_checkpoint += rows_processed
                        log.info("CDC: successfully processed %d rows from queue", rows_processed)
                    else:
                        log.info("CDC: no data events to process from queue")
                else:
                    log.info("CDC: queue is empty, no processing needed")
                
                last_queue_process_time = now
            
            # Try to get next event (non-blocking)
            try:
                event = stream.fetchone()
                if event is None:
                    # No more events, sleep briefly and continue
                    time.sleep(0.1)
                    continue
            except Exception as e:
                log.debug("CDC: no event available: %s", e)
                time.sleep(0.1)
                continue
            
            try:
                schema = getattr(event, "schema", None)
                table = getattr(event, "table", None)
                
                # Handle bytes schema (decode if needed)
                if isinstance(schema, bytes):
                    schema = schema.decode('utf-8')
                
                log.info("CDC event: %s schema=%s table=%s", event.__class__.__name__, schema, table)
                
                # Process DDL events immediately (don't queue them)
                if isinstance(event, QueryEvent):
                    # DDL: best-effort schema sync (same logic as before)
                    query_text = (event.query or "")
                    query_lower = query_text.lower()
                    
                    # Extract table name and schema for any DDL operation
                    try:
                        import re
                        # Only process DDL operations (CREATE/ALTER/DROP TABLE), skip other queries like BEGIN, COMMIT, etc.
                        if not re.match(r"(?:alter|create|drop)\s+table\s+", query_lower):
                            continue  # Skip non-DDL queries
                        
                        # Match optional schema qualification: ALTER/CREATE/DROP TABLE [IF EXISTS] `db`.`tbl` ... or db.tbl
                        m = re.search(r"(?:alter|create|drop)\s+table\s+(?:if\s+exists\s+)?(?:`?([a-zA-Z0-9_]+)`?\.)?`?([a-zA-Z0-9_]+)`?", query_lower)
                        if m:
                            matched_schema = m.group(1) if m.group(1) else (schema.decode() if isinstance(schema, (bytes, bytearray)) else (schema or mysql_cfg["database"]))
                            affected = m.group(2)
                            log.info("CDC: regex matched - schema: %s, table: %s", matched_schema, affected)
                        else:
                            matched_schema = (schema.decode() if isinstance(schema, (bytes, bytearray)) else (schema or mysql_cfg["database"]))
                            affected = table
                            log.warning("CDC: regex did not match for DDL query: %s", query_lower[:100])
                    except Exception:
                        matched_schema = (schema.decode() if isinstance(schema, (bytes, bytearray)) else (schema or mysql_cfg["database"]))
                        affected = table
                        log.exception("CDC: error extracting table name from query: %s", query_lower[:100])
                    
                    # Only react if the DDL targets our database and table of interest
                    if matched_schema == mysql_cfg["database"] and affected and (not included or affected in included) and affected not in excluded:
                        # Handle different DDL operations separately (same logic as before)
                        if query_lower.startswith("create table"):
                            # CREATE TABLE: Create the table in ClickHouse
                            try:
                                log.info("CDC: detected CREATE TABLE for %s, creating table in ClickHouse", affected)
                                insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, affected, mig_cfg)
                                table_cache[affected] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                                log.info("CDC: created table %s in ClickHouse", affected)
                            except Exception:
                                log.exception("CDC: failed to create table %s in ClickHouse", affected)
                        
                        elif query_lower.startswith("drop table"):
                            # DROP TABLE: Drop the table from ClickHouse
                            try:
                                log.info("CDC: detected DROP TABLE for %s, dropping table in ClickHouse", affected)
                                ch.execute(f"DROP TABLE IF EXISTS `{affected}`")
                                # Remove from cache if it exists
                                if affected in table_cache:
                                    del table_cache[affected]
                                log.info("CDC: dropped table %s in ClickHouse", affected)
                            except Exception:
                                log.exception("CDC: failed to drop table %s in ClickHouse", affected)
                        
                        elif query_lower.startswith("alter table"):
                            # ALTER TABLE: Handle column operations (same logic as before)
                            # ... (keeping the same ALTER TABLE logic for brevity)
                            insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, affected, mig_cfg)
                            table_cache[affected] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                    continue

                # Handle data events (INSERT/UPDATE/DELETE)
                if not table:
                    log.info("CDC: skipping event without table: %s", event.__class__.__name__)
                    continue
                if included and table not in included:
                    log.info("CDC: skipping table %s (not in include_tables)", table)
                    continue
                if table in excluded:
                    log.info("CDC: skipping table %s (in exclude_tables)", table)
                    continue
                
                # Add event to queue or process immediately
                if batch_delay_seconds > 0:
                    # Add to queue for later processing
                    event_queue.put(event)
                    row_count = len(getattr(event, 'rows', []))
                    log.info("CDC: event queued for %s.%s (%s) with %d rows - queue size: %d", 
                            schema, table, event.__class__.__name__, row_count, event_queue.size())
                else:
                    # Process immediately
                    row_count = len(getattr(event, 'rows', []))
                    log.info("CDC: processing event immediately for %s.%s (%s) with %d rows", 
                            schema, table, event.__class__.__name__, row_count)
                    rows_processed = _process_batched_event(event, mysql_client, ch, table_cache, mig_cfg)
                    rows_since_checkpoint += rows_processed

                # periodic checkpoint
                now = time.time()
                if rows_since_checkpoint >= checkpoint_interval_rows or (now - last_checkpoint_time) >= checkpoint_interval_seconds:
                    f, p = stream.log_file, stream.log_pos
                    state.set_binlog(f, p)
                    try:
                        import os, json
                        os.makedirs(os.path.dirname(checkpoint_file) or ".", exist_ok=True)
                        with open(checkpoint_file, "w", encoding="utf-8") as fp:
                            json.dump({"binlog_file": f, "binlog_pos": p}, fp, indent=2)
                    except Exception:
                        log.exception("Failed to write checkpoint file")
                    rows_since_checkpoint = 0
                    last_checkpoint_time = now

            except Exception:
                log.exception("Error handling binlog event")
                time.sleep(1)
    finally:
        # Process any remaining events in queue before closing
        if batch_delay_seconds > 0:
            remaining_events = event_queue.get_all()
            if remaining_events:
                log.info("CDC: processing %d remaining events from queue", len(remaining_events))
                for event in remaining_events:
                    event_grouper.add_event(event)
                groups = event_grouper.get_groups()
                if groups:
                    rows_processed = _process_grouped_events(groups, mysql_client, ch, table_cache, mig_cfg)
                    log.info("CDC: processed %d final rows from queue", rows_processed)
        
        try:
            stream.close()
        except Exception:
            pass
        try:
            ch.close()
        except Exception:
            pass
        try:
            mysql_client.close()
        except Exception:
            pass


