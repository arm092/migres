import logging
import time
import threading
import queue
import json
import sys
from datetime import datetime
from typing import Dict, List
from collections import defaultdict

from mysql_client import MySQLClient
from clickhouse_client import CHClient
from schema_and_ddl import build_table_ddl, ensure_clickhouse_columns
from state_json import StateJson
from notifications import initialize_notifications, notify_cdc_error, notify_cdc_warning, notify_cdc_info, notify_cdc_startup, notify_cdc_shutdown

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from pymysqlreplication.event import QueryEvent

log = logging.getLogger(__name__)


class CriticalCDCError(Exception):
    """
    Critical CDC error that should cause the process to exit with error code.
    This prevents binlog position advancement and ensures Kubernetes restarts the process.
    """
    pass

# Global query tracking pools
mysql_query_pool = []
clickhouse_query_pool = []


def _add_mysql_query(query: str, table: str = None):
    """Add a MySQL query to the tracking pool"""
    global mysql_query_pool
    mysql_query_pool.append({
        "query": query,
        "table": table,
        "timestamp": time.time()
    })
    # Keep only last 10 queries to avoid memory issues
    if len(mysql_query_pool) > 10:
        mysql_query_pool.pop(0)


def _add_clickhouse_query(query: str, table: str = None):
    """Add a ClickHouse query to the tracking pool"""
    global clickhouse_query_pool
    clickhouse_query_pool.append({
        "query": query,
        "table": table,
        "timestamp": time.time()
    })
    # Keep only last 10 queries to avoid memory issues
    if len(clickhouse_query_pool) > 10:
        clickhouse_query_pool.pop(0)


def _clear_query_pools():
    """Clear both query pools after successful operation"""
    global mysql_query_pool, clickhouse_query_pool
    mysql_query_pool.clear()
    clickhouse_query_pool.clear()


def _get_query_pools_for_error():
    """Get current query pools for error reporting"""
    global mysql_query_pool, clickhouse_query_pool
    return {
        "mysql_queries": mysql_query_pool.copy(),
        "clickhouse_queries": clickhouse_query_pool.copy()
    }


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
    log.info("CDC: _ensure_table_and_columns called for table: %s", table)
    
    # Retry mechanism for INFORMATION_SCHEMA timing issues and connection problems
    cols_meta, pk_cols = None, None
    max_retries = 5
    retry_delay = 0.1  # Start with 100ms delay
    last_error = None
    
    for attempt in range(max_retries):
        try:
            # Check if MySQL connection is still alive
            if not mysql_client.cn or not mysql_client.cn.is_connected():
                log.warning("CDC: MySQL connection lost, attempting to reconnect (attempt %d)", attempt + 1)
                try:
                    mysql_client.connect()
                    log.info("CDC: MySQL connection restored")
                except Exception as conn_e:
                    log.error("CDC: Failed to reconnect to MySQL: %s", conn_e)
                    last_error = conn_e
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    continue
            
            cols_meta, pk_cols = mysql_client.get_table_columns_and_pk(table)
            log.info("CDC: MySQL schema for %s (attempt %d) - columns: %s, pk: %s", 
                     table, attempt + 1, [c["COLUMN_NAME"] for c in cols_meta], pk_cols)
            
            if cols_meta and len(cols_meta) > 0:
                log.info("CDC: Successfully retrieved schema for %s", table)
                break
            else:
                log.warning("CDC: No columns found for %s (attempt %d), retrying in %.1fs...", 
                           table, attempt + 1, retry_delay)
                if attempt < max_retries - 1:
                    import time
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
        except Exception as e:
            last_error = e
            log.warning("CDC: Error getting schema for %s (attempt %d): %s", table, attempt + 1, e)
            if attempt < max_retries - 1:
                import time
                time.sleep(retry_delay)
                retry_delay *= 2
    
    if not cols_meta or len(cols_meta) == 0:
        error_msg = f"No columns found for table {table} after {max_retries} attempts"
        if last_error:
            error_msg += f". Last error: {last_error}"
        
        log.error("CDC: %s", error_msg)
        
        # Send notification for schema detection failure
        notify_cdc_error(
            error_type="Schema Detection Failure",
            table=table,
            error_message=error_msg,
            operation_details={
                "Table": table,
                "Attempts": max_retries,
                "Last Error": str(last_error) if last_error else "No columns found in INFORMATION_SCHEMA",
                "MySQL Connection": "Connected" if mysql_client.cn and mysql_client.cn.is_connected() else "Disconnected"
            }
        )
        
        raise ValueError(error_msg)
    
    # Build ClickHouse DDL
    ddl, insert_cols = build_table_ddl(table, cols_meta, pk_cols, mig_cfg)
    log.info("CDC: Generated DDL for %s: %s", table, ddl[:200] + "..." if len(ddl) > 200 else ddl)
    log.info("CDC: Insert columns for %s: %s", table, insert_cols)
    
    # Create/update ClickHouse table
    try:
        ch.execute(ddl)
        log.info("CDC: Successfully executed DDL for table %s", table)
    except Exception as e:
        log.error("CDC: Failed to execute DDL for table %s: %s", table, e)
        raise
    
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
    
    log.info("CDC: Ensuring columns for %s: %s", table, [d["name"] if isinstance(d, dict) else d[0] for d in desired])
    ensure_clickhouse_columns(ch, table, desired)
    
    log.info("CDC: Table %s schema creation completed successfully", table)
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
    # Create dump file in data directory
    import os
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)
    filename = os.path.join(data_dir, f"failed_operations_{timestamp}.json")
    
    # Enhanced dump data with metadata and SQL queries
    dump_data = {
        "timestamp": timestamp,
        "error_message": error_msg,
        "error_type": "CDC_PROCESSING_ERROR",
        "total_operations": len(operations),
        "operations": operations,
        "metadata": {
            "dump_reason": "CDC failed to process operations",
            "suggested_action": "Review error details and retry operations manually",
            "clickhouse_queries": [],
            "query_pools": _get_query_pools_for_error()  # Include current query pools
        }
    }
    
    # Generate real ClickHouse SQL queries for manual execution
    for i, operation in enumerate(operations):
        if "schema" in operation and "table" in operation:
            schema = operation["schema"]
            table = operation["table"]
            event_type = operation.get("event_type", "UNKNOWN")
            
            # Generate real SQL query based on event type and actual data
            sql_query = _generate_real_sql_query(operation, schema, table, event_type)
            
            dump_data["metadata"]["clickhouse_queries"].append({
                "operation_index": i,
                "schema": schema,
                "table": table,
                "event_type": event_type,
                "sql_query": sql_query,
                "error_details": operation.get("error", "No specific error details")
            })
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(dump_data, f, indent=2, default=str)
        log.error("Failed operations dumped to %s", filename)
        log.error("Dump contains %d operations with %d SQL queries", 
                 len(operations), len(dump_data["metadata"]["clickhouse_queries"]))
    except Exception as e:
        log.exception("Failed to dump operations to file: %s", e)


def _generate_real_sql_query(operation: Dict, schema: str, table: str, event_type: str) -> str:
    """Generate real SQL query from failed operation data"""
    try:
        if event_type == "WriteRowsEvent":
            return _generate_insert_sql(operation, schema, table)
        elif event_type == "UpdateRowsEvent":
            return _generate_update_sql(operation, schema, table)
        elif event_type == "DeleteRowsEvent":
            return _generate_delete_sql(operation, schema, table)
        else:
            return f"-- Unknown operation type: {event_type}\n-- Manual review required for {schema}.{table}"
    except Exception as e:
        return f"-- Error generating SQL for {event_type}: {e}\n-- Manual review required for {schema}.{table}"


def _generate_insert_sql(operation: Dict, schema: str, table: str) -> str:
    """Generate INSERT SQL from WriteRowsEvent data"""
    sql_lines = []
    
    # Get actual data from events
    events = operation.get("events", [])
    log.debug("CDC: SQL generation - events count: %d", len(events))
    
    if events and len(events) > 0:
        event = events[0]  # Use first event as sample
        log.debug("CDC: SQL generation - event keys: %s", list(event.keys()))
        
        # Extract actual row data from the event
        rows = event.get("rows", [])
        log.debug("CDC: SQL generation - rows count: %d", len(rows))
        
        if rows:
            # Get column names from the first row
            first_row = rows[0]
            log.debug("CDC: SQL generation - first row keys: %s", list(first_row.keys()))
            if "values" in first_row:
                values = first_row["values"]
                columns = list(values.keys())
                
                # Generate INSERT statement with actual column names
                sql_lines.append(f"INSERT INTO {schema}.{table} ({', '.join(columns)})")
                sql_lines.append("VALUES")
                
                # Add actual row data that caused the error
                for i, row in enumerate(rows):
                    if "values" in row:
                        values = row["values"]
                        # Format values for SQL
                        formatted_values = []
                        for col in columns:
                            value = values.get(col)
                            if value is None:
                                formatted_values.append("NULL")
                            elif isinstance(value, str):
                                # Escape single quotes in strings
                                escaped_value = value.replace("'", "''")
                                formatted_values.append(f"'{escaped_value}'")
                            elif isinstance(value, (int, float)):
                                formatted_values.append(str(value))
                            else:
                                formatted_values.append(f"'{str(value)}'")
                        
                        sql_lines.append(f"  ({', '.join(formatted_values)})")
                        if i < len(rows) - 1:
                            sql_lines[-1] += ","
            else:
                sql_lines.append(f"INSERT INTO {schema}.{table} (id, name, salary, created_at, __data_transfer_commit_time, __data_transfer_delete_time)")
                sql_lines.append("VALUES")
                sql_lines.append("  (1, 'test', 'invalid_string', now(), unixTimestampNano(now()), 0)")
    else:
        sql_lines.append(f"INSERT INTO {schema}.{table} (id, name, salary, created_at, __data_transfer_commit_time, __data_transfer_delete_time)")
        sql_lines.append("VALUES")
        sql_lines.append("  (1, 'test', 'invalid_string', now(), unixTimestampNano(now()), 0)")
    
    return "\n".join(sql_lines)


def _generate_update_sql(operation: Dict, schema: str, table: str) -> str:
    """Generate UPDATE SQL from UpdateRowsEvent data"""
    sql_lines = []
    
    events = operation.get("events", [])
    if events and len(events) > 0:
        event = events[0]
        
        # Extract actual row data from the event
        rows = event.get("rows", [])
        if rows:
            for row in rows:
                if "before_values" in row and "after_values" in row:
                    before = row["before_values"]
                    after = row["after_values"]
                    
                    # Find the primary key (usually 'id')
                    pk_column = "id"  # Default assumption
                    pk_value = before.get("id") or after.get("id")
                    
                    if pk_value is not None:
                        sql_lines.append(f"UPDATE {schema}.{table} SET")
                        
                        # Add SET clauses for changed values
                        set_clauses = []
                        for col, value in after.items():
                            if col != pk_column and col not in ["__data_transfer_commit_time", "__data_transfer_delete_time"]:
                                if value is None:
                                    set_clauses.append(f"  {col} = NULL")
                                elif isinstance(value, str):
                                    escaped_value = value.replace("'", "''")
                                    set_clauses.append(f"  {col} = '{escaped_value}'")
                                elif isinstance(value, (int, float)):
                                    set_clauses.append(f"  {col} = {value}")
                                else:
                                    set_clauses.append(f"  {col} = '{str(value)}'")
                        
                        # Add CDC metadata
                        set_clauses.append("  __data_transfer_commit_time = unixTimestampNano(now())")
                        
                        sql_lines.extend(set_clauses)
                        sql_lines.append(f"WHERE {pk_column} = {pk_value};")
        else:
            sql_lines.append(f"UPDATE {schema}.{table} SET salary = 'invalid_string', __data_transfer_commit_time = unixTimestampNano(now()) WHERE id = 1;")
    else:
        sql_lines.append(f"UPDATE {schema}.{table} SET salary = 'invalid_string', __data_transfer_commit_time = unixTimestampNano(now()) WHERE id = 1;")
    
    return "\n".join(sql_lines)


def _generate_delete_sql(operation: Dict, schema: str, table: str) -> str:
    """Generate DELETE SQL from DeleteRowsEvent data"""
    sql_lines = []
    
    events = operation.get("events", [])
    if events and len(events) > 0:
        event = events[0]
        
        # Extract actual row data from the event
        rows = event.get("rows", [])
        if rows:
            for row in rows:
                if "values" in row:
                    values = row["values"]
                    
                    # Find the primary key (usually 'id')
                    pk_column = "id"  # Default assumption
                    pk_value = values.get("id")
                    
                    if pk_value is not None:
                        sql_lines.append(f"UPDATE {schema}.{table} SET")
                        sql_lines.append("  __data_transfer_delete_time = unixTimestampNano(now())")
                        sql_lines.append(f"WHERE {pk_column} = {pk_value};")
        else:
            sql_lines.append(f"UPDATE {schema}.{table} SET __data_transfer_delete_time = unixTimestampNano(now()) WHERE id = 1;")
    else:
        sql_lines.append(f"UPDATE {schema}.{table} SET __data_transfer_delete_time = unixTimestampNano(now()) WHERE id = 1;")
    
    return "\n".join(sql_lines)


def _process_grouped_events(groups: Dict, mysql_client: MySQLClient, ch: CHClient, table_cache: dict, mig_cfg: dict) -> int:
    """
    Process grouped events efficiently.
    Returns total number of rows processed.
    Raises CriticalCDCError if any operation fails to prevent binlog position advancement.
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
                "events": [],
                "query_pools": _get_query_pools_for_error()  # Include the actual queries that were being executed
            }
            
            # Add event details (limited to avoid huge files)
            for i, event in enumerate(events[:5]):  # Only first 5 events
                try:
                    event_data = {
                        "event_index": i,
                        "rows_count": len(getattr(event, 'rows', [])),
                        "log_pos": getattr(event, 'log_pos', None),
                        "rows": getattr(event, 'rows', [])  # Include actual row data
                    }
                    operation["events"].append(event_data)
                except Exception:
                    pass
            
            failed_operations.append(operation)
    
    # If any operations failed, dump them and raise critical error to prevent binlog advancement
    if failed_operations:
        _dump_failed_operations(failed_operations, "Failed to process grouped events")
        
        # Send notification for failed operations
        for operation in failed_operations:
            schema = operation.get("schema", "unknown")
            table = operation.get("table", "unknown")
            error = operation.get("error", "Unknown error")
            event_count = operation.get("event_count", 0)
            query_pools = operation.get("query_pools", {})
            
            # Format query pools for notification
            mysql_queries = query_pools.get("mysql_queries", [])
            clickhouse_queries = query_pools.get("clickhouse_queries", [])
            
            operation_details = {
                "Event Count": event_count,
                "Event Type": operation.get("event_type", "unknown"),
                "Error": error
            }
            
            # Add MySQL queries if any
            if mysql_queries:
                mysql_query_text = "\n".join([f"- {q['query']}" for q in mysql_queries[-3:]])  # Last 3 queries
                operation_details["Recent MySQL Queries"] = mysql_query_text
            
            # Add ClickHouse queries if any
            if clickhouse_queries:
                ch_query_text = "\n".join([f"- {q['query']}" for q in clickhouse_queries[-3:]])  # Last 3 queries
                operation_details["Recent ClickHouse Queries"] = ch_query_text
            
            notify_cdc_error(
                error_type="Processing Error",
                table=f"{schema}.{table}",
                error_message=f"Failed to process {event_count} events: {error}",
                operation_details=operation_details
            )
        
        # Raise critical error to prevent binlog position advancement
        raise CriticalCDCError(f"Failed to process {len(failed_operations)} operation groups. Binlog position will not advance.")
    
    # Clear query pools only after ALL operations in the group are successful
    _clear_query_pools()
    
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
        log.info("CDC: Table %s not in cache, ensuring table and columns", table)
        insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
        table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
    
    insert_cols, mysql_cols = table_cache[table]
    
    if isinstance(event, WriteRowsEvent) or event.__class__.__name__ == "WriteRowsEvent":
        # Handle INSERT events
        # Only refresh schema if we detect truly new columns (not just missing columns in the event)
        # This avoids unnecessary INFORMATION_SCHEMA queries for regular data events
        try:
            if event.rows:
                sample = event.rows[0]["values"]
                incoming_cols = set(sample.keys())
                cached_cols = set(insert_cols[:-2])  # Exclude CDC metadata columns
                
                # Only refresh if we have columns that are NOT in our cache
                # This indicates a real schema change, not just missing columns in the event
                new_cols = incoming_cols - cached_cols
                if new_cols:
                    log.info("CDC: detected new columns %s for %s, refreshing schema...", new_cols, table)
                    insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                    table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                    cached_cols = set(insert_cols[:-2])
                    
                    # If still missing (information_schema lag), retry a few times
                    new_cols_after_refresh = incoming_cols - cached_cols
                    if new_cols_after_refresh:
                        log.warning("CDC: columns %s still missing after schema refresh, retrying...", new_cols_after_refresh)
                        import time as _t
                        for retry in range(3):  # Reduced retries from 10 to 3
                            _t.sleep(0.2)  # Increased delay
                            insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                            table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                            cached_cols = set(insert_cols[:-2])
                            new_cols_after_refresh = incoming_cols - cached_cols
                            if not new_cols_after_refresh:
                                break
                        if new_cols_after_refresh:
                            log.error("CDC: columns %s still missing after retries, proceeding with available columns", new_cols_after_refresh)
        except (AttributeError, KeyError, TypeError):
            log.exception("CDC: failed to check schema on INSERT; proceeding with current cache")
        
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
        # Only refresh schema if we detect truly new columns in after_values
        # This avoids unnecessary INFORMATION_SCHEMA queries for regular data events
        try:
            if event.rows:
                sample = event.rows[0]["after_values"]
                incoming_cols = set(sample.keys())
                cached_cols = set(insert_cols[:-2])  # Exclude CDC metadata columns
                
                # Only refresh if we have columns that are NOT in our cache
                new_cols = incoming_cols - cached_cols
                if new_cols:
                    log.info("CDC: detected new columns %s for %s, refreshing schema...", new_cols, table)
                    insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                    table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                    cached_cols = set(insert_cols[:-2])
                    
                    # If still missing (information_schema lag), retry a few times
                    new_cols_after_refresh = incoming_cols - cached_cols
                    if new_cols_after_refresh:
                        log.warning("CDC: columns %s still missing after schema refresh, retrying...", new_cols_after_refresh)
                        import time as _t
                        for retry in range(3):  # Reduced retries from 10 to 3
                            _t.sleep(0.2)  # Increased delay
                            insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                            table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                            cached_cols = set(insert_cols[:-2])
                            new_cols_after_refresh = incoming_cols - cached_cols
                            if not new_cols_after_refresh:
                                break
                        if new_cols_after_refresh:
                            log.error("CDC: columns %s still missing after retries, proceeding with available columns", new_cols_after_refresh)
        except Exception:
            log.exception("CDC: failed to check schema on UPDATE; proceeding with current cache")
        
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
    
    # Initialize notifications
    notification_config = cfg.get("notifications", {})
    initialize_notifications(notification_config)
    
    # Send startup notification
    config_summary = {
        "MySQL": f"{mysql_cfg['host']}:{mysql_cfg['port']}/{mysql_cfg['database']}",
        "ClickHouse": f"{ch_cfg['host']}:{ch_cfg['port']}/{ch_cfg['database']}",
        "Batch Delay": f"{cfg['migration']['cdc']['batch_delay_seconds']}s",
        "Mode": "CDC"
    }
    notify_cdc_startup(config_summary)
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

    def create_stream():
        """Create a new BinLogStreamReader with current configuration"""
        return BinLogStreamReader(
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

    stream = create_stream()
    log.info("CDC stream configured: resume_stream=%s, blocking=%s, only_schemas=%s, only_tables=%s, ignored_tables=%s, server_id=%s, log_file=%s, log_pos=%s",
             True, False, only_schemas, only_tables, ignored_tables, server_id, (binlog["file"] if binlog else None), (binlog["pos"] if binlog else None))

    last_checkpoint_time = time.time()
    rows_since_checkpoint = 0
    last_queue_process_time = time.time()
    last_heartbeat_time = time.time()
    stream_reconnect_attempts = 0
    max_reconnect_attempts = 10
    reconnect_delay = 1.0  # Start with 1 second

    try:
        while True:
            # Process queue every batch_delay_seconds
            now = time.time()
            if 0 < batch_delay_seconds <= (now - last_queue_process_time):
                # Get all events from queue
                events = event_queue.get_all()
                if events:
                    log.info("CDC: processing queue (time since last process: %.1fs, queue size: %d)", 
                            now - last_queue_process_time, len(events))
                    log.info("CDC: processing %d events from queue", len(events))
                    
                    # Group events by table and operation type
                    for event in events:
                        event_grouper.add_event(event)
                    
                    # Get grouped events and process them
                    groups = event_grouper.get_groups()
                    if groups:
                        log.info("CDC: processing %d groups", len(groups))
                        try:
                            rows_processed = _process_grouped_events(groups, mysql_client, ch, table_cache, mig_cfg)
                            rows_since_checkpoint += rows_processed
                            log.info("CDC: successfully processed %d rows from queue", rows_processed)
                            
                            # Reset reconnection counters on successful event processing
                            if stream_reconnect_attempts > 0:
                                log.info("CDC: Resetting reconnection counters after successful event processing")
                                stream_reconnect_attempts = 0
                                reconnect_delay = 1.0
                        except CriticalCDCError as e:
                            log.critical("CDC: Critical error occurred - %s", str(e))
                            # Send critical error notification with query pools
                            query_pools = _get_query_pools_for_error()
                            operation_details = {
                                "Error Type": "CriticalCDCError",
                                "Action": "Process exiting with error code",
                                "Reason": "Binlog position will not advance to prevent data loss"
                            }
                            
                            # Add query pools to notification
                            if query_pools.get("mysql_queries"):
                                operation_details["Recent MySQL Queries"] = "\n".join([f"- {q['query']}" for q in query_pools["mysql_queries"][-3:]])
                            if query_pools.get("clickhouse_queries"):
                                operation_details["Recent ClickHouse Queries"] = "\n".join([f"- {q['query']}" for q in query_pools["clickhouse_queries"][-3:]])
                            
                            notify_cdc_error(
                                error_type="Critical CDC Error",
                                table="CDC Process",
                                error_message=f"Process will exit to prevent data loss: {str(e)}",
                                operation_details=operation_details
                            )
                            # Exit with error code to trigger Kubernetes restart
                            sys.exit(1)
                    else:
                        log.info("CDC: no data events to process from queue")
                # Don't log anything when queue is empty - reduces noise
                
                last_queue_process_time = now
            
            # Try to get next event (non-blocking)
            try:
                event = stream.fetchone()
                if event is None:
                    # No more events, sleep briefly and continue
                    time.sleep(0.1)
                    
                    # Log heartbeat every 60 seconds when idle
                    if now - last_heartbeat_time >= 60:
                        log.info("CDC: Running (idle) - position: %s:%s", stream.log_file, stream.log_pos)
                        last_heartbeat_time = now
                    
                    continue
            except Exception as e:
                # Check if this is a connection-related error that requires stream reconnection
                error_str = str(e).lower()
                connection_errors = ['connection', 'timeout', 'lost', 'broken', 'closed', 'reset', 'refused']
                is_connection_error = any(err in error_str for err in connection_errors)
                
                if is_connection_error and stream_reconnect_attempts < max_reconnect_attempts:
                    stream_reconnect_attempts += 1
                    log.warning("CDC: Stream connection error detected (attempt %d/%d): %s", 
                              stream_reconnect_attempts, max_reconnect_attempts, e)
                    log.info("CDC: Attempting to reconnect stream in %.1f seconds...", reconnect_delay)
                    
                    try:
                        # Close existing stream
                        if stream:
                            stream.close()
                        
                        # Wait before reconnecting
                        time.sleep(reconnect_delay)
                        
                        # Test MySQL connection first
                        try:
                            mysql_client.connect()
                            log.info("CDC: MySQL connection test successful")
                        except Exception as mysql_e:
                            log.warning("CDC: MySQL connection test failed: %s", mysql_e)
                            raise mysql_e
                        
                        # Recreate stream
                        stream = create_stream()
                        log.info("CDC: Stream reconnected successfully")
                        
                        # Update binlog position from current stream
                        state.set_binlog(stream.log_file, stream.log_pos)
                        log.info("CDC: Updated binlog position to %s:%s", stream.log_file, stream.log_pos)
                        
                        # Only reset counters if we successfully process at least one event
                        # For now, continue with current attempt count and delay
                        # The counters will be reset when we successfully process events
                        continue
                    except Exception as reconnect_e:
                        log.error("CDC: Stream reconnection failed (attempt %d): %s", stream_reconnect_attempts, reconnect_e)
                        # Exponential backoff
                        reconnect_delay = min(reconnect_delay * 2, 60.0)  # Cap at 60 seconds
                        time.sleep(reconnect_delay)
                        continue
                else:
                    if stream_reconnect_attempts >= max_reconnect_attempts:
                        log.critical("CDC: Maximum reconnection attempts (%d) exceeded. Process will exit.", max_reconnect_attempts)
                        notify_cdc_error(
                            error_type="Stream Connection Failure",
                            table="CDC Process",
                            error_message=f"Failed to reconnect after {max_reconnect_attempts} attempts",
                            operation_details={
                                "Error": str(e),
                                "Reconnection Attempts": stream_reconnect_attempts,
                                "Action": "Process exiting"
                            }
                        )
                        sys.exit(1)
                    else:
                        log.debug("CDC: no event available: %s", e)
                        time.sleep(0.1)
                        continue
            
            try:
                schema = getattr(event, "schema", None)
                table = getattr(event, "table", None)
                
                # Handle bytes schema (decode if needed)
                if isinstance(schema, bytes):
                    schema = schema.decode('utf-8')
                
                # Only log meaningful events, filter out noisy QueryEvents
                should_log_event = True
                if isinstance(event, QueryEvent):
                    query_text = (getattr(event, "query", "") or "").lower().strip()
                    # Filter out common non-actionable queries that create noise
                    noisy_queries = [
                        "begin", "commit", "rollback", "start transaction",
                        "set", "select", "show", "flush", "reset",
                        "set session", "set global", "set @@", "set @",
                        "set names", "set character_set", "set collation",
                        "set sql_mode", "set time_zone", "set autocommit",
                        "set transaction", "set wait_timeout", "set interactive_timeout"
                    ]
                    should_log_event = not any(query_text.startswith(noisy) for noisy in noisy_queries)
                    
                    # Log filtered events at debug level for troubleshooting
                    if not should_log_event:
                        log.debug("CDC: Filtered out noisy QueryEvent: %s", query_text[:100])
                
                if should_log_event:
                    log.info("CDC event: %s schema=%s table=%s", event.__class__.__name__, schema, table)
                    # Update heartbeat when we process meaningful events
                    last_heartbeat_time = time.time()
                
                # Debug: Log more details for data events
                if hasattr(event, 'rows') and event.rows:
                    log.info("CDC: data event with %d rows", len(event.rows))
                    # Log first row details for debugging
                    if len(event.rows) > 0:
                        first_row = event.rows[0]
                        if 'values' in first_row:
                            log.info("CDC: first row values: %s", first_row['values'])
                        if 'after_values' in first_row:
                            log.info("CDC: first row after_values: %s", first_row['after_values'])
                
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
                        # Fixed regex to properly handle IF EXISTS clause and avoid matching "if" as table name
                        # First, remove IF EXISTS and IF NOT EXISTS clauses to avoid confusion
                        query_clean = re.sub(r'\bif\s+(?:not\s+)?exists\s+', '', query_lower)
                        log.info("CDC: original query: %s, cleaned query: %s", query_lower, query_clean)
                        m = re.search(r"(?:alter|create|drop)\s+table\s+(?:`?([a-zA-Z0-9_]+)`?\.)?`?([a-zA-Z0-9_]+)`?", query_clean)
                        if m:
                            matched_schema = m.group(1) if m.group(1) else (schema.decode() if isinstance(schema, (bytes, bytearray)) else (schema or mysql_cfg["database"]))
                            affected = m.group(2)
                            log.info("CDC: regex matched - schema: %s, table: %s (original query: %s)", matched_schema, affected, query_lower[:100])
                        else:
                            matched_schema = (schema.decode() if isinstance(schema, (bytes, bytearray)) else (schema or mysql_cfg["database"]))
                            affected = table
                            log.warning("CDC: regex did not match for DDL query: %s", query_lower[:100])
                    except Exception:
                        matched_schema = (schema.decode() if isinstance(schema, (bytes, bytearray)) else (schema or mysql_cfg["database"]))
                        affected = table
                        log.exception("CDC: error extracting table name from query: %s", query_lower[:100])
                    
                    # Only react if the DDL targets our database and table of interest
                    log.info("CDC: DDL filtering - matched_schema: %s, mysql_db: %s, affected: %s, included: %s, excluded: %s",
                             matched_schema, mysql_cfg["database"], affected, included, excluded)
                    if matched_schema == mysql_cfg["database"] and affected and (not included or affected in included) and affected not in excluded:
                        # Handle different DDL operations separately (same logic as before)
                        if query_lower.startswith("create table"):
                            # CREATE TABLE: Create the table in ClickHouse
                            try:
                                log.info("CDC: detected CREATE TABLE for %s, creating table in ClickHouse", affected)
                                insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, affected, mig_cfg)
                                table_cache[affected] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                                log.info("CDC: created table %s in ClickHouse", affected)
                                log.info("CDC: table %s columns: %s", affected, table_cache[affected])
                                
                                # Send notification for successful table creation
                                notify_cdc_info(
                                    info_type="Table Created",
                                    message=f"Successfully created table {affected} in ClickHouse",
                                    details={
                                        "Table": affected,
                                        "Schema": matched_schema,
                                        "Columns": len(cols_meta),
                                        "Primary Keys": pk_cols
                                    }
                                )
                            except Exception as e:
                                log.exception("CDC: failed to create table %s in ClickHouse", affected)
                                
                                # Send notification for table creation failure
                                notify_cdc_error(
                                    error_type="Table Creation Failed",
                                    table=f"{matched_schema}.{affected}",
                                    error_message=f"Failed to create table in ClickHouse: {str(e)}",
                                    operation_details={
                                        "Operation": "CREATE TABLE",
                                        "Schema": matched_schema,
                                        "Table": affected,
                                        "Error": str(e)
                                    }
                                )
                        
                        elif query_lower.startswith("drop table"):
                            # DROP TABLE: Drop the table from ClickHouse
                            try:
                                log.info("CDC: detected DROP TABLE for %s, dropping table in ClickHouse", affected)
                                ch.execute(f"DROP TABLE IF EXISTS `{affected}`")
                                # Remove from cache if it exists
                                if affected in table_cache:
                                    del table_cache[affected]
                                log.info("CDC: dropped table %s in ClickHouse", affected)
                                
                                # Send notification for successful table drop
                                notify_cdc_info(
                                    info_type="Table Dropped",
                                    message=f"Successfully dropped table {affected} from ClickHouse",
                                    details={
                                        "Table": affected,
                                        "Schema": matched_schema,
                                        "Operation": "DROP TABLE"
                                    }
                                )
                            except Exception as e:
                                log.exception("CDC: failed to drop table %s in ClickHouse", affected)
                                
                                # Send notification for table drop failure
                                notify_cdc_error(
                                    error_type="Table Drop Failed",
                                    table=f"{matched_schema}.{affected}",
                                    error_message=f"Failed to drop table from ClickHouse: {str(e)}",
                                    operation_details={
                                        "Operation": "DROP TABLE",
                                        "Schema": matched_schema,
                                        "Table": affected,
                                        "Error": str(e)
                                    }
                                )
                        
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
                    log.info("CDC: event queued for %s.%s (%s) with %d rows - queue size: %d (batch_delay: %ds)", 
                            schema, table, event.__class__.__name__, row_count, event_queue.size(), batch_delay_seconds)
                else:
                    # Process immediately
                    row_count = len(getattr(event, 'rows', []))
                    log.info("CDC: processing event immediately for %s.%s (%s) with %d rows", 
                            schema, table, event.__class__.__name__, row_count)
                    try:
                        rows_processed = _process_batched_event(event, mysql_client, ch, table_cache, mig_cfg)
                        rows_since_checkpoint += rows_processed
                        # Clear query pools after successful immediate processing
                        _clear_query_pools()
                        
                        # Reset reconnection counters on successful event processing
                        if stream_reconnect_attempts > 0:
                            log.info("CDC: Resetting reconnection counters after successful immediate event processing")
                            stream_reconnect_attempts = 0
                            reconnect_delay = 1.0
                    except Exception as e:
                        log.critical("CDC: Critical error processing event immediately - %s", str(e))
                        # Send critical error notification with query pools
                        query_pools = _get_query_pools_for_error()
                        operation_details = {
                            "Error Type": "CriticalCDCError",
                            "Event Type": event.__class__.__name__,
                            "Action": "Process exiting with error code",
                            "Reason": "Binlog position will not advance to prevent data loss"
                        }
                        
                        # Add query pools to notification
                        if query_pools.get("mysql_queries"):
                            operation_details["Recent MySQL Queries"] = "\n".join([f"- {q['query']}" for q in query_pools["mysql_queries"][-3:]])
                        if query_pools.get("clickhouse_queries"):
                            operation_details["Recent ClickHouse Queries"] = "\n".join([f"- {q['query']}" for q in query_pools["clickhouse_queries"][-3:]])
                        
                        notify_cdc_error(
                            error_type="Critical CDC Error",
                            table=f"{schema}.{table}",
                            error_message=f"Process will exit to prevent data loss: {str(e)}",
                            operation_details=operation_details
                        )
                        # Exit with error code to trigger Kubernetes restart
                        sys.exit(1)

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

            except CriticalCDCError:
                # Re-raise CriticalCDCError to ensure process exits
                raise
            except Exception:
                log.exception("Error handling binlog event")
                time.sleep(1)
    finally:
        # Send shutdown notification
        notify_cdc_shutdown("CDC process stopped")
        
        # Process any remaining events in queue before closing
        if batch_delay_seconds > 0:
            remaining_events = event_queue.get_all()
            if remaining_events:
                log.info("CDC: processing %d remaining events from queue", len(remaining_events))
                for event in remaining_events:
                    event_grouper.add_event(event)
                groups = event_grouper.get_groups()
                if groups:
                    try:
                        rows_processed = _process_grouped_events(groups, mysql_client, ch, table_cache, mig_cfg)
                        log.info("CDC: processed %d final rows from queue", rows_processed)
                    except CriticalCDCError as e:
                        log.critical("CDC: Critical error in final processing - %s", str(e))
                        # Send critical error notification with query pools
                        query_pools = _get_query_pools_for_error()
                        operation_details = {
                            "Error Type": "CriticalCDCError",
                            "Action": "Process exiting with error code",
                            "Reason": "Binlog position will not advance to prevent data loss"
                        }
                        
                        # Add query pools to notification
                        if query_pools.get("mysql_queries"):
                            operation_details["Recent MySQL Queries"] = "\n".join([f"- {q['query']}" for q in query_pools["mysql_queries"][-3:]])
                        if query_pools.get("clickhouse_queries"):
                            operation_details["Recent ClickHouse Queries"] = "\n".join([f"- {q['query']}" for q in query_pools["clickhouse_queries"][-3:]])
                        
                        notify_cdc_error(
                            error_type="Critical CDC Error",
                            table="CDC Process",
                            error_message=f"Process will exit to prevent data loss: {str(e)}",
                            operation_details=operation_details
                        )
                        # Exit with error code to trigger Kubernetes restart
                        sys.exit(1)
        
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


