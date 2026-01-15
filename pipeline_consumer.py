import logging
import time
import re
import threading
from datetime import datetime, date
from clickhouse_client import CHClient
from buffer import BufferDB
from notifications import notify_cdc_error

log = logging.getLogger(__name__)

# Regex patterns for datetime detection
ISO_DATETIME_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}')
ISO_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def _convert_for_clickhouse(v, column_type=None):
    """
    Convert Python types to ClickHouse-compatible types.
    - Converts bytes to strings
    - Converts datetime strings to datetime objects for Date/DateTime columns
    - Converts date strings to date objects for Date columns
    - Keeps strings as-is for String columns (including JSON)
    """
    if v is None:
        return None
    elif isinstance(v, bytes):
        # Convert bytes to string
        try:
            return v.decode('utf-8')
        except UnicodeDecodeError:
            return v.decode('latin-1')
    elif isinstance(v, (datetime, date)):
        # Already datetime/date objects - pass through
        return v
    elif isinstance(v, str) and column_type:
        # Check if column type requires datetime/date conversion
        column_type_lower = column_type.lower()
        # Check for Date type
        if column_type_lower.startswith('date') and not column_type_lower.startswith('datetime'):
            # Date column - convert date strings to date objects
            if ISO_DATE_PATTERN.match(v):
                try:
                    return date.fromisoformat(v)
                except (ValueError, TypeError):
                    return v
        # Check for DateTime/DateTime64 types
        elif 'datetime' in column_type_lower:
            # DateTime column - convert datetime strings to datetime objects
            if ISO_DATETIME_PATTERN.match(v):
                try:
                    # Handle both 'T' and ' ' separators, and 'Z' timezone
                    if 'T' in v:
                        return datetime.fromisoformat(v.replace('Z', '+00:00'))
                    else:
                        return datetime.strptime(v[:19], '%Y-%m-%d %H:%M:%S')
                except (ValueError, TypeError):
                    return v
    # Pass through all other types (strings for String columns, numbers, etc.) as-is
    return v


def _deserialize_value(v):
    """
    Convert JSON-serialized values back to Python types that ClickHouse expects.
    Note: We keep datetime strings as strings - conversion happens later based on column types.
    """
    if v is None:
        return None
    # Keep all values as-is from JSON (strings stay strings, numbers stay numbers)
    return v


def _deserialize_row(row):
    """Deserialize all values in a row (list)"""
    return [_deserialize_value(v) for v in row]


def _convert_row_for_clickhouse(row, column_types=None):
    """
    Convert values in a row to ClickHouse-compatible types.
    column_types: list of column type strings (e.g., ['Int64', 'String', 'DateTime64(3)'])
    """
    if column_types and len(column_types) == len(row):
        return [_convert_for_clickhouse(v, column_types[i]) for i, v in enumerate(row)]
    else:
        # Fallback: convert without type info (only bytes conversion)
        return [_convert_for_clickhouse(v) for v in row]


class PipelineConsumer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.mig_cfg = cfg.get("migration", {})
        self.buffer = BufferDB()
        self.ch = CHClient(cfg["clickhouse"], self.mig_cfg)
        self._slow_query_threshold = 5.0  # Log queries slower than 5 seconds
        # Cache for ClickHouse column types: {table: [column_name, column_type, ...]}
        self._column_types_cache = {}
        self._column_types_cache_lock = threading.Lock()
    
    def _get_column_types(self, table, sql):
        """
        Get column types for a table from ClickHouse.
        Extracts column names from SQL INSERT statement and queries ClickHouse for types.
        Returns: list of column type strings matching the order of columns in SQL
        """
        # Check cache first
        with self._column_types_cache_lock:
            if table in self._column_types_cache:
                cached_cols, cached_types = self._column_types_cache[table]
                # Extract column names from SQL
                import re
                match = re.search(r'INSERT INTO[^(]*\(([^)]+)\)', sql, re.IGNORECASE)
                if match:
                    sql_columns = [col.strip().strip('`') for col in match.group(1).split(',')]
                    # If columns match cached columns, return cached types
                    if sql_columns == cached_cols:
                        return cached_types
        
        # Extract column names and table name from SQL INSERT statement
        import re
        # Match: INSERT INTO `db`.`table` (columns) VALUES
        table_match = re.search(r'INSERT INTO\s+`([^`]+)`\.`([^`]+)`', sql, re.IGNORECASE)
        if table_match:
            db_name, table_name = table_match.groups()
            # Use separate backticks for db and table (ClickHouse format)
            full_table_sql = f"`{db_name}`.`{table_name}`"
        else:
            # Fallback: try without schema
            table_match = re.search(r'INSERT INTO\s+`([^`]+)`', sql, re.IGNORECASE)
            if table_match:
                table_name_only = table_match.group(1)
                full_table_sql = f"`{self.ch.db}`.`{table_name_only}`"
            else:
                # Last resort: use table parameter
                if '.' in table:
                    # Table already has schema
                    parts = table.split('.', 1)
                    full_table_sql = f"`{parts[0]}`.`{parts[1]}`"
                else:
                    full_table_sql = f"`{self.ch.db}`.`{table}`"
        
        match = re.search(r'INSERT INTO[^(]*\(([^)]+)\)', sql, re.IGNORECASE)
        if not match:
            return None  # Can't extract columns
        
        sql_columns = [col.strip().strip('`') for col in match.group(1).split(',')]
        
        try:
            # Query ClickHouse for column types - use separate backticks for db.table
            result = self.ch.execute(f"DESCRIBE TABLE {full_table_sql}")
            # result is list of tuples: [(name, type, ...), ...]
            column_info = {row[0]: row[1] for row in result}
            
            # Map SQL columns to types
            column_types = [column_info.get(col, 'String') for col in sql_columns]
            
            # Cache the result
            with self._column_types_cache_lock:
                self._column_types_cache[table] = (sql_columns, column_types)
            
            return column_types
        except Exception as e:
            log.warning(f"Failed to get column types for table {table}: {e}")
            return None

    def run(self):
        log.info("Starting Pipeline Consumer...")
        
        while True:
            try:
                # 1. Fetch Batch of Prepared Queries
                queries = self.buffer.fetch_prepared_queries_batch(limit=100)

                if not queries:
                    time.sleep(self.mig_cfg.get("cdc", {}).get('batch_delay_seconds', 5))
                    continue

                processed_ids = []

                # 2. Execute Queries
                for q in queries:
                    try:
                        sql = q['sql']
                        params = q['params']
                        table = q.get('table')
                        is_ddl = sql.strip().upper().startswith(('DROP ', 'CREATE ', 'ALTER ', 'TRUNCATE '))

                        # For DDL queries (like deferred DROP), execute directly
                        if is_ddl:
                            start_exec = time.time()
                            self.ch.execute(sql)
                            exec_time = time.time() - start_exec
                            if self.mig_cfg.get('debug'):
                                log.info(f"Consumer: DDL executed: {sql[:100]}... ({exec_time:.2f}s)")
                            if exec_time > self._slow_query_threshold:
                                log.warning(f"Consumer: SLOW DDL query took {exec_time:.2f}s: {sql[:200]}")
                            processed_ids.append(q['id'])
                            continue

                        # Execute query directly - retry once if table doesn't exist
                        # params is a list of rows for bulk insert
                        # Deserialize datetime strings back to datetime objects, then convert to ClickHouse-compatible format
                        start_exec = time.time()
                        retry_count = 0
                        max_retries = 1  # Retry once for table doesn't exist errors
                        row_count = len(params) if params else 0
                        
                        while retry_count <= max_retries:
                            try:
                                if params:
                                    # Step 1: Deserialize JSON values
                                    try:
                                        deserialized_params = [_deserialize_row(row) for row in params]
                                    except Exception as deserialize_err:
                                        # Wrap error with context about deserialization step
                                        raise RuntimeError(f"Failed to deserialize params: {deserialize_err}") from deserialize_err
                                    
                                    # Step 2: Get column types and convert for ClickHouse compatibility
                                    try:
                                        column_types = self._get_column_types(table, sql)
                                        clickhouse_params = [_convert_row_for_clickhouse(row, column_types) for row in deserialized_params]
                                    except Exception as convert_err:
                                        # Wrap error with context about conversion step
                                        raise RuntimeError(f"Failed to convert params for ClickHouse: {convert_err}") from convert_err
                                    
                                    # Step 3: Execute query
                                    try:
                                        self.ch.client.execute(sql, clickhouse_params)
                                    except Exception as exec_err:
                                        # Wrap error with context about execution step
                                        raise RuntimeError(f"Failed to execute query: {exec_err}") from exec_err
                                else:
                                    # Handling for cases where only SQL is provided
                                    self.ch.execute(sql)
                                # Success - break out of retry loop
                                break
                            except Exception as exec_err:
                                error_str = str(exec_err).lower()
                                # Check if it's a "table doesn't exist" error
                                is_table_not_exists = (
                                    "doesn't exist" in error_str or 
                                    "does not exist" in error_str or
                                    "table" in error_str and "exist" in error_str
                                )
                                
                                if is_table_not_exists and retry_count < max_retries:
                                    # Table might be created by transformer, wait 2 seconds and retry once
                                    # Clear column types cache for this table so we re-fetch after table is created
                                    with self._column_types_cache_lock:
                                        self._column_types_cache.pop(table, None)
                                    log.warning(f"Table {table} doesn't exist, retrying after 2s (attempt {retry_count + 1}/{max_retries + 1})")
                                    time.sleep(2)
                                    retry_count += 1
                                    continue
                                else:
                                    # Not a table existence error, or retries exhausted - re-raise
                                    raise
                        
                        exec_time = time.time() - start_exec
                        
                        if exec_time > self._slow_query_threshold:
                            log.warning(f"Consumer: SLOW query took {exec_time:.2f}s for table {table} ({row_count} rows)")
                        if self.mig_cfg.get('debug'):
                            log.info(
                                f"Consumer: INSERT {row_count} rows into {table} ({exec_time:.2f}s)")

                        processed_ids.append(q['id'])

                    except Exception as e:
                        log.exception(f"Consumer failed to execute query id={q['id']}")
                        # Send notification with full query details before crashing
                        error_msg = f"Execution failed: {str(e)}"
                        notify_cdc_error(
                            "Consumer Error",
                            f"{q.get('schema')}.{q.get('table')}",
                            error_msg,
                            {
                                "Query ID": q['id'],
                                "SQL": q['sql'],
                                "Params": str(q.get('params', []))[:500] if q.get('params') else None,
                                "Schema": q.get('schema'),
                                "Table": q.get('table'),
                                "Error Type": type(e).__name__
                            },
                            exc=e
                        )
                        # Query remains in prepared_queries for retry after restart
                        # Crash consumer to stop processing - main loop will detect and shut down gracefully
                        raise

                # 3. Commit (Delete from Buffer) - only delete successfully processed queries
                if processed_ids:
                    self.buffer.delete_prepared_queries(processed_ids)
                    if self.mig_cfg.get('debug'):
                        log.info(f"Consumer executed {len(processed_ids)} queries")

            except Exception as e:
                # Outer exception handler - any unhandled error crashes the consumer
                log.critical(f"Pipeline Consumer fatal error: {str(e)}")
                notify_cdc_error(
                    "Consumer Fatal Error",
                    "N/A",
                    f"Consumer crashed: {str(e)}",
                    {"Error Type": type(e).__name__},
                    exc=e
                )
                # Re-raise to crash the thread - main loop will detect and shut down
                raise
            time.sleep(self.mig_cfg.get("cdc", {}).get('batch_delay_seconds', 5))
