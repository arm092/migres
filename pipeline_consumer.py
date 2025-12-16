import logging
import time
import re
from datetime import datetime, date
from clickhouse_client import CHClient
from buffer import BufferDB
from notifications import notify_cdc_error

log = logging.getLogger(__name__)

# Regex patterns for datetime detection
ISO_DATETIME_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}')
ISO_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def _convert_for_clickhouse(v):
    """
    Convert Python types to ClickHouse-compatible types.
    ClickHouse driver handles datetime/date objects natively for DateTime64/Date columns.
    Only convert bytes to strings.
    """
    if v is None:
        return None
    elif isinstance(v, bytes):
        # Convert bytes to string
        try:
            return v.decode('utf-8')
        except UnicodeDecodeError:
            return v.decode('latin-1')
    else:
        # Pass through datetime, date, and other types - ClickHouse driver handles them
        return v


def _deserialize_value(v):
    """
    Convert JSON-serialized values back to Python types that ClickHouse expects.
    Specifically handles datetime strings -> datetime objects.
    """
    if v is None:
        return None
    if isinstance(v, str):
        # Try to parse as datetime
        if ISO_DATETIME_PATTERN.match(v):
            try:
                # Handle various ISO formats
                if 'T' in v:
                    return datetime.fromisoformat(v.replace('Z', '+00:00'))
                else:
                    return datetime.strptime(v[:19], '%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                pass
        # Try to parse as date
        if ISO_DATE_PATTERN.match(v):
            try:
                return datetime.strptime(v, '%Y-%m-%d').date()
            except (ValueError, TypeError):
                pass
    return v


def _deserialize_row(row):
    """Deserialize all values in a row (list)"""
    return [_deserialize_value(v) for v in row]


def _convert_row_for_clickhouse(row):
    """Convert bytes objects in a row to strings for ClickHouse compatibility"""
    return [_convert_for_clickhouse(v) for v in row]


class PipelineConsumer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.mig_cfg = cfg.get("migration", {})
        self.buffer = BufferDB()
        self.ch = CHClient(cfg["clickhouse"], self.mig_cfg)
        self._slow_query_threshold = 5.0  # Log queries slower than 5 seconds

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
                                    deserialized_params = [_deserialize_row(row) for row in params]
                                    # Convert datetime/date objects to strings for ClickHouse compatibility
                                    clickhouse_params = [_convert_row_for_clickhouse(row) for row in deserialized_params]
                                    self.ch.client.execute(sql, clickhouse_params)
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
