import logging
import time
import re
from datetime import datetime
from clickhouse_client import CHClient
from buffer import BufferDB
from notifications import notify_cdc_error

log = logging.getLogger(__name__)

# Regex patterns for datetime detection
ISO_DATETIME_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}')
ISO_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')


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


class PipelineConsumer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.mig_cfg = cfg.get("migration", {})
        self.buffer = BufferDB()
        self.ch = CHClient(cfg["clickhouse"], self.mig_cfg)

    def run(self):
        if self.mig_cfg.get('debug'):
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
                            processed_ids.append(q['id'])
                            continue

                        # Verify table exists before insert (with retry)
                        if table and params:
                            max_wait = 20  # seconds
                            waited = 0
                            table_exists = False

                            while waited < max_wait:
                                exists = self.ch.client.execute(
                                    f"EXISTS TABLE `{self.ch.db}`.`{table}`"
                                )
                                if exists and exists[0][0] == 1:
                                    table_exists = True
                                    break
                                time.sleep(1)
                                waited += 1

                            if not table_exists:
                                msg = f"Table {table} does not exist after {max_wait}s wait. Skipping query."
                                log.error(msg)
                                notify_cdc_error('Consumer Error', table, msg, {"Query ID": q['id'], "SQL": q['sql']})
                                self.buffer.move_to_failed(q['id'], msg)
                                continue  # Skip execution, don't add to processed_ids (handled by move_to_failed)

                        # params is a list of rows for bulk insert
                        # Deserialize datetime strings back to datetime objects
                        start_exec = time.time()
                        if params:
                            deserialized_params = [_deserialize_row(row) for row in params]
                            self.ch.client.execute(sql, deserialized_params)
                        else:
                            # Handling for cases where only SQL is provided
                            self.ch.execute(sql)
                        exec_time = time.time() - start_exec
                        if self.mig_cfg.get('debug'):
                            log.info(
                                f"Consumer: INSERT {len(params) if params else 1} rows into {table} ({exec_time:.2f}s)")

                        processed_ids.append(q['id'])

                    except Exception as e:
                        log.exception(f"Consumer failed to execute query id={q['id']}")
                        # Move failed query to failed_queries table so it doesn't block CDC sync
                        # This allows other queries to continue processing
                        error_msg = f"Execution failed: {str(e)}"
                        self.buffer.move_to_failed(q['id'], error_msg)
                        log.warning(f"Moved query id={q['id']} to failed_queries: {error_msg}")
                        notify_cdc_error(
                            "Consumer Error",
                            f"{q.get('schema')}.{q.get('table')}",
                            error_msg,
                            {"Query ID": q['id'], "SQL": q['sql']}
                        )
                        # Continue processing other queries in the batch instead of raising
                        continue

                        # 3. Commit (Delete from Buffer)
                if processed_ids:
                    self.buffer.delete_prepared_queries(processed_ids)
                    if self.mig_cfg.get('debug'):
                        log.info(f"Consumer executed {len(processed_ids)} queries")

            except Exception as e:
                log.error(f"Pipeline Consumer loop error: {str(e)}")
                notify_cdc_error(
                    "Consumer Error",
                    "N/A",
                    str(e)
                )
                time.sleep(1)

            time.sleep(self.mig_cfg.get("cdc", {}).get('batch_delay_seconds', 15))
