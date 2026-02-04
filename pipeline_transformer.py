import logging
import threading
import time
import re
from collections import defaultdict
from mysql_client import MySQLClient
from clickhouse_client import CHClient
from buffer import BufferDB
from schema_and_ddl import build_table_ddl, ensure_clickhouse_columns

log = logging.getLogger(__name__)

# DDL patterns for detection
# Matches: CREATE TABLE [IF NOT EXISTS] [db.]table ...
# Removed ^ anchor to allow comments/whitespace at start
DDL_CREATE_TABLE_PATTERN = re.compile(r'create\s+table\s+(?:if\s+not\s+exists\s+)?(?:`?\w+`?\.)?`?(\w+)`?', re.IGNORECASE)
DDL_DROP_TABLE_PATTERN = re.compile(r'drop\s+table\s+(?:if\s+exists\s+)?(?:`?\w+`?\.)?`?(\w+)`?', re.IGNORECASE)
DDL_ALTER_TABLE_PATTERN = re.compile(r'alter\s+table\s+(?:`?\w+`?\.)?`?(\w+)`?', re.IGNORECASE)
DDL_TRUNCATE_TABLE_PATTERN = re.compile(r'truncate\s+(?:table\s+)?(?:`?\w+`?\.)?`?(\w+)`?', re.IGNORECASE)

class PipelineTransformer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.mig_cfg = cfg.get("migration", {})
        cdc_cfg = self.mig_cfg.get("cdc", {})
        db_debug = cdc_cfg.get("db_debug", False)
        self.buffer = BufferDB(db_debug=db_debug)
        self.mysql_client = MySQLClient(cfg["mysql"])
        self.ch = CHClient(cfg["clickhouse"], self.mig_cfg)
        self._last_commit_ns = 0
        self._shutdown_flag = threading.Event()

        # Cache for table schema
        # table_name -> (insert_cols, mysql_col_names)
        self.table_cache = {}

    def _ensure_table_schema(self, table: str) -> tuple:
        """Ensure ClickHouse table exists and has correct columns"""
        if table in self.table_cache:
            # Verify table still exists (might have been dropped)
            try:
                exists_check = self.ch.client.execute(
                    f"EXISTS TABLE `{self.ch.db}`.`{table}`"
                )
                if exists_check and exists_check[0][0] == 1:
                    return self.table_cache[table]
                else:
                    # Table was dropped, remove from cache and recreate
                    log.warning(f"Table {table} no longer exists, recreating...")
                    del self.table_cache[table]
            except Exception as e:
                log.warning(f"Failed to check table existence for {table}: {e}")
                del self.table_cache[table]
        
        # Retry logic for table creation (in case MySQL table was just created)
        # Reduced retries and wait times to avoid blocking event processing
        max_retries = 3
        last_error = None
        
        for attempt in range(max_retries):
            try:
                self.mysql_client.connect()
                cols_meta, pk_cols = self.mysql_client.get_table_columns_and_pk(table)
                
                if not cols_meta:
                    if attempt < max_retries - 1:
                        wait_time = min(1 * (attempt + 1), 3)  # Linear backoff, max 3s
                        log.warning(f"Table {table} not found in MySQL (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    raise ValueError(f"No columns found for table {table} after {max_retries} attempts")
                    
                # Create/Update ClickHouse Table
                ddl, insert_cols = build_table_ddl(table, cols_meta, pk_cols, self.mig_cfg)
                if self.mig_cfg.get('debug'):
                    log.info(f"Creating/updating ClickHouse table: {table}")
                self.ch.execute(ddl)
                
                # Verify table was created
                exists_check = self.ch.client.execute(
                    f"EXISTS TABLE `{self.ch.db}`.`{table}`"
                )
                if not exists_check or exists_check[0][0] != 1:
                    if attempt < max_retries - 1:
                        wait_time = min(1 * (attempt + 1), 3)
                        log.warning(f"Table {table} creation verification failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    raise RuntimeError(f"Table {table} was not created successfully after {max_retries} attempts")
                
                # Ensure columns exist (for schema evolution)
                desired = []
                for col in cols_meta:
                    name = col["COLUMN_NAME"]
                    # Helper for low cardinality
                    from cdc import _map_with_low_cardinality
                    ch_type = _map_with_low_cardinality(col, self.mig_cfg)
                    
                    # Default expression
                    from schema_and_ddl import _default_expr_for_column
                    try:
                        default_expr = _default_expr_for_column(col, ch_type)
                    except (ValueError, TypeError):
                        default_expr = None
                    desired.append({"name": name, "type_sql": ch_type, "default_expr": default_expr})
                
                desired.extend([
                    ("__data_transfer_commit_time", "UInt64"),
                    ("__data_transfer_delete_time", "UInt64")
                ])
                
                ensure_clickhouse_columns(self.ch, table, desired)
                
                self.table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                return self.table_cache[table]
                
            except ValueError as e:
                # If table doesn't exist in MySQL, retry a few times
                last_error = e
                if attempt < max_retries - 1:
                    wait_time = min(1 * (attempt + 1), 3)
                    log.warning(f"Table {table} schema fetch failed (attempt {attempt + 1}/{max_retries}): {e}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                raise
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    wait_time = min(1 * (attempt + 1), 3)
                    log.warning(f"Schema sync failed for {table} (attempt {attempt + 1}/{max_retries}): {e}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                log.error(f"Schema sync failed for {table} after {max_retries} attempts: {e}")
                raise
        
        # Should not reach here, but just in case
        if last_error:
            raise last_error
        raise RuntimeError(f"Failed to ensure table schema for {table} after {max_retries} attempts")

    def _handle_ddl_event(self, event_data: dict) -> bool:
        """
        Handle DDL events (CREATE TABLE, DROP TABLE, ALTER TABLE, etc.)
        Returns True if handled successfully, False if should be skipped.
        """
        query = event_data.get("query", "")
        if not query:
            return True  # No query to process
            
        query_lower = query.lower().strip()

        # Skip transaction control statements (check before logging)
        if query_lower in ("begin", "commit", "rollback", "xa start", "xa end", "xa prepare", "xa commit", "xa rollback") or query_lower.startswith("savepoint"):
            return True

        if self.mig_cfg.get('debug'):
            log.info(f"DDL: Processing query: {query[:200]}...")
            
        # CREATE TABLE
        match = DDL_CREATE_TABLE_PATTERN.search(query)
        if match:
            table_name = match.group(1)
            if self.mig_cfg.get('debug'):
                log.info(f"DDL: Detected CREATE TABLE for {table_name}")
            try:
                # Invalidate cache if exists
                if table_name in self.table_cache:
                    del self.table_cache[table_name]
                # The table will be created when first data event arrives,
                # or we can proactively create it now.
                # Retry with delay and reconnect since table might not be visible 
                # immediately after CREATE due to INFORMATION_SCHEMA caching
                max_retries = 10
                for attempt in range(max_retries):
                    try:
                        # Close and reconnect to get fresh metadata view
                        if attempt > 0:
                            try:
                                self.mysql_client.close()
                                time.sleep(0.5)  # Brief pause to allow connection cleanup
                            except Exception:
                                pass
                        self._ensure_table_schema(table_name)
                        if self.mig_cfg.get('debug'):
                            log.info(f"DDL: Created table {table_name} in ClickHouse")
                        return True
                    except (ValueError, RuntimeError) as e:
                        error_msg = str(e)
                        # Check for table not found errors (both ValueError and RuntimeError)
                        if ("No columns found" in error_msg or "not found" in error_msg.lower() or 
                            "was not created successfully" in error_msg) and attempt < max_retries - 1:
                            wait_time = min(2 ** attempt, 10)  # Exponential backoff, max 10s
                            log.warning(f"DDL: Table {table_name} not visible yet, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                            time.sleep(wait_time)
                        else:
                            raise
                    except Exception as e:
                        # For other exceptions, log and retry if not last attempt
                        if attempt < max_retries - 1:
                            wait_time = min(2 ** attempt, 5)  # Shorter backoff for other errors
                            log.warning(f"DDL: Error creating table {table_name} (attempt {attempt + 1}/{max_retries}): {e}, retrying in {wait_time}s...")
                            time.sleep(wait_time)
                        else:
                            log.error(f"DDL: Failed to create table {table_name} after {max_retries} attempts: {e}")
                            raise
            except Exception as e:
                log.error(f"DDL: Failed to create table {table_name}: {e}")
                raise
                
        # DROP TABLE
        match = DDL_DROP_TABLE_PATTERN.search(query)
        if match:
            table_name = match.group(1)
            if self.mig_cfg.get('debug'):
                log.info(f"DDL: Detected DROP TABLE for {table_name}")
            try:
                # Invalidate cache
                if table_name in self.table_cache:
                    del self.table_cache[table_name]
                # Drop in ClickHouse
                self.ch.execute(f"DROP TABLE IF EXISTS `{self.ch.db}`.`{table_name}`")
                if self.mig_cfg.get('debug'):
                    log.info(f"DDL: Dropped table {table_name} in ClickHouse")
                return True
            except Exception as e:
                log.error(f"DDL: Failed to drop table {table_name}: {e}")
                raise
                
        # ALTER TABLE
        match = DDL_ALTER_TABLE_PATTERN.search(query)
        if match:
            table_name = match.group(1)
            if self.mig_cfg.get('debug'):
                log.info(f"DDL: Detected ALTER TABLE for {table_name}")
            try:
                # Invalidate cache to force schema refresh
                if table_name in self.table_cache:
                    del self.table_cache[table_name]
                
                # Retry logic for ALTER TABLE - MySQL might need time to commit the ALTER
                max_retries = 5
                for attempt in range(max_retries):
                    try:
                        # Close and reconnect to get fresh metadata view
                        if attempt > 0:
                            try:
                                self.mysql_client.close()
                                time.sleep(0.5)  # Brief pause to allow connection cleanup
                            except Exception:
                                pass
                        # Re-sync schema - this will add/modify columns as needed
                        self._ensure_table_schema(table_name)
                        if self.mig_cfg.get('debug'):
                            log.info(f"DDL: Synchronized schema for {table_name} after ALTER")
                        return True
                    except ValueError as e:
                        if "No columns found" in str(e) and attempt < max_retries - 1:
                            wait_time = min(2 ** attempt, 5)  # Exponential backoff, max 5s
                            log.warning(f"DDL: Table {table_name} schema not updated yet, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                            time.sleep(wait_time)
                        else:
                            raise
                    except Exception as e:
                        if attempt < max_retries - 1:
                            wait_time = min(2 ** attempt, 5)
                            log.warning(f"DDL: Error syncing schema for {table_name}, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries}): {e}")
                            time.sleep(wait_time)
                        else:
                            log.error(f"DDL: Failed to sync schema for {table_name} after {max_retries} attempts: {e}")
                            raise
            except Exception as e:
                log.error(f"DDL: Failed to sync schema for {table_name}: {e}")
                raise
                
        # TRUNCATE TABLE
        match = DDL_TRUNCATE_TABLE_PATTERN.search(query)
        if match:
            table_name = match.group(1)
            if self.mig_cfg.get('debug'):
                log.info(f"DDL: Detected TRUNCATE TABLE for {table_name}")
            try:
                self.ch.execute(f"TRUNCATE TABLE IF EXISTS `{self.ch.db}`.`{table_name}`")
                if self.mig_cfg.get('debug'):
                    log.info(f"DDL: Truncated table {table_name} in ClickHouse")
                return True
            except Exception as e:
                log.error(f"DDL: Failed to truncate table {table_name}: {e}")
                raise
                
        # Unknown DDL - log and continue
        if any(kw in query_lower for kw in ('create ', 'drop ', 'alter ', 'rename ', 'truncate ')):
            log.warning(f"DDL: Unhandled DDL statement: {query[:100]}...")
            
        return True

    def _next_commit_ts(self) -> int:
        """Return strictly increasing commit timestamp in nanoseconds."""
        now = time.time_ns()
        if now <= self._last_commit_ns:
            now = self._last_commit_ns + 1
        self._last_commit_ns = now
        return now

    def _generate_sql_for_batch(self, schema: str, table: str, event_type: str, events: list):
        """Generate optimized SQL for a batch of events"""
        try:
            try:
                insert_cols, _ = self._ensure_table_schema(table)
            except (ValueError, RuntimeError) as e:
                # If table doesn't exist in MySQL anymore (e.g. was dropped),
                # we can't generate SQL for it. If these are leftover events, skip them.
                error_msg = str(e)
                if "No columns found" in error_msg or "not found" in error_msg.lower() or "was not created successfully" in error_msg:
                    total_rows = sum(len(ev.get("rows", [])) for ev in events)
                    log.warning(f"Skipping {len(events)} {event_type} events ({total_rows} rows) for {table}: Table not found in MySQL/Schema - {error_msg}")
                    return None
                raise

            if event_type == "WriteRowsEvent":
                # Bulk INSERT
                rows = []
                for event_data in events:
                    if "rows" in event_data:
                        for row in event_data["rows"]:
                            values = row.get("values", {})
                            vals = [values.get(col) for col in insert_cols[:-2]]
                            # Add metadata: commit_time, delete_time=0
                            commit_ns = self._next_commit_ts()
                            rows.append(vals + [commit_ns, 0])
                
                if not rows: return None
                
                # Construct query
                # Use parameterized query format for Python client, or manual string construction
                # Here we construct the SQL string with placeholders for values
                # But since we store "prepared queries", we need the full SQL or params
                # To be safe and simple, we'll store params and let the consumer execute
                
                cols_sql = ", ".join([f"`{c}`" for c in insert_cols])
                sql = f"INSERT INTO `{self.ch.db}`.`{table}` ({cols_sql}) VALUES"
                return {"sql": sql, "params": rows, "schema": schema, "table": table}

            elif event_type == "UpdateRowsEvent":
                # Convert UPDATE to INSERT (ReplacingMergeTree)
                rows = []
                for event_data in events:
                    if "rows" in event_data:
                        for row in event_data["rows"]:
                            after_vals = row.get("after_values", {})
                            vals = [after_vals.get(col) for col in insert_cols[:-2]]
                            commit_ns = self._next_commit_ts()
                            rows.append(vals + [commit_ns, 0])
                            
                if not rows: return None
                cols_sql = ", ".join([f"`{c}`" for c in insert_cols])
                sql = f"INSERT INTO `{self.ch.db}`.`{table}` ({cols_sql}) VALUES"
                return {"sql": sql, "params": rows, "schema": schema, "table": table}

            elif event_type == "DeleteRowsEvent":
                # Convert DELETE to Tombstone INSERT
                rows = []
                for event_data in events:
                    if "rows" in event_data:
                        for row in event_data["rows"]:
                            values = row.get("values", {})
                            vals = [values.get(col) for col in insert_cols[:-2]]
                            commit_ns = self._next_commit_ts()
                            # Set delete_time = commit_time
                            rows.append(vals + [commit_ns, commit_ns])
                            
                if not rows: return None
                cols_sql = ", ".join([f"`{c}`" for c in insert_cols])
                sql = f"INSERT INTO `{self.ch.db}`.`{table}` ({cols_sql}) VALUES"
                return {"sql": sql, "params": rows, "schema": schema, "table": table}
                
        except Exception as e:
            log.error(f"Failed to generate SQL for {table} ({event_type}): {e}")
            raise

    def run(self):
        if self.mig_cfg.get('debug'):
            log.info("Starting Pipeline Transformer...")
        
        last_process_time = time.time()
        
        while not self._shutdown_flag.is_set():
            try:
                # 1. Wait for enough raw events (unless disabled)
                checkpoint_rows = int(self.mig_cfg.get('cdc', {}).get('checkpoint_interval_rows', 5000))
                batch_max_wait = int(self.mig_cfg.get('cdc', {}).get('batch_max_wait_seconds', 60))
                fetch_limit = checkpoint_rows if checkpoint_rows > 0 else 5000

                if checkpoint_rows > 0:
                    while not self._shutdown_flag.is_set():
                        stats = self.buffer.get_queue_stats()
                        raw_count = stats.get("raw_events", 0)
                        
                        # Check if we should process based on row count
                        if raw_count >= checkpoint_rows:
                            break
                            
                        # Check if we should process based on time (if we have any events)
                        time_since_last = time.time() - last_process_time
                        if raw_count > 0 and time_since_last >= batch_max_wait:
                            if self.mig_cfg.get('debug'):
                                log.info(f"Transformer processing due to time limit ({time_since_last:.1f}s >= {batch_max_wait}s), events: {raw_count}")
                            break
                            
                        wait_seconds = self.mig_cfg.get('cdc', {}).get('batch_delay_seconds', 5)
                        if wait_seconds <= 0:
                            wait_seconds = 0.5
                        if self.mig_cfg.get('debug'):
                            log.info(f"Transformer waiting for raw_events to reach {checkpoint_rows} (current={raw_count})")
                        if self._shutdown_flag.wait(timeout=wait_seconds):
                            break

                if self._shutdown_flag.is_set():
                    break

                # 2. Fetch Batch
                raw_events = self.buffer.fetch_raw_events_batch(limit=fetch_limit)
                
                if not raw_events:
                    # Check shutdown flag during sleep
                    if self._shutdown_flag.wait(timeout=5):
                        break
                    continue
                
                # Update last process time since we are processing events
                last_process_time = time.time()
                
                # 3. Separate events: DDL and data events (preserve DDL order)
                ddl_events = []  # Keep ALL DDL in original order
                data_events = []
                tables_with_data = set()  # Track tables that have data in this batch
                
                for event in raw_events:
                    if event['event_type'] == 'QueryEvent':
                        # Filter out transaction control statements that shouldn't be processed as DDL
                        query = event['event_data'].get('query', '').lower().strip()
                        if query not in ("begin", "commit", "rollback", "xa start", "xa end", "xa prepare", "xa commit", "xa rollback"):
                            ddl_events.append(event)
                        # Skip transaction control statements - they're already handled by MySQL
                    else:
                        if event.get('table'):
                            data_events.append(event)
                            tables_with_data.add(event.get('table'))
                
                # 4. Process DDL events in original order
                # But defer DROP for tables that have data in this batch
                ddl_processed = 0
                deferred_drops = []
                tables_created_in_batch = set()  # Track tables created in this batch
                
                for event in ddl_events:
                    query = event['event_data'].get('query', '').lower().strip()
                    
                    # Check if this is a DROP for a table with data in this batch
                    drop_match = DDL_DROP_TABLE_PATTERN.search(query)
                    if drop_match:
                        table_name = drop_match.group(1)
                        if table_name in tables_with_data:
                            # Defer this DROP until after data is processed
                            deferred_drops.append(event)
                            continue
                    
                    # Track CREATE TABLE events
                    create_match = DDL_CREATE_TABLE_PATTERN.search(query)
                    if create_match:
                        tables_created_in_batch.add(create_match.group(1))
                    
                    try:
                        self._handle_ddl_event(event['event_data'])
                        ddl_processed += 1
                    except Exception as e:
                        log.error(f"DDL handling failed: {str(e)}")
                        raise
                
                # 5. Process data events (INSERT, UPDATE, DELETE)
                groups = defaultdict(lambda: {'events': [], 'event_ids': []})  # Track both events and their IDs
                skipped_no_table = 0
                skipped_not_included = 0
                skipped_event_ids = []  # Track events to skip (not in include_tables)
                
                # Check include_tables filter
                included_tables = set(self.cfg.get("mysql", {}).get("include_tables") or [])
                
                for event in data_events:
                    if not event['table']:
                        skipped_no_table += 1
                        skipped_event_ids.append(event['id'])  # Mark as processed to avoid accumulation
                        continue
                    
                    # Skip events for tables not in include_tables (if include_tables is configured)
                    if included_tables and event['table'] not in included_tables:
                        skipped_not_included += 1
                        skipped_event_ids.append(event['id'])  # Mark as processed to avoid accumulation
                        continue
                    
                    key = (event['schema'], event['table'], event['event_type'])
                    groups[key]['events'].append(event['event_data'])
                    groups[key]['event_ids'].append(event['id'])
                
                # Log grouping details
                if groups and self.mig_cfg.get('debug'):
                    group_summary = ", ".join([f"{t}:{et}={len(group['events'])}" for (s, t, et), group in groups.items()])
                    log.info(f"Transformer grouping: {group_summary}")
                
                if skipped_no_table > 0:
                    log.warning(f"Skipped {skipped_no_table} events without table name")
                if skipped_not_included > 0:
                    log.debug(f"Skipped {skipped_not_included} events for tables not in include_tables")
                
                # 6. Generate prepared queries for data events
                prepared_queries = []
                total_rows = 0
                processed_event_ids_for_queries = []  # Track which events successfully generated queries
                
                for (schema, table, event_type), group_data in groups.items():
                    if event_type in ('WriteRowsEvent', 'UpdateRowsEvent', 'DeleteRowsEvent'):
                        events = group_data['events']
                        group_event_ids = group_data['event_ids']
                        
                        try:
                            query = self._generate_sql_for_batch(schema, table, event_type, events)
                            if query:
                                prepared_queries.append(query)
                                total_rows += len(query.get('params', []))
                                # Only mark events as processed if query was successfully generated
                                processed_event_ids_for_queries.extend(group_event_ids)
                            else:
                                log.warning(f"Failed to generate query for {table} ({event_type}), {len(group_event_ids)} events will be retried")
                        except Exception as e:
                            log.error(f"Error generating query for {table} ({event_type}): {e}, {len(group_event_ids)} events will be retried")
                            # Don't add to processed_event_ids_for_queries, so events stay for retry
                
                # 7. Queue deferred DROP DDL events as prepared queries
                # This ensures they execute AFTER the data inserts in the consumer
                # BUT: Skip if the table was recreated (CREATE) in this same batch
                for event in deferred_drops:
                    query = event['event_data'].get('query', '')
                    drop_match = DDL_DROP_TABLE_PATTERN.search(query.lower())
                    if drop_match:
                        table_name = drop_match.group(1)
                        
                        # Skip DROP if table was recreated in this batch
                        # This handles the case where old DROP events from previous runs
                        # are replayed alongside new CREATE events
                        if table_name in tables_created_in_batch:
                            if self.mig_cfg.get('debug'):
                                log.info(f"DDL: Skipping DROP TABLE for {table_name} (table was recreated in this batch)")
                            ddl_processed += 1
                            continue
                        
                        # Invalidate cache now (transformer side)
                        if table_name in self.table_cache:
                            del self.table_cache[table_name]
                        # Queue the DROP as a prepared query for consumer
                        drop_sql = f"DROP TABLE IF EXISTS `{self.ch.db}`.`{table_name}`"
                        prepared_queries.append({
                            "sql": drop_sql,
                            "params": None,  # DDL has no params
                            "schema": event.get('schema'),
                            "table": table_name,
                            "is_ddl": True
                        })
                        ddl_processed += 1
                        if self.mig_cfg.get('debug'):
                            log.info(f"DDL: Queued deferred DROP TABLE for {table_name} (will execute after data inserts)")
                
                if ddl_processed > 0 and self.mig_cfg.get('debug'):
                    log.info(f"Processed {ddl_processed} DDL event(s)")
                
                # 8. Atomic Commit (Insert Queries + Delete Raw Events)
                # Only commit events that successfully generated queries, plus DDL events, plus skipped events
                ddl_event_ids = [e['id'] for e in ddl_events]
                events_to_commit = processed_event_ids_for_queries + ddl_event_ids + skipped_event_ids
                
                if prepared_queries or events_to_commit:
                    self.buffer.commit_prepared_queries(prepared_queries, events_to_commit)
                    if self.mig_cfg.get('debug'):
                        log.info(f"Transformer: {len(events_to_commit)} events -> {len(prepared_queries)} queries ({total_rows} rows), skipped={len(skipped_event_ids)})")
                
            except Exception as e:
                log.exception(f"Pipeline Transformer failed: {str(e)}")
                # On error, check if shutdown was requested
                if self._shutdown_flag.is_set():
                    log.info("Transformer shutting down due to shutdown flag")
                    break
                time.sleep(1) # Retry loop

        log.info("Pipeline Transformer shutdown complete")
