# Module Reference

All modules live in the repository root (flat layout, no package).

## migres.py — entry point
- `main()`: parses `--config`, loads config, registers SIGUSR1/SIGUSR2 handlers (once), initializes
  notifications, dispatches to snapshot or CDC mode. Exit codes: 1 config/critical, 2 snapshot
  failure, 3 CDC failure, 4 unknown mode.
- `run_cdc_pipeline(cfg)`: builds Producer/Transformer/Consumer, starts daemon threads, stores
  everything in module-global `_reset_context`, runs the watchdog loop (signal flags, thread
  liveness + queue stats logging).
- Signal handlers (`_signal_reset`, `_signal_reposition`): set `_signal_flags` events only; the
  main loop calls `_perform_reset()` / `_perform_reposition()` (no work in signal context).
- `_perform_reset` (SIGUSR1): graceful thread shutdown → `_cleanup_reset_artifacts` (drop migres
  CH tables via `list_migres_tables`, delete buffer file and state.json) → `os._exit(0)`.
- `_perform_reposition` (SIGUSR2): shutdown threads → delete buffer file → write
  `force_binlog_position` into state.json → recreate and restart all three stages.

## config.py
- `load_config(path)`: YAML load → `_apply_env_overrides` → defaults. Note: env overrides are
  applied **before** defaults are set.
- `_apply_env_overrides(cfg)`: maps env vars (MYSQL_*, CLICKHOUSE_*, MIGRATION_*, CDC_*,
  NOTIFICATIONS_*, STATE_FILE, BUFFER_FILE, ENVIRONMENT) onto the config dict with type coercion.

## pipeline_producer.py — stage 1
- `PipelineProducer.__init__`: computes `server_id = base + pid % 1000`; resolves the start
  binlog position (buffer → state.json → current master).
- `_create_stream()`: builds `BinLogStreamReader` (non-blocking, `resume_stream=True`,
  only WriteRows/UpdateRows/DeleteRows/QueryEvent, filtered by `only_schemas`/`only_tables`).
- `run()`: calls `assert_cdc_binlog_settings()`; fetch loop with reconnect-on-error (backoff,
  max 10 consecutive errors), batching (`producer_batch_size`, flush every
  `producer_flush_interval`), transaction-control filtering, CREATE TABLE include-list filtering,
  JSON serialization (`_serialize_event`, Decimal→string, bytes→utf8/base64,
  datetime→isoformat), flush to `buffer.insert_raw_events`. On fatal error: notify +
  `sys.exit(1)` (kills the thread; watchdog handles the rest).

## pipeline_transformer.py — stage 2
- DDL detection via anchored parsers in `schema_and_ddl` (`parse_ddl_table_name`, etc.).
- `_table_allowed(table)`: honors `mysql.include_tables` for data and DDL.
- `_ensure_table_schema(table)`: cached per table; verifies CH table still exists; reads MySQL
  schema; builds DDL via `schema_and_ddl.build_table_ddl`; executes it; verifies existence;
  aligns columns via `ensure_clickhouse_columns`. 3 retries. Returns `(insert_cols, mysql_cols)`.
- `_handle_ddl_event(event_data)`: CREATE (10 retries) / DROP / ALTER (5 retries) / TRUNCATE /
  RENAME; inline CREATE/ALTER vs queued DROP/TRUNCATE/RENAME.
- `_generate_sql_for_batch(schema, table, events)`: builds one multi-row INSERT for all events
  of one table; appends `__data_transfer_commit_time` / `__data_transfer_delete_time` values.
- `run()`: waits until `checkpoint_interval_rows` raw events accumulate (or
  `batch_max_wait_seconds` elapse), fetches the batch, processes events in binlog order (flush
  data before inline DDL), groups data events per table preserving per-table order, generates
  queries, commits atomically via `buffer.commit_prepared_queries`. Exceptions are logged and
  the loop retries after 1s. Poll wait: `transformer_poll_interval`.

## pipeline_consumer.py — stage 3
- `_convert_for_clickhouse(v, column_type)`: bytes→str; Decimal/date/datetime conversion by CH
  column type (handles `Nullable(...)`).
- `_is_transient_error(exc)`: network/timeout markers → re-raise (crash for restart retry).
- `_get_column_types(table, sql)`: parses column list from the INSERT SQL, `DESCRIBE TABLE` in
  ClickHouse, caches per table (lock-protected); warns on missing columns.
- `run()`: fetch `prepared_queries` (limit `prepared_queries_batch_limit`), detect DDL by SQL
  prefix, execute; for inserts: deserialize params → type-convert → execute, with a single
  retry (2s) when the error looks like "table doesn't exist". Permanent failures:
  `buffer.move_to_failed()` + Teams notification (no SQL/params in card) + continue. Transient
  failures: re-raise → thread dies → watchdog exits process. Sleeps `consumer_poll_interval`
  only when fetch returned fewer rows than the limit.

## buffer.py — SQLite queue
- `BufferDB(db_path=None, db_debug, cfg)`: path from `cfg["buffer_file"]` (default
  `data/buffer.db`); fails fast if directory not writable (no `/tmp` fallback); thread-local
  connections, WAL, `busy_timeout=30000`.
- Tables: `raw_events(...)`, `prepared_queries(...)`, `failed_queries(id, sql_query, params,
  schema_name, table_name, error_reason, failed_at)`.
- Key methods: `insert_raw_events`, `fetch_raw_events_batch`, `commit_prepared_queries`
  (atomic insert+delete), `fetch_prepared_queries_batch`, `delete_prepared_queries`,
  `move_to_failed` (atomic insert into `failed_queries` + delete from `prepared_queries`),
  `get_last_committed_pos`, `get_queue_stats`.
- `db_debug=true`: before deleting, rows are copied to ClickHouse tables
  `debug_processed_events` / `debug_processed_queries`.

## snapshot.py
- `run_snapshot(cfg)`: binlog position bookkeeping, table listing, parallel workers; raises
  `SnapshotError` if any worker fails.
- `_process_table_worker(table, cfg, state)`: per-table copy with REPEATABLE READ snapshot;
  PK keyset pagination for single integer PKs, otherwise ORDER BY all columns + LIMIT/OFFSET;
  DROP+CREATE for fresh tables, resume without drop for `in_progress`; appends `commit_ns, 0`
  metadata to each row; state updates per batch.

## schema_and_ddl.py
- Shared helpers (formerly in removed `cdc.py`): `quote_ident`, `map_with_low_cardinality`,
  `binlog_position_key`, DDL parsers (`parse_ddl_table_name`, `parse_rename_table_pairs`, …).
- `map_mysql_to_ch_type(column, mig_cfg)`: MySQL → ClickHouse type mapping.
- `_default_expr_for_column(col, ch_type)`: maps MySQL COLUMN_DEFAULT with proper escaping.
- `build_table_ddl(table, columns_meta, pk_columns, mig_cfg)`: CREATE TABLE with metadata
  columns; synthesizes `__migres_pk` when no PK; ReplacingMergeTree by default.
- `ensure_clickhouse_columns(ch, table, desired)`: ALTER TABLE ADD COLUMN IF NOT EXISTS.

## mysql_client.py
- `MySQLClient`: thin wrapper over `mysql.connector`; optional TLS via `ssl_ca` / `ssl_disabled`;
  `assert_cdc_binlog_settings()`, `start_repeatable_snapshot()`, keyset/ordered pagination.

## clickhouse_client.py
- `CHClient(cfg, mig_cfg)`: native driver `Client` on port 9000; optional TLS via `secure`,
  `verify`, `ca_certs`; creates database on first init; `list_migres_tables()` for reset.
- `execute(sql, params)`, `insert_rows(table, columns, rows)`, `close()`.

## state_json.py
- `StateJson(path)`: JSON state file with atomic writes (tempfile + `os.replace`), a
  threading.Lock, and read-modify-write on every access (no in-memory cache).
- Shape: `{"binlog": {"file", "pos"} | null, "tables": {name: {status, last_pk, rows_processed}}}`.

## notifications/
- `NotificationDispatcher`: shared per-type rate limit, then fan-out to Teams / Slack / Telegram.
- One failed provider does not block the others; `notify_*` is true if any channel sent.
- Module-level singleton: `initialize_notifications(config, environment)` +
  `notify_cdc_*` convenience functions (no-ops when handler is None/disabled).
- Flat `webhook_url` is a Teams alias for existing configs.

## logger.py
- `setup_logging()`: idempotent root-logger setup, single stdout StreamHandler, INFO level.

## Other files
- `Dockerfile`: python:3.11-slim + tini; `docker-compose.yml`: mounts repo into /app.
- `docker-compose.test.yml`: MySQL + ClickHouse for e2e tests.
- `config.yml` (gitignored), `config.yml.example` (template).
- `pytest.ini`, `test/unit/` (unit tests), `test/e2e/` (e2e), other integration tests under `test/`.
- `docs/ENVIRONMENT_VARIABLES.md`: env var reference.
