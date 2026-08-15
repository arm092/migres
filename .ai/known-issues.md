# Known Issues and Pitfalls

Severity: 🔴 critical · 🟠 major · 🟡 minor. Line numbers in historical entries referred to the state at review time (2026-07).

## Open issues

### 🔴 SEC-1: Real credentials in the working tree
`config.yml` contains live MySQL/ClickHouse passwords and may contain real Teams/Slack webhooks or a Telegram bot token.
The file is gitignored, but secrets exist on disk and in any backups/shares. **Fix:** move secrets
to env vars (already supported), rotate any exposed passwords and webhooks. Do not copy production
`config.yml` into docs or examples.

---

## Fixed (code review 2026-07)

### 🔴 SEC-2: SQL injection through MySQL `COLUMN_DEFAULT` — **FIXED**
`_escape_sql_string` in `schema_and_ddl.py` now escapes backslashes before single quotes.

### 🟠 SEC-3: Unescaped identifiers everywhere — **FIXED**
Central `quote_ident()` in `schema_and_ddl.py`; used across pipeline, clients, and DDL builders.

### 🟠 SEC-4: Row data and SQL leak to MS Teams — **FIXED**
Consumer notifications send table/schema/query IDs and error class only; SQL and row params stay in logs.

### 🟡 SEC-5: Silent fallback of buffer DB to `/tmp/buffer.db` — **FIXED**
Buffer path is configurable via `buffer_file` / `BUFFER_FILE`; startup fails fast if the directory is not writable.

### 🟡 SEC-6: No TLS anywhere — **FIXED**
Optional MySQL TLS (`ssl_ca`, `ssl_disabled`) and ClickHouse TLS (`secure`, `verify`, `ca_certs`).

### 🟡 SEC-7: Outdated dependency — **FIXED**
`mysql-connector-python` upgraded to 9.3.0 in `requirements.txt`.

### 🔴 BUG-1: `failed_queries` never populated — **FIXED**
Consumer calls `BufferDB.move_to_failed()` for non-transient errors and continues; transient errors still re-raise to crash for restart retry.

### 🔴 BUG-2: Snapshot offset pagination has no ORDER BY — **FIXED**
`fetch_stream_with_offset` requires deterministic `ORDER BY`; snapshot passes PK or all columns.

### 🔴 BUG-3: UPDATE with `binlog_row_image=MINIMAL` corrupts rows — **FIXED**
Producer calls `assert_cdc_binlog_settings()` at startup; refuses CDC unless `binlog_row_image=FULL`.

### 🟠 BUG-4: DDL for non-included tables creates tables in ClickHouse — **FIXED**
`_table_allowed()` / `include_tables` checks in transformer DDL and producer filtering.

### 🟠 BUG-5: DDL vs data reordering within a batch — **FIXED**
Transformer flushes pending data events before inline CREATE/ALTER DDL in binlog order.

### 🟠 BUG-6: Duplicate events on producer restart — **FIXED (upgrade)**
Producer writes a `checkpoint` row in the same SQLite transaction as each `raw_events` flush.
Resume order: checkpoint → last raw_events row → `state.json` (manual override). At-least-once
still possible across crash windows; ReplacingMergeTree deduplicates.

### 🟠 BUG-7: Binlog file names compared lexicographically — **FIXED**
`binlog_position_key()` in `schema_and_ddl.py` compares `(basename, numeric_suffix, pos)` tuples.

### 🟠 BUG-8: Snapshot not transactionally consistent / errors don't fail the run — **FIXED**
Workers call `start_repeatable_snapshot()`; `run_snapshot` raises `SnapshotError` on table failures.

### 🟠 BUG-9: DDL regex false positives / gaps — **FIXED**
Anchored DDL parsers in `schema_and_ddl.py`; multi-table DROP and RENAME TABLE supported.

### 🟡 BUG-10: Non-JSON-native MySQL types degrade to `str()` — **FIXED**
Producer serializes `Decimal` as string; consumer converts back using column type from `DESCRIBE`.

### 🟡 BUG-11: Signal handlers re-registered and race-prone — **FIXED**
Handlers registered once; set `_signal_flags` events processed by the main watchdog loop.

### 🟡 BUG-12: `_reset_handler` drops all tables in the CH database — **FIXED**
Reset uses `CHClient.list_migres_tables()` (tables with `__data_transfer_commit_time` only).

### 🟡 BUG-13: Consumer sleeps after every batch — **FIXED**
Sleeps `consumer_poll_interval` only when the fetch returned fewer rows than the batch limit.

### 🟡 BUG-14: Notification timestamps mislabeled as UTC — **FIXED**
Uses `datetime.now(timezone.utc)`.

### 🟡 BUG-15: Overly-specific exception filters — **FIXED**
ClickHouse paths catch `clickhouse_driver.errors.Error` where appropriate.

### 🟡 BUG-16: `_get_column_types` defaults missing columns to `'String'` — **FIXED**
Logs warning and invalidates cache when DESCRIBE is missing expected columns.

### 🟡 BUG-17: `prepared_queries_merge_rows_limit` is dead config — **FIXED**
Removed from config defaults and documentation.

---

## Remaining operational notes

- **At-least-once delivery** end-to-end; use `SELECT … FINAL` or periodic `OPTIMIZE … FINAL` for deduplicated reads.
- **Shared ClickHouse database**: reset/reposition only touch migres-managed tables; other tables in the same DB are preserved.
- **Windows**: SIGUSR1/SIGUSR2 handlers are not available; reset/reposition require Linux/Kubernetes.
- **Tests**: unit tests in `test/unit/`; e2e via `docker-compose.test.yml` and `pytest -m e2e`.
