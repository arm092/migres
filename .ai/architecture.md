# Architecture

## High-level overview

```
                     snapshot mode
MySQL  ─────────────────────────────────────────►  ClickHouse
  │        (parallel table workers, batched SELECT → INSERT)
  │
  │ binlog (ROW format)                cdc mode
  ▼
PipelineProducer ──► SQLite buffer.db ──► PipelineTransformer ──► SQLite buffer.db ──► PipelineConsumer ──► ClickHouse
 (binlog reader)      raw_events +         (SQL generation,        prepared_queries      (per-table workers)
                      checkpoint            DDL via sqlglot/regex)
```

Package layout: `migres/` (`producer`, `transformer`, `consumer`, `clients/`, `schema/`, `cli.py`).
Entry: `python -m migres` or root `migres.py`. Typed config: `migres.config.MigresConfig`.

Both modes may run in one process: with `migration.cdc.snapshot_before: true` the CDC mode first
runs a full snapshot, then starts the pipeline. The binlog position is recorded in `state.json`
**before** the snapshot starts, so changes made during the snapshot are replayed by CDC and
deduplicated by `ReplacingMergeTree`.

## Process / thread model (CDC mode)

`migres.cli:run_cdc_pipeline()` starts three non-daemon threads and then loops forever in the main
thread as a watchdog (SIGTERM triggers graceful flush + join):

| Thread | Class | Role |
|--------|-------|------|
| Producer | `migres.producer.PipelineProducer` | Reads binlog (`file:pos` or optional GTID), batches with `producer_flush_interval`, writes `raw_events` + `checkpoint` atomically; pauses on `raw_events_max` backpressure |
| Transformer | `migres.transformer.PipelineTransformer` | Fetches `raw_events`, handles DDL (sqlglot with regex fallback), groups DML, writes `prepared_queries` |
| Consumer | `migres.consumer.PipelineConsumer` | Per-table worker threads (order preserved within a table); DDL drains workers then runs serially; permanent failures → `failed_queries` |

Watchdog behavior in the main loop:
- Processes `_signal_flags` (SIGUSR1 reset / SIGUSR2 reposition) before thread checks — handlers only set events; heavy work runs in the main thread.
- If Producer or Transformer thread dies → log critical, notify Teams, `sys.exit(1)`.
- If Consumer thread dies → signal shutdown to the other threads, join with timeout,
  notify, `sys.exit(1)`. Unprocessed `prepared_queries` remain in buffer.db for retry
  after restart (typical cause: transient network error).
- Every 10s (debug) / 60s logs queue stats (`raw_events`, `prepared_queries`, `failed_queries`).

Each stage owns its **own** `BufferDB` instance; SQLite concurrency is handled with WAL mode,
`busy_timeout=30000`, and thread-local connections.

## Delivery semantics

- Producer → buffer: at-least-once. On reconnect it resumes from the `checkpoint` table
  (updated in the same transaction as each flush), then last `raw_events` row, then
  `state.json` (manual override). Optional GTID via `migration.cdc.use_gtid`.
- Transformer commit is atomic in SQLite: inserting `prepared_queries` + deleting consumed
  `raw_events` happens in one transaction.
- Consumer deletes a query only after successful execution → a crash between execute and delete
  causes re-execution after restart (duplicate insert with identical
  `__data_transfer_commit_time`, collapsed by ReplacingMergeTree on merge).
- Permanent consumer errors (bad data/types) are moved to `failed_queries` and skipped — they do
  not block subsequent queries or crash the process.
- Transient consumer errors (network/timeouts) re-raise → consumer thread dies → process exits
  for orchestrator restart; `prepared_queries` are retried.
- Net result: **at-least-once end-to-end**, with dedup via ReplacingMergeTree version column;
  poison queries quarantined in `failed_queries`.

## Binlog position management

Priority when the Producer starts (`PipelineProducer.__init__`):
1. `checkpoint` row in `buffer.db` (updated atomically with each producer flush)
2. Last row in `raw_events` (`buffer.get_last_committed_pos()`)
3. `state.json` `binlog` entry (manual override / snapshot baseline)
4. Current master position (stream starts from "now")

`state.json` binlog is written by **snapshot** (start position before copy) and **manual ops**
(SIGUSR2 / `force_binlog_position`). The Transformer does not update it.

## DDL handling (Transformer)

DDL arrives as `QueryEvent`. Regex-based detection for CREATE TABLE / DROP TABLE / ALTER TABLE /
TRUNCATE. Handling:
- **CREATE/ALTER**: re-reads the MySQL schema (`INFORMATION_SCHEMA`) and re-issues
  `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ADD COLUMN IF NOT EXISTS` for missing columns
  (`_ensure_table_schema`), with retries and MySQL reconnects because INFORMATION_SCHEMA may
  lag right after DDL. Max 10 retries for CREATE, 5 for ALTER, exponential backoff.
- **DROP TABLE**: executed immediately, unless the same batch contains data events for that
  table — then the DROP is deferred and queued as a prepared query so it executes *after*
  the data inserts (and skipped entirely if the same batch re-creates the table).
- **TRUNCATE**: forwarded to ClickHouse.
- RENAME TABLE and other DDL: logged as "Unhandled DDL" and skipped.
- Transaction control statements (BEGIN/COMMIT/SAVEPOINT/XA…) are filtered out both in the
  Producer and Transformer.

## Event → row transformation

For every row event the Transformer appends metadata values:
- `WriteRowsEvent` → `[...values, commit_ns, 0]`
- `UpdateRowsEvent` → merge `before_values` + `after_values`, `[...merged, commit_ns, 0]`
- `DeleteRowsEvent` → `[...values, commit_ns, commit_ns]` (delete_time != 0 marks soft delete)

`commit_ns` comes from `_next_commit_ts()` — strictly monotonically increasing nanoseconds within
the Transformer, used as the ReplacingMergeTree version.

## Snapshot mode

`snapshot.run_snapshot()`:
1. Record binlog position in `state.json` (only if not already recorded — resume keeps the old one).
2. List tables honoring `include_tables` / `exclude_tables`.
3. `ThreadPoolExecutor(max_workers=migration.workers)`, one worker per table:
   - skip if state says `done`; resume if `in_progress` (keeps existing CH table),
     otherwise DROP + CREATE the ClickHouse table;
   - copy rows in batches: keyset pagination by single integer PK when available,
     otherwise `LIMIT/OFFSET` pagination;
   - progress persisted per batch to `state.json` (`last_pk` / `rows_processed`).

## Signals (Unix only; no-ops on Windows)

Handlers are registered once in `main()`. They set `_signal_flags` threading events; the main
watchdog loop in `run_cdc_pipeline()` performs the actual work (avoids re-entrancy in signal context).

- **SIGUSR1 — full reset**: stop all pipeline threads (join timeout 30s), DROP **migres-managed**
  ClickHouse tables only (those with `__data_transfer_commit_time` via `list_migres_tables()`),
  delete the buffer file and `state.json`, `os._exit(0)`.
  Registered early in `main()` so it also works during the snapshot phase.
- **SIGUSR2 — reposition**: requires `migration.cdc.force_binlog_position` ("file:pos").
  Stops threads, deletes the buffer file, writes the forced position into `state.json`,
  creates fresh Producer/Transformer/Consumer objects and restarts the threads.
  Process keeps running. Thread references live in the module-global `_reset_context` dict;
  the watchdog loop re-reads them each iteration to survive reposition.

## Notifications

`notifications.py` sends MS Teams adaptive cards (startup, shutdown, errors, warnings, info)
through a global singleton handler initialized in `main()`. Rate limiting is per notification
type (`rate_limit_seconds`). Card color encodes the environment (dev=green, stage=yellow,
prod=red). Title suffix `[ENVIRONMENT]`.
