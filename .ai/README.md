# Migres — AI Documentation Index

Migres is a MySQL → ClickHouse migration and replication tool written in Python.
It has two modes:

- **snapshot** — one-shot bulk copy of tables (parallel workers, resumable via `state.json`)
- **cdc** — continuous replication from the MySQL binlog via a 3-stage pipeline
  (Producer → Transformer → Consumer) buffered through a local SQLite database (`data/buffer.db`)

## Documentation map

| File | Contents |
|------|----------|
| [architecture.md](architecture.md) | Runtime architecture, threads, data flow, signal handling |
| [modules.md](modules.md) | File-by-file reference of every source module |
| [data-and-schema.md](data-and-schema.md) | Type mapping, metadata columns, buffer DB schema, state.json format |
| [configuration.md](configuration.md) | config.yml keys, defaults, environment variable overrides |
| [known-issues.md](known-issues.md) | Known bugs, pitfalls, and discrepancies between docs and code (from code review) |

## Quick facts

- Entry point: `migres.py --config config.yml` (Docker: `python migres.py --config /app/config.yml`)
- Python 3.11, no package structure — all modules are flat files in the repo root
- Key dependencies: `mysql-replication` (binlog), `clickhouse-driver` (native TCP, port 9000),
  `mysql-connector-python`, `PyYAML`, `requests` (MS Teams notifications)
- Target tables use `ReplacingMergeTree(__data_transfer_commit_time)`; UPDATE/DELETE are modeled
  as inserts of new row versions (upsert / soft-delete semantics), deduplicated on merge
- Persistent local state: `data/state.json` (binlog position + per-table snapshot progress)
  and `data/buffer.db` (SQLite queue between pipeline stages)
- Notifications: MS Teams webhook (adaptive cards), configured under `notifications:` in config
- Tests: `test/` folder (pytest); `test_cdc.py` and `test_sales_savepoint.py` also exist at repo root
- Legacy: `cdc.py` used to contain the single-threaded CDC loop; now it only holds
  `CriticalCDCError`, query-tracking pools, and `_map_with_low_cardinality` used by other modules

## Conventions

- Logging via stdlib `logging`, configured once in `logger.py` (`setup_logging()`), format
  `[%(asctime)s] [%(levelname)s] %(message)s`, INFO level to stdout
- Most verbose logs are gated behind `migration.debug: true`
- Identifiers in SQL are interpolated with f-strings wrapped in backticks (no identifier escaping)
- Errors in the Consumer are fatal by design: the thread crashes, the main loop detects the dead
  thread and exits so the orchestrator (Kubernetes) restarts the pod; unprocessed queries stay
  in `prepared_queries` and are retried after restart
