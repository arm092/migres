# Configuration

Config file: YAML, passed via `--config` (Docker default `/app/config.yml`).
`config.yml` is gitignored; `config.yml.example` is the template.
Environment variables override file values (`config._apply_env_overrides`), then defaults are
applied (`config.load_config`).

## mysql

| Key | Env override | Notes |
|-----|--------------|-------|
| host / port / user / password / database | MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DATABASE | port coerced to int |
| include_tables | — | list; empty = all tables. Filters snapshot tables, binlog row events (`only_tables`), transformer data/DDL, and producer CREATE TABLE DDL |
| exclude_tables | — | list; applied in snapshot and as binlog `ignored_tables` |
| ssl_ca | MYSQL_SSL_CA | path to CA PEM for TLS |
| ssl_disabled | MYSQL_SSL_DISABLED | bool; force plaintext |

MySQL prerequisites for CDC: `binlog_format=ROW`, `binlog_row_image=FULL`,
`binlog_row_metadata=FULL`, user with `REPLICATION SLAVE` privilege. Producer validates
ROW/FULL at startup via `assert_cdc_binlog_settings()`.

## clickhouse

| Key | Env override | Notes |
|-----|--------------|-------|
| host / port / user / password / database | CLICKHOUSE_HOST / CLICKHOUSE_PORT / CLICKHOUSE_USER / CLICKHOUSE_PASSWORD / CLICKHOUSE_DATABASE | |
| secure | CLICKHOUSE_SECURE | bool; enable TLS on native protocol |
| verify | CLICKHOUSE_VERIFY | bool; verify server certificate (when secure) |
| ca_certs | CLICKHOUSE_CA_CERTS | path to CA bundle |

Port is the **native protocol** port (9000), not HTTP (8123). Database is auto-created.

## migration

| Key | Default | Env | Meaning |
|-----|---------|-----|---------|
| mode | "snapshot" | MIGRATION_MODE | "snapshot" or "cdc" |
| debug | false | MIGRATION_DEBUG | verbose logging, 10s stats interval |
| batch_rows | 5000 | MIGRATION_BATCH_ROWS | snapshot SELECT batch size |
| workers | 4 | MIGRATION_WORKERS | snapshot parallel table workers |
| low_cardinality_strings | true (implicit) | — | wrap String in LowCardinality (CDC schema sync) |
| ddl_engine | "ReplacingMergeTree" | — | anything else → plain MergeTree |
| clickhouse_timezone | unset | MIGRATION_CLICKHOUSE_TIMEZONE | timezone for DateTime64 columns and CH session |

## migration.cdc

| Key | Default | Env | Meaning |
|-----|---------|-----|---------|
| snapshot_before | true | — | run snapshot before starting the pipeline |
| server_id | 4379 | — | binlog replication server_id base; actual = base + pid % 1000 |
| heartbeat_seconds | 5 | CDC_HEARTBEAT_SECONDS | binlog slave heartbeat |
| producer_flush_interval | 5.0 | CDC_PRODUCER_FLUSH_INTERVAL | producer flush interval (seconds) |
| transformer_poll_interval | 0.5 | CDC_TRANSFORMER_POLL_INTERVAL | transformer poll wait when below checkpoint_interval_rows |
| consumer_poll_interval | 0.5 | CDC_CONSUMER_POLL_INTERVAL | consumer sleep when queue empty or batch not full |
| producer_batch_size | 100 | CDC_PRODUCER_BATCH_SIZE | events per producer flush |
| checkpoint_interval_rows | 5000 | CDC_CHECKPOINT_INTERVAL_ROWS | transformer waits for this many raw events (0 = process immediately); also the fetch limit |
| batch_max_wait_seconds | 60 | CDC_BATCH_MAX_WAIT_SECONDS | transformer processes a partial batch after this time |
| prepared_queries_batch_limit | 100 | CDC_PREPARED_QUERIES_BATCH_LIMIT | consumer fetch limit |
| force_binlog_position | null | CDC_FORCE_BINLOG_POSITION | "file:pos"; loaded at process start; used only by SIGUSR2 (not on normal producer start) |
| db_debug | false | CDC_DB_DEBUG | archive consumed buffer rows to ClickHouse debug tables |

## notifications

| Key | Default | Env |
|-----|---------|-----|
| enabled | false | NOTIFICATIONS_ENABLED |
| rate_limit_seconds | 60 | NOTIFICATIONS_RATE_LIMIT_SECONDS |
| webhook_url | — | NOTIFICATIONS_WEBHOOK_URL (Teams alias) |
| teams.enabled | true | — |
| teams.webhook_url | — | NOTIFICATIONS_TEAMS_WEBHOOK_URL |
| slack.enabled | true | — |
| slack.webhook_url | — | NOTIFICATIONS_SLACK_WEBHOOK_URL |
| telegram.enabled | true | — |
| telegram.bot_token | — | NOTIFICATIONS_TELEGRAM_BOT_TOKEN |
| telegram.chat_id | — | NOTIFICATIONS_TELEGRAM_CHAT_ID |

## top-level

| Key | Default | Env |
|-----|---------|-----|
| state_file | "/app/state.json" | STATE_FILE |
| buffer_file | "data/buffer.db" | BUFFER_FILE |
| environment | "prod" | ENVIRONMENT (drives notification card color and title tag) |

## Gotchas

- `buffer_file` / `BUFFER_FILE` must point to a writable path; there is **no** silent fallback
  to `/tmp/buffer.db` — startup fails if the directory cannot be created/written.
- `batch_delay_seconds` was removed in 3.0.0; each stage now has its own interval knob
  (`producer_flush_interval`, `transformer_poll_interval`, `consumer_poll_interval`).
- Do not copy production credentials from `config.yml` into examples, docs, or logs.
- `heartbeat_seconds` and `server_id` have no defaults in `load_config`; they default inside
  `PipelineProducer` (5 / 4379).
