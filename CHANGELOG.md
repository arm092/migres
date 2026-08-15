# Changelog

## 3.1.0

### Added

- Multi-channel notifications: MS Teams, Slack, and Telegram can run in parallel.
  Nested config (`notifications.teams` / `.slack` / `.telegram`). Flat
  `notifications.webhook_url` is still accepted as a Teams alias.
- Env: `NOTIFICATIONS_TEAMS_WEBHOOK_URL`, `NOTIFICATIONS_SLACK_WEBHOOK_URL`,
  `NOTIFICATIONS_TELEGRAM_BOT_TOKEN`, `NOTIFICATIONS_TELEGRAM_CHAT_ID`.
  `NOTIFICATIONS_WEBHOOK_URL` remains the Teams alias.

## 3.0.0

Breaking config change plus the 3-stage package rewrite.

### Breaking

- Removed `migration.cdc.batch_delay_seconds` and `CDC_BATCH_DELAY_SECONDS`.
  They are no longer honored (a warning is logged if still present).
  Use the per-stage knobs instead:

  | Key | Env | Default |
  |-----|-----|---------|
  | `producer_flush_interval` | `CDC_PRODUCER_FLUSH_INTERVAL` | `5` |
  | `transformer_poll_interval` | `CDC_TRANSFORMER_POLL_INTERVAL` | `0.5` |
  | `consumer_poll_interval` | `CDC_CONSUMER_POLL_INTERVAL` | `0.5` |

### Added

- Installable `migres` package (`python -m migres`, `migres` CLI)
- Producer `checkpoint` table in `buffer.db` (resume position; `state.json` is snapshot baseline / manual override)
- Optional GTID (`use_gtid` / `CDC_USE_GTID`)
- Producer backpressure (`raw_events_max`, `raw_events_resume_ratio`)
- Per-table consumer workers, sqlglot DDL parsing, SIGTERM graceful shutdown
- MIT license

### Fixed

- Python 3.12 CI: `mysql-replication==0.46` still imports `distutils`; `setuptools` is now a runtime dependency so `LooseVersion` resolves.

## 2.1.0

CDC reliability and ops (from `master` before the 3.x rewrite).

## 2.0.0

3-stage pipeline (Producer → Transformer → Consumer) with SQLite buffer.

## 1.x

Snapshot + CDC as a single-process tool (`1.0.0`–`1.1.8`).
