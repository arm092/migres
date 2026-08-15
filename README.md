# Migres - MySQL to ClickHouse Migration Tool

This project is a **complete migration tool** that transfers tables from MySQL into ClickHouse with type mapping, logging, and resumable state.  
It supports both **snapshot mode** (initial data migration) and **CDC mode** (real-time change data capture), with automatic schema synchronization.

**Package:** installable as `migres` (`pyproject.toml`). Run with `python -m migres --config config.yml` or `python migres.py --config config.yml`.

---

## Features

### Core Migration
- 🚀 **MySQL → ClickHouse migration** (snapshot + CDC modes)
- 🗂 **Intelligent type mapping** (INT, DECIMAL, DATE, DATETIME, VARCHAR, etc.)
- 📝 **Transferia metadata columns** added automatically:
  - `__data_transfer_commit_time UInt64` → nanosecond commit timestamp
  - `__data_transfer_delete_time UInt64 DEFAULT 0`
  - `__data_transfer_is_deleted UInt8 MATERIALIZED if(__data_transfer_delete_time != 0, 1, 0)`

### Snapshot Mode
- 🔁 **Resumable migration** (state stored in `state.json`)
- ⚡ **Parallel table processing** for large datasets
- 🎯 **Included/excluded tables filtering**

### CDC Mode (Change Data Capture)
- 🔄 **Real-time replication** from MySQL binlog via 3-stage pipeline architecture
- ⚡ **Queue-based event batching** with configurable delay (SQLite buffer database)
- 🎯 **Smart event grouping** (combines multiple events into single operations)
- 🏗️ **Automatic schema synchronization** with retry logic:
  - ✅ CREATE TABLE (new table creation with up to 10 retry attempts)
  - ✅ DROP TABLE (table deletion)
  - ✅ ADD COLUMN (with defaults, retries if metadata not available)
  - ✅ ALTER TABLE (enhanced retry logic with exponential backoff)
  - ✅ DROP COLUMN
  - ✅ RENAME COLUMN (CHANGE COLUMN)
  - ✅ MODIFY COLUMN (type changes, defaults)
- 📊 **ReplacingMergeTree** for upsert semantics
- 🎯 **Table filtering** (include/exclude) - events for excluded tables are automatically skipped
- 💾 **Checkpoint persistence** (resume from last committed position)
- 🔒 **Unique server_id per process** (PID-based) prevents MySQL replication conflicts
- 🌍 **Timezone-aware datetime handling** (DateTime64 with timezone)
- 🛡️ **Robust error handling** — permanent data/type errors move queries to `failed_queries`; transient network errors crash the consumer for orchestrator restart/retry
- 📱 **MS Teams notifications** for errors, warnings, and important events

### Operations
- 📑 **Detailed logging** (visible via `docker compose logs -f`)
- 🐳 **Docker support** with hot-reload for development
- 📢 **Real-time notifications** to MS Teams channels

---

## How It Works

### Snapshot Mode
1. **Initial setup**
   - Connects to MySQL & ClickHouse
   - Records binlog position for CDC start point
   - Loads migration state from `state.json`

2. **Table filtering & processing**
   - Filters tables by `include_tables`/`exclude_tables`
   - Processes tables in parallel workers
   - Each worker:
     - Inspects MySQL schema
     - Creates ClickHouse table with mapped types
     - Migrates data in batches
     - Marks table as complete

3. **Resumable migration**
   - If interrupted, resumes from last completed table
   - State persisted in `state.json`

### CDC Mode
1. **Initial snapshot** (optional)
   - Runs snapshot mode first if `snapshot_before: true`
   - Ensures complete baseline before streaming

2. **3-Stage Pipeline Architecture**
   - **Producer**: Reads events from MySQL binlog stream and stores them in buffer
   - **Transformer**: Processes raw events, generates SQL queries, handles DDL operations
   - **Consumer**: Executes queries against ClickHouse, handles failures gracefully
   - All stages run in parallel threads for optimal performance

3. **Queue-based event processing**
   - Events are accumulated in a SQLite buffer database as they arrive from binlog
   - Separate poll/flush intervals per stage: `producer_flush_interval`, `transformer_poll_interval`, `consumer_poll_interval`
   - Continuous operation: keeps receiving events while processing queue
   - Events for tables not in `include_tables` are automatically filtered to prevent buffer accumulation

4. **Event batching and grouping**
   - **INSERT events**: Multiple INSERTs for same table → Single INSERT with multiple rows
   - **UPDATE events**: Multiple UPDATEs for same table → Single INSERT with multiple rows
   - **DELETE events**: Multiple DELETEs for same table → Single INSERT with multiple rows
   - **DDL events**: Processed immediately with retry logic for reliability

5. **Real-time streaming**
   - Connects to MySQL binlog stream (non-blocking)
   - Unique `server_id` per process (based on PID) prevents replication conflicts
   - Processes INSERT/UPDATE/DELETE events
   - Auto-detects schema changes (ADD/DROP/RENAME/MODIFY)
   - Applies changes to ClickHouse in batches

6. **Schema synchronization with retry logic**
   - **CREATE TABLE**: Creates new table in ClickHouse with retry logic (up to 10 attempts)
   - **DROP TABLE**: Removes table from ClickHouse
   - **ADD COLUMN**: Creates new column with defaults, retries if MySQL metadata not yet available
   - **ALTER TABLE**: Enhanced retry logic (up to 5 attempts) with exponential backoff
   - **DROP COLUMN**: Removes column from ClickHouse
   - **RENAME COLUMN**: Renames column in ClickHouse
   - **MODIFY COLUMN**: Changes type and defaults

7. **Error handling and reliability**
   - **Permanent errors** (type conversion, bad data, schema mismatch): query is moved to the `failed_queries` table via `BufferDB.move_to_failed`; CDC continues with remaining queries
   - **Transient errors** (timeouts, connection loss, network): consumer re-raises and the process exits so Kubernetes/Docker can restart and retry `prepared_queries`
   - Failed operations don't prevent other queries in the same batch from processing
   - `failed_queries` stores timestamp, error reason, SQL, and params for manual review/recovery

8. **Checkpoint persistence**
   - Saves binlog position periodically to buffer database
   - Resumes from last committed position on restart
   - State persisted in both buffer database and `state.json` file
   - By default, producer prioritizes buffer DB position over state.json
   - Set `db_debug: true` to archive processed events/queries in `raw_events_processed` and `prepared_queries_processed` tables for debugging

---

## Requirements

- **MySQL** server (with data to migrate)
- **ClickHouse** server (can be remote)
- **Docker + Docker Compose**

---

## Setup

### 1. MySQL Configuration (Required for CDC)

For CDC mode to work properly, configure MySQL with:

```sql
-- Set binlog format to ROW (required for CDC)
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';
SET GLOBAL binlog_row_metadata = 'FULL';

-- Make changes persistent (MySQL 8.0+)
SET PERSIST binlog_format = 'ROW';
SET PERSIST binlog_row_image = 'FULL';
SET PERSIST binlog_row_metadata = 'FULL';
```

Or add to `my.cnf`:
```ini
[mysqld]
binlog_format=ROW
binlog_row_image=FULL
binlog_row_metadata=FULL
```

### 2. Configure `config.yml`

```yaml
mysql:
  host: "localhost"
  port: 3306
  user: "your_user"
  password: "your_password"
  database: "your_database"
  include_tables: []  # Leave empty for all tables
  exclude_tables: []  # Tables to skip
  # Optional TLS:
  # ssl_ca: /path/to/ca.pem
  # ssl_disabled: false

clickhouse:
  host: "localhost"
  port: 9000
  user: "default"
  password: ""
  database: "your_ch_database"
  # Optional TLS:
  # secure: true
  # verify: true
  # ca_certs: /path/to/ca.pem

migration:
  mode: "snapshot"  # or "cdc"
  debug: false # enables verbose logging for CDC events
  batch_rows: 5000
  workers: 4
  low_cardinality_strings: true
  ddl_engine: "ReplacingMergeTree"
  
  # Timezone configuration for datetime/timestamp columns
  clickhouse_timezone: "Asia/Yerevan" # Set to desired ClickHouse timezone
  
  # CDC-specific settings
  cdc:
    snapshot_before: true  # Run snapshot before CDC
    heartbeat_seconds: 5
    checkpoint_interval_rows: 1000  # Transformer waits until this many raw events (0 = disable waiting)
    prepared_queries_batch_limit: 100 # Consumer batch size for execution
    producer_flush_interval: 5      # Producer: flush binlog batch to buffer (seconds)
    transformer_poll_interval: 0.5  # Transformer: poll wait when below checkpoint_interval_rows
    consumer_poll_interval: 0.5     # Consumer: sleep when queue is empty / batch not full
    raw_events_max: 50000
    use_gtid: false
    batch_max_wait_seconds: 60 # Max wait time for batch processing even if checkpoint_interval_rows is not reached
    producer_batch_size: 100  # Number of events producer accumulates before flushing to buffer
    force_binlog_position: null  # Optional: "mysql-bin.000123:6855245" format to force specific binlog position (used by SIGUSR2 handler)
    db_debug: false  # If true, move processed events/queries to processed tables instead of deleting them
    server_id: 4379  # Unique ID for binlog replication

# MS Teams Notifications
notifications:
  enabled: true
  webhook_url: "https://your-org.webhook.office.com/webhookb2/your-webhook-url"
  rate_limit_seconds: 60  # Minimum seconds between notifications (0 = no limit)

# Local persistence paths (must be writable)
state_file: /app/data/state.json
buffer_file: /app/data/buffer.db  # SQLite CDC buffer; override with BUFFER_FILE env var
```

---

## Running

### Snapshot Mode (Initial Migration)
```bash
# Edit config.yml: mode: "snapshot"
docker compose up
```

### CDC Mode (Real-time Replication)
```bash
# Edit config.yml: mode: "cdc"
docker compose up
```

### Development Mode (Hot Reload)
```bash
# Code changes are automatically reflected
docker compose up
```

### View Logs
```bash
docker compose logs -f
```

### Reset Feature

The application supports a **reset mechanism** that allows you to perform a complete reset: gracefully shutdown all pipeline threads, drop all ClickHouse tables, delete local data files (`buffer.db` and `state.json`), and exit cleanly. This is useful for starting from scratch in Kubernetes deployments.

**How it works:**
- Sends SIGUSR1 signal to trigger reset
- All pipeline threads (Producer, Transformer, Consumer) shutdown gracefully
- ClickHouse tables managed by migres are dropped (only tables that have the `__data_transfer_commit_time` metadata column)
- Local data files (`buffer.db` and `state.json`) are deleted
- Application exits with code 0, allowing Kubernetes to restart the pod

**Usage:**

**From Kubernetes:**
```bash
# Get pod name
kubectl get pods | grep migres

# Send reset signal (PID 1 is the main process in containers)
kubectl exec <pod-name> -- kill -USR1 1

# Monitor reset progress
kubectl logs -f <pod-name>
```

**From command line:**
```bash
# Find process ID
ps aux | grep migres
# or check application logs for: "Starting CDC Pipeline... [PID: 12345]"

# Send reset signal
kill -USR1 <process_id>
```

**Reset Process:**
1. Signal received → Logs: "Received reset signal (SIGUSR1). Initiating reset..."
2. Threads shutdown → All pipeline threads are signaled and wait for completion (30s timeout)
3. Tables dropped → Migres-managed ClickHouse tables are dropped (identified by `__data_transfer_commit_time`)
4. Files deleted → `buffer.db` and `state.json` are removed
5. Exit → Application exits with code 0

**Notes:**
- SIGUSR1 is the standard Unix user-defined signal (signal number 10)
- Reset is **destructive** — migres-managed tables and local state are deleted; unrelated ClickHouse tables in the same database are kept
- The application will exit cleanly, allowing Kubernetes to restart it
- On Windows, SIGUSR1 is not available (handler registration will log a warning)
- The reset handler logs each step for monitoring progress

### Reposition Feature (SIGUSR2)

The application supports a **reposition mechanism** that allows you to change the binlog position without a full reset: gracefully shutdown all pipeline threads, delete buffer.db, update state.json with a new binlog position from config, and restart all threads. This is useful for repositioning the CDC stream to a specific binlog position.

**How it works:**
- Requires `force_binlog_position` config to be set (format: "file:position", e.g., "mysql-bin.000123:6855245")
- Sends SIGUSR2 signal to trigger reposition
- All pipeline threads (Producer, Transformer, Consumer) shutdown gracefully
- Buffer database (`buffer.db`) is deleted
- State file (`state.json`) is updated with new binlog position from config
- All pipeline threads are restarted (producer will start from new position)
- Application continues running (does not exit)

**Usage:**

**From Kubernetes:**
```bash
# Get pod name
kubectl get pods | grep migres

# Send reposition signal (PID 1 is the main process in containers)
kubectl exec <pod-name> -- kill -USR2 1

# Monitor reposition progress
kubectl logs -f <pod-name>
```

**From command line:**
```bash
# Find process ID
ps aux | grep migres
# or check application logs for: "Starting CDC Pipeline... [PID: 12345]"

# Send reposition signal
kill -USR2 <process_id>
```

**Configuration:**

Set `force_binlog_position` in your `config.yml`:
```yaml
migration:
  cdc:
    force_binlog_position: "mysql-bin.000123:6855245"  # Format: "file:position"
```

Or use environment variable:
```bash
export CDC_FORCE_BINLOG_POSITION="mysql-bin.000123:6855245"
```

**Reposition Process:**
1. Signal received → Logs: "Received reposition signal (SIGUSR2). Initiating reposition..."
2. Config check → If `force_binlog_position` not set, logs warning and skips
3. Threads shutdown → All pipeline threads are signaled and wait for completion (30s timeout)
4. Buffer deleted → `buffer.db` is removed (safe after threads stopped)
5. State updated → `state.json` is updated with new binlog position from config
6. Threads restarted → All pipeline threads are restarted (producer picks up new position)
7. Continue → Application continues running normally

**Notes:**
- SIGUSR2 is the standard Unix user-defined signal (signal number 12)
- Requires `force_binlog_position` config to be set, otherwise signal is ignored
- Does NOT kill the application - only restarts threads
- On Windows, SIGUSR2 is not available (handler registration will log a warning)
- The reposition handler logs each step for monitoring progress
- All threads are restarted to ensure clean state

### Environment Variables Support

All configuration options can be overridden using environment variables. This is useful for containerized deployments:

```bash
# MySQL configuration
export MYSQL_HOST=mysql-server.example.com
export MYSQL_PASSWORD=your-password

# ClickHouse configuration  
export CLICKHOUSE_HOST=clickhouse-server.example.com
export CLICKHOUSE_PASSWORD=your-password

# Migration configuration
export MIGRATION_DEBUG=true

# Local paths
export STATE_FILE=/app/data/state.json
export BUFFER_FILE=/app/data/buffer.db

# Optional TLS
export MYSQL_SSL_CA=/path/to/ca.pem
export CLICKHOUSE_SECURE=true

# Notifications
export NOTIFICATIONS_ENABLED=true
export NOTIFICATIONS_WEBHOOK_URL=https://your-webhook-url

# Environment name (used in notification titles)
export ENVIRONMENT=prod
```

See [Environment Variables Documentation](docs/ENVIRONMENT_VARIABLES.md) for complete list of supported variables.

## Testing

Tests use **pytest**. See `config.yml.example` / `test/config.test.yml` for test configuration — never use production credentials.

### Unit tests (no Docker)

Fast tests under `test/unit/` (buffer, config, DDL, type mapping, etc.):

```bash
pytest test/unit
```

### End-to-end tests (Docker)

Start MySQL and ClickHouse test services, then run e2e tests:

```bash
docker compose -f docker-compose.test.yml up -d
pytest -m e2e
docker compose -f docker-compose.test.yml down
```

Additional integration/reliability tests live under `test/` (batching, crash recovery, schema evolution, etc.). See `test/README.md` for details.

---

## Examples

### Example Logs

**Snapshot Mode:**
```
[INFO] Starting migres (snapshot) mode...
[INFO] MySQL connected: localhost:3306/mydb
[INFO] ClickHouse client initialized for localhost:9000/mydb
[INFO] Tables to snapshot (count=5): ['users', 'orders', 'products']
[INFO] Worker: table users migrated successfully
[INFO] Snapshot completed for all tables.
```

**CDC Mode:**
```
[INFO] Starting migres (CDC) mode...
[INFO] CDC: running initial snapshot before starting binlog streaming...
[INFO] CDC: initial snapshot completed, starting binlog streaming...
[INFO] CDC: producer_flush_interval=5.0, queue-based processing=True
[INFO] CDC: event queued for mydb.users (UpdateRowsEvent) with 1 rows - queue size: 1
[INFO] CDC: event queued for mydb.users (UpdateRowsEvent) with 1 rows - queue size: 2
[INFO] CDC: processing queue (time since last process: 5.0s, queue size: 2)
[INFO] CDC: processing 2 events from queue
[INFO] CDC: processing 1 groups
[INFO] CDC: processing group mydb.users (UpdateRowsEvent) with 2 events containing 2 total rows
[INFO] CDC: inserted 2 row(s) into users (UPDATE->upsert)
[INFO] CDC: successfully processed 2 rows for mydb.users (UpdateRowsEvent)
[INFO] CDC: successfully processed 2 rows from queue
[INFO] CDC: added column email_verified to users (direct ALTER)
[INFO] CDC: detected CREATE TABLE for new_table, creating table in ClickHouse
[INFO] CDC: created table new_table in ClickHouse
[INFO] CDC: detected DROP TABLE for old_table, dropping table in ClickHouse
[INFO] CDC: dropped table old_table in ClickHouse
```

**MS Teams Notifications:**
```
🚀 CDC Process Started
CDC (Change Data Capture) process has started successfully

Level: INFO
Timestamp: 2025-01-24 10:30:00 UTC

Details:
- MySQL: localhost:3306/mydb
- ClickHouse: localhost:9000/mydb
- Batch Delay: 5s
- Mode: CDC
```

```
🚨 CDC Error: Processing Error
Table: mydb.users
Error: Failed to process 5 events: Connection timeout

Level: ERROR
Timestamp: 2025-01-24 10:30:00 UTC

Details:
- Error Type: Processing Error
- Table: mydb.users
- Event Count: 5
- Event Type: WriteRowsEvent
- Error: Connection timeout
```

### Schema Changes in Action

**Adding a column:**
```sql
-- MySQL
ALTER TABLE users ADD COLUMN email_verified BOOLEAN DEFAULT FALSE;
```
```
[INFO] CDC: added column email_verified to users (direct ALTER)
[INFO] CDC: synchronized schema for table users due to DDL
```

**Modifying column type:**
```sql
-- MySQL  
ALTER TABLE users MODIFY COLUMN age VARCHAR(10) DEFAULT 'unknown';
```
```
[INFO] CDC: MODIFY target type for users.age -> LowCardinality(String)
[INFO] CDC: modified column age on users (direct MODIFY)
```

**Creating a new table:**
```sql
-- MySQL
CREATE TABLE new_table (id INT PRIMARY KEY, name VARCHAR(100));
```
```
[INFO] CDC: detected CREATE TABLE for new_table, creating table in ClickHouse
[INFO] CDC: created table new_table in ClickHouse
```

**Dropping a table:**
```sql
-- MySQL
DROP TABLE old_table;
```
```
[INFO] CDC: detected DROP TABLE for old_table, dropping table in ClickHouse
[INFO] CDC: dropped table old_table in ClickHouse
```

---

## Troubleshooting

### Common Issues

**1. CDC not detecting changes:**
- Verify MySQL binlog settings: `SHOW VARIABLES LIKE 'binlog_%';`
- Check user permissions: `GRANT REPLICATION SLAVE ON *.* TO 'user'@'%';`
- Ensure `server_id` is unique in your network

**2. Schema changes not applied:**
- Check logs for "DDL: Synchronized schema" or "DDL: Created table" messages
- Verify table is in `include_tables` (not excluded)
- For MODIFY COLUMN issues, check ClickHouse version compatibility
- DDL operations have retry logic - check logs for retry attempts if schema changes are slow

**3. Duplicate rows in ClickHouse:**
- Use `SELECT * FROM table FINAL` to see deduplicated results
- ReplacingMergeTree automatically handles duplicates on merge
- Run `OPTIMIZE TABLE table_name FINAL` to force merge if needed
- UPDATE events create new rows with higher `__data_transfer_commit_time` - ensure OPTIMIZE runs periodically

**4. Migration stuck:**
- Check `state.json` for tables stuck in `in_progress`
- Delete state file to restart from beginning (or use SIGUSR1 reset in CDC mode)
- Verify MySQL/ClickHouse connectivity

**5. Timezone issues with datetime columns:**
- Configure `clickhouse_timezone` in config.yml
- Ensure it matches your MySQL server timezone for consistency
- Use `DateTime64(3, 'timezone')` for proper timezone handling

**6. Events accumulating in buffer (raw_events growing):**
- Check if tables are in `include_tables` - events for excluded tables are automatically filtered
- Verify CDC pipeline is running (check logs for Producer/Transformer/Consumer threads)
- Inspect `failed_queries` for poison queries (these no longer block the consumer, but indicate data that needs manual fix)

**7. MySQL replication conflicts (server_id errors):**
- Each migres process now uses unique server_id (base + PID modulo)
- Conflicts should be resolved automatically
- If issues persist, ensure only one migres instance connects to MySQL at a time

### Debug Mode

Enable detailed logging by setting log level in your config or environment:
```bash
export MIGRATION_DEBUG=true
docker compose up
```

### Performance Tuning

- **Batch size**: Increase `batch_rows` for faster snapshot (default: 5000)
- **Workers**: Adjust `workers` based on CPU cores (default: 4)
- **Checkpoint batching**: Increase `checkpoint_interval_rows` for larger transformer batches (lower latency when reduced)
- **Low cardinality**: Disable `low_cardinality_strings` if memory is limited
- **CDC batching**: Adjust `producer_flush_interval` for optimal performance:
  - `0` = immediate flush (no batching)
  - `5-15` = good balance for most workloads
  - `30+` = for high-volume, less time-sensitive scenarios
### CDC Batching Configuration

Each pipeline stage has its own interval (the single `batch_delay_seconds` knob was removed in 3.0.0):

**Immediate Processing:**
```yaml
cdc:
  producer_flush_interval: 0      # Flush each event to buffer immediately
  transformer_poll_interval: 0.2
  consumer_poll_interval: 0.1
```
- ✅ Lowest latency
- ❌ More ClickHouse operations
- ❌ Higher load on ClickHouse

**Batched Processing (default):**
```yaml
cdc:
  producer_flush_interval: 5      # Events accumulated for 5 seconds
  transformer_poll_interval: 0.5
  consumer_poll_interval: 0.5
```
- ✅ Reduced ClickHouse load
- ✅ Better performance for bulk operations
- ✅ Smart grouping of similar events
- ⚠️ ~5-second delay for data availability

**High-Volume Batching:**
```yaml
cdc:
  producer_flush_interval: 30     # Events accumulated for 30 seconds
  transformer_poll_interval: 2
  consumer_poll_interval: 2
```
- ✅ Maximum ClickHouse efficiency
- ✅ Best for bulk data processing
- ❌ 30-second delay for data availability

### Batching Examples

**Example 1: Multiple INSERTs**
```
MySQL: 100 INSERT statements for table 'orders'
Result: 1 ClickHouse INSERT with 100 rows
```

**Example 2: Mixed Operations**
```
MySQL: 50 UPDATEs for 'users' + 30 INSERTs for 'orders'
Result: 2 ClickHouse INSERTs (1 with 50 rows, 1 with 30 rows)
```
