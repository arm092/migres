# Migres - MySQL to ClickHouse Migration Tool

This project is a **complete migration tool** that transfers tables from MySQL into ClickHouse with type mapping, logging, and resumable state.  
It supports both **snapshot mode** (initial data migration) and **CDC mode** (real-time change data capture), with automatic schema synchronization.

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
- 🔄 **Real-time replication** from MySQL binlog
- ⚡ **Queue-based event batching** with configurable delay
- 🎯 **Smart event grouping** (combines multiple events into single operations)
- 🏗️ **Automatic schema synchronization**:
  - ✅ CREATE TABLE (new table creation)
  - ✅ DROP TABLE (table deletion)
  - ✅ ADD COLUMN (with defaults)
  - ✅ DROP COLUMN
  - ✅ RENAME COLUMN (CHANGE COLUMN)
  - ✅ MODIFY COLUMN (type changes, defaults)
- 📊 **ReplacingMergeTree** for upsert semantics
- 🎯 **Table filtering** (include/exclude)
- 💾 **Checkpoint persistence** (resume from last position)
- 🌍 **Timezone-aware datetime handling** (DateTime64 with timezone)
- 🛡️ **Error handling** with failed operation dumps
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

2. **Queue-based event processing**
   - Events are accumulated in a queue as they arrive from binlog
   - Timer-based processing every `batch_delay_seconds` (configurable)
   - Continuous operation: keeps receiving events while processing queue

3. **Event batching and grouping**
   - **INSERT events**: Multiple INSERTs for same table → Single INSERT with multiple rows
   - **UPDATE events**: Multiple UPDATEs for same table → Single INSERT with multiple rows
   - **DELETE events**: Multiple DELETEs for same table → Single INSERT with multiple rows
   - **DDL events**: Processed immediately (not queued)

4. **Real-time streaming**
   - Connects to MySQL binlog stream (non-blocking)
   - Processes INSERT/UPDATE/DELETE events
   - Auto-detects schema changes (ADD/DROP/RENAME/MODIFY)
   - Applies changes to ClickHouse in batches

5. **Schema synchronization**
   - **CREATE TABLE**: Creates new table in ClickHouse
   - **DROP TABLE**: Removes table from ClickHouse
   - **ADD COLUMN**: Creates new column with defaults
   - **DROP COLUMN**: Removes column from ClickHouse
   - **RENAME COLUMN**: Renames column in ClickHouse
   - **MODIFY COLUMN**: Changes type and defaults

6. **Error handling**
   - Failed operations are dumped to JSON files for manual review
   - Includes timestamp, error details, and operation information
   - Allows for manual recovery of failed operations

7. **Checkpoint persistence**
   - Saves binlog position periodically
   - Resumes from last position on restart

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

clickhouse:
  host: "localhost"
  port: 9000
  user: "default"
  password: ""
  database: "your_ch_database"

migration:
  mode: "snapshot"  # or "cdc"
  batch_rows: 5000
  workers: 4
  low_cardinality_strings: true
  ddl_engine: "ReplacingMergeTree"
  
  # Timezone configuration for datetime/timestamp columns
  mysql_timezone: "Europe/Moscow"      # Set to your MySQL server timezone
  clickhouse_timezone: "Europe/Moscow" # Set to desired ClickHouse timezone
  
  # CDC-specific settings
  cdc:
    snapshot_before: true  # Run snapshot before CDC
    heartbeat_seconds: 5
    checkpoint_interval_rows: 1000
    checkpoint_interval_seconds: 5
    batch_delay_seconds: 5  # Delay in seconds before processing accumulated events (0 = immediate processing)
    server_id: 4379  # Unique ID for binlog replication

state_file: "data/state.json"
checkpoint_file: "data/binlog_checkpoint.json"

# MS Teams Notifications
notifications:
  enabled: true
  webhook_url: "https://your-org.webhook.office.com/webhookb2/your-webhook-url"
  rate_limit_seconds: 60  # Minimum seconds between notifications (0 = no limit)
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

### Environment Variables Support

All configuration options can be overridden using environment variables. This is useful for containerized deployments:

```bash
# MySQL configuration
export MYSQL_HOST=mysql-server.example.com
export MYSQL_PASSWORD=your-password

# ClickHouse configuration  
export CLICKHOUSE_HOST=clickhouse-server.example.com
export CLICKHOUSE_PASSWORD=your-password

# Notifications
export NOTIFICATIONS_ENABLED=true
export NOTIFICATIONS_WEBHOOK_URL=https://your-webhook-url
```

See [Environment Variables Documentation](docs/ENVIRONMENT_VARIABLES.md) for complete list of supported variables.

## Testing

The project includes a comprehensive test suite in the `test/` directory:

- **`test/test_cdc_batching.py`** - Main CDC batching test (5000 operations)
- **`test/test_forced_errors.py`** - Forced error test with type conversion errors
- **`test/test_notifications.py`** - MS Teams notification system test
- **`test/run_test.py`** - Test runner for different scenarios
- **`test/monitor_cdc.py`** - Real-time CDC monitoring

### Running Tests
```bash
cd test
python run_test.py
```

See `test/README.md` for detailed testing instructions.

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
[INFO] CDC: batch_delay_seconds=5.0, queue-based processing=True
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
- Check logs for "CDC: synchronized schema" messages
- Verify table is in `include_tables` (not excluded)
- For MODIFY COLUMN issues, check ClickHouse version compatibility

**3. Duplicate rows in ClickHouse:**
- Use `SELECT * FROM table FINAL` to see deduplicated results
- ReplacingMergeTree automatically handles duplicates on merge

**4. Migration stuck:**
- Check `state.json` for incomplete tables
- Delete state file to restart from beginning
- Verify MySQL/ClickHouse connectivity

**5. Timezone issues with datetime columns:**
- Configure `mysql_timezone` and `clickhouse_timezone` in config.yml
- Ensure both are set to the same timezone for consistency
- Use `DateTime64(3, 'timezone')` for proper timezone handling

**6. Duplicate inserts in ClickHouse:**
- This was a bug that has been fixed in recent versions
- Each MySQL event now results in exactly one ClickHouse insert
- Use `SELECT * FROM table FINAL` to see deduplicated results

### Debug Mode

Enable detailed logging by setting log level in your config or environment:
```bash
export PYTHONPATH=/app
export LOG_LEVEL=DEBUG
docker compose up
```

### Performance Tuning

- **Batch size**: Increase `batch_rows` for faster snapshot (default: 5000)
- **Workers**: Adjust `workers` based on CPU cores (default: 4)
- **Checkpoint frequency**: Reduce `checkpoint_interval_seconds` for more frequent saves
- **Low cardinality**: Disable `low_cardinality_strings` if memory is limited
- **CDC batching**: Adjust `batch_delay_seconds` for optimal performance:
  - `0` = immediate processing (no batching)
  - `5-15` = good balance for most workloads
  - `30+` = for high-volume, less time-sensitive scenarios

### CDC Batching Configuration

The `batch_delay_seconds` setting controls how events are processed:

**Immediate Processing (`batch_delay_seconds: 0`):**
```yaml
cdc:
  batch_delay_seconds: 0  # Each event processed immediately
```
- ✅ Lowest latency
- ❌ More ClickHouse operations
- ❌ Higher load on ClickHouse

**Batched Processing (`batch_delay_seconds: 5`):**
```yaml
cdc:
  batch_delay_seconds: 5  # Events accumulated for 5 seconds
```
- ✅ Reduced ClickHouse load
- ✅ Better performance for bulk operations
- ✅ Smart grouping of similar events
- ⚠️ 5-second delay for data availability

**High-Volume Batching (`batch_delay_seconds: 30`):**
```yaml
cdc:
  batch_delay_seconds: 30  # Events accumulated for 30 seconds
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

**Example 3: Error Handling**
```
Failed operation → Dumped to failed_operations_20250922_151207.json
Contains: timestamp, error details, operation data for manual review
```
