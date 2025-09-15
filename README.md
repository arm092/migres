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

### Operations
- 📑 **Detailed logging** (visible via `docker compose logs -f`)
- 🐳 **Docker support** with hot-reload for development

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

2. **Real-time streaming**
   - Connects to MySQL binlog stream
   - Processes INSERT/UPDATE/DELETE events
   - Auto-detects schema changes (ADD/DROP/RENAME/MODIFY)
   - Applies changes to ClickHouse immediately

3. **Schema synchronization**
   - **CREATE TABLE**: Creates new table in ClickHouse
   - **DROP TABLE**: Removes table from ClickHouse
   - **ADD COLUMN**: Creates new column with defaults
   - **DROP COLUMN**: Removes column from ClickHouse
   - **RENAME COLUMN**: Renames column in ClickHouse
   - **MODIFY COLUMN**: Changes type and defaults

4. **Checkpoint persistence**
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
    server_id: 4379  # Unique ID for binlog replication

state_file: "data/state.json"
checkpoint_file: "data/binlog_checkpoint.json"
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
[INFO] CDC EVENT: {"type": "UpdateRowsEvent", "table": "users", "rows": [...]}
[INFO] CDC: inserted 1 row(s) into users (UPDATE->upsert)
[INFO] CDC: added column email_verified to users (direct ALTER)
[INFO] CDC: detected CREATE TABLE for new_table, creating table in ClickHouse
[INFO] CDC: created table new_table in ClickHouse
[INFO] CDC: detected DROP TABLE for old_table, dropping table in ClickHouse
[INFO] CDC: dropped table old_table in ClickHouse
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
