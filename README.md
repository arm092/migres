# Migres Snapshot Tool (MySQL → ClickHouse)

This project is a **snapshot migration tool** that transfers tables from MySQL into ClickHouse with type mapping, logging, and resumable state.  
It is designed for **snapshot-only mode** (no CDC yet), supporting large datasets with parallel table processing.

---

## Features

- 🚀 **MySQL → ClickHouse snapshot migration**
- 🗂 **Keeps real column types** (INT, DECIMAL, DATE, DATETIME, VARCHAR, etc.)
- 📝 **Transferia metadata columns** added automatically:
  - `__data_transfer_commit_time UInt64` → nanosecond commit timestamp
  - `__data_transfer_delete_time UInt64 DEFAULT 0`
  - `__data_transfer_is_deleted UInt8 MATERIALIZED if(__data_transfer_delete_time != 0, 1, 0)`
- 🔁 **Resumable migration** (state stored in `state.json`)
- 🎯 **Included/excluded tables filtering**
- ⚡ **Parallel table processing** for large tables
- 📑 **Detailed logging** (visible via `docker compose logs -f`)

---

## How It Works

1. **Start MySQL & ClickHouse connections**
   - Logs `mysql connected successfully`
   - Logs `clickhouse connected successfully`

2. **Load state.json**
   - Keeps track of already migrated tables  
   - If migration crashes or is stopped, it will **resume** on next run

3. **Filter tables**
   - Tables are selected by `included_tables` and `excluded_tables` in `config.py`  
   - `included_tables` takes priority

4. **Parallel table migration**
   - Each table is processed by a worker
   - Worker steps:
     1. Inspect MySQL schema (`get_table_columns_and_pk`)
     2. Create table in ClickHouse with mapped types
     3. Insert data in batches
     4. Append Transferia columns (`__data_transfer_commit_time`, etc.)
     5. Mark table as migrated in `state.json`

5. **Logging**
   - Example logs:
     ```
     [INFO] mysql connected successfully
     [INFO] clickhouse connected successfully
     [INFO] Migrating table: commission
     [INFO] Reading batch 1 from commission
     [INFO] Inserted batch 1 into commission
     [INFO] Table commission migrated successfully
     [INFO] Snapshot completed
     ```

---

## Requirements

- **MySQL** server (with data to snapshot)
- **ClickHouse** server (can be remote)
- **Docker + Docker Compose**

---

## Running

### 0. Make sure `binlog_format=ROW`, `binlog_row_image=FULL` on MySQL/MariaDB
### 1. Configure connections in `config.yml`
### 2. Set include or exclude tables, or leave them empty
### 3. Run with docker compose
