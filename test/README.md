# CDC Testing Suite

This directory contains test scripts for the CDC (Change Data Capture) batching functionality.

## Test Files

- **`test_cdc_batching.py`** - Main CDC batching test with 5000 INSERT/UPDATE/DELETE operations
- **`test_notifications.py`** - Live Teams/Slack/Telegram test (opt-in; skips without credentials)
- **`test_data_integrity.py`** - Data integrity test with checksum verification
- **`test_crash_recovery.py`** - Crash recovery scenarios test
- **`test_network_failure.py`** - Network failure and reconnection test
- **`test_ordering.py`** - Event ordering test
- **`test_schema_evolution.py`** - Schema evolution (DDL) test
- **`test_edge_cases.py`** - Edge cases test (NULLs, unicode, large fields)
- **`test_stress.py`** - Stress test with high volume operations
- **`test_pipeline_reliability.py`** - Pipeline reliability test
- **`test_buffer_overflow.py`** - Buffer overflow scenarios test
- **`test_partial_failures.py`** - Partial batch failures test
- **`test_consumer_retry.py`** - Consumer retry logic test
- **`test_clickhouse_failures.py`** - ClickHouse connection failures test
- **`test_binlog_rotation.py`** - Binlog rotation test
- **`test_transaction_rollback.py`** - Transaction rollback test
- **`test_checkpoint_corruption.py`** - Checkpoint corruption recovery test
- **`test_schema_mismatch.py`** - Schema mismatch handling test
- **`monitor_cdc.py`** - Real-time monitoring script to watch ClickHouse table counts

## How to Run Tests

### Prerequisites
1. Ensure MySQL and ClickHouse are accessible
2. Tests will automatically start/stop migres process as needed

### Running Tests

#### Using pytest (Recommended)
```bash
# Run all tests
pytest test/

# Run specific test file
pytest test/test_data_integrity.py

# Run with verbose output
pytest test/ -v

# Run specific test
pytest test/test_ordering.py::test_insert_then_update -v

# Run tests by marker
pytest test/ -m integration
pytest test/ -m slow
pytest test/ -m crash
```

#### Monitor CDC processing
```bash
cd test
python monitor_cdc.py
```

## Expected Results

### Main Test (test_cdc_batching.py)
- ✅ Creates test table in ClickHouse
- ✅ Processes 5000 INSERT operations
- ✅ Processes 5000 UPDATE operations (as upserts)
- ✅ Processes 2500 DELETE operations (as tombstones)
- ✅ Verifies final state: 12,500 total versions, 2,500 active, 2,500 deleted


### Notifications Test (test_notifications.py)
- ✅ Reads notification configuration from config.yml
- ✅ Skips unless at least one live provider is configured (Teams / Slack / Telegram)
- ✅ Sends test notifications (Error, Warning, Info)
- ✅ Tests shared rate limiting (one cooldown for all channels)

## Error Dump Files

When errors occur, dump files are created in the `../data/` directory:
- `failed_operations_YYYYMMDD_HHMMSS.json` - Contains failed operation details
- Files include timestamp, error message, actual row data, and executable SQL queries
- SQL queries contain the real problematic data that caused the conversion errors
- Perfect for debugging and manual recovery of failed operations

## Configuration

Tests use the main `config.yml` file from the parent directory. Make sure:
- `test_table` and `test_error_table` are in the `include_tables` list
- CDC batch delay settings are appropriate for your testing needs
