# CDC Testing Suite

This directory contains test scripts for the CDC (Change Data Capture) batching functionality.

## Test Files

- **`test_cdc_batching.py`** - Main CDC batching test with 5000 INSERT/UPDATE/DELETE operations
- **`test_forced_errors.py`** - Forced error test that creates ClickHouse constraints that will definitely fail
- **`run_test.py`** - Test runner script to execute different test scenarios
- **`monitor_cdc.py`** - Real-time monitoring script to watch ClickHouse table counts

## How to Run Tests

### Prerequisites
1. Make sure CDC process is running: `python migres.py`
2. Ensure MySQL and ClickHouse are accessible

### Running Tests

#### Option 1: Use the test runner
```bash
cd test
python run_test.py
```

#### Option 2: Run individual tests
```bash
cd test
python test_cdc_batching.py
python test_forced_errors.py
```

#### Option 3: Monitor CDC processing
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

### Forced Error Test (test_forced_errors.py)
- ✅ Creates MySQL table with VARCHAR salary column
- ✅ Waits for CDC to create ClickHouse table, then manually alters it to DECIMAL
- ✅ Inserts string data that will cause ClickHouse DECIMAL conversion errors
- ✅ Forces CDC to fail when trying to replicate string data to DECIMAL column
- ✅ Generates error dump files with real SQL queries containing the actual problematic data

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
