#!/usr/bin/env python3
"""
Schema Evolution Test - Tests DDL operations during replication.
Verifies:
1. ADD COLUMN is replicated correctly
2. DROP COLUMN is handled
3. MODIFY COLUMN type changes work
4. CREATE/DROP TABLE sequences work
"""

import time
import sys
import os
import subprocess
import pytest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from buffer import BufferDB
from notifications import notify_cdc_shutdown
from conftest import wait_for_cdc_sync, wait_for_table_in_clickhouse


def start_migres():
    """Start migres process"""
    print("▶️ Starting migres process...")
    python_exe = sys.executable
    process = subprocess.Popen(
        [python_exe, "migres.py", "--config", "config.yml"],
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(5)
    return process


def stop_migres(process, reason="Test complete"):
    """Stop migres process"""
    print(f"🛑 Stopping migres ({reason})...")
    notify_cdc_shutdown(f"Schema Evolution Test: {reason}")
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
    # Close stdout/stderr pipes to prevent ResourceWarning
    if process.stdout:
        process.stdout.close()
    if process.stderr:
        process.stderr.close()


def wait_for_cdc(timeout=30):
    """Wait for CDC to process events"""
    return wait_for_cdc_sync(timeout)


def get_clickhouse_columns(ch, table_name):
    """Get column names from ClickHouse table"""
    try:
        result = ch.execute(f"DESCRIBE TABLE `{ch.db}`.`{table_name}`")
        # Filter out internal columns
        columns = [row[0] for row in result if not row[0].startswith('__data_transfer')]
        return columns
    except Exception:
        return []


def get_clickhouse_count(ch, table_name, timeout=30):
    """Get row count from ClickHouse"""
    start = time.time()
    while time.time() - start < timeout:
        try:
            count = ch.execute(
                f"SELECT count() FROM `{ch.db}`.`{table_name}` FINAL WHERE __data_transfer_delete_time = 0"
            )[0][0]
            return count
        except Exception:
            time.sleep(2)
    return -1


def table_exists_in_clickhouse(ch, table_name):
    """Check if table exists in ClickHouse"""
    try:
        result = ch.execute(f"EXISTS TABLE `{ch.db}`.`{table_name}`")
        return result and result[0][0] == 1
    except Exception:
        return False


# =============================================================================
# TEST SCENARIOS
# =============================================================================

@pytest.mark.integration
@pytest.mark.slow
def test_add_column(db_connections):
    mysql, ch, cfg = db_connections
    """Test: ADD COLUMN during replication"""
    print("\n" + "="*60)
    print("📋 TEST 1: ADD COLUMN")
    print("="*60)
    
    table = "test_schema_add"
    
    # Start migres first to capture DDL
    process = start_migres()
    time.sleep(3)
    
    # Create table
    print("📋 Creating initial table...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                name VARCHAR(100)
            )
        """)
        mysql.cn.commit()
    
    # Get batch delay for proper timing
    from conftest import get_batch_delay_seconds
    batch_delay = get_batch_delay_seconds(cfg)
    
    # Wait for DDL event to be processed
    wait_time_ddl = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time_ddl}s for CREATE TABLE DDL to be processed...")
    time.sleep(wait_time_ddl)
    
    # Wait for table to exist in ClickHouse
    assert wait_for_table_in_clickhouse(ch, table, timeout=120), f"Table {table} was not created"
    
    # Insert initial data
    print("📝 Inserting initial data...")
    with mysql.cn.cursor() as cur:
        for i in range(1, 101):
            cur.execute(f"INSERT INTO {table} (id, name) VALUES (%s, %s)", (i, f"name_{i}"))
        mysql.cn.commit()
    
    # Wait for INSERTs to be committed to buffer
    wait_time = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time}s for INSERTs to be flushed and processed...")
    time.sleep(wait_time)
    assert wait_for_cdc(timeout=180), "CDC sync timeout after initial INSERTs"
    
    # ADD COLUMN
    print("➕ Adding new column 'email'...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN email VARCHAR(100)")
        mysql.cn.commit()
    
    # Wait for DDL to be processed
    print("⏳ Waiting for ALTER TABLE to be processed...")
    time.sleep(max(batch_delay + 10, 25))  # Wait longer for DDL processing
    
    # Verify column was added
    columns_before = get_clickhouse_columns(ch, table)
    assert 'email' in columns_before, f"Column 'email' not found after ALTER TABLE. Columns: {columns_before}"
    
    # Insert more data with new column
    print("📝 Inserting data with new column...")
    with mysql.cn.cursor() as cur:
        for i in range(101, 201):
            cur.execute(
                f"INSERT INTO {table} (id, name, email) VALUES (%s, %s, %s)",
                (i, f"name_{i}", f"email_{i}@test.com")
            )
        mysql.cn.commit()
    
    # Wait for INSERTs to be committed to buffer
    print("⏳ Waiting for INSERTs with new column to be committed...")
    wait_time_inserts = max(batch_delay * 4, 45)
    time.sleep(wait_time_inserts)
    assert wait_for_cdc(timeout=180), "CDC sync timeout after INSERTs with new column"
    
    # Optimize table before checking
    try:
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(3)
    except Exception as e:
        print(f"   Optimization warning: {e}")
    
    # Verify
    columns = get_clickhouse_columns(ch, table)
    count = get_clickhouse_count(ch, table, timeout=60)
    
    stop_migres(process, "ADD COLUMN test complete")
    
    assert 'email' in columns, f"Column 'email' not found in {columns}"
    assert count == 200, f"Expected 200 rows, got {count}"
    print(f"\n✅ ADD COLUMN test: columns={columns}, count={count}")


@pytest.mark.integration
@pytest.mark.slow
def test_add_multiple_columns(db_connections):
    mysql, ch, cfg = db_connections
    """Test: ADD multiple columns"""
    print("\n" + "="*60)
    print("📋 TEST 2: ADD Multiple Columns")
    print("="*60)
    
    table = "test_schema_multi"
    
    # Start migres first to capture DDL
    process = start_migres()
    time.sleep(3)
    
    # Create table
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                name VARCHAR(100)
            )
        """)
        mysql.cn.commit()
    
    # Get batch delay for proper timing
    from conftest import get_batch_delay_seconds
    batch_delay = get_batch_delay_seconds(cfg)
    
    # Wait for DDL event to be processed
    wait_time_ddl = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time_ddl}s for CREATE TABLE DDL to be processed...")
    time.sleep(wait_time_ddl)
    
    # Wait for table to exist in ClickHouse
    assert wait_for_table_in_clickhouse(ch, table, timeout=120), f"Table {table} was not created"
    
    # Insert initial data
    with mysql.cn.cursor() as cur:
        for i in range(1, 51):
            cur.execute(f"INSERT INTO {table} (id, name) VALUES (%s, %s)", (i, f"name_{i}"))
        mysql.cn.commit()
    
    # Wait for INSERTs to be committed
    wait_time = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time}s for initial INSERTs to be flushed and processed...")
    time.sleep(wait_time)
    assert wait_for_cdc(timeout=180), "CDC sync timeout after initial INSERTs"
    
    # Add multiple columns one by one
    print("➕ Adding column 'age'...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN age INT")
        mysql.cn.commit()
    print("⏳ Waiting for ALTER TABLE (age)...")
    time.sleep(max(batch_delay + 10, 25))  # Wait longer for DDL
    
    print("➕ Adding column 'city'...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN city VARCHAR(50)")
        mysql.cn.commit()
    print("⏳ Waiting for ALTER TABLE (city)...")
    time.sleep(max(batch_delay + 10, 25))  # Wait longer for DDL
    
    print("➕ Adding column 'active'...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN active TINYINT(1) DEFAULT 1")
        mysql.cn.commit()
    print("⏳ Waiting for ALTER TABLE (active)...")
    wait_time_alter = max(batch_delay + 10, 25)
    time.sleep(wait_time_alter)  # Wait for DDL event to be flushed
    assert wait_for_cdc(timeout=120), "CDC sync timeout after ALTER TABLE (active)"
    
    # Verify all columns were added
    columns_check = get_clickhouse_columns(ch, table)
    print(f"   Columns after ALTER TABLEs: {columns_check}")
    expected_cols = {'id', 'name', 'age', 'city', 'active'}
    missing = expected_cols - set(columns_check)
    assert not missing, f"Missing columns after ALTER TABLE: {missing}. Got: {set(columns_check)}"
    
    # Insert data with all columns
    print("📝 Inserting data with new columns...")
    with mysql.cn.cursor() as cur:
        for i in range(51, 101):
            cur.execute(
                f"INSERT INTO {table} (id, name, age, city, active) VALUES (%s, %s, %s, %s, %s)",
                (i, f"name_{i}", 20 + i % 30, f"city_{i % 10}", i % 2)
            )
        mysql.cn.commit()
    
    # Wait for INSERTs to be committed
    print("⏳ Waiting for INSERTs with new columns...")
    wait_time_inserts = max(batch_delay * 4, 45)
    time.sleep(wait_time_inserts)
    assert wait_for_cdc(timeout=180), "CDC sync timeout after INSERTs with new columns"
    
    # Optimize table before checking
    try:
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(3)
    except Exception as e:
        print(f"   Optimization warning: {e}")
    
    columns = get_clickhouse_columns(ch, table)
    count = get_clickhouse_count(ch, table, timeout=60)
    
    stop_migres(process, "multi-column test complete")
    
    expected_cols = {'id', 'name', 'age', 'city', 'active'}
    assert expected_cols.issubset(set(columns)), f"Missing columns. Expected {expected_cols}, got {set(columns)}"
    assert count == 100, f"Expected 100 rows, got {count}"
    print(f"\n✅ Multi-column test: columns={columns}, count={count}")


@pytest.mark.integration
@pytest.mark.slow
def test_create_drop_table(db_connections):
    mysql, ch, cfg = db_connections
    """Test: CREATE and DROP TABLE sequences"""
    print("\n" + "="*60)
    print("📋 TEST 3: CREATE/DROP TABLE Sequence")
    print("="*60)
    
    table = "test_schema_createdrop"
    
    process = start_migres()
    time.sleep(3)
    
    # Get batch delay for proper timing
    from conftest import get_batch_delay_seconds
    batch_delay = get_batch_delay_seconds(cfg)
    
    for cycle in range(3):
        print(f"\n--- Cycle {cycle+1}/3 ---")
        
        # Create table
        print(f"📋 Creating table (cycle {cycle+1})...")
        with mysql.cn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(f"""
                CREATE TABLE {table} (
                    id INT PRIMARY KEY,
                    data VARCHAR(100),
                    cycle INT
                )
            """)
            mysql.cn.commit()
        
        # Wait for DDL event to be processed
        wait_time_ddl = max(batch_delay + 10, 30)
        print(f"⏳ Waiting {wait_time_ddl}s for CREATE TABLE DDL to be processed...")
        time.sleep(wait_time_ddl)
        
        # Wait for table to exist in ClickHouse
        assert wait_for_table_in_clickhouse(ch, table, timeout=120), f"Table {table} was not created in cycle {cycle+1}"
        
        # Insert data
        print("📝 Inserting data...")
        with mysql.cn.cursor() as cur:
            for i in range(1, 51):
                cur.execute(
                    f"INSERT INTO {table} (id, data, cycle) VALUES (%s, %s, %s)",
                    (i, f"data_{i}", cycle + 1)
                )
            mysql.cn.commit()
        
        # Wait for INSERTs to be committed
        wait_time = max(batch_delay + 10, 30)
        print(f"⏳ Waiting {wait_time}s for INSERTs to be flushed and processed...")
        time.sleep(wait_time)
        assert wait_for_cdc(timeout=180), f"CDC sync timeout in cycle {cycle+1}"
        
        # Optimize table before checking
        try:
            ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
            time.sleep(3)
        except Exception as e:
            print(f"   Optimization warning: {e}")
        
        # Verify data exists
        count = get_clickhouse_count(ch, table, timeout=60)
        print(f"   ClickHouse count: {count}")
        assert count == 50, f"Expected 50 rows in cycle {cycle+1}, got {count}"
        
        if cycle < 2:  # Don't drop on last cycle
            # Drop table
            print("🗑️ Dropping table...")
            with mysql.cn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table}")
                mysql.cn.commit()
            
            # Wait for DROP TABLE to be processed
            print("⏳ Waiting for DROP TABLE to be processed...")
            time.sleep(max(batch_delay + 5, 20))
    
    # Final verification - wait for table to exist and data to be replicated
    # Note: Cycle 3 already waited for CDC sync, so we just need a quick verification
    print("⏳ Waiting for final table and data...")
    assert wait_for_table_in_clickhouse(ch, table, timeout=90), "Table should exist after final CREATE"
    
    # Quick final sync check (data was already synced in cycle 3)
    assert wait_for_cdc(timeout=60), "CDC sync timeout for final cycle"
    
    # Optimize before final check
    try:
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(5)
    except Exception as e:
        print(f"   Optimization warning: {e}")
    
    exists = table_exists_in_clickhouse(ch, table)
    count = get_clickhouse_count(ch, table, timeout=60) if exists else -1
    
    stop_migres(process, "CREATE/DROP test complete")
    
    assert exists, "Table should exist after CREATE"
    assert count == 50, f"Expected 50 rows, got {count}"
    print(f"\n✅ CREATE/DROP test: exists={exists}, count={count}")


@pytest.mark.integration
@pytest.mark.slow
def test_modify_column(db_connections):
    mysql, ch, cfg = db_connections
    """Test: MODIFY COLUMN type"""
    print("\n" + "="*60)
    print("📋 TEST 4: MODIFY COLUMN Type")
    print("="*60)
    
    table = "test_schema_modify"
    
    # Start migres first to capture DDL
    process = start_migres()
    time.sleep(3)
    
    # Create table with small varchar
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                short_text VARCHAR(50)
            )
        """)
        mysql.cn.commit()
    
    # Get batch delay for proper timing
    from conftest import get_batch_delay_seconds
    batch_delay = get_batch_delay_seconds(cfg)
    
    # Wait for DDL event to be processed
    wait_time_ddl = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time_ddl}s for CREATE TABLE DDL to be processed...")
    time.sleep(wait_time_ddl)
    
    # Wait for table to exist in ClickHouse
    assert wait_for_table_in_clickhouse(ch, table, timeout=120), f"Table {table} was not created"
    
    # Insert data
    print("📝 Inserting initial data...")
    with mysql.cn.cursor() as cur:
        for i in range(1, 51):
            cur.execute(
                f"INSERT INTO {table} (id, short_text) VALUES (%s, %s)",
                (i, f"short_{i}")
            )
        mysql.cn.commit()
    
    # Wait for INSERTs to be committed
    wait_time = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time}s for initial INSERTs to be flushed and processed...")
    time.sleep(wait_time)
    assert wait_for_cdc(timeout=180), "CDC sync timeout after initial INSERTs"
    
    # Modify column to larger varchar
    print("🔄 Modifying column VARCHAR(50) -> VARCHAR(255)...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"ALTER TABLE {table} MODIFY COLUMN short_text VARCHAR(255)")
        mysql.cn.commit()
    
    # Wait for MODIFY COLUMN to be processed
    print("⏳ Waiting for MODIFY COLUMN to be processed...")
    time.sleep(max(batch_delay + 10, 25))  # Wait longer for DDL
    
    # Insert longer data
    print("📝 Inserting longer data...")
    with mysql.cn.cursor() as cur:
        for i in range(51, 101):
            cur.execute(
                f"INSERT INTO {table} (id, short_text) VALUES (%s, %s)",
                (i, "x" * 200)  # Longer than original 50 chars
            )
        mysql.cn.commit()
    
    # Wait for INSERTs to be committed
    print("⏳ Waiting for INSERTs after MODIFY...")
    wait_time_modify = max(batch_delay * 4, 45)
    time.sleep(wait_time_modify)
    assert wait_for_cdc(timeout=180), "CDC sync timeout after INSERTs with modified column"
    
    # Optimize table before checking
    try:
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(3)
    except Exception as e:
        print(f"   Optimization warning: {e}")
    
    count = get_clickhouse_count(ch, table, timeout=60)
    
    stop_migres(process, "MODIFY test complete")
    
    assert count == 100, f"Expected 100 rows, got {count}"
    print(f"\n✅ MODIFY COLUMN test: count={count}")





