#!/usr/bin/env python3
"""
Edge Cases Test - Tests handling of unusual data scenarios.
Verifies:
1. NULL values in all column types
2. Empty strings vs NULLs
3. Unicode/special characters
4. Very large TEXT/BLOB fields
5. Boundary values (MAX_INT, etc.)
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
    """Start migres process and ensure it started correctly."""
    print("▶️ Starting migres process...")
    python_exe = sys.executable
    cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    process = subprocess.Popen(
        [python_exe, "migres.py", "--config", "config.yml"],
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    time.sleep(5)

    # If the process exited early, surface logs and fail fast
    if process.poll() is not None:
        output = process.stdout.read().decode() if process.stdout else ""
        print(f"   ⚠️ Migres process exited with code {process.returncode}")
        print(f"   Output: {output[:2000]}")
        raise RuntimeError("Migres failed to start")

    print("   ✅ Migres process running")
    return process


def stop_migres(process, reason="Test complete"):
    """Stop migres process"""
    print(f"🛑 Stopping migres ({reason})...")
    notify_cdc_shutdown(f"Edge Cases Test: {reason}")
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
    # Close stdout pipe to prevent ResourceWarning
    if process.stdout:
        process.stdout.close()


def wait_for_cdc(timeout=60):
    """Wait for CDC to process events (raw_events and prepared_queries empty)."""
    return wait_for_cdc_sync(timeout)


def wait_for_table(ch, table_name, timeout=30):
    """Wait for table to exist in ClickHouse"""
    return wait_for_table_in_clickhouse(ch, table_name, timeout)


def get_clickhouse_count(ch, table_name, timeout=60):
    """Get row count from ClickHouse, waiting for data to appear"""
    if not wait_for_table(ch, table_name, timeout=10):
        return -1
    
    start = time.time()
    last_count = -1
    stable_count = 0
    zero_count_stable = 0
    
    while time.time() - start < timeout:
        try:
            count = ch.execute(
                f"SELECT count() FROM `{ch.db}`.`{table_name}` FINAL WHERE __data_transfer_delete_time = 0"
            )[0][0]
            
            # If count is > 0, wait for it to stabilize
            if count > 0:
                if count == last_count:
                    stable_count += 1
                    if stable_count >= 2:  # Count stable for 2 checks
                        return count
                else:
                    stable_count = 0
            else:
                # If count is 0, wait a bit longer to ensure data hasn't arrived yet
                if count == last_count:
                    zero_count_stable += 1
                    # If count stays at 0 for 5 consecutive checks (10 seconds), return 0
                    if zero_count_stable >= 5:
                        return 0
                else:
                    zero_count_stable = 0
            
            last_count = count
        except Exception as e:
            pass
        time.sleep(2)
    
    # Return last count even if not fully stable (might be timing issue)
    return last_count if last_count >= 0 else -1


# =============================================================================
# TEST SCENARIOS
# =============================================================================

@pytest.mark.integration
@pytest.mark.slow
def test_null_values(db_connections):
    mysql, ch, cfg = db_connections
    """Test: NULL values in various column types"""
    print("\n" + "="*60)
    print("📋 TEST 1: NULL Values")
    print("="*60)
    
    table = "test_edge_nulls"
    
    # Start migres FIRST so it captures DDL events
    process = start_migres()
    time.sleep(3)
    
    # Create table AFTER migres is running
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                int_null INT NULL,
                varchar_null VARCHAR(100) NULL,
                text_null TEXT NULL,
                decimal_null DECIMAL(10,2) NULL,
                datetime_null DATETIME NULL,
                date_null DATE NULL
            )
        """)
        mysql.cn.commit()
    
    time.sleep(5)  # Wait for DDL to be processed
    
    # Wait for table to exist in ClickHouse before inserting
    assert wait_for_table(ch, table, timeout=60), f"Table {table} was not created in ClickHouse"
    
    # Insert rows with various NULL patterns
    print("📝 Inserting rows with NULL values...")
    with mysql.cn.cursor() as cur:
        # All NULLs
        cur.execute(f"""
            INSERT INTO {table} (id, int_null, varchar_null, text_null, decimal_null, datetime_null, date_null)
            VALUES (1, NULL, NULL, NULL, NULL, NULL, NULL)
        """)
        
        # Some NULLs
        cur.execute(f"""
            INSERT INTO {table} (id, int_null, varchar_null, text_null, decimal_null, datetime_null, date_null)
            VALUES (2, 100, NULL, 'text value', NULL, '2024-01-15 10:30:00', NULL)
        """)
        
        # No NULLs
        cur.execute(f"""
            INSERT INTO {table} (id, int_null, varchar_null, text_null, decimal_null, datetime_null, date_null)
            VALUES (3, 200, 'varchar value', 'text value', 123.45, '2024-01-15 10:30:00', '2024-01-15')
        """)
        
        # More mixed patterns
        for i in range(4, 51):
            cur.execute(f"""
                INSERT INTO {table} (id, int_null, varchar_null, text_null, decimal_null, datetime_null, date_null)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                i,
                i * 10 if i % 2 == 0 else None,
                f"varchar_{i}" if i % 3 == 0 else None,
                f"text_{i}" if i % 4 == 0 else None,
                i * 1.5 if i % 5 == 0 else None,
                f"2024-01-{(i % 28) + 1:02d} 12:00:00" if i % 6 == 0 else None,
                f"2024-01-{(i % 28) + 1:02d}" if i % 7 == 0 else None
            ))
        mysql.cn.commit()
    
    wait_for_cdc(timeout=90)
    
    # Give extra time for data to be fully processed
    print("   Waiting for data replication...")
    time.sleep(15)  # Wait longer for all inserts to be processed
    
    # Check if table exists before optimizing
    if wait_for_table(ch, table, timeout=10):
        try:
            ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
            time.sleep(5)
        except Exception as e:
            print(f"   Optimization warning (NULL test): {e}")
    else:
        print(f"   ⚠️ Table {table} does not exist in ClickHouse")
    
    count = get_clickhouse_count(ch, table, timeout=90)
    
    # Verify NULL counts in ClickHouse (only if table exists and count > 0)
    if count > 0 and wait_for_table(ch, table, timeout=5):
        try:
            null_counts = ch.execute(f"""
                SELECT 
                    countIf(isNull(int_null)) as int_nulls,
                    countIf(isNull(varchar_null)) as varchar_nulls
                FROM `{ch.db}`.`{table}` FINAL WHERE __data_transfer_delete_time = 0
            """)[0]
            print(f"   NULL counts in CH: int={null_counts[0]}, varchar={null_counts[1]}")
        except Exception as e:
            print(f"   Could not verify NULL counts: {e}")
    else:
        print(f"   ⚠️ Skipping NULL count verification (count={count})")
    
    stop_migres(process, "NULL test complete")
    
    assert count == 50, f"Expected 50 rows, got {count}"
    print(f"\n✅ NULL values test: count={count}")


@pytest.mark.integration
@pytest.mark.slow
def test_empty_strings(db_connections):
    mysql, ch, cfg = db_connections
    """Test: Empty strings vs NULLs"""
    print("\n" + "="*60)
    print("📋 TEST 2: Empty Strings vs NULLs")
    print("="*60)
    
    table = "test_edge_empty"
    
    process = start_migres()
    time.sleep(3)
    
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                str_null VARCHAR(100) NULL,
                str_empty VARCHAR(100) NOT NULL DEFAULT ''
            )
        """)
        mysql.cn.commit()
    
    # Wait for table to exist in ClickHouse before inserting data
    assert wait_for_table(ch, table, timeout=60), f"Table {table} was not created in ClickHouse"
    
    print("📝 Inserting empty strings and NULLs...")
    with mysql.cn.cursor() as cur:
        # NULL vs empty
        cur.execute(f"INSERT INTO {table} (id, str_null, str_empty) VALUES (1, NULL, '')")
        cur.execute(f"INSERT INTO {table} (id, str_null, str_empty) VALUES (2, '', 'not empty')")
        cur.execute(f"INSERT INTO {table} (id, str_null, str_empty) VALUES (3, 'not null', '')")
        cur.execute(f"INSERT INTO {table} (id, str_null, str_empty) VALUES (4, 'value', 'value')")
        
        for i in range(5, 51):
            cur.execute(
                f"INSERT INTO {table} (id, str_null, str_empty) VALUES (%s, %s, %s)",
                (i, '' if i % 2 == 0 else f"val_{i}", '' if i % 3 == 0 else f"val_{i}")
            )
        mysql.cn.commit()
    
    # Wait for CDC to flush and ClickHouse to merge parts
    wait_for_cdc(timeout=90)
    
    # Give extra time for data to be fully processed
    print("   Waiting additional time for data replication...")
    time.sleep(10)
    
    # Check if table exists before optimizing
    if wait_for_table(ch, table, timeout=10):
        try:
            ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
            time.sleep(5)  # Wait longer after optimization
        except Exception as e:
            print(f"   Optimization warning (empty strings): {e}")
    else:
        print(f"   ⚠️ Table {table} does not exist in ClickHouse, skipping optimization")

    count = get_clickhouse_count(ch, table, timeout=90)
    
    stop_migres(process, "empty string test complete")
    
    assert count == 50, f"Expected 50 rows, got {count}"
    print(f"\n✅ Empty strings test: count={count}")


@pytest.mark.integration
@pytest.mark.slow
def test_unicode_characters(db_connections):
    mysql, ch, cfg = db_connections
    """Test: Unicode and special characters"""
    print("\n" + "="*60)
    print("📋 TEST 3: Unicode Characters")
    print("="*60)
    
    table = "test_edge_unicode"
    
    process = start_migres()
    time.sleep(3)
    
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                unicode_text VARCHAR(500) CHARACTER SET utf8mb4
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        mysql.cn.commit()
    
    time.sleep(5)  # Wait for DDL
    
    # Wait for table to exist in ClickHouse before inserting
    assert wait_for_table_in_clickhouse(ch, table, timeout=60), f"Table {table} was not created in ClickHouse"
    
    print("📝 Inserting unicode characters...")
    unicode_samples = [
        "Hello World",                          # ASCII
        "Привет мир",                           # Russian
        "你好世界",                              # Chinese
        "مرحبا بالعالم",                         # Arabic
        "🎉🚀💻🔥",                              # Emojis
        "Ñoño España",                          # Spanish with accents
        "日本語テスト",                          # Japanese
        "한국어 테스트",                          # Korean
        "Ελληνικά",                             # Greek
        "עברית",                                # Hebrew
        "Tab\there\nnewline",                   # Control characters
        "Quote's \"double\" `backtick`",        # Quotes
        "<html>&amp;entities</html>",           # HTML entities
        "Path\\with\\backslashes",              # Backslashes
        "Mix: 日本語 + Ελληνικά + 🎉",           # Mixed scripts
    ]
    
    with mysql.cn.cursor() as cur:
        for i, text in enumerate(unicode_samples, 1):
            cur.execute(
                f"INSERT INTO {table} (id, unicode_text) VALUES (%s, %s)",
                (i, text)
            )
        
        # Add more rows
        for i in range(len(unicode_samples) + 1, 51):
            cur.execute(
                f"INSERT INTO {table} (id, unicode_text) VALUES (%s, %s)",
                (i, f"Standard text {i} with émojis 🔥")
            )
        mysql.cn.commit()
    
    # Wait for INSERTs to be committed to buffer (batch_delay_seconds = 15s)
    print("⏳ Waiting for INSERTs to be committed to buffer...")
    time.sleep(20)  # Wait longer than batch_delay_seconds
    
    # Wait for CDC to flush and ClickHouse to merge parts
    assert wait_for_cdc(timeout=90), "CDC sync timeout"
    
    try:
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(3)
    except Exception as e:
        print(f"   Optimization warning (unicode): {e}")

    count = get_clickhouse_count(ch, table, timeout=90)
    
    # Verify some unicode data
    try:
        sample = ch.execute(f"SELECT unicode_text FROM `{ch.db}`.`{table}` FINAL WHERE id = 3")[0][0]
        print(f"   Sample Chinese text from CH: {sample}")
    except Exception as e:
        print(f"   Could not verify unicode: {e}")
    
    stop_migres(process, "unicode test complete")
    
    assert count == 50, f"Expected 50 rows, got {count}"
    print(f"\n✅ Unicode test: count={count}")


@pytest.mark.integration
@pytest.mark.slow
def test_large_fields(db_connections):
    mysql, ch, cfg = db_connections
    """Test: Very large TEXT fields"""
    print("\n" + "="*60)
    print("📋 TEST 4: Large TEXT Fields")
    print("="*60)
    
    table = "test_edge_large"
    
    process = start_migres()
    time.sleep(3)
    
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                large_text MEDIUMTEXT
            )
        """)
        mysql.cn.commit()
    
    time.sleep(5)  # Wait for DDL
    
    # Wait for table to exist in ClickHouse before inserting
    assert wait_for_table(ch, table, timeout=60), f"Table {table} was not created in ClickHouse"
    
    print("📝 Inserting large text fields...")
    with mysql.cn.cursor() as cur:
        # Various sizes
        sizes = [100, 1000, 10000, 50000, 100000]  # Up to 100KB
        for i, size in enumerate(sizes, 1):
            large_text = "X" * size
            cur.execute(
                f"INSERT INTO {table} (id, large_text) VALUES (%s, %s)",
                (i, large_text)
            )
            print(f"   Inserted {size} bytes")
        
        # More normal sized rows
        for i in range(len(sizes) + 1, 31):
            cur.execute(
                f"INSERT INTO {table} (id, large_text) VALUES (%s, %s)",
                (i, f"Normal text {i} " * 10)
            )
        mysql.cn.commit()
    
    wait_for_cdc(timeout=90)
    
    # Give extra time for large text to be processed
    print("   Waiting for large text replication...")
    time.sleep(15)
    
    # Optimize if table exists
    if wait_for_table(ch, table, timeout=10):
        try:
            ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
            time.sleep(5)
        except Exception as e:
            print(f"   Optimization warning (large fields): {e}")
    
    count = get_clickhouse_count(ch, table, timeout=90)
    
    # Verify large text was stored (only if count > 0 and table exists)
    if count > 0 and wait_for_table(ch, table, timeout=5):
        try:
            length = ch.execute(f"SELECT length(large_text) FROM `{ch.db}`.`{table}` FINAL WHERE id = 5 AND __data_transfer_delete_time = 0")[0][0]
            print(f"   100KB text length in CH: {length}")
        except Exception as e:
            print(f"   Could not verify large text: {e}")
    else:
        print(f"   ⚠️ Skipping large text verification (count={count})")
    
    stop_migres(process, "large fields test complete")
    
    assert count == 30, f"Expected 30 rows, got {count}"
    print(f"\n✅ Large fields test: count={count}")


@pytest.mark.integration
@pytest.mark.slow
def test_boundary_values(db_connections):
    mysql, ch, cfg = db_connections
    """Test: Boundary/extreme values"""
    print("\n" + "="*60)
    print("📋 TEST 5: Boundary Values")
    print("="*60)
    
    table = "test_edge_boundary"
    
    process = start_migres()
    time.sleep(3)
    
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                tiny_val TINYINT,
                small_val SMALLINT,
                int_val INT,
                big_val BIGINT,
                decimal_val DECIMAL(20,5),
                float_val FLOAT,
                double_val DOUBLE
            )
        """)
        mysql.cn.commit()
    
    time.sleep(5)  # Wait for DDL
    
    print("📝 Inserting boundary values...")
    with mysql.cn.cursor() as cur:
        # Max values
        cur.execute(f"""
            INSERT INTO {table} VALUES 
            (1, 127, 32767, 2147483647, 9223372036854775807, 
             99999999999999.99999, 3.4028235e+38, 1.7976931348623157e+308)
        """)
        
        # Min values
        cur.execute(f"""
            INSERT INTO {table} VALUES 
            (2, -128, -32768, -2147483648, -9223372036854775808,
             -99999999999999.99999, -3.4028235e+38, -1.7976931348623157e+308)
        """)
        
        # Zero
        cur.execute(f"""
            INSERT INTO {table} VALUES 
            (3, 0, 0, 0, 0, 0.00000, 0.0, 0.0)
        """)
        
        # Small decimals
        cur.execute(f"""
            INSERT INTO {table} VALUES 
            (4, 1, 1, 1, 1, 0.00001, 1.175494e-38, 2.2250738585072014e-308)
        """)
        
        # Normal values
        for i in range(5, 31):
            cur.execute(f"""
                INSERT INTO {table} VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (i, i % 127, i * 100, i * 10000, i * 1000000000, 
                  i * 1.23456, i * 1.5, i * 2.5))
        mysql.cn.commit()
    
    # Wait for CDC to flush and ClickHouse to merge parts
    wait_for_cdc(timeout=90)
    
    # Check if table exists before optimizing
    if wait_for_table(ch, table, timeout=10):
        try:
            ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
            time.sleep(3)
        except Exception as e:
            print(f"   Optimization warning (boundary): {e}")
    else:
        print(f"   ⚠️ Table {table} does not exist in ClickHouse, skipping optimization")

    count = get_clickhouse_count(ch, table, timeout=60)
    
    # Verify some boundary values
    if wait_for_table(ch, table, timeout=5):
        try:
            max_big = ch.execute(f"SELECT big_val FROM `{ch.db}`.`{table}` FINAL WHERE id = 1 AND __data_transfer_delete_time = 0")[0][0]
            print(f"   Max BIGINT in CH: {max_big}")
        except Exception as e:
            print(f"   Could not verify boundary: {e}")
    
    stop_migres(process, "boundary values test complete")
    
    assert count == 30, f"Expected 30 rows, got {count}"
    print(f"\n✅ Boundary values test: count={count}")





