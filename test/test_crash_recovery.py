#!/usr/bin/env python3
"""
Crash Recovery Scenarios Test - Tests various crash scenarios.
Verifies the pipeline recovers correctly from:
1. Crash during INSERT batch
2. Crash during UPDATE batch
3. Crash during DELETE batch
4. Multiple sequential crashes
5. Crash with pending buffer data
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


def start_migres():
    """Start migres process and verify it started correctly."""
    print("▶️ Starting migres process...")
    python_exe = sys.executable
    cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Capture stdout/stderr so we can surface CDC pipeline failures
    process = subprocess.Popen(
        [python_exe, "migres.py", "--config", "config.yml"],
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    time.sleep(5)  # Let it initialize

    # If the process exited early, show logs and fail fast
    if process.poll() is not None:
        output = process.stdout.read().decode() if process.stdout else ""
        print(f"   ⚠️ Migres process exited with code {process.returncode}")
        print(f"   Output: {output[:2000]}")
        raise RuntimeError("Migres failed to start")

    print("   ✅ Migres process running")
    return process


def stop_migres(process, reason="Test crash"):
    """Stop migres process"""
    print(f"🛑 Stopping migres ({reason})...")
    notify_cdc_shutdown(f"Crash Recovery Test: {reason}")
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
    # Close stdout pipe to prevent ResourceWarning
    if process.stdout:
        process.stdout.close()
    print("✅ Process stopped")


def create_test_table(mysql, table_name):
    """Create a test table"""
    print(f"📋 Creating table {table_name}...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(f"""
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY,
                data VARCHAR(100),
                counter INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        mysql.cn.commit()


def insert_records(mysql, table_name, start_id, count):
    """Insert records into test table"""
    print(f"📝 Inserting {count} records (IDs {start_id}-{start_id+count-1})...")
    with mysql.cn.cursor() as cur:
        for i in range(start_id, start_id + count):
            cur.execute(
                f"INSERT INTO {table_name} (id, data) VALUES (%s, %s)",
                (i, f"data_{i}")
            )
            if i % 100 == 0:
                mysql.cn.commit()
        mysql.cn.commit()
    print(f"✅ Inserted {count} records")


def update_records(mysql, table_name, start_id, count):
    """Update records in test table"""
    print(f"🔄 Updating {count} records...")
    with mysql.cn.cursor() as cur:
        for i in range(start_id, start_id + count):
            cur.execute(
                f"UPDATE {table_name} SET data = %s, counter = counter + 1 WHERE id = %s",
                (f"updated_data_{i}", i)
            )
            if i % 100 == 0:
                mysql.cn.commit()
        mysql.cn.commit()
    print(f"✅ Updated {count} records")


def delete_records(mysql, table_name, start_id, count):
    """Delete records from test table"""
    print(f"🗑️ Deleting {count} records...")
    with mysql.cn.cursor() as cur:
        for i in range(start_id, start_id + count):
            cur.execute(f"DELETE FROM {table_name} WHERE id = %s", (i,))
            if i % 100 == 0:
                mysql.cn.commit()
        mysql.cn.commit()
    print(f"✅ Deleted {count} records")


def get_mysql_count(mysql, table_name):
    """Get row count from MySQL"""
    with mysql.cn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cur.fetchone()[0]


def wait_for_table(ch, table_name, timeout=60):
    """Wait for table to exist in ClickHouse"""
    from conftest import wait_for_table_in_clickhouse
    return wait_for_table_in_clickhouse(ch, table_name, timeout)


def get_clickhouse_count(ch, table_name, timeout=60):
    """Get row count from ClickHouse with retry.
    
    This function explicitly waits for the ClickHouse table to exist to avoid
    noisy 'Unknown table' errors while the DDL is still being processed.
    """
    start = time.time()
    last_count = -1
    stable_count = 0
    table_ready = False

    while time.time() - start < timeout:
        try:
            # First, check if table exists (cheap and avoids error spam)
            if not table_ready:
                exists = ch.execute(f"EXISTS TABLE `{ch.db}`.`{table_name}`")
                if exists and exists[0][0] == 1:
                    table_ready = True
                else:
                    print(f"   Waiting for ClickHouse table {table_name} to be created...")
                    time.sleep(2)
                    continue

            # Once table exists, query count of non-deleted rows
            res = ch.execute(
                f"SELECT count() FROM `{ch.db}`.`{table_name}` FINAL WHERE __data_transfer_delete_time = 0"
            )
            count = res[0][0] if res and res[0] else 0

            # If the count has stabilized (2 consecutive same counts), assume replication has finished
            if count == last_count and table_ready:
                stable_count += 1
                if stable_count >= 2:
                    print(f"   ClickHouse count stabilized at: {count}")
                    return count
            else:
                stable_count = 0

            last_count = count
            print(f"   ClickHouse count: {count}")
        except Exception as e:
            print(f"   Waiting for table... ({e})")

        time.sleep(2)

    # On timeout, return the last observed count (may be -1 if table never appeared)
    return last_count


def check_buffer_empty(timeout=30):
    """Wait for buffer to be empty"""
    buf = BufferDB()
    start = time.time()
    
    while time.time() - start < timeout:
        stats = buf.get_queue_stats()
        if stats['raw_events'] == 0 and stats['prepared_queries'] == 0:
            return True
        time.sleep(1)
    return False


def wait_for_mysql_connection_release():
    """Wait for MySQL to release old connection"""
    print("⏳ Waiting for MySQL connection release...")
    time.sleep(10)


# =============================================================================
# TEST SCENARIOS
# =============================================================================

@pytest.mark.integration
@pytest.mark.crash
@pytest.mark.slow
def test_crash_during_insert(db_connections):
    """Test: Crash during INSERT batch"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 1: Crash During INSERT")
    print("="*60)
    
    table = "test_crash_insert"
    
    # Start migres first to capture DDL
    process = start_migres()
    time.sleep(3)
    
    try:
        create_test_table(mysql, table)
        time.sleep(5)  # Wait for DDL
        
        # Insert batch 1
        insert_records(mysql, table, 1, 200)
        
        # Wait for events to be committed to buffer (batch_delay_seconds = 15s, but we need events in buffer)
        # Events are committed to buffer in batches, so wait a bit for them to be written
        print("⏳ Waiting for events to be committed to buffer...")
        time.sleep(5)  # Wait for producer to commit events to buffer
        
        # Verify events are in buffer before crashing
        buf = BufferDB()
        stats = buf.get_queue_stats()
        print(f"📊 Buffer before crash: {stats}")
        
        # Crash mid-way
        stop_migres(process, "crash during insert")
        
        # Insert batch 2 while down
        insert_records(mysql, table, 201, 200)
        
        # Restart
        wait_for_mysql_connection_release()
        process = start_migres()
        
        # Wait for all events to be processed
        print("⏳ Waiting for all events to be processed...")
        time.sleep(20)  # Wait for batch processing (batch_delay_seconds = 15s)
        
        # Wait for buffer to be empty
        check_buffer_empty(timeout=60)
        
        # Optimize table to ensure all data is merged
        if wait_for_table(ch, table, timeout=10):
            try:
                ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
                time.sleep(5)
            except Exception as e:
                print(f"   Optimization warning: {e}")
        
        # Verify
        mysql_count = get_mysql_count(mysql, table)
        ch_count = get_clickhouse_count(ch, table, timeout=90)
        
        assert mysql_count == 400, f"Expected MySQL count=400, got {mysql_count}"
        assert ch_count == 400, f"Expected ClickHouse count=400, got {ch_count}"
        assert mysql_count == ch_count, f"Count mismatch: MySQL={mysql_count}, CH={ch_count}"
        
    finally:
        if process:
            stop_migres(process, "test complete")
        # Cleanup
        with mysql.cn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            mysql.cn.commit()


@pytest.mark.integration
@pytest.mark.crash
@pytest.mark.slow
def test_crash_during_update(db_connections):
    """Test: Crash during UPDATE batch"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 2: Crash During UPDATE")
    print("="*60)
    
    table = "test_crash_update"
    
    # Start migres first to capture DDL
    process = start_migres()
    time.sleep(3)
    
    try:
        create_test_table(mysql, table)
        time.sleep(5)  # Wait for DDL
        
        # Insert initial data
        insert_records(mysql, table, 1, 300)
        time.sleep(5)  # Wait for initial inserts to be committed
        
        # Start updates
        update_records(mysql, table, 1, 100)
        
        # Wait for update events to be committed to buffer before crashing
        print("⏳ Waiting for update events to be committed to buffer...")
        time.sleep(5)
        
        # Crash
        stop_migres(process, "crash during update")
        
        # More updates while down
        update_records(mysql, table, 101, 100)
        
        # Restart
        wait_for_mysql_connection_release()
        process = start_migres()
        
        # Wait for CDC to process all updates (including those inserted while down)
        print("⏳ Waiting for CDC to process updates...")
        time.sleep(20)  # Wait for batch processing (batch_delay_seconds = 15s)
        
        # Wait for buffer to be empty
        check_buffer_empty(timeout=60)
        
        # Optimize table to ensure all updates are merged
        if wait_for_table(ch, table, timeout=10):
            try:
                ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
                time.sleep(5)
            except Exception as e:
                print(f"   Optimization warning: {e}")
        
        # Verify count (should still be 300)
        mysql_count = get_mysql_count(mysql, table)
        ch_count = get_clickhouse_count(ch, table, timeout=90)
        
        # Verify data was updated
        with mysql.cn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE data LIKE 'updated_%'")
            updated_count = cur.fetchone()[0]
        
        assert mysql_count == 300, f"Expected MySQL count=300, got {mysql_count}"
        assert ch_count == 300, f"Expected ClickHouse count=300, got {ch_count}"
        assert updated_count == 200, f"Expected 200 updated rows, got {updated_count}"
        
    finally:
        if process:
            stop_migres(process, "test complete")
        # Cleanup
        with mysql.cn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            mysql.cn.commit()


@pytest.mark.integration
@pytest.mark.crash
@pytest.mark.slow
def test_crash_during_delete(db_connections):
    """Test: Crash during DELETE batch"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 3: Crash During DELETE")
    print("="*60)
    
    table = "test_crash_delete"
    
    # Start migres first to capture DDL
    process = start_migres()
    time.sleep(3)
    
    try:
        create_test_table(mysql, table)
        time.sleep(5)  # Wait for DDL
        insert_records(mysql, table, 1, 400)
        time.sleep(5)  # Wait for inserts to be committed
        
        # Start deletes
        delete_records(mysql, table, 1, 100)
        
        # Wait for delete events to be committed to buffer before crashing
        print("⏳ Waiting for delete events to be committed to buffer...")
        time.sleep(5)
        
        # Crash
        stop_migres(process, "crash during delete")
        
        # More deletes while down
        delete_records(mysql, table, 101, 100)
        
        # Restart
        wait_for_mysql_connection_release()
        process = start_migres()
        
        # Wait for DELETE events to be processed (batch_delay_seconds = 15s)
        print("⏳ Waiting for DELETE events to be processed...")
        time.sleep(20)  # Wait for batch processing
        
        # Wait for buffer to be empty
        check_buffer_empty(timeout=60)
        
        # Optimize table to ensure deleted rows are merged
        if wait_for_table(ch, table, timeout=10):
            try:
                ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
                time.sleep(5)
            except Exception as e:
                print(f"   Optimization warning: {e}")
        
        # Verify count (should be 200)
        mysql_count = get_mysql_count(mysql, table)
        ch_count = get_clickhouse_count(ch, table, timeout=90)
        
        assert mysql_count == 200, f"Expected MySQL count=200, got {mysql_count}"
        assert ch_count == 200, f"Expected ClickHouse count=200, got {ch_count}"
        assert mysql_count == ch_count, f"Count mismatch: MySQL={mysql_count}, CH={ch_count}"
        
    finally:
        if process:
            stop_migres(process, "test complete")
        # Cleanup
        with mysql.cn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            mysql.cn.commit()


@pytest.mark.integration
@pytest.mark.crash
@pytest.mark.slow
def test_multiple_crashes(db_connections):
    """Test: Multiple sequential crashes"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 4: Multiple Sequential Crashes")
    print("="*60)
    
    table = "test_crash_multi"
    
    # Start migres first to capture DDL
    process = start_migres()
    time.sleep(3)
    
    try:
        create_test_table(mysql, table)
        time.sleep(5)  # Wait for DDL
        
        stop_migres(process, "initial setup")
        
        total_inserted = 0
        
        for i in range(3):
            print(f"\n--- Crash cycle {i+1}/3 ---")
            
            wait_for_mysql_connection_release()
            
            # Start migres
            process = start_migres()
            
            # Insert batch
            start_id = total_inserted + 1
            insert_records(mysql, table, start_id, 100)
            total_inserted += 100
            
            # Wait for events to be flushed to buffer before crashing
            # Need to wait for batch_delay_seconds to ensure events are committed
            from conftest import get_batch_delay_seconds
            batch_delay = get_batch_delay_seconds(cfg)
            wait_time = max(batch_delay + 5, 20)
            print(f"⏳ Waiting {wait_time}s for events to be flushed to buffer before crash...")
            time.sleep(wait_time)
            
            # Crash
            stop_migres(process, f"crash {i+1}")
            
            wait_for_mysql_connection_release()
        
        # Final restart and verify
        process = start_migres()
        
        # Wait for migres to process all events after restart
        from conftest import get_batch_delay_seconds, wait_for_cdc_sync
        batch_delay = get_batch_delay_seconds(cfg)
        wait_time = max(batch_delay + 10, 30)
        print(f"⏳ Waiting {wait_time}s for migres to process events after restart...")
        time.sleep(wait_time)
        assert wait_for_cdc_sync(timeout=180), "CDC sync timeout after final restart"
        
        mysql_count = get_mysql_count(mysql, table)
        ch_count = get_clickhouse_count(ch, table)
        
        assert mysql_count == 300, f"Expected MySQL count=300, got {mysql_count}"
        assert ch_count == 300, f"Expected ClickHouse count=300, got {ch_count}"
        assert mysql_count == ch_count, f"Count mismatch: MySQL={mysql_count}, CH={ch_count}"
        
    finally:
        if process:
            stop_migres(process, "test complete")
        # Cleanup
        with mysql.cn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            mysql.cn.commit()


@pytest.mark.integration
@pytest.mark.crash
@pytest.mark.slow
def test_crash_with_pending_buffer(db_connections):
    """Test: Crash with data in buffer"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 5: Crash With Pending Buffer Data")
    print("="*60)
    
    table = "test_crash_buffer"
    
    # Start migres first to capture DDL
    process = start_migres()
    time.sleep(3)
    
    try:
        create_test_table(mysql, table)
        time.sleep(5)  # Wait for DDL
        
        # Insert large batch quickly (some will be in buffer)
        insert_records(mysql, table, 1, 500)
        
        # Immediately crash (buffer should have pending data)
        time.sleep(0.5)
        stop_migres(process, "crash with pending buffer")
        
        # Check buffer state
        buf = BufferDB()
        stats = buf.get_queue_stats()
        print(f"📊 Buffer after crash: {stats}")
        
        # Insert more while down
        insert_records(mysql, table, 501, 200)
        
        # Restart
        wait_for_mysql_connection_release()
        process = start_migres()
        
        # Wait for full sync - buffer needs time to process all pending events
        print("⏳ Waiting for buffer to process all pending events...")
        time.sleep(20)  # Wait for batch processing (batch_delay_seconds = 15s)
        
        # Wait for buffer to be empty
        if not check_buffer_empty(timeout=90):
            print("   ⚠️ Buffer still has pending events, but continuing...")
        
        # Optimize table to ensure all data is merged
        if wait_for_table(ch, table, timeout=10):
            try:
                ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
                time.sleep(5)
            except Exception as e:
                print(f"   Optimization warning: {e}")
        
        mysql_count = get_mysql_count(mysql, table)
        ch_count = get_clickhouse_count(ch, table, timeout=90)
        
        assert mysql_count == 700, f"Expected MySQL count=700, got {mysql_count}"
        assert ch_count == 700, f"Expected ClickHouse count=700, got {ch_count}"
        assert mysql_count == ch_count, f"Count mismatch: MySQL={mysql_count}, CH={ch_count}"
        
    finally:
        if process:
            stop_migres(process, "test complete")
        # Cleanup
        with mysql.cn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            mysql.cn.commit()


