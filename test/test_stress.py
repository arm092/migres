#!/usr/bin/env python3
"""
High Volume Stress Test - Tests the pipeline under heavy load.
Verifies:
1. 100K+ operations can be processed
2. No data loss under high throughput
3. System remains stable under load
"""

import time
import sys
import os
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
import pytest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from migres.buffer import BufferDB
from migres.notifications import notify_cdc_shutdown


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
    notify_cdc_shutdown(f"Stress Test: {reason}")
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


def create_test_table(mysql, table_name):
    """Create test table"""
    print(f"📋 Creating table {table_name}...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(f"""
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY,
                data VARCHAR(255),
                counter INT DEFAULT 0,
                category VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        mysql.cn.commit()


def bulk_insert(mysql, table_name, start_id, count, batch_size=1000):
    """Bulk insert records with progress"""
    print(f"📝 Bulk inserting {count} records...")
    start_time = time.time()
    
    with mysql.cn.cursor() as cur:
        for i in range(start_id, start_id + count):
            cur.execute(
                f"INSERT INTO {table_name} (id, data, category) VALUES (%s, %s, %s)",
                (i, f"stress_data_{i}_" + "x" * 50, f"cat_{i % 10}")
            )
            if (i - start_id + 1) % batch_size == 0:
                mysql.cn.commit()
                progress = (i - start_id + 1)
                elapsed = time.time() - start_time
                rate = progress / elapsed
                print(f"   Inserted {progress}/{count} ({rate:.0f} rows/sec)")
        mysql.cn.commit()
    
    elapsed = time.time() - start_time
    print(f"✅ Inserted {count} records in {elapsed:.1f}s ({count/elapsed:.0f} rows/sec)")
    return count


def bulk_update(mysql, table_name, start_id, count, batch_size=1000):
    """Bulk update records"""
    print(f"🔄 Bulk updating {count} records...")
    start_time = time.time()
    
    with mysql.cn.cursor() as cur:
        for i in range(start_id, start_id + count):
            cur.execute(
                f"UPDATE {table_name} SET data = %s, counter = counter + 1 WHERE id = %s",
                (f"updated_stress_{i}_" + "y" * 50, i)
            )
            if (i - start_id + 1) % batch_size == 0:
                mysql.cn.commit()
        mysql.cn.commit()
    
    elapsed = time.time() - start_time
    print(f"✅ Updated {count} records in {elapsed:.1f}s ({count/elapsed:.0f} rows/sec)")
    return count


def bulk_delete(mysql, table_name, start_id, count, batch_size=1000):
    """Bulk delete records"""
    print(f"🗑️ Bulk deleting {count} records...")
    start_time = time.time()
    
    with mysql.cn.cursor() as cur:
        for i in range(start_id, start_id + count):
            cur.execute(f"DELETE FROM {table_name} WHERE id = %s", (i,))
            if (i - start_id + 1) % batch_size == 0:
                mysql.cn.commit()
        mysql.cn.commit()
    
    elapsed = time.time() - start_time
    print(f"✅ Deleted {count} records in {elapsed:.1f}s ({count/elapsed:.0f} rows/sec)")
    return count


def wait_for_sync(ch, table_name, expected, timeout=300):
    """Wait for ClickHouse to sync"""
    print(f"⏳ Waiting for sync (expecting {expected} rows, timeout {timeout}s)...")
    start = time.time()
    last_count = -1
    stable_count = 0
    
    while time.time() - start < timeout:
        try:
            count = ch.execute(
                f"SELECT count() FROM `{ch.db}`.`{table_name}` FINAL WHERE __data_transfer_delete_time = 0"
            )[0][0]
            
            elapsed = time.time() - start
            print(f"   ClickHouse: {count} (elapsed: {elapsed:.0f}s)")
            
            if count == expected:
                return True
            
            # Check if count is stable (for detecting stalls)
            if count == last_count:
                stable_count += 1
                if stable_count > 10:  # Stable for 30s
                    print(f"⚠️ Count stable at {count}, might be stalled")
            else:
                stable_count = 0
            last_count = count
            
        except Exception as e:
            print(f"   Error: {e}")
        
        time.sleep(3)
    
    return False


def check_buffer_status():
    """Check buffer queue status"""
    buf = BufferDB()
    stats = buf.get_queue_stats()
    print(f"📊 Buffer: raw={stats['raw_events']}, prepared={stats['prepared_queries']}")
    return stats


# =============================================================================
# TEST SCENARIOS
# =============================================================================

@pytest.mark.integration
@pytest.mark.stress
@pytest.mark.slow
def test_100k_inserts(db_connections):
    mysql, ch, cfg = db_connections
    """Test: 100,000 INSERT operations"""
    print("\n" + "="*60)
    print("📋 TEST 1: 100K INSERT Operations")
    print("="*60)
    
    table = "test_stress_100k"
    
    process = start_migres()
    time.sleep(3)
    
    create_test_table(mysql, table)
    time.sleep(5)  # Wait for DDL
    
    # Insert 100K records
    count = bulk_insert(mysql, table, 1, 100000)
    
    # Wait for sync
    assert wait_for_sync(ch, table, count, timeout=300), "Sync timeout"
    
    check_buffer_status()
    stop_migres(process, "100K insert test complete")
    
    print(f"\n✅ 100K INSERT test: PASSED")


@pytest.mark.integration
@pytest.mark.stress
@pytest.mark.slow
def test_mixed_operations(db_connections):
    mysql, ch, cfg = db_connections
    """Test: Mixed INSERT/UPDATE/DELETE operations"""
    print("\n" + "="*60)
    print("📋 TEST 2: Mixed Operations (50K each)")
    print("="*60)
    
    table = "test_stress_mixed"
    
    process = start_migres()
    time.sleep(3)
    
    create_test_table(mysql, table)
    time.sleep(5)  # Wait for DDL
    
    # Insert 50K
    bulk_insert(mysql, table, 1, 50000)
    
    # Update 25K
    bulk_update(mysql, table, 1, 25000)
    
    # Delete 10K
    bulk_delete(mysql, table, 1, 10000)
    
    # Final count should be 40K
    expected = 40000
    assert wait_for_sync(ch, table, expected, timeout=300), f"Expected {expected} rows, sync timeout"
    
    check_buffer_status()
    stop_migres(process, "mixed ops test complete")
    
    print(f"\n✅ Mixed operations test: PASSED")


@pytest.mark.integration
@pytest.mark.stress
@pytest.mark.slow
def test_burst_load(db_connections):
    mysql, ch, cfg = db_connections
    """Test: Burst load (rapid inserts in waves)"""
    print("\n" + "="*60)
    print("📋 TEST 3: Burst Load (10 waves of 5K)")
    print("="*60)
    
    table = "test_stress_burst"
    
    process = start_migres()
    time.sleep(3)
    
    create_test_table(mysql, table)
    time.sleep(5)  # Wait for DDL
    time.sleep(3)
    
    total = 0
    for wave in range(10):
        print(f"\n--- Wave {wave+1}/10 ---")
        start_id = wave * 5000 + 1
        bulk_insert(mysql, table, start_id, 5000)
        total += 5000
        time.sleep(1)  # Brief pause between waves
    
    assert wait_for_sync(ch, table, total, timeout=300), f"Expected {total} rows, sync timeout"
    
    check_buffer_status()
    stop_migres(process, "burst load test complete")
    
    print(f"\n✅ Burst load test: PASSED")


@pytest.mark.integration
@pytest.mark.stress
@pytest.mark.slow
def test_sustained_load(db_connections):
    mysql, ch, cfg = db_connections
    """Test: Sustained load over time"""
    print("\n" + "="*60)
    print("📋 TEST 4: Sustained Load (30 batches over 60s)")
    print("="*60)
    
    table = "test_stress_sustained"
    
    process = start_migres()
    time.sleep(3)
    
    create_test_table(mysql, table)
    time.sleep(5)  # Wait for DDL
    
    total = 0
    start_time = time.time()
    
    for batch in range(30):
        start_id = batch * 1000 + 1
        with mysql.cn.cursor() as cur:
            for i in range(start_id, start_id + 1000):
                cur.execute(
                    f"INSERT INTO {table} (id, data, category) VALUES (%s, %s, %s)",
                    (i, f"sustained_{i}", f"cat_{i % 5}")
                )
            mysql.cn.commit()
        total += 1000
        
        elapsed = time.time() - start_time
        print(f"   Batch {batch+1}/30: {total} total, {elapsed:.1f}s elapsed")
        time.sleep(2)  # 2 second gap between batches
    
    print(f"📝 Inserted {total} records over {time.time() - start_time:.0f}s")
    
    assert wait_for_sync(ch, table, total, timeout=180), f"Expected {total} rows, sync timeout"
    
    check_buffer_status()
    stop_migres(process, "sustained load test complete")
    
    print(f"\n✅ Sustained load test: PASSED")





