#!/usr/bin/env python3
"""
Network Failure Test - Tests connection drop and recovery scenarios.
Since we can't actually drop network connections in a test, we simulate
failures by:
1. Stopping and restarting the migres process (simulates MySQL disconnect)
2. Verifying reconnection works correctly
3. Verifying data integrity after reconnection
"""

import time
import sys
import os
import subprocess
import pytest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
    
    # Check if process started successfully
    if process.poll() is not None:
        output = process.stdout.read().decode() if process.stdout else ""
        print(f"   ⚠️ Migres process exited with code {process.returncode}")
        print(f"   Output: {output[:2000]}")
        raise RuntimeError("Migres failed to start")
    
    print("   ✅ Migres process running")
    return process


def stop_migres(process, reason="Test"):
    """Stop migres process"""
    print(f"🛑 Stopping migres ({reason})...")
    notify_cdc_shutdown(f"Network Failure Test: {reason}")
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
                data VARCHAR(100),
                batch INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        mysql.cn.commit()


def insert_batch(mysql, table_name, batch_num, start_id, count):
    """Insert a batch of records"""
    print(f"📝 Inserting batch {batch_num}: {count} records...")
    with mysql.cn.cursor() as cur:
        for i in range(start_id, start_id + count):
            cur.execute(
                f"INSERT INTO {table_name} (id, data, batch) VALUES (%s, %s, %s)",
                (i, f"data_{i}", batch_num)
            )
        mysql.cn.commit()
    return count


def get_counts(mysql, ch, table_name, timeout=30):
    """Get counts from both databases"""
    # MySQL count
    with mysql.cn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        mysql_count = cur.fetchone()[0]
    
    # ClickHouse count with retry
    ch_count = -1
    start = time.time()
    while time.time() - start < timeout:
        try:
            ch_count = ch.execute(
                f"SELECT count() FROM `{ch.db}`.`{table_name}` FINAL WHERE __data_transfer_delete_time = 0"
            )[0][0]
            break
        except Exception:
            time.sleep(2)
    
    return mysql_count, ch_count


def wait_for_sync(mysql, ch, table_name, expected, timeout=60):
    """Wait for ClickHouse to sync to expected count"""
    print(f"⏳ Waiting for sync (expecting {expected} rows)...")
    start = time.time()
    
    while time.time() - start < timeout:
        mysql_count, ch_count = get_counts(mysql, ch, table_name, timeout=5)
        print(f"   MySQL: {mysql_count}, ClickHouse: {ch_count}")
        
        if ch_count == expected:
            return True
        time.sleep(3)
    
    return False


@pytest.mark.integration
@pytest.mark.network
@pytest.mark.slow
def test_reconnection_after_restart(db_connections):
    """Test: Process restarts and reconnects successfully"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 1: Reconnection After Restart")
    print("="*60)
    
    table = "test_network_reconnect"
    process = None
    
    try:
        # Start migres first, then create table
        process = start_migres()
        time.sleep(3)
        
        create_test_table(mysql, table)
        time.sleep(5)  # Wait for DDL
        
        # Insert data
        insert_batch(mysql, table, 1, 1, 100)
        time.sleep(3)
        stop_migres(process, "simulating disconnect")
        
        # Wait and restart
        time.sleep(10)
        process = start_migres()
        
        # Insert more after reconnect
        insert_batch(mysql, table, 2, 101, 100)
        
        # Verify all data arrived
        assert wait_for_sync(mysql, ch, table, 200), "Data sync failed"
        
    finally:
        if process:
            stop_migres(process, "test complete")
        # Cleanup
        with mysql.cn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            mysql.cn.commit()


@pytest.mark.integration
@pytest.mark.network
@pytest.mark.slow
def test_data_during_disconnect(db_connections):
    """Test: Data inserted during disconnect is replayed"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 2: Data During Disconnect")
    print("="*60)
    
    table = "test_network_disconnect"
    process = None
    
    try:
        # Start migres first, then create table
        process = start_migres()
        time.sleep(3)
        
        create_test_table(mysql, table)
        time.sleep(5)  # Wait for DDL
        
        # Insert initial data
        insert_batch(mysql, table, 1, 1, 100)
        time.sleep(3)
        
        # Disconnect (stop process)
        stop_migres(process, "simulating network failure")
        
        # Insert data while "disconnected"
        print("📝 Inserting data while disconnected...")
        insert_batch(mysql, table, 2, 101, 200)
        insert_batch(mysql, table, 3, 301, 200)
        
        # Wait and reconnect
        time.sleep(10)
        process = start_migres()
        
        # Verify all 500 records arrived
        assert wait_for_sync(mysql, ch, table, 500, timeout=90), "Data sync failed"
        
    finally:
        if process:
            stop_migres(process, "test complete")
        # Cleanup
        with mysql.cn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            mysql.cn.commit()


@pytest.mark.integration
@pytest.mark.network
@pytest.mark.slow
def test_rapid_reconnections(db_connections):
    """Test: Multiple rapid reconnections"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 3: Rapid Reconnections")
    print("="*60)
    
    table = "test_network_rapid"
    process = None
    
    try:
        # Start migres first to capture DDL
        process = start_migres()
        time.sleep(3)
        
        create_test_table(mysql, table)
        time.sleep(5)  # Wait for DDL
        
        stop_migres(process, "initial setup")
        
        total_records = 0
        
        for i in range(5):
            print(f"\n--- Reconnection cycle {i+1}/5 ---")
            
            # Start
            process = start_migres()
            
            # Quick insert
            start_id = total_records + 1
            insert_batch(mysql, table, i+1, start_id, 50)
            total_records += 50
            
            # Quick stop
            time.sleep(2)
            stop_migres(process, f"rapid cycle {i+1}")
            
            # Short wait between cycles
            time.sleep(5)
        
        # Final restart and verify
        process = start_migres()
        assert wait_for_sync(mysql, ch, table, total_records, timeout=90), \
            f"Expected {total_records} records, sync failed"
        
    finally:
        if process:
            stop_migres(process, "test complete")
        # Cleanup
        with mysql.cn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            mysql.cn.commit()


@pytest.mark.integration
@pytest.mark.network
@pytest.mark.slow
def test_long_disconnect(db_connections):
    """Test: Long disconnect with accumulated data"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 4: Long Disconnect")
    print("="*60)
    
    table = "test_network_long"
    process = None
    
    try:
        # Start migres first, then create table
        process = start_migres()
        time.sleep(3)
        
        create_test_table(mysql, table)
        time.sleep(5)  # Wait for DDL
        
        # Insert initial data
        insert_batch(mysql, table, 1, 1, 100)
        time.sleep(3)
        
        # Long disconnect
        stop_migres(process, "long disconnect")
        
        # Insert lots of data over "time"
        print("📝 Simulating data accumulation during long disconnect...")
        for batch in range(2, 12):  # 10 batches
            insert_batch(mysql, table, batch, (batch-1)*100 + 1, 100)
            time.sleep(0.5)
        
        # Total should be 1100 records
        
        # Wait (simulating long disconnect)
        print("⏳ Simulating long disconnect period...")
        time.sleep(15)
        
        # Reconnect
        process = start_migres()
        
        # Wait for full sync (longer timeout for more data)
        assert wait_for_sync(mysql, ch, table, 1100, timeout=120), "Data sync failed"
        
    finally:
        if process:
            stop_migres(process, "test complete")
        # Cleanup
        with mysql.cn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            mysql.cn.commit()
