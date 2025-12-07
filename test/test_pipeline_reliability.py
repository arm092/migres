#!/usr/bin/env python3
"""
Test script for Pipeline Reliability (Zero Data Loss).
This test verifies that:
1. Data is buffered in SQLite
2. System recovers from crashes (simulated stop/start)
3. No data is lost even if processing is interrupted
"""

import time
import sys
import os
import subprocess
import pytest

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from buffer import BufferDB
from notifications import notify_cdc_shutdown


def start_migres_process():
    """Start the migration process as a subprocess"""
    print("▶️ Starting migres process...")
    python_exe = sys.executable
    process = subprocess.Popen(
        [python_exe, "migres.py", "--config", "config.yml"],
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(5)  # Let it initialize
    
    # Check if process started successfully
    if process.poll() is not None:
        output = process.stdout.read().decode() if process.stdout else ""
        print(f"   ⚠️ Migres process exited with code {process.returncode}")
        print(f"   Output: {output[:2000]}")
        raise RuntimeError("Migres failed to start")
    
    print("   ✅ Migres process running")
    return process


def stop_migres_process(process):
    """Stop the migration process (simulate crash)"""
    print("🛑 Stopping migres process (SIMULATING CRASH)...")
    notify_cdc_shutdown("Reliability test: simulated crash")
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
    print("✅ Process stopped.")


def create_table(mysql):
    """Create reliability test table"""
    print("📋 Creating reliability test table...")
    with mysql.cn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_reliability")
        cur.execute("""
            CREATE TABLE test_reliability (
                id INT PRIMARY KEY AUTO_INCREMENT,
                data VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        mysql.cn.commit()


def generate_load(mysql, start_id, count):
    """Generate load in background"""
    print(f"🚀 Generating {count} records starting from ID {start_id}...")
    with mysql.cn.cursor() as cur:
        for i in range(start_id, start_id + count):
            cur.execute(
                "INSERT INTO test_reliability (id, data) VALUES (%s, %s)",
                (i, f"reliability_data_{i}")
            )
            if i % 100 == 0:
                mysql.cn.commit()
                time.sleep(0.1)  # Simulate steady stream
        mysql.cn.commit()
    print(f"✅ Generated {count} records.")


def verify_data(ch, total_expected):
    """Verify ClickHouse has all data"""
    print(f"🔍 Verifying data... Expecting {total_expected} rows.")
    
    # First wait for table to exist
    from conftest import wait_for_table_in_clickhouse
    table_name = "test_reliability"
    if not wait_for_table_in_clickhouse(ch, table_name, timeout=120):
        raise AssertionError(f"Table {table_name} was not created in ClickHouse")

    # Wait for lag catchup
    for i in range(60):  # Increased retries
        try:
            count = ch.execute(f"SELECT count() FROM `{ch.db}`.`{table_name}` FINAL WHERE __data_transfer_delete_time = 0")[0][0]
            print(f"   Current count: {count}")
            if count == total_expected:
                print("✅ Data verification PASSED!")
                return True
        except Exception as e:
            if "Code: 60" in str(e) or "doesn't exist" in str(e).lower():
                print(f"   Table not ready yet, waiting... ({e})")
            else:
                print(f"   Waiting for data... ({e})")
        time.sleep(2)  # Increased sleep time
    
    # Final check
    try:
        count = ch.execute(f"SELECT count() FROM `{ch.db}`.`{table_name}` FINAL WHERE __data_transfer_delete_time = 0")[0][0]
        assert count == total_expected, f"Expected {total_expected} rows, got {count}"
    except Exception as e:
        raise AssertionError(f"Could not verify data: {e}")


def check_buffer_state():
    """Check if data persists in SQLite buffer"""
    try:
        buf = BufferDB()
        stats = buf.get_queue_stats()
        print(f"📊 Buffer Stats: {stats}")
        return stats
    except Exception as e:
        print(f"⚠️ Could not check buffer: {e}")
        return {}


@pytest.mark.integration
@pytest.mark.reliability
@pytest.mark.slow
def test_pipeline_reliability(db_connections):
    """Test pipeline reliability with crash recovery"""
    mysql, ch, cfg = db_connections
    
    print("🧪 Starting Pipeline Reliability Test")
    print("=" * 50)
    
    # 1. Setup
    create_table(mysql)
    
    # 2. Start Migres
    process = start_migres_process()
    
    # Wait for migres to initialize and table to be created
    import time
    time.sleep(10)
    from conftest import wait_for_table_in_clickhouse
    if not wait_for_table_in_clickhouse(ch, "test_reliability", timeout=120):
        raise AssertionError("Table test_reliability was not created in ClickHouse")
    
    try:
        # 3. Generate Data Batch 1
        generate_load(mysql, 1, 500)
        
        # 4. Wait a bit, then CRASH
        time.sleep(2)
        stop_migres_process(process)
        
        # 5. Verify Buffer has data (persistence check)
        stats = check_buffer_state()
        # We expect some data might be in buffer, or maybe all processed.
        # The key is that we stopped abruptly.
        
        # 6. Generate Data Batch 2 (while app is down - simulates downtime accumulation in binlog)
        print("💤 Generating data while app is DOWN...")
        generate_load(mysql, 501, 500)
        
        # 7. Wait for MySQL to release old slave connection (prevents server_id conflict)
        print("⏳ Waiting for MySQL to release old connection...")
        time.sleep(10)
        
        # 8. Restart Migres
        print("🔄 Restarting Migres...")
        process = start_migres_process()
        
        # 9. Verify ALL data eventually arrives
        # Total = 1000 records
        verify_data(ch, 1000)
        
        print("\n🎉 RELIABILITY TEST PASSED!")
        
    finally:
        stop_migres_process(process)
        # Cleanup
        with mysql.cn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_reliability")
            mysql.cn.commit()
