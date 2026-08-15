#!/usr/bin/env python3
"""
Checkpoint Corruption Test - Tests recovery from corrupted checkpoints.
Verifies:
1. System recovers from corrupted state.json
2. System recovers from corrupted buffer checkpoint
3. No data loss after recovery
"""

import time
import sys
import os
import json
import subprocess
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from migres.buffer import BufferDB
from conftest import wait_for_cdc_sync, wait_for_table_in_clickhouse, get_batch_delay_seconds, optimize_clickhouse_table
from migres.notifications import notify_cdc_shutdown


def start_migres():
    """Start migres process"""
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
    notify_cdc_shutdown(f"Checkpoint Corruption Test: {reason}")
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
    if process.stdout:
        process.stdout.close()
    print("✅ Process stopped")


@pytest.mark.integration
@pytest.mark.slow
def test_corrupted_state_recovery(db_connections):
    """Test: System recovers from corrupted state.json"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST: Corrupted State Recovery")
    print("="*60)
    
    table = "test_checkpoint_corrupt"
    batch_delay = get_batch_delay_seconds(cfg)
    state_file = cfg.get("state_file", "state.json")
    
    # Create table
    print(f"📋 Creating table {table}...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                data VARCHAR(100)
            )
        """)
        mysql.cn.commit()
    
    # Start migres
    process = start_migres()
    time.sleep(3)
    
    try:
        # Wait for table to be created (migres will create it when processing first data event)
        # But first, wait a bit for migres to initialize
        time.sleep(5)
        
        # Insert initial data - migres should create table when processing this
        print("📝 Inserting initial data...")
        with mysql.cn.cursor() as cur:
            for i in range(1, 101):
                cur.execute(
                    f"INSERT INTO {table} (id, data) VALUES (%s, %s)",
                    (i, f"data_{i}")
                )
            mysql.cn.commit()
        
        # Wait for table to be created and initial processing
        wait_time = max(batch_delay * 3, 30)
        time.sleep(wait_time)
        
        # Now wait for table to exist (migres creates it when processing first event)
        assert wait_for_table_in_clickhouse(ch, table, timeout=90), f"Table {table} was not created"
        
        # Corrupt state file
        print("⚠️ Corrupting state file...")
        if os.path.exists(state_file):
            with open(state_file, 'w') as f:
                f.write("INVALID JSON {{{{")
        
        # Stop and restart
        stop_migres(process, "corruption test")
        time.sleep(5)
        
        process = start_migres()
        time.sleep(3)
        
        # Insert more data
        print("📝 Inserting more data after restart...")
        with mysql.cn.cursor() as cur:
            for i in range(101, 201):
                cur.execute(
                    f"INSERT INTO {table} (id, data) VALUES (%s, %s)",
                    (i, f"data_{i}")
                )
            mysql.cn.commit()
        
        # Wait for processing after restart
        wait_time_after_restart = max(batch_delay * 4, 45)
        time.sleep(wait_time_after_restart)
        assert wait_for_cdc_sync(timeout=240), "CDC sync timeout"
        
        optimize_clickhouse_table(ch, table, wait_after=5)
        
        # Verify all data
        mysql_count = None
        with mysql.cn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            mysql_count = cur.fetchone()[0]
        
        from conftest import get_clickhouse_count_reliable
        ch_count = get_clickhouse_count_reliable(ch, table, timeout=90)
        
        assert mysql_count == 200, f"Expected MySQL count=200, got {mysql_count}"
        assert ch_count == 200, f"Expected ClickHouse count=200, got {ch_count}"
        
        print(f"✅ Corrupted state recovery test passed: {ch_count} rows replicated")
        
    finally:
        stop_migres(process, "test complete")
        
        # Cleanup
        with mysql.cn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            mysql.cn.commit()

