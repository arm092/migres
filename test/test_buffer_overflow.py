#!/usr/bin/env python3
"""
Buffer Overflow Test - Tests behavior when buffer database fills up.
Verifies:
1. System handles buffer queue full scenarios gracefully
2. No data loss when buffer is under pressure
3. System recovers when buffer space becomes available
"""

import time
import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from migres.buffer import BufferDB
from conftest import wait_for_cdc_sync, wait_for_table_in_clickhouse, get_batch_delay_seconds, optimize_clickhouse_table, get_clickhouse_count_reliable


@pytest.mark.integration
@pytest.mark.slow
def test_rapid_inserts_buffer_pressure(db_connections, migres_process):
    """Test: Rapid inserts create buffer pressure but no data loss"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST: Rapid Inserts Buffer Pressure")
    print("="*60)
    
    table = "test_buffer_pressure"
    batch_delay = get_batch_delay_seconds(cfg)
    
    # Create table
    print(f"📋 Creating table {table}...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                data VARCHAR(100),
                batch_num INT
            )
        """)
        mysql.cn.commit()
    
    # Wait for DDL event to be processed
    time.sleep(max(batch_delay + 5, 20))
    
    # Wait for table in ClickHouse
    assert wait_for_table_in_clickhouse(ch, table, timeout=90), f"Table {table} was not created"
    
    # Insert large batch rapidly (more than can be processed immediately)
    print("📝 Inserting 2000 records rapidly...")
    with mysql.cn.cursor() as cur:
        for i in range(1, 2001):
            cur.execute(
                f"INSERT INTO {table} (id, data, batch_num) VALUES (%s, %s, %s)",
                (i, f"data_{i}", 1)
            )
            if i % 500 == 0:
                mysql.cn.commit()
                print(f"   Committed {i}/2000...")
        mysql.cn.commit()
    
    # Check buffer stats immediately
    buf = BufferDB()
    stats = buf.get_queue_stats()
    print(f"📊 Buffer stats after rapid inserts: {stats}")
    
    # Wait for batch delay + processing time - need more time for large batch
    wait_time = max(batch_delay * 4, 45)  # Increased multiplier for large batch
    print(f"⏳ Waiting {wait_time}s for batch processing...")
    time.sleep(wait_time)
    
    # Wait for CDC sync - increased timeout for large batch
    assert wait_for_cdc_sync(timeout=240), "CDC sync timeout"
    
    # Optimize table
    optimize_clickhouse_table(ch, table, wait_after=5)
    
    # Verify all data arrived
    mysql_count = None
    with mysql.cn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        mysql_count = cur.fetchone()[0]
    
    # Use reliable count method
    from conftest import get_clickhouse_count_reliable
    ch_count = get_clickhouse_count_reliable(ch, table, timeout=60)
    
    assert mysql_count == 2000, f"Expected MySQL count=2000, got {mysql_count}"
    assert ch_count == 2000, f"Expected ClickHouse count=2000, got {ch_count}"
    assert mysql_count == ch_count, f"Count mismatch: MySQL={mysql_count}, CH={ch_count}"
    
    print(f"✅ Buffer pressure test passed: {ch_count} rows replicated")
    
    # Cleanup
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mysql.cn.commit()


@pytest.mark.integration
@pytest.mark.slow
def test_sustained_load_buffer_handling(db_connections, migres_process):
    """Test: Sustained load over time maintains buffer stability"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST: Sustained Load Buffer Handling")
    print("="*60)
    
    table = "test_buffer_sustained"
    batch_delay = get_batch_delay_seconds(cfg)
    
    # Create table
    print(f"📋 Creating table {table}...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                data VARCHAR(100),
                wave INT
            )
        """)
        mysql.cn.commit()
    
    assert wait_for_table_in_clickhouse(ch, table, timeout=60), f"Table {table} was not created"
    
    # Insert in waves to create sustained load
    total_inserted = 0
    for wave in range(5):
        print(f"📝 Wave {wave+1}/5: Inserting 500 records...")
        with mysql.cn.cursor() as cur:
            for i in range(wave * 500 + 1, (wave + 1) * 500 + 1):
                cur.execute(
                    f"INSERT INTO {table} (id, data, wave) VALUES (%s, %s, %s)",
                    (i, f"data_{i}", wave + 1)
                )
            mysql.cn.commit()
        total_inserted += 500
        
        # Check buffer stats after each wave
        buf = BufferDB()
        stats = buf.get_queue_stats()
        print(f"   Buffer: raw={stats['raw_events']}, prepared={stats['prepared_queries']}")
        
        # Small delay between waves
        time.sleep(2)
    
    # Wait for all waves to be processed
    wait_time = max(batch_delay * 3, 30)
    print(f"⏳ Waiting {wait_time}s for all waves to be processed...")
    time.sleep(wait_time)
    
    assert wait_for_cdc_sync(timeout=180), "CDC sync timeout"
    
    optimize_clickhouse_table(ch, table, wait_after=5)
    
    # Verify all data
    mysql_count = None
    with mysql.cn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        mysql_count = cur.fetchone()[0]
    
    ch_count = get_clickhouse_count_reliable(ch, table, timeout=90)
    
    assert mysql_count == total_inserted, f"Expected MySQL count={total_inserted}, got {mysql_count}"
    assert ch_count == total_inserted, f"Expected ClickHouse count={total_inserted}, got {ch_count}"
    
    print(f"✅ Sustained load test passed: {ch_count} rows replicated")
    
    # Cleanup
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mysql.cn.commit()

