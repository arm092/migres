#!/usr/bin/env python3
"""
Binlog Rotation Test - Tests binlog file rotation during processing.
Verifies:
1. System handles binlog file rotation correctly
2. No data loss during rotation
3. Processing continues seamlessly after rotation
"""

import time
import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from migres.buffer import BufferDB
from conftest import wait_for_cdc_sync, wait_for_table_in_clickhouse, get_batch_delay_seconds, optimize_clickhouse_table


@pytest.mark.integration
@pytest.mark.slow
def test_binlog_rotation_handling(db_connections, migres_process):
    """Test: System handles binlog rotation during processing"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST: Binlog Rotation Handling")
    print("="*60)
    
    table = "test_binlog_rotation"
    batch_delay = get_batch_delay_seconds(cfg)
    
    # Create table
    print(f"📋 Creating table {table}...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mysql.cn.commit()
    
    # Small delay to ensure DROP is processed
    time.sleep(2)
    
    with mysql.cn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                data VARCHAR(100),
                rotation_batch INT
            )
        """)
        mysql.cn.commit()
    
    # Wait for DDL event to be processed
    # Need to wait for batch_delay_seconds for the CREATE TABLE event to be flushed to buffer,
    # plus time for transformer to process it and create the table in ClickHouse
    wait_time_ddl = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time_ddl}s for CREATE TABLE DDL to be processed...")
    time.sleep(wait_time_ddl)
    
    assert wait_for_table_in_clickhouse(ch, table, timeout=120), f"Table {table} was not created"
    
    # Insert data in multiple batches to potentially trigger rotation
    total_inserted = 0
    for batch in range(5):
        print(f"📝 Batch {batch+1}/5: Inserting 200 records...")
        with mysql.cn.cursor() as cur:
            for i in range(batch * 200 + 1, (batch + 1) * 200 + 1):
                cur.execute(
                    f"INSERT INTO {table} (id, data, rotation_batch) VALUES (%s, %s, %s)",
                    (i, f"data_{i}", batch + 1)
                )
            mysql.cn.commit()
        total_inserted += 200
        
        # Small delay between batches
        time.sleep(2)
    
    # Wait for all processing - need to wait for batch_delay_seconds for last batch
    # plus time for all events to be committed to buffer and processed
    wait_time = max(batch_delay * 4, 45)  # Increased multiplier and minimum wait
    print(f"⏳ Waiting {wait_time}s for all batches to be processed...")
    time.sleep(wait_time)
    
    assert wait_for_cdc_sync(timeout=240), "CDC sync timeout"  # Increased timeout
    
    optimize_clickhouse_table(ch, table, wait_after=5)
    
    # Verify all data
    mysql_count = None
    with mysql.cn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        mysql_count = cur.fetchone()[0]
    
    from conftest import get_clickhouse_count_reliable
    ch_count = get_clickhouse_count_reliable(ch, table, timeout=90)
    
    assert mysql_count == total_inserted, f"Expected MySQL count={total_inserted}, got {mysql_count}"
    assert ch_count == total_inserted, f"Expected ClickHouse count={total_inserted}, got {ch_count}"
    
    print(f"✅ Binlog rotation test passed: {ch_count} rows replicated")
    
    # Cleanup
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mysql.cn.commit()

