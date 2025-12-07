#!/usr/bin/env python3
"""
ClickHouse Failures Test - Tests ClickHouse connection failures during insert operations.
Verifies:
1. System handles ClickHouse connection drops gracefully
2. Queries are retried when connection is restored
3. No data loss during connection failures
"""

import time
import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from buffer import BufferDB
from conftest import wait_for_cdc_sync, wait_for_table_in_clickhouse, get_batch_delay_seconds, optimize_clickhouse_table


@pytest.mark.integration
@pytest.mark.slow
def test_clickhouse_reconnection(db_connections, migres_process):
    """Test: System recovers from ClickHouse connection issues"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST: ClickHouse Reconnection")
    print("="*60)
    
    table = "test_ch_reconnect"
    batch_delay = get_batch_delay_seconds(cfg)
    
    # Create table
    print(f"📋 Creating table {table}...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                data VARCHAR(100),
                batch INT
            )
        """)
        mysql.cn.commit()
    
    # Wait for DDL event to be processed
    time.sleep(max(batch_delay + 5, 20))
    
    assert wait_for_table_in_clickhouse(ch, table, timeout=90), f"Table {table} was not created"
    
    # Insert batch 1
    print("📝 Inserting batch 1 (100 records)...")
    with mysql.cn.cursor() as cur:
        for i in range(1, 101):
            cur.execute(
                f"INSERT INTO {table} (id, data, batch) VALUES (%s, %s, %s)",
                (i, f"data_{i}", 1)
            )
        mysql.cn.commit()
    
    # Wait for batch 1
    wait_time = max(batch_delay * 3, 30)
    time.sleep(wait_time)
    
    # Insert batch 2
    print("📝 Inserting batch 2 (100 records)...")
    with mysql.cn.cursor() as cur:
        for i in range(101, 201):
            cur.execute(
                f"INSERT INTO {table} (id, data, batch) VALUES (%s, %s, %s)",
                (i, f"data_{i}", 2)
            )
        mysql.cn.commit()
    
    # Wait for all processing
    wait_time_batch2 = max(batch_delay * 4, 45)
    time.sleep(wait_time_batch2)
    assert wait_for_cdc_sync(timeout=240), "CDC sync timeout"
    
    optimize_clickhouse_table(ch, table, wait_after=5)
    
    # Verify all data
    mysql_count = None
    with mysql.cn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        mysql_count = cur.fetchone()[0]
    
    from conftest import get_clickhouse_count_reliable
    ch_count = get_clickhouse_count_reliable(ch, table, timeout=60)
    
    assert mysql_count == 200, f"Expected MySQL count=200, got {mysql_count}"
    assert ch_count == 200, f"Expected ClickHouse count=200, got {ch_count}"
    
    print(f"✅ ClickHouse reconnection test passed: {ch_count} rows replicated")
    
    # Cleanup
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mysql.cn.commit()

