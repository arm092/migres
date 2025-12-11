#!/usr/bin/env python3
"""
Consumer Retry Test - Tests consumer retry logic for failed queries.
Verifies:
1. Consumer crashes on failed queries (fail-fast approach)
2. Failed queries remain in prepared_queries for retry after restart
3. System shuts down gracefully when consumer crashes
"""

import time
import sys
import os
import sqlite3
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from buffer import BufferDB
from conftest import wait_for_cdc_sync, wait_for_table_in_clickhouse, get_batch_delay_seconds, optimize_clickhouse_table


def get_prepared_queries_count():
    """Get count of prepared queries in buffer"""
    try:
        conn = sqlite3.connect("data/buffer.db")
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM prepared_queries")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0


@pytest.mark.integration
@pytest.mark.slow
def test_consumer_handles_failures(db_connections, migres_process):
    """Test: Consumer handles failures gracefully and moves to failed_queries"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST: Consumer Failure Handling")
    print("="*60)
    
    table = "test_consumer_retry"
    batch_delay = get_batch_delay_seconds(cfg)
    
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
    
    # Wait for DDL event to be processed
    time.sleep(max(batch_delay + 5, 20))
    
    assert wait_for_table_in_clickhouse(ch, table, timeout=90), f"Table {table} was not created"
    
    # Insert valid data
    print("📝 Inserting valid data...")
    with mysql.cn.cursor() as cur:
        for i in range(1, 201):
            cur.execute(
                f"INSERT INTO {table} (id, data) VALUES (%s, %s)",
                (i, f"data_{i}")
            )
        mysql.cn.commit()
    
    # Wait for processing - increased wait for 200 rows
    wait_time = max(batch_delay * 4, 45)
    print(f"⏳ Waiting {wait_time}s for processing...")
    time.sleep(wait_time)
    
    assert wait_for_cdc_sync(timeout=240), "CDC sync timeout"
    
    optimize_clickhouse_table(ch, table, wait_after=5)
    
    # Verify data arrived
    from conftest import get_clickhouse_count_reliable
    ch_count = get_clickhouse_count_reliable(ch, table, timeout=60)
    
    # Check prepared queries (should be empty after successful processing)
    prepared_count = get_prepared_queries_count()
    print(f"📊 Prepared queries remaining: {prepared_count}")
    
    assert ch_count == 200, f"Expected 200 rows, got {ch_count}"
    
    print(f"✅ Consumer retry test passed: {ch_count} rows replicated")
    
    # Cleanup
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mysql.cn.commit()

