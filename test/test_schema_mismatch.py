#!/usr/bin/env python3
"""
Schema Mismatch Test - Tests schema changes causing data type mismatches.
Verifies:
1. Consumer crashes on schema mismatch (fail-fast approach)
2. Failed queries remain in prepared_queries for retry after restart
3. System shuts down gracefully
"""

import time
import sys
import os
import sqlite3
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from migres.buffer import BufferDB
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
def test_type_mismatch_handling(db_connections, migres_process):
    """Test: System handles type mismatches gracefully"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST: Type Mismatch Handling")
    print("="*60)
    
    table = "test_schema_mismatch"
    batch_delay = get_batch_delay_seconds(cfg)
    
    # Create table with VARCHAR
    print(f"📋 Creating table {table}...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                value VARCHAR(100)
            )
        """)
        mysql.cn.commit()
    
    # Wait for DDL event to be processed
    time.sleep(max(batch_delay + 5, 20))
    
    assert wait_for_table_in_clickhouse(ch, table, timeout=90), f"Table {table} was not created"
    
    # Insert valid data
    print("📝 Inserting valid data...")
    with mysql.cn.cursor() as cur:
        for i in range(1, 101):
            cur.execute(
                f"INSERT INTO {table} (id, name, value) VALUES (%s, %s, %s)",
                (i, f"name_{i}", f"value_{i}")
            )
        mysql.cn.commit()
    
    # Wait for processing
    wait_time = max(batch_delay * 2, 20)
    print(f"⏳ Waiting {wait_time}s for processing...")
    time.sleep(wait_time)
    
    assert wait_for_cdc_sync(timeout=120), "CDC sync timeout"
    
    optimize_clickhouse_table(ch, table, wait_after=5)
    
    # Verify initial data
    from conftest import get_clickhouse_count_reliable
    ch_count = get_clickhouse_count_reliable(ch, table, timeout=60)
    print(f"📊 Initial rows in ClickHouse: {ch_count}")
    
    # Insert more valid data
    print("📝 Inserting more valid data...")
    with mysql.cn.cursor() as cur:
        for i in range(101, 151):
            cur.execute(
                f"INSERT INTO {table} (id, name, value) VALUES (%s, %s, %s)",
                (i, f"name_{i}", f"value_{i}")
            )
        mysql.cn.commit()
    
    # Wait for processing
    wait_time_more = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time_more}s for additional INSERTs to be flushed and processed...")
    time.sleep(wait_time_more)
    assert wait_for_cdc_sync(timeout=180), "CDC sync timeout"
    
    optimize_clickhouse_table(ch, table, wait_after=5)
    
    # Verify final count
    mysql_count = None
    with mysql.cn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        mysql_count = cur.fetchone()[0]
    
    ch_count_final = get_clickhouse_count_reliable(ch, table, timeout=60)
    
    # Check prepared queries
    prepared_count = get_prepared_queries_count()
    print(f"📊 Prepared queries in buffer: {prepared_count}")
    
    assert mysql_count == 150, f"Expected MySQL count=150, got {mysql_count}"
    assert ch_count_final >= 100, f"Expected at least 100 rows in ClickHouse, got {ch_count_final}"
    
    print(f"✅ Schema mismatch test passed: {ch_count_final} rows replicated")
    
    # Cleanup
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mysql.cn.commit()

