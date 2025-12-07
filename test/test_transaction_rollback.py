#!/usr/bin/env python3
"""
Transaction Rollback Test - Tests MySQL transaction rollbacks.
Verifies:
1. Rolled back transactions are not replicated
2. Only committed transactions appear in ClickHouse
3. No data corruption from partial transactions
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
def test_transaction_rollback_not_replicated(db_connections, migres_process):
    """Test: Rolled back transactions are not replicated"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST: Transaction Rollback Handling")
    print("="*60)
    
    table = "test_txn_rollback"
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
    wait_time_ddl = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time_ddl}s for CREATE TABLE DDL to be processed...")
    time.sleep(wait_time_ddl)
    
    assert wait_for_table_in_clickhouse(ch, table, timeout=120), f"Table {table} was not created"
    
    # Insert committed data
    print("📝 Inserting committed data...")
    with mysql.cn.cursor() as cur:
        for i in range(1, 51):
            cur.execute(
                f"INSERT INTO {table} (id, data) VALUES (%s, %s)",
                (i, f"committed_{i}")
            )
        mysql.cn.commit()
    
    # Insert and rollback
    print("📝 Inserting data that will be rolled back...")
    with mysql.cn.cursor() as cur:
        for i in range(51, 101):
            cur.execute(
                f"INSERT INTO {table} (id, data) VALUES (%s, %s)",
                (i, f"rolled_back_{i}")
            )
        mysql.cn.rollback()  # Rollback this transaction
    
    # Insert more committed data
    print("📝 Inserting more committed data...")
    with mysql.cn.cursor() as cur:
        for i in range(101, 151):
            cur.execute(
                f"INSERT INTO {table} (id, data) VALUES (%s, %s)",
                (i, f"committed_{i}")
            )
        mysql.cn.commit()
    
    # Wait for processing
    wait_time = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time}s for INSERTs to be flushed and processed...")
    time.sleep(wait_time)
    
    assert wait_for_cdc_sync(timeout=180), "CDC sync timeout"
    
    optimize_clickhouse_table(ch, table, wait_after=5)
    
    # Verify only committed data exists
    mysql_count = None
    with mysql.cn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        mysql_count = cur.fetchone()[0]
    
    from conftest import get_clickhouse_count_reliable
    ch_count = get_clickhouse_count_reliable(ch, table, timeout=60)
    
    # Should only have 100 rows (50 + 50), not 150
    expected_count = 100
    assert mysql_count == expected_count, f"Expected MySQL count={expected_count}, got {mysql_count}"
    assert ch_count == expected_count, f"Expected ClickHouse count={expected_count}, got {ch_count}"
    
    # Verify rolled back data is not present
    with mysql.cn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE data LIKE 'rolled_back_%'")
        rolled_back_count = cur.fetchone()[0]
    
    assert rolled_back_count == 0, f"Rolled back data should not exist, found {rolled_back_count} rows"
    
    print(f"✅ Transaction rollback test passed: {ch_count} rows replicated (rollback correctly ignored)")
    
    # Cleanup
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mysql.cn.commit()

