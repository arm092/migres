#!/usr/bin/env python3
"""
Ordering Test - Verifies events are processed in correct sequence.
Verifies:
1. INSERT -> UPDATE sequence preserves latest values
2. INSERT -> DELETE sequence removes rows
3. UPDATE -> DELETE sequence removes rows
4. Multiple updates preserve final state
5. Cross-table ordering is maintained
"""

import time
import sys
import os
import pytest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from conftest import wait_for_cdc, wait_for_table, get_batch_delay_seconds


def get_clickhouse_value(ch, table_name, id_val, column, timeout=30):
    """Get a specific value from ClickHouse with retry, using reliable ReplacingMergeTree query"""
    # First wait for table to exist
    if not wait_for_table(ch, table_name, timeout=10):
        return None
    
    start = time.time()
    while time.time() - start < timeout:
        try:
            # Use GROUP BY with argMax to get the latest version of the row
            # This is more reliable than FINAL for ReplacingMergeTree
            result = ch.execute(f"""
                SELECT argMax({column}, __data_transfer_commit_time) as final_value,
                       argMax(__data_transfer_delete_time, __data_transfer_commit_time) as final_delete_time
                FROM `{ch.db}`.`{table_name}`
                WHERE id = {id_val}
                GROUP BY id
                HAVING final_delete_time = 0
            """)
            if result and len(result) > 0 and len(result[0]) > 0:
                return result[0][0]
            # No result - row might be deleted or not synced yet
            # Check if there's any version of this row at all
            any_result = ch.execute(f"""
                SELECT count() FROM `{ch.db}`.`{table_name}` WHERE id = {id_val}
            """)
            if any_result and any_result[0][0] > 0:
                # Row exists but is deleted (has delete_time > 0)
                return None
            # Row doesn't exist yet, keep waiting
        except Exception as e:
            pass
        time.sleep(2)
    return None


def get_clickhouse_count(ch, table_name, timeout=30):
    """Get row count from ClickHouse, waiting for data to appear"""
    # First wait for table to exist
    if not wait_for_table(ch, table_name, timeout=10):
        return -1
    
    start = time.time()
    last_count = -1
    stable_count = 0
    
    while time.time() - start < timeout:
        try:
            # Use GROUP BY with argMax to get the latest version of each row
            # This is more reliable than FINAL for ReplacingMergeTree
            result = ch.execute(f"""
                SELECT count() FROM (
                    SELECT id,
                           argMax(__data_transfer_delete_time, __data_transfer_commit_time) as final_delete_time
                    FROM `{ch.db}`.`{table_name}`
                    GROUP BY id
                ) WHERE final_delete_time = 0
            """)
            if result and len(result) > 0:
                count = result[0][0]
                # Wait for count to stabilize (same count twice in a row)
                if count == last_count:
                    stable_count += 1
                    if stable_count >= 2:  # Count stable for 2 checks
                        return count
                else:
                    stable_count = 0
                last_count = count
        except Exception as e:
            # Fallback to FINAL method if GROUP BY fails
            try:
                result = ch.execute(
                    f"SELECT count() FROM `{ch.db}`.`{table_name}` FINAL WHERE __data_transfer_delete_time = 0"
                )
                if result and len(result) > 0:
                    count = result[0][0]
                    if count == last_count:
                        stable_count += 1
                        if stable_count >= 2:
                            return count
                    else:
                        stable_count = 0
                    last_count = count
            except Exception:
                pass
        time.sleep(2)
    
    # Return last count even if not fully stable
    return last_count if last_count >= 0 else -1


@pytest.mark.integration
@pytest.mark.slow
def test_insert_then_update(db_connections, migres_process):
    """Test: INSERT followed by UPDATE preserves final value"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 1: INSERT -> UPDATE Sequence")
    print("="*60)
    
    table = "test_order_insert_update"
    time.sleep(5)  # Wait for migres to initialize
    
    # Create table AFTER migres is running
    print(f"   Creating table {table} in MySQL...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                version INT,
                data VARCHAR(100)
            )
        """)
        mysql.cn.commit()
    
    # Wait for DDL event to be processed
    batch_delay = get_batch_delay_seconds(cfg)
    time.sleep(max(batch_delay + 5, 20))
    
    # Wait for table to exist in ClickHouse (DDL processing)
    assert wait_for_table(ch, table, timeout=90), f"Table {table} was not created in ClickHouse"
    
    print("📝 Testing INSERT -> UPDATE sequence...")
    batch_delay = get_batch_delay_seconds(cfg)
    
    with mysql.cn.cursor() as cur:
        # Insert
        cur.execute(f"INSERT INTO {table} (id, version, data) VALUES (1, 1, 'initial')")
        mysql.cn.commit()
        
        # Immediate update
        cur.execute(f"UPDATE {table} SET version = 2, data = 'updated' WHERE id = 1")
        mysql.cn.commit()
        
        # Another update
        cur.execute(f"UPDATE {table} SET version = 3, data = 'final' WHERE id = 1")
        mysql.cn.commit()
    
    # Wait for events to be flushed and processed
    wait_time = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time}s for INSERT and UPDATEs to be flushed and processed...")
    time.sleep(wait_time)
    assert wait_for_cdc(timeout=180), "CDC sync timeout"
    
    # Force table optimization for ReplacingMergeTree - need multiple passes
    print("🔄 Optimizing table to ensure UPDATEs are merged...")
    try:
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(5)
        # Optimize again to ensure all merges complete
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(5)
    except Exception as e:
        print(f"   Optimization warning: {e}")
    
    # Verify final state
    version = get_clickhouse_value(ch, table, 1, 'version', timeout=30)
    data = get_clickhouse_value(ch, table, 1, 'data', timeout=30)
    
    assert version == 3, f"Expected version=3, got {version}"
    assert data == 'final', f"Expected data='final', got {data}"
    
    # Cleanup
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mysql.cn.commit()


@pytest.mark.integration
@pytest.mark.slow
def test_insert_then_delete(db_connections, migres_process):
    """Test: INSERT followed by DELETE removes row"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 2: INSERT -> DELETE Sequence")
    print("="*60)
    
    table = "test_order_insert_delete"
    # Wait for migres to be fully initialized
    time.sleep(5)
    
    print(f"   Creating table {table} in MySQL...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                data VARCHAR(100)
            )
        """)
        mysql.cn.commit()
    
    # Wait for DDL event to be processed - need to wait for batch_delay_seconds
    # plus processing time for CREATE TABLE to be captured and processed
    batch_delay = get_batch_delay_seconds(cfg)
    time.sleep(max(batch_delay + 5, 20))  # Wait for batch delay + processing time
    
    # Wait for table to exist in ClickHouse
    assert wait_for_table(ch, table, timeout=90), f"Table {table} was not created in ClickHouse"
    
    print("📝 Testing INSERT -> DELETE sequence...")
    with mysql.cn.cursor() as cur:
        # Insert rows
        for i in range(1, 11):
            cur.execute(f"INSERT INTO {table} (id, data) VALUES ({i}, 'data_{i}')")
        mysql.cn.commit()
    
    # Wait for INSERTs to be processed first
    batch_delay = get_batch_delay_seconds(cfg)
    wait_time_inserts = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time_inserts}s for INSERTs to be flushed and processed...")
    time.sleep(wait_time_inserts)
    assert wait_for_cdc(timeout=120), "CDC sync timeout after INSERTs"
    time.sleep(5)  # Extra wait for INSERT processing
    
    with mysql.cn.cursor() as cur:
        # Delete some
        cur.execute(f"DELETE FROM {table} WHERE id IN (2, 4, 6, 8, 10)")
        mysql.cn.commit()
    
    # Wait for DELETEs to be processed (need to wait for batch_delay_seconds)
    batch_delay = get_batch_delay_seconds(cfg)
    wait_time_deletes = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time_deletes}s for DELETE events to be flushed and processed...")
    time.sleep(wait_time_deletes)
    assert wait_for_cdc(timeout=120), "CDC sync timeout after DELETEs"
    
    # Force optimization before checking - need multiple passes for ReplacingMergeTree
    print("🔄 Optimizing table to ensure DELETEs are merged...")
    try:
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(5)
        # Optimize again to ensure all merges complete
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(5)  # Wait longer after optimization
    except Exception as e:
        print(f"   Optimization warning: {e}")
    
    count = get_clickhouse_count(ch, table, timeout=90)
    
    # Verify specific rows
    deleted_exists = get_clickhouse_value(ch, table, 2, 'data', timeout=60) is not None
    kept_exists = get_clickhouse_value(ch, table, 1, 'data', timeout=60) is not None
    
    assert count == 5, f"Expected count=5, got {count}"
    assert not deleted_exists, "Deleted row should not exist"
    assert kept_exists, "Kept row should exist"
    
    # Cleanup
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mysql.cn.commit()


@pytest.mark.integration
@pytest.mark.slow
def test_multiple_updates(db_connections, migres_process):
    """Test: Multiple rapid updates preserve final state"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 3: Multiple Rapid Updates")
    print("="*60)
    
    table = "test_order_multi_update"
    time.sleep(5)
    
    print(f"   Creating table {table} in MySQL...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                counter INT,
                last_update VARCHAR(100)
            )
        """)
        mysql.cn.commit()
    
    # Wait for DDL event to be processed
    batch_delay = get_batch_delay_seconds(cfg)
    time.sleep(max(batch_delay + 5, 20))
    
    # Wait for table to exist in ClickHouse
    assert wait_for_table(ch, table, timeout=90), f"Table {table} was not created in ClickHouse"
    
    print("📝 Testing multiple rapid updates...")
    with mysql.cn.cursor() as cur:
        # Insert
        cur.execute(f"INSERT INTO {table} (id, counter, last_update) VALUES (1, 0, 'initial')")
        mysql.cn.commit()
        
        # 10 rapid updates
        for i in range(1, 11):
            cur.execute(f"""
                UPDATE {table} SET counter = {i}, last_update = 'update_{i}' WHERE id = 1
            """)
            mysql.cn.commit()
    
    assert wait_for_cdc(timeout=90), "CDC sync timeout"
    
    # Force table optimization for ReplacingMergeTree
    try:
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(2)
    except Exception:
        pass
    
    counter = get_clickhouse_value(ch, table, 1, 'counter', timeout=60)
    last_update = get_clickhouse_value(ch, table, 1, 'last_update', timeout=60)
    
    assert counter == 10, f"Expected counter=10, got {counter}"
    assert last_update == 'update_10', f"Expected last_update='update_10', got {last_update}"
    
    # Cleanup
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mysql.cn.commit()


@pytest.mark.integration
@pytest.mark.slow
def test_update_then_delete(db_connections, migres_process):
    """Test: UPDATE followed by DELETE removes row"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 4: UPDATE -> DELETE Sequence")
    print("="*60)
    
    table = "test_order_update_delete"
    time.sleep(5)
    
    print(f"   Creating table {table} in MySQL...")
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
    batch_delay = get_batch_delay_seconds(cfg)
    time.sleep(max(batch_delay + 5, 20))
    
    # Wait for table to exist in ClickHouse
    assert wait_for_table(ch, table, timeout=90), f"Table {table} was not created in ClickHouse"
    
    print("📝 Testing UPDATE -> DELETE sequence...")
    with mysql.cn.cursor() as cur:
        # Insert
        for i in range(1, 6):
            cur.execute(f"INSERT INTO {table} (id, data) VALUES ({i}, 'data_{i}')")
        mysql.cn.commit()
    
    # Wait for INSERTs first
    batch_delay = get_batch_delay_seconds(cfg)
    wait_time_inserts = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time_inserts}s for INSERTs to be flushed and processed...")
    time.sleep(wait_time_inserts)
    assert wait_for_cdc(timeout=120), "CDC sync timeout after INSERTs"
    time.sleep(5)
    
    with mysql.cn.cursor() as cur:
        # Update then delete
        cur.execute(f"UPDATE {table} SET data = 'updated' WHERE id = 3")
        mysql.cn.commit()
    
    # Wait for UPDATE - need to wait for batch_delay_seconds before events are even in buffer
    wait_time_update = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time_update}s for UPDATE to be flushed and processed...")
    time.sleep(wait_time_update)
    assert wait_for_cdc(timeout=120), "CDC sync timeout after UPDATE"
    time.sleep(5)
    
    with mysql.cn.cursor() as cur:
        cur.execute(f"DELETE FROM {table} WHERE id = 3")
        mysql.cn.commit()
    
    # Wait for DELETE (need to wait for batch_delay_seconds)
    wait_time_delete = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time_delete}s for DELETE event to be flushed and processed...")
    time.sleep(wait_time_delete)
    assert wait_for_cdc(timeout=120), "CDC sync timeout after DELETE"
    
    # Force optimization before checking - need multiple passes for ReplacingMergeTree
    print("🔄 Optimizing table to ensure DELETEs are merged...")
    try:
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(5)
        # Optimize again to ensure all merges complete
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(5)
    except Exception as e:
        print(f"   Optimization warning: {e}")
    
    count = get_clickhouse_count(ch, table, timeout=90)
    
    # Check if deleted row exists (should return None if properly deleted)
    deleted_exists = get_clickhouse_value(ch, table, 3, 'data', timeout=60) is not None
    
    # Expected: 4 rows remain (ids 1, 2, 4, 5), row 3 should be deleted
    assert count == 4, f"Expected count=4, got {count}"
    assert not deleted_exists, "Deleted row should not exist"
    
    # Cleanup
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mysql.cn.commit()


@pytest.mark.integration
@pytest.mark.slow
def test_interleaved_operations(db_connections, migres_process):
    """Test: Interleaved INSERT/UPDATE/DELETE on multiple rows"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 5: Interleaved Operations")
    print("="*60)
    
    table = "test_order_interleaved"
    time.sleep(5)
    
    print(f"   Creating table {table} in MySQL...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                status VARCHAR(50),
                version INT
            )
        """)
        mysql.cn.commit()
    
    # Wait for DDL event to be processed
    batch_delay = get_batch_delay_seconds(cfg)
    time.sleep(max(batch_delay + 5, 20))
    
    # Wait for table to exist in ClickHouse
    assert wait_for_table(ch, table, timeout=90), f"Table {table} was not created in ClickHouse"
    
    print("📝 Testing interleaved operations...")
    with mysql.cn.cursor() as cur:
        # Complex interleaved sequence
        cur.execute(f"INSERT INTO {table} VALUES (1, 'created', 1)")
        cur.execute(f"INSERT INTO {table} VALUES (2, 'created', 1)")
        cur.execute(f"UPDATE {table} SET status='updated', version=2 WHERE id=1")
        cur.execute(f"INSERT INTO {table} VALUES (3, 'created', 1)")
        cur.execute(f"DELETE FROM {table} WHERE id=2")
        cur.execute(f"UPDATE {table} SET status='final', version=3 WHERE id=1")
        cur.execute(f"INSERT INTO {table} VALUES (4, 'created', 1)")
        cur.execute(f"UPDATE {table} SET status='updated', version=2 WHERE id=3")
        cur.execute(f"INSERT INTO {table} VALUES (5, 'created', 1)")
        cur.execute(f"DELETE FROM {table} WHERE id=4")
        mysql.cn.commit()
    
    # Wait for all operations including DELETEs
    batch_delay = get_batch_delay_seconds(cfg)
    wait_time_all = max(batch_delay + 10, 40)  # Need more time for complex interleaved operations
    print(f"⏳ Waiting {wait_time_all}s for all operations to be flushed and processed...")
    time.sleep(wait_time_all)
    assert wait_for_cdc(timeout=180), "CDC sync timeout"
    
    # Optimize before checking - need multiple passes for ReplacingMergeTree
    print("🔄 Optimizing table to ensure DELETEs are merged...")
    try:
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(5)
        # Optimize again to ensure all merges complete
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(5)
    except Exception as e:
        print(f"   Optimization warning: {e}")
    
    # Verify final state
    count = get_clickhouse_count(ch, table, timeout=90)
    
    # Expected: rows 1, 3, 5 exist (2 and 4 deleted)
    row1_status = get_clickhouse_value(ch, table, 1, 'status')
    row1_version = get_clickhouse_value(ch, table, 1, 'version')
    row3_status = get_clickhouse_value(ch, table, 3, 'status')
    
    assert count == 3, f"Expected count=3, got {count}"
    assert row1_status == 'final', f"Expected row1_status='final', got {row1_status}"
    assert row1_version == 3, f"Expected row1_version=3, got {row1_version}"
    assert row3_status == 'updated', f"Expected row3_status='updated', got {row3_status}"
    
    # Cleanup
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mysql.cn.commit()


@pytest.mark.integration
@pytest.mark.slow
def test_cross_table_ordering(db_connections, migres_process):
    """Test: Operations across multiple tables maintain order"""
    mysql, ch, cfg = db_connections
    
    print("\n" + "="*60)
    print("📋 TEST 6: Cross-Table Ordering")
    print("="*60)
    
    table1 = "test_order_cross1"
    table2 = "test_order_cross2"
    time.sleep(5)
    
    print(f"   Creating tables {table1} and {table2} in MySQL...")
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table1}")
        cur.execute(f"DROP TABLE IF EXISTS {table2}")
        cur.execute(f"""
            CREATE TABLE {table1} (
                id INT PRIMARY KEY,
                ref_id INT,
                data VARCHAR(100)
            )
        """)
        cur.execute(f"""
            CREATE TABLE {table2} (
                id INT PRIMARY KEY,
                data VARCHAR(100)
            )
        """)
        mysql.cn.commit()
    
    # Wait for DDL events to be processed
    batch_delay = get_batch_delay_seconds(cfg)
    time.sleep(max(batch_delay + 5, 20))
    
    # Wait for tables to exist in ClickHouse
    assert wait_for_table(ch, table1, timeout=90), f"Table {table1} was not created in ClickHouse"
    assert wait_for_table(ch, table2, timeout=90), f"Table {table2} was not created in ClickHouse"
    
    print("📝 Testing cross-table operations...")
    with mysql.cn.cursor() as cur:
        # Interleaved operations across tables - commit between each to ensure binlog order
        cur.execute(f"INSERT INTO {table2} VALUES (1, 'table2_data_1')")
        mysql.cn.commit()
        cur.execute(f"INSERT INTO {table1} VALUES (1, 1, 'table1_refs_1')")
        mysql.cn.commit()
        cur.execute(f"INSERT INTO {table2} VALUES (2, 'table2_data_2')")
        mysql.cn.commit()
        cur.execute(f"INSERT INTO {table1} VALUES (2, 2, 'table1_refs_2')")
        mysql.cn.commit()
        cur.execute(f"UPDATE {table2} SET data='table2_updated' WHERE id=1")
        mysql.cn.commit()
        cur.execute(f"UPDATE {table1} SET data='table1_updated' WHERE id=1")
        mysql.cn.commit()
    
    # Wait for INSERTs and UPDATEs
    batch_delay = get_batch_delay_seconds(cfg)
    wait_time_inserts_updates = max(batch_delay + 10, 40)
    print(f"⏳ Waiting {wait_time_inserts_updates}s for INSERTs and UPDATEs to be flushed and processed...")
    time.sleep(wait_time_inserts_updates)
    assert wait_for_cdc(timeout=180), "CDC sync timeout after INSERTs/UPDATEs"
    
    with mysql.cn.cursor() as cur:
        cur.execute(f"DELETE FROM {table2} WHERE id=2")
        mysql.cn.commit()
        cur.execute(f"DELETE FROM {table1} WHERE id=2")
        mysql.cn.commit()
    
    # Wait for DELETEs (need to wait for batch_delay_seconds)
    batch_delay = get_batch_delay_seconds(cfg)
    wait_time_deletes = max(batch_delay + 10, 30)
    print(f"⏳ Waiting {wait_time_deletes}s for DELETE events to be flushed and processed...")
    time.sleep(wait_time_deletes)
    assert wait_for_cdc(timeout=180), "CDC sync timeout after DELETEs"
    
    # Force table optimization multiple times to ensure merges complete
    print("🔄 Optimizing tables to ensure DELETEs are merged...")
    try:
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table1}` FINAL")
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table2}` FINAL")
        time.sleep(5)
        # Optimize again to ensure all merges complete
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table1}` FINAL")
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table2}` FINAL")
        time.sleep(5)  # Wait longer after optimization
    except Exception as e:
        print(f"   Optimization warning: {e}")
    
    # Verify DELETE was processed by checking if deleted row exists
    deleted_row1 = get_clickhouse_value(ch, table1, 2, 'data', timeout=30)
    deleted_row2 = get_clickhouse_value(ch, table2, 2, 'data', timeout=30)
    
    if deleted_row1 is not None or deleted_row2 is not None:
        print(f"   ⚠️ Deleted rows still visible, optimizing again...")
        time.sleep(5)
        try:
            ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table1}` FINAL")
            ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table2}` FINAL")
            time.sleep(5)
        except Exception:
            pass
    
    count1 = get_clickhouse_count(ch, table1, timeout=90)
    count2 = get_clickhouse_count(ch, table2, timeout=90)
    
    table1_data = get_clickhouse_value(ch, table1, 1, 'data')
    table2_data = get_clickhouse_value(ch, table2, 1, 'data')
    
    assert count1 == 1, f"Expected count1=1, got {count1}"
    assert count2 == 1, f"Expected count2=1, got {count2}"
    assert table1_data == 'table1_updated', f"Expected table1_data='table1_updated', got {table1_data}"
    assert table2_data == 'table2_updated', f"Expected table2_data='table2_updated', got {table2_data}"
    
    # Cleanup
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table1}")
        cur.execute(f"DROP TABLE IF EXISTS {table2}")
        mysql.cn.commit()


