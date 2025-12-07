#!/usr/bin/env python3
"""
Test script for CDC batching functionality.
This test verifies that the queue-based batching system works correctly
by performing bulk operations and checking the results in ClickHouse.
"""

import time
import sys
import os
import sqlite3
import pytest

# Add the parent directory to Python path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from conftest import wait_for_table_in_clickhouse


def create_test_table(mysql_client):
    """Create test table in MySQL (drops existing table first for clean state)"""
    print("📋 Creating test table in MySQL...")
    
    # Drop existing table first to ensure clean state
    cur = mysql_client.cn.cursor()
    cur.execute("DROP TABLE IF EXISTS test_table")
    cur.close()
    mysql_client.cn.commit()
    print("   (Dropped existing table if any)")
    
    # Wait for CDC to process the DROP
    time.sleep(2)
    
    create_sql = """
    CREATE TABLE test_table (
        id INT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100),
        age INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
    """
    
    cur = mysql_client.cn.cursor()
    cur.execute(create_sql)
    cur.close()
    mysql_client.cn.commit()
    print("✅ Test table created in MySQL")
    
    # Verify table exists
    cur = mysql_client.cn.cursor()
    cur.execute("SHOW TABLES LIKE 'test_table'")
    tables = cur.fetchall()
    cur.close()
    assert len(tables) > 0, "Test table was not created in MySQL"
    print("✅ Test table verified in MySQL")


def perform_bulk_operations(mysql_client, operation_count=5000):
    """Perform bulk INSERT, UPDATE, and DELETE operations"""
    print(f"🚀 Performing {operation_count} INSERT operations...")
    
    # INSERT operations
    start_time = time.time()
    cur = mysql_client.cn.cursor()
    for i in range(1, operation_count + 1):
        cur.execute(
            "INSERT INTO test_table (name, email, age) VALUES (%s, %s, %s)",
            (f"user_{i}", f"user_{i}@example.com", 20 + (i % 50))
        )
        if i % 1000 == 0:
            print(f"  📝 Inserted {i} records...")
    cur.close()
    mysql_client.cn.commit()
    
    insert_time = time.time() - start_time
    print(f"✅ Completed {operation_count} INSERTs in {insert_time:.2f} seconds")
    
    # Wait for CDC to process (need to wait longer than batch_delay_seconds)
    print("⏳ Waiting for CDC to process INSERTs...")
    time.sleep(20)  # Wait longer than the 15-second batch delay
    
    print(f"🔄 Performing {operation_count} UPDATE operations...")
    
    # UPDATE operations
    start_time = time.time()
    cur = mysql_client.cn.cursor()
    for i in range(1, operation_count + 1):
        cur.execute(
            "UPDATE test_table SET name = %s, email = %s, age = %s WHERE id = %s",
            (f"updated_user_{i}", f"updated_user_{i}@example.com", 30 + (i % 50), i)
        )
        if i % 1000 == 0:
            print(f"  📝 Updated {i} records...")
    cur.close()
    mysql_client.cn.commit()
    
    update_time = time.time() - start_time
    print(f"✅ Completed {operation_count} UPDATEs in {update_time:.2f} seconds")
    
    # Verify updates in MySQL
    cur = mysql_client.cn.cursor()
    cur.execute("SELECT COUNT(*) FROM test_table WHERE name LIKE 'updated_user_%'")
    updated_count = cur.fetchone()[0]
    cur.close()
    print(f"📊 MySQL records with updated names: {updated_count}")
    
    # Wait for CDC to process (need to wait longer than batch_delay_seconds)
    print("⏳ Waiting for CDC to process UPDATEs...")
    time.sleep(20)  # Wait longer than the 15-second batch delay
    
    print(f"🗑️ Performing {operation_count} DELETE operations...")
    
    # DELETE operations (delete every other record)
    start_time = time.time()
    deleted_count = 0
    cur = mysql_client.cn.cursor()
    for i in range(1, operation_count + 1, 2):  # Delete every other record
        cur.execute("DELETE FROM test_table WHERE id = %s", (i,))
        deleted_count += 1
        if deleted_count % 500 == 0:
            print(f"  📝 Deleted {deleted_count} records...")
    cur.close()
    mysql_client.cn.commit()
    
    # Verify deletes in MySQL
    cur = mysql_client.cn.cursor()
    cur.execute("SELECT COUNT(*) FROM test_table")
    mysql_count = cur.fetchone()[0]
    cur.close()
    print(f"📊 MySQL table count after DELETEs: {mysql_count}")
    
    delete_time = time.time() - start_time
    print(f"✅ Completed {deleted_count} DELETEs in {delete_time:.2f} seconds")
    
    # Wait for CDC to process (need to wait longer than batch_delay_seconds)
    print("⏳ Waiting for CDC to process DELETEs...")
    time.sleep(40)  # Wait longer to ensure all deletes are processed before verification/cleanup
    
    return operation_count, deleted_count


def check_cdc_queue_status():
    """Check if there are pending events in the CDC queue"""
    try:
        conn = sqlite3.connect("data/buffer.db")
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw_events")
        raw_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM prepared_queries")
        prep_count = cursor.fetchone()[0]
        conn.close()
        return raw_count, prep_count
    except Exception as e:
        print(f"⚠️ Could not check queue status: {e}")
        return -1, -1


def verify_clickhouse_data(ch_client, expected_inserts, expected_deletes):
    """Verify data in ClickHouse matches expectations"""
    print("🔍 Verifying data in ClickHouse...")
    
    # Check queue status before verification
    raw_count, prep_count = check_cdc_queue_status()
    print(f"📊 CDC Queue Status: raw_events={raw_count}, prepared_queries={prep_count}")
    
    # If there are pending events, wait more
    if raw_count > 0 or prep_count > 0:
        print(f"⚠️ Found pending events in queue! Waiting additional 30s...")
        time.sleep(30)
        raw_count, prep_count = check_cdc_queue_status()
        print(f"📊 CDC Queue Status after wait: raw_events={raw_count}, prepared_queries={prep_count}")
    
    # Wait a bit more for final processing (ensure all batches are processed)
    time.sleep(10)
    
    # Count total records (all versions including tombstones)
    total_count = ch_client.execute(f"SELECT count() FROM `{ch_client.db}`.`test_table`")[0][0]
    print(f"📊 Total records in ClickHouse (all versions): {total_count}")
    
    # Count active records using FINAL to get the final state
    active_count = ch_client.execute(f"SELECT count() FROM `{ch_client.db}`.`test_table` FINAL WHERE __data_transfer_delete_time = 0")[0][0]
    print(f"📊 Active records in ClickHouse (FINAL): {active_count}")
    
    # Count deleted records using FINAL
    deleted_count = ch_client.execute(f"SELECT count() FROM `{ch_client.db}`.`test_table` FINAL WHERE __data_transfer_delete_time != 0")[0][0]
    print(f"📊 Deleted records in ClickHouse (FINAL): {deleted_count}")
    
    # Alternative: Count by grouping by primary key and taking the latest version
    try:
        latest_count = ch_client.execute(f"""
            SELECT count() FROM (
                SELECT id, 
                       argMax(__data_transfer_delete_time, __data_transfer_commit_time) as final_delete_time
                FROM `{ch_client.db}`.`test_table` 
                GROUP BY id
            ) WHERE final_delete_time = 0
        """)[0][0]
        print(f"📊 Active records (GROUP BY method): {latest_count}")
    except Exception as e:
        print(f"⚠️ Could not use GROUP BY method: {e}")
        latest_count = active_count
    
    # Verify some sample data
    sample_data = ch_client.execute(f"SELECT id, name, email, age FROM `{ch_client.db}`.`test_table` FINAL WHERE __data_transfer_delete_time = 0 LIMIT 5")
    print(f"📊 Sample active records: {sample_data}")
    
    # Check if we have the expected number of records
    expected_active = expected_inserts - expected_deletes
    expected_total = expected_inserts + expected_inserts + expected_deletes
    
    print(f"📊 Expected: {expected_total} total versions (approx), {expected_active} active, {expected_deletes} deleted")
    print(f"📊 Actual: {total_count} total versions, {latest_count} active, {deleted_count} deleted")
    
    assert latest_count == expected_active, f"Expected {expected_active} active records, got {latest_count}"
    assert deleted_count == expected_deletes, f"Expected {expected_deletes} deleted records, got {deleted_count}"


@pytest.mark.integration
@pytest.mark.slow
def test_cdc_batching(db_connections):
    """Test CDC batching functionality with bulk operations"""
    mysql, ch, cfg = db_connections
    
    print("🧪 Starting CDC Batching Test")
    print("=" * 50)
    
    # Note: This test assumes CDC process is already running
    # Wait a bit for CDC to initialize
    time.sleep(5)
    
    # Create test table
    create_test_table(mysql)
    
    # Wait for table to appear in ClickHouse
    assert wait_for_table_in_clickhouse(ch, "test_table", timeout=30), \
        "Test table did not appear in ClickHouse"
    
    # Perform bulk operations
    insert_count, delete_count = perform_bulk_operations(mysql, 5000)
    
    # Verify results
    verify_clickhouse_data(ch, insert_count, delete_count)
    
    print("\n🎉 CDC Batching Test PASSED!")
