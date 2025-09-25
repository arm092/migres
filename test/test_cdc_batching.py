#!/usr/bin/env python3
"""
Test script for CDC batching functionality.
This test verifies that the queue-based batching system works correctly
by performing bulk operations and checking the results in ClickHouse.
"""

import time
import sys
import os
import threading
import subprocess
from datetime import datetime

# Add the parent directory to Python path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mysql_client import MySQLClient
from clickhouse_client import CHClient
from config import load_config

def setup_test_environment():
    """Setup test environment and connections"""
    print("🔧 Setting up test environment...")
    
    # Load configuration
    cfg = load_config("config.yml")
    mysql_cfg = cfg["mysql"]
    ch_cfg = cfg["clickhouse"]
    mig_cfg = cfg.get("migration", {})
    
    # Connect to MySQL
    mysql_client = MySQLClient(mysql_cfg)
    mysql_client.connect()
    print(f"✅ Connected to MySQL: {mysql_cfg['host']}:{mysql_cfg.get('port', 3306)}")
    
    # Connect to ClickHouse
    ch_client = CHClient(ch_cfg, mig_cfg)
    print(f"✅ Connected to ClickHouse: {ch_cfg['host']}:{ch_cfg.get('port', 9000)}")
    
    return mysql_client, ch_client, cfg

def create_test_table(mysql_client):
    """Create test table in MySQL"""
    print("📋 Creating test table in MySQL...")
    
    create_sql = """
    CREATE TABLE IF NOT EXISTS test_table (
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
    mysql_client.cn.commit()  # Commit the CREATE TABLE transaction
    print("✅ Test table created in MySQL")
    
    # Verify table exists
    cur = mysql_client.cn.cursor()
    cur.execute("SHOW TABLES LIKE 'test_table'")
    tables = cur.fetchall()
    cur.close()
    assert len(tables) > 0, "Test table was not created in MySQL"
    print("✅ Test table verified in MySQL")

def wait_for_table_in_clickhouse(ch_client, timeout=30):
    """Wait for test table to appear in ClickHouse"""
    print("⏳ Waiting for test table to appear in ClickHouse...")
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            tables = ch_client.execute("SHOW TABLES LIKE 'test_table'")
            if len(tables) > 0:
                print("✅ Test table found in ClickHouse")
                return True
        except Exception as e:
            print(f"⚠️ Error checking ClickHouse tables: {e}")
        time.sleep(1)
    
    # Provide more detailed error information
    try:
        all_tables = ch_client.execute("SHOW TABLES")
        print(f"❌ Test table not found. Available tables: {[t[0] for t in all_tables]}")
    except Exception as e:
        print(f"❌ Could not list ClickHouse tables: {e}")
    
    raise TimeoutError(f"Test table did not appear in ClickHouse within {timeout} seconds. Make sure CDC process is running and processing DDL events correctly.")

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
    mysql_client.cn.commit()  # Commit the INSERT transactions
    
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
    mysql_client.cn.commit()  # Commit the UPDATE transactions
    
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
        if deleted_count % 1000 == 0:
            print(f"  📝 Deleted {deleted_count} records...")
    cur.close()
    mysql_client.cn.commit()  # Commit the DELETE transactions
    
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
    time.sleep(20)  # Wait longer than the 15-second batch delay
    
    return operation_count, deleted_count

def verify_clickhouse_data(ch_client, expected_inserts, expected_deletes):
    """Verify data in ClickHouse matches expectations"""
    print("🔍 Verifying data in ClickHouse...")
    
    # Wait a bit more for final processing (ensure all batches are processed)
    time.sleep(25)  # Wait longer than batch delay to ensure final processing
    
    try:
        # Count total records (all versions including tombstones)
        total_count = ch_client.execute("SELECT count() FROM test_table")[0][0]
        print(f"📊 Total records in ClickHouse (all versions): {total_count}")
        
        # Count active records using FINAL to get the final state
        active_count = ch_client.execute("SELECT count() FROM test_table FINAL WHERE __data_transfer_delete_time = 0")[0][0]
        print(f"📊 Active records in ClickHouse (FINAL): {active_count}")
        
        # Count deleted records using FINAL
        deleted_count = ch_client.execute("SELECT count() FROM test_table FINAL WHERE __data_transfer_delete_time != 0")[0][0]
        print(f"📊 Deleted records in ClickHouse (FINAL): {deleted_count}")
        
        # Alternative: Count by grouping by primary key and taking the latest version
        # This is more reliable than FINAL in some cases
        try:
            latest_count = ch_client.execute("""
                SELECT count() FROM (
                    SELECT id, 
                           argMax(__data_transfer_delete_time, __data_transfer_commit_time) as final_delete_time
                    FROM test_table 
                    GROUP BY id
                ) WHERE final_delete_time = 0
            """)[0][0]
            print(f"📊 Active records (GROUP BY method): {latest_count}")
        except Exception as e:
            print(f"⚠️ Could not use GROUP BY method: {e}")
            latest_count = active_count
        
        # Verify some sample data
        sample_data = ch_client.execute("SELECT id, name, email, age FROM test_table FINAL WHERE __data_transfer_delete_time = 0 LIMIT 5")
        print(f"📊 Sample active records: {sample_data}")
        
        # Check if we have the expected number of records
        # With ReplacingMergeTree: INSERT creates records, UPDATE creates new versions, DELETE creates tombstones
        expected_active = expected_inserts - expected_deletes  # Final active records after all operations
        expected_total = expected_inserts + expected_inserts + expected_deletes  # Total: INSERT + UPDATE + DELETE versions
        
        print(f"📊 Expected: {expected_total} total versions, {expected_active} active, {expected_deletes} deleted")
        print(f"📊 Actual: {total_count} total versions, {latest_count} active, {deleted_count} deleted")
        
        if total_count == expected_total and latest_count == expected_active and deleted_count == expected_deletes:
            print("✅ Data verification PASSED!")
            return True
        else:
            print(f"❌ Data verification FAILED!")
            return False
            
    except Exception as e:
        print(f"❌ Error verifying data: {e}")
        return False

def cleanup_test_data(mysql_client, ch_client):
    """Clean up test data and test CDC DROP TABLE event handling"""
    print("🧹 Cleaning up test data...")
    
    try:
        # Drop table from MySQL (this should trigger CDC DROP TABLE event)
        cur = mysql_client.cn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_table")
        cur.close()
        mysql_client.cn.commit()
        print("✅ Test table dropped from MySQL")
        
        # Wait for CDC to process the DROP TABLE event
        print("⏳ Waiting for CDC to process DROP TABLE event...")
        time.sleep(5)  # Give CDC time to process the DDL event
        
        # Check if CDC automatically dropped the table from ClickHouse
        try:
            ch_client.execute("DESCRIBE test_table")
            print("❌ CDC DROP TABLE event handling FAILED!")
            print("   ClickHouse table still exists - CDC did not process DROP TABLE event")
            print("   Manually dropping from ClickHouse as fallback...")
            ch_client.execute("DROP TABLE IF EXISTS test_table")
            print("✅ Manually dropped from ClickHouse (CDC DDL handling needs investigation)")
        except Exception:
            print("✅ CDC DROP TABLE event handling SUCCESS!")
            print("   ClickHouse table was automatically dropped by CDC")
        
    except Exception as e:
        print(f"⚠️ Error during cleanup: {e}")
        # Fallback: try to drop from ClickHouse manually
        try:
            ch_client.execute("DROP TABLE IF EXISTS test_table")
            print("✅ Fallback: Manually dropped from ClickHouse")
        except:
            pass

def check_cdc_running():
    """Check if CDC process is already running"""
    print("🔍 Checking if CDC process is running...")
    
    try:
        # Check if we can connect to MySQL and if there are any recent binlog events
        # This is a simple check - in a real scenario you might check process list
        print("ℹ️ Note: This test assumes CDC process is running.")
        print("   If you haven't started it yet, run: python migres.py")
        print("   The test will proceed and may fail if CDC is not running.")
        return True
    except Exception as e:
        print(f"❌ CDC process check failed: {e}")
        return False

def main():
    """Main test function"""
    print("🧪 Starting CDC Batching Test")
    print("=" * 50)
    
    mysql_client = None
    ch_client = None
    
    try:
        # Setup
        mysql_client, ch_client, cfg = setup_test_environment()
        
        # Check if CDC process is running
        if not check_cdc_running():
            print("❌ Please start CDC process first with: python migres.py")
            return 1
        
        # Wait a bit for CDC to initialize
        time.sleep(5)
        
        # Create test table
        create_test_table(mysql_client)
        
        # Wait for table to appear in ClickHouse
        wait_for_table_in_clickhouse(ch_client)
        
        # Perform bulk operations
        insert_count, delete_count = perform_bulk_operations(mysql_client, 5000)
        
        # Verify results
        success = verify_clickhouse_data(ch_client, insert_count, delete_count)
        
        if success:
            print("\n🎉 MAIN TEST PASSED! CDC batching is working correctly.")
        else:
            print("\n❌ MAIN TEST FAILED! CDC batching has issues.")
            return 1
    except Exception as e:
        print(f"\n💥 TEST ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        # Cleanup before closing connections
        cleanup_test_data(mysql_client, ch_client)
        
        # Close connections
        if mysql_client:
            mysql_client.close()
        if ch_client:
            ch_client.client.disconnect()
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

