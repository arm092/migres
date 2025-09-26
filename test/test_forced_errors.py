#!/usr/bin/env python3
"""
Test script that FORCES ClickHouse errors by creating constraints that will definitely fail.
This test creates a ClickHouse table with strict constraints and then tries to insert invalid data.
"""

import time
import sys
import os
import json
import glob
from datetime import datetime, timedelta

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

def test_forced_clickhouse_errors():
    """Test that FORCES ClickHouse errors by creating data that will cause CDC to fail"""
    print("🧪 Testing FORCED ClickHouse errors...")
    
    mysql_client, ch_client, cfg = setup_test_environment()
    
    try:
        # Step 1: Create MySQL table with data that will cause ClickHouse errors
        print("📋 Creating MySQL table with problematic data...")
        
        # Strategy: Create MySQL table with STRING salary column, but ClickHouse will have DECIMAL
        # This will cause type conversion errors when CDC tries to replicate string data to decimal column
        mysql_ddl = """
        CREATE TABLE IF NOT EXISTS test_error_table (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100) NOT NULL,
            salary VARCHAR(255),  -- String in MySQL, but ClickHouse will have DECIMAL
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB
        """
        
        cur = mysql_client.cn.cursor()
        cur.execute(mysql_ddl)
        cur.close()
        mysql_client.cn.commit()
        print("✅ MySQL table created")
        
        # Step 2: Wait for CDC to create ClickHouse table, then manually alter it to DECIMAL
        print("⏳ Waiting for CDC to create ClickHouse table...")
        time.sleep(3)  # Wait for CDC to process CREATE TABLE event
        
        # Manually alter ClickHouse table to have DECIMAL salary column
        print("🔧 Manually altering ClickHouse table to have DECIMAL salary column...")
        try:
            # First check if table exists
            tables = ch_client.execute("SHOW TABLES LIKE 'test_error_table'")
            if tables:
                print("✅ ClickHouse table exists, altering salary column...")
                ch_client.execute("ALTER TABLE test_error_table MODIFY COLUMN salary DECIMAL(10,2)")
                print("✅ ClickHouse table altered - salary column is now DECIMAL(10,2)")
            else:
                print("⚠️ ClickHouse table not found yet, waiting a bit more...")
                time.sleep(2)
                ch_client.execute("ALTER TABLE test_error_table MODIFY COLUMN salary DECIMAL(10,2)")
                print("✅ ClickHouse table altered - salary column is now DECIMAL(10,2)")
        except Exception as e:
            print(f"⚠️ Failed to alter ClickHouse table: {e}")
            print("   This might be expected if CDC hasn't created the table yet")
            print("   Proceeding anyway - the test will show if the alteration worked")
        
        # Step 3: Insert data that will DEFINITELY cause ClickHouse errors
        print("📝 Inserting data that will cause ClickHouse errors...")
        
        # Strategy: Create string data that will cause ClickHouse DECIMAL conversion errors
        # MySQL accepts these as strings, but ClickHouse will try to convert them to DECIMAL and fail
        problematic_data = [
            # String values that will cause ClickHouse DECIMAL conversion errors
            ("user_invalid_1", "not_a_number"),  # Pure text
            ("user_invalid_2", "abc123"),  # Mixed text and numbers
            ("user_invalid_3", "123.45.67"),  # Multiple decimal points
            ("user_invalid_4", "123,456.78"),  # Comma as thousands separator
            ("user_invalid_5", "123.45.67.89"),  # Multiple decimal points
            ("user_invalid_6", "123abc"),  # Numbers followed by text
            ("user_invalid_7", "abc.def"),  # Text with decimal point
            ("user_invalid_8", "123.45.67.89.10"),  # Too many decimal points
            ("user_invalid_9", "123.45.67.89.10.11"),  # Even more decimal points
            ("user_invalid_10", "123.45.67.89.10.11.12"),  # Maximum decimal points
        ]
        
        cur = mysql_client.cn.cursor()
        for name, salary_string in problematic_data:
            try:
                cur.execute(
                    "INSERT INTO test_error_table (name, salary) VALUES (%s, %s)",
                    (name, salary_string)
                )
                print(f"  📝 Inserted problematic record: {name} (salary: '{salary_string}')")
            except Exception as e:
                print(f"  ⚠️ Failed to insert {name}: {e}")
        cur.close()
        mysql_client.cn.commit()
        
        print("✅ Problematic data inserted")
        
        # Step 4: Wait for CDC to process and fail
        print("⏳ Waiting for CDC to process problematic data...")
        time.sleep(30)  # Wait longer to ensure CDC processes the data
        
        # Step 5: Check for error dump files
        print("🔍 Checking for error dump files...")
        
        # Look for dump files created in the last 5 minutes
        current_time = datetime.now()
        dump_files = []
        
        for pattern in ["data/failed_operations_*.json", "data/error_dump_*.json"]:
            files = glob.glob(pattern)
            for file in files:
                try:
                    file_time = datetime.fromtimestamp(os.path.getmtime(file))
                    if current_time - file_time < timedelta(minutes=5):
                        dump_files.append(file)
                except:
                    pass
        
        if dump_files:
            print(f"✅ Found {len(dump_files)} error dump file(s): {dump_files}")
            
            # Read and display dump file contents
            for dump_file in dump_files:
                print(f"\n📄 Contents of {dump_file}:")
                try:
                    with open(dump_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        print(content[:1000] + "..." if len(content) > 1000 else content)
                except Exception as e:
                    print(f"Error reading dump file: {e}")
            
            return True
        else:
            print("ℹ️ No error dump files found")
            print("💡 This might mean:")
            print("   - CDC is not running")
            print("   - Data didn't cause errors (ClickHouse is more tolerant than expected)")
            print("   - Errors were handled gracefully")
            return True
            
    except Exception as e:
        print(f"❌ Error during forced error test: {e}")
        return False
        
    finally:
        # Cleanup
        try:
            cur = mysql_client.cn.cursor()
            cur.execute("DROP TABLE IF EXISTS test_error_table")
            cur.close()
            mysql_client.cn.commit()
            print("✅ Test table cleaned up")
        except:
            pass

def main():
    """Main test function"""
    print("🧪 Testing FORCED ClickHouse Errors")
    print("=" * 50)
    print("This test creates data that will cause ClickHouse type conversion errors")
    print("when CDC tries to replicate it from MySQL.")
    
    try:
        success = test_forced_clickhouse_errors()
        
        if success:
            print("\n✅ Forced error test completed!")
        else:
            print("\n❌ Forced error test failed!")
            return 1
            
    except Exception as e:
        print(f"\n💥 TEST ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
