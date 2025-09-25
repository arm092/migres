#!/usr/bin/env python3
"""
Simple script to monitor CDC processing by checking ClickHouse table counts.
This helps verify that CDC is processing events correctly.
"""

import time
import sys
import os
from datetime import datetime

# Add the parent directory to Python path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clickhouse_client import CHClient
from config import load_config

def monitor_test_table():
    """Monitor the test_table in ClickHouse to see if CDC is processing events"""
    print("🔍 CDC Monitor - Watching test_table in ClickHouse")
    print("=" * 60)
    
    # Load configuration
    cfg = load_config("config.yml")
    ch_cfg = cfg["clickhouse"]
    mig_cfg = cfg.get("migration", {})
    
    # Connect to ClickHouse
    ch_client = CHClient(ch_cfg, mig_cfg)
    
    try:
        while True:
            try:
                # Check if table exists
                tables = ch_client.execute("SHOW TABLES LIKE 'test_table'")
                if len(tables) == 0:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ test_table not found in ClickHouse")
                    time.sleep(5)
                    continue
                
                # Get counts
                total_count = ch_client.execute("SELECT count() FROM test_table")[0][0]
                active_count = ch_client.execute("SELECT count() FROM test_table WHERE __data_transfer_delete_time = 0")[0][0]
                deleted_count = ch_client.execute("SELECT count() FROM test_table WHERE __data_transfer_delete_time != 0")[0][0]
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 📊 Total: {total_count}, Active: {active_count}, Deleted: {deleted_count}")
                
                # If we have data, show some sample records
                if total_count > 0:
                    sample = ch_client.execute("SELECT id, name, email, age FROM test_table WHERE __data_transfer_delete_time = 0 LIMIT 3")
                    if sample:
                        print(f"    Sample records: {sample}")
                
            except Exception as e:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Error: {e}")
            
            time.sleep(2)  # Check every 2 seconds
            
    except KeyboardInterrupt:
        print("\n👋 Monitor stopped by user")
    finally:
        ch_client.client.disconnect()

if __name__ == "__main__":
    monitor_test_table()
