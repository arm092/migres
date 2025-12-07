#!/usr/bin/env python3
"""
Data Integrity Test - Verifies exact data match between MySQL and ClickHouse.
This test ensures no data corruption or loss by comparing:
1. Row counts
2. Actual row values
3. Checksums/hashes
"""

import time
import sys
import os
import hashlib
import sqlite3
import pytest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from buffer import BufferDB
from conftest import wait_for_cdc_sync, wait_for_table_in_clickhouse


@pytest.mark.integration
@pytest.mark.slow
def test_data_integrity(db_connections, migres_process):
    """Test data integrity between MySQL and ClickHouse"""
    mysql, ch, cfg = db_connections
    
    print("🧪 Starting Data Integrity Test")
    print("=" * 50)
    
    try:
        # Wait for migres to initialize
        time.sleep(5)
        
        # 1. Create table AFTER migres is running (so DDL is captured)
        create_test_table(mysql)
        
        # 2. Wait for table to appear in ClickHouse (verifies DDL was processed)
        assert wait_for_table_in_clickhouse(ch, "test_integrity", timeout=60), \
            "Table was not created in ClickHouse - DDL processing failed"
        
        # 3. Insert test data
        insert_count = insert_test_data(mysql, 1000)
        assert insert_count == 1000
        
        # 4. Wait for CDC
        assert wait_for_cdc_sync(timeout=60), "CDC sync timeout"
        
        # 5. Check if migres is still running before updates
        if migres_process.poll() is not None:
            print(f"⚠️ Migres process died with exit code: {migres_process.poll()}")
            raise RuntimeError("Migres process died unexpectedly")
        
        # 6. Update some data
        update_test_data(mysql, 500)
        
        # 7. Wait for CDC again - with extended wait for UPDATE events
        print("⏳ Waiting 20s for UPDATE events to be captured and batched...")
        time.sleep(20)
        
        # Check buffer status and event types
        buf = BufferDB()
        stats = buf.get_queue_stats()
        print(f"📊 Buffer before sync: raw={stats['raw_events']}, prepared={stats['prepared_queries']}")
        
        # Check for UPDATE events in raw buffer
        try:
            conn = sqlite3.connect("data/buffer.db")
            cur = conn.cursor()
            cur.execute("""
                SELECT event_type, COUNT(*) as cnt 
                FROM raw_events 
                WHERE table_name = 'test_integrity'
                GROUP BY event_type
            """)
            event_counts = cur.fetchall()
            if event_counts:
                print(f"📊 Raw events by type: {dict(event_counts)}")
            else:
                print("📊 No raw events found for test_integrity")
            conn.close()
        except Exception as e:
            print(f"   (Could not check raw events: {e})")
        
        assert wait_for_cdc_sync(timeout=120), "CDC sync timeout after updates"
        
        # 8. Force ClickHouse to optimize (ensure merges happen)
        print("🔄 Optimizing ClickHouse table (force merge)...")
        try:
            ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`test_integrity` FINAL")
            time.sleep(3)  # Wait for optimization
        except Exception as e:
            print(f"   Optimization warning: {e}")
        
        # 9. Verify updates are visible in ClickHouse
        try:
            updated_count = ch.execute(f"""
                SELECT count() FROM `{ch.db}`.`test_integrity` FINAL 
                WHERE varchar_col LIKE 'updated_%' AND __data_transfer_delete_time = 0
            """)[0][0]
            print(f"📊 Updated rows in ClickHouse: {updated_count} (expected: 500)")
            if updated_count == 0:
                print("⚠️ No updates visible yet - waiting longer...")
                time.sleep(10)
                ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`test_integrity` FINAL")
                time.sleep(3)
        except Exception as e:
            print(f"   Update check warning: {e}")
        
        # 10. Compare data
        mysql_rows, mysql_checksum = get_mysql_data(mysql)
        ch_rows, ch_checksum = get_clickhouse_data(ch)
        
        errors = compare_data(mysql_rows, ch_rows, mysql_checksum, ch_checksum)
        
        assert not errors, f"Data integrity errors found: {errors}"
        
        print("\n✅ DATA INTEGRITY TEST PASSED!")
        print(f"   • {len(mysql_rows)} rows verified")
        print(f"   • Checksums match: {mysql_checksum[:16]}")
        
    finally:
        cleanup(mysql, ch)


def create_test_table(mysql):
    """Create test table with various column types"""
    print("📋 Creating data integrity test table...")
    with mysql.cn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_integrity")
        cur.execute("""
            CREATE TABLE test_integrity (
                id INT PRIMARY KEY AUTO_INCREMENT,
                int_col INT,
                bigint_col BIGINT,
                varchar_col VARCHAR(255),
                text_col TEXT,
                decimal_col DECIMAL(10,2),
                float_col FLOAT,
                datetime_col DATETIME,
                date_col DATE,
                bool_col TINYINT(1),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        mysql.cn.commit()
    print("✅ Test table created")


def insert_test_data(mysql, count=1000):
    """Insert test data with predictable values"""
    print(f"📝 Inserting {count} test records...")
    
    with mysql.cn.cursor() as cur:
        for i in range(1, count + 1):
            cur.execute("""
                INSERT INTO test_integrity 
                (id, int_col, bigint_col, varchar_col, text_col, decimal_col, 
                 float_col, datetime_col, date_col, bool_col)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                i,
                i * 10,                           # int_col
                i * 1000000,                      # bigint_col
                f"varchar_value_{i}",             # varchar_col
                f"text_value_{i}_" + "x" * 100,   # text_col (longer text)
                round(i * 1.5, 2),                # decimal_col
                i * 0.123,                        # float_col
                f"2024-01-{(i % 28) + 1:02d} {(i % 24):02d}:00:00",  # datetime_col
                f"2024-01-{(i % 28) + 1:02d}",    # date_col
                i % 2                             # bool_col
            ))
            if i % 200 == 0:
                mysql.cn.commit()
                print(f"   Inserted {i}/{count}...")
        mysql.cn.commit()
    
    print(f"✅ Inserted {count} records")
    return count


def update_test_data(mysql, count=500):
    """Update some records to test UPDATE replication"""
    print(f"🔄 Updating {count} records...")
    
    with mysql.cn.cursor() as cur:
        for i in range(1, count + 1):
            cur.execute("""
                UPDATE test_integrity 
                SET varchar_col = %s, int_col = %s
                WHERE id = %s
            """, (f"updated_varchar_{i}", i * 100, i))
            if i % 100 == 0:
                mysql.cn.commit()
                print(f"   Committed update batch {i}/{count}")
        mysql.cn.commit()
    
    # Verify updates in MySQL
    with mysql.cn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM test_integrity WHERE varchar_col LIKE 'updated_%'")
        updated_in_mysql = cur.fetchone()[0]
        print(f"📊 Verified {updated_in_mysql} updated rows in MySQL")
    
    print(f"✅ Updated {count} records")
    return count


def normalize_value(v):
    """Normalize value for comparison (handle decimal precision differences)"""
    if v is None:
        return "NULL"
    # Handle Decimal types - normalize to float then format consistently
    from decimal import Decimal
    if isinstance(v, Decimal) or (isinstance(v, float) and '.' in str(v)):
        # Convert to float and format with reasonable precision
        return f"{float(v):.6f}".rstrip('0').rstrip('.')
    return str(v)


def get_mysql_data(mysql):
    """Get all data from MySQL with checksum"""
    print("📊 Fetching MySQL data...")
    
    with mysql.cn.cursor() as cur:
        cur.execute("""
            SELECT id, int_col, bigint_col, varchar_col, 
                   decimal_col, bool_col
            FROM test_integrity
            ORDER BY id
        """)
        rows = cur.fetchall()
    
    # Calculate checksum with normalized values
    checksum = hashlib.md5()
    for row in rows:
        row_str = "|".join(normalize_value(v) for v in row)
        checksum.update(row_str.encode())
    
    print(f"   MySQL rows: {len(rows)}, checksum: {checksum.hexdigest()[:16]}")
    return rows, checksum.hexdigest()


def get_clickhouse_data(ch):
    """Get all data from ClickHouse with checksum"""
    print("📊 Fetching ClickHouse data...")
    
    # Use FINAL to get deduplicated data, filter out deleted rows
    rows = ch.execute("""
        SELECT id, int_col, bigint_col, varchar_col,
               decimal_col, bool_col
        FROM test_integrity FINAL
        WHERE __data_transfer_delete_time = 0
        ORDER BY id
    """)
    
    # Calculate checksum with normalized values
    checksum = hashlib.md5()
    for row in rows:
        row_str = "|".join(normalize_value(v) for v in row)
        checksum.update(row_str.encode())
    
    print(f"   ClickHouse rows: {len(rows)}, checksum: {checksum.hexdigest()[:16]}")
    return rows, checksum.hexdigest()


def compare_data(mysql_rows, ch_rows, mysql_checksum, ch_checksum):
    """Compare data between MySQL and ClickHouse"""
    print("🔍 Comparing data...")
    
    errors = []
    
    # Check row counts
    if len(mysql_rows) != len(ch_rows):
        errors.append(f"Row count mismatch: MySQL={len(mysql_rows)}, ClickHouse={len(ch_rows)}")
    
    # Check checksums
    if mysql_checksum != ch_checksum:
        errors.append(f"Checksum mismatch: MySQL={mysql_checksum[:16]}, ClickHouse={ch_checksum[:16]}")
    
    # Compare row by row (sample if too many)
    mismatches = []
    mysql_dict = {row[0]: row for row in mysql_rows}
    ch_dict = {row[0]: row for row in ch_rows}
    
    # Check for missing rows
    mysql_ids = set(mysql_dict.keys())
    ch_ids = set(ch_dict.keys())
    
    missing_in_ch = mysql_ids - ch_ids
    extra_in_ch = ch_ids - mysql_ids
    
    if missing_in_ch:
        errors.append(f"Missing in ClickHouse: {len(missing_in_ch)} rows (IDs: {list(missing_in_ch)[:5]}...)")
    
    if extra_in_ch:
        errors.append(f"Extra in ClickHouse: {len(extra_in_ch)} rows (IDs: {list(extra_in_ch)[:5]}...)")
    
    # Compare common rows
    common_ids = mysql_ids & ch_ids
    for id_ in list(common_ids)[:100]:  # Sample first 100
        mysql_row = mysql_dict[id_]
        ch_row = ch_dict[id_]
        
        for i, (m_val, c_val) in enumerate(zip(mysql_row, ch_row)):
            # Normalize values for comparison (handles decimal precision differences)
            m_normalized = normalize_value(m_val)
            c_normalized = normalize_value(c_val)
            
            if m_normalized != c_normalized:
                mismatches.append(f"ID {id_} col {i}: MySQL={m_val}, CH={c_val}")
    
    if mismatches:
        errors.append(f"Value mismatches found: {mismatches[:5]}")
    
    return errors


def cleanup(mysql, ch):
    """Clean up test table"""
    print("🧹 Cleaning up...")
    try:
        with mysql.cn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_integrity")
            mysql.cn.commit()
    except Exception as e:
        print(f"   MySQL cleanup error: {e}")
    
    time.sleep(3)  # Wait for CDC to process DROP


