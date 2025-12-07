"""
Pytest configuration and shared fixtures for migres tests.
"""

import sys
import os
import time
import subprocess
import pytest

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mysql_client import MySQLClient
from clickhouse_client import CHClient
from config import load_config
from buffer import BufferDB
from notifications import initialize_notifications, notify_cdc_shutdown


@pytest.fixture(scope="session")
def config():
    """Load and return configuration"""
    return load_config("config.yml")


@pytest.fixture(scope="session")
def mysql_client(config):
    """Create MySQL client connection (session-scoped)"""
    mysql = MySQLClient(config["mysql"])
    mysql.connect()
    yield mysql
    mysql.close()


@pytest.fixture(scope="session")
def clickhouse_client(config):
    """Create ClickHouse client connection (session-scoped)"""
    ch = CHClient(config["clickhouse"], config.get("migration", {}))
    yield ch
    ch.client.disconnect()


@pytest.fixture(scope="function")
def db_connections(mysql_client, clickhouse_client, config):
    """Setup database connections and initialize notifications (function-scoped)"""
    initialize_notifications(
        config.get("notifications", {}),
        config.get("environment", "prod")
    )
    yield mysql_client, clickhouse_client, config


@pytest.fixture(scope="function")
def migres_process():
    """Start migres process and yield it, stopping on teardown"""
    print("▶️ Starting migres process...")
    python_exe = sys.executable
    cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    process = subprocess.Popen(
        [python_exe, "migres.py", "--config", "config.yml"],
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    time.sleep(5)  # Wait for initialization
    
    # Check if process started successfully
    if process.poll() is not None:
        output = process.stdout.read().decode() if process.stdout else ""
        print(f"   ⚠️ Migres process exited with code {process.returncode}")
        print(f"   Output: {output[:2000]}")
        raise RuntimeError(f"Migres failed to start: {output[:500] if output else 'Unknown error'}")
    
    print("   ✅ Migres process running")
    
    yield process
    
    # Teardown: stop migres
    print("🛑 Stopping migres process...")
    notify_cdc_shutdown("Test complete")
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
    # Close stdout pipe to prevent ResourceWarning
    if process.stdout:
        process.stdout.close()
    print("✅ Migres process stopped")


def wait_for_cdc_sync(timeout=60):
    """Wait for CDC to process all events"""
    print(f"⏳ Waiting for CDC sync (max {timeout}s)...")
    
    buf = BufferDB()
    start = time.time()
    
    while time.time() - start < timeout:
        stats = buf.get_queue_stats()
        if stats['raw_events'] == 0 and stats['prepared_queries'] == 0:
            print("✅ CDC queues are empty")
            # Wait a bit more for final processing
            time.sleep(5)
            return True
        print(f"   Queue: raw={stats['raw_events']}, prepared={stats['prepared_queries']}")
        time.sleep(2)
    
    print("⚠️ Timeout waiting for CDC sync")
    return False


def wait_for_table_in_clickhouse(ch, table_name, timeout=60):
    """Wait for table to exist in ClickHouse"""
    print(f"⏳ Waiting for table '{table_name}' in ClickHouse (max {timeout}s)...")
    start = time.time()
    
    while time.time() - start < timeout:
        try:
            result = ch.execute(f"EXISTS TABLE `{ch.db}`.`{table_name}`")
            if result and result[0][0] == 1:
                print(f"✅ Table '{table_name}' exists in ClickHouse")
                return True
        except Exception as e:
            print(f"   Checking table existence: {e}")
        time.sleep(2)
    
    print(f"⚠️ Timeout waiting for table '{table_name}' in ClickHouse")
    return False


def wait_for_cdc(timeout=60):
    """Wait for CDC to process events (alias for wait_for_cdc_sync with retry logic)"""
    # Some tests use wait_for_cdc instead of wait_for_cdc_sync
    # This provides compatibility
    try:
        buf = BufferDB()
    except Exception as e:
        print(f"   Warning: Buffer DB issue: {e}, retrying...")
        time.sleep(3)
        try:
            buf = BufferDB()
        except Exception as e2:
            print(f"   Error: Could not open buffer DB after retry: {e2}")
            return False
    
    start = time.time()
    while time.time() - start < timeout:
        try:
            stats = buf.get_queue_stats()
            if stats['raw_events'] == 0 and stats['prepared_queries'] == 0:
                time.sleep(3)
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def wait_for_table(ch, table_name, timeout=60):
    """Wait for table to exist in ClickHouse (alias for wait_for_table_in_clickhouse)"""
    return wait_for_table_in_clickhouse(ch, table_name, timeout)


@pytest.fixture(scope="function")
def clean_test_table(mysql_client, clickhouse_client):
    """Fixture to clean up test tables after test"""
    yield
    # Cleanup happens after test
    print("🧹 Cleaning up test tables...")
    # This is a placeholder - individual tests should clean their own tables
    time.sleep(2)  # Wait for CDC to process any DROP statements


def get_batch_delay_seconds(config):
    """Get batch_delay_seconds from config, with default fallback"""
    return config.get("migration", {}).get("cdc", {}).get("batch_delay_seconds", 15)


def wait_for_batch_delay(config, multiplier=1.5):
    """
    Wait for batch delay to ensure events are committed to buffer.
    Uses config's batch_delay_seconds with a multiplier for safety.
    """
    delay = get_batch_delay_seconds(config)
    wait_time = max(delay * multiplier, 5)  # At least 5 seconds
    print(f"⏳ Waiting {wait_time:.1f}s for batch delay (batch_delay_seconds={delay}s)...")
    time.sleep(wait_time)


def optimize_clickhouse_table(ch, table_name, wait_after=3):
    """
    Optimize ClickHouse table to ensure merges complete.
    This is important for ReplacingMergeTree to show final state.
    """
    try:
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table_name}` FINAL")
        if wait_after > 0:
            time.sleep(wait_after)
        return True
    except Exception as e:
        print(f"   Optimization warning: {e}")
        return False


def get_clickhouse_count_reliable(ch, table_name, timeout=60):
    """
    Get row count from ClickHouse using reliable method for ReplacingMergeTree.
    Uses GROUP BY with argMax to correctly handle deleted rows.
    """
    if not wait_for_table_in_clickhouse(ch, table_name, timeout=10):
        return -1
    
    start = time.time()
    last_count = -1
    stable_count = 0
    
    while time.time() - start < timeout:
        try:
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
                if count == last_count:
                    stable_count += 1
                    if stable_count >= 2:
                        return count
                else:
                    stable_count = 0
                last_count = count
        except Exception as e:
            # Fallback to FINAL method
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
    
    return last_count if last_count >= 0 else -1

