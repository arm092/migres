"""
Pytest configuration and shared fixtures for migres tests.
"""

import sys
import os
import time
import subprocess
import tempfile
import shutil
import pytest

# Add parent directory to path to import modules
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from migres.clients.mysql import MySQLClient
from migres.clients.clickhouse import CHClient
from migres.config import load_config
from migres.buffer import BufferDB
from migres.notifications import initialize_notifications, notify_cdc_shutdown


def _test_config_path():
    return os.environ.get(
        "MIGRES_TEST_CONFIG",
        os.path.join(os.path.dirname(__file__), "config.test.yml"),
    )


def _ensure_test_dirs(cfg):
    for key in ("state_file", "buffer_file"):
        path = cfg.get(key)
        if path:
            d = os.path.dirname(path)
            if d:
                os.makedirs(d, exist_ok=True)


@pytest.fixture(scope="session")
def config():
    """Load and return test configuration (never production config.yml)."""
    path = _test_config_path()
    if not os.path.exists(path):
        pytest.skip(f"Test config not found: {path}")
    cfg = load_config(path)
    _ensure_test_dirs(cfg)
    return cfg


def _db_available(cfg):
    try:
        mysql = MySQLClient(cfg["mysql"])
        mysql.connect()
        mysql.close()
        ch = CHClient(cfg["clickhouse"], cfg.get("migration", {}))
        ch.execute("SELECT 1")
        ch.close()
        return True
    except Exception as e:
        return False


@pytest.fixture(scope="session")
def mysql_client(config):
    """Create MySQL client connection (session-scoped)"""
    if not _db_available(config):
        pytest.skip("MySQL/ClickHouse test databases are not available")
    mysql = MySQLClient(config["mysql"])
    mysql.connect()
    yield mysql
    mysql.close()


@pytest.fixture(scope="session")
def clickhouse_client(config):
    """Create ClickHouse client connection (session-scoped)"""
    if not _db_available(config):
        pytest.skip("MySQL/ClickHouse test databases are not available")
    ch = CHClient(config["clickhouse"], config.get("migration", {}))
    yield ch
    ch.client.disconnect()


@pytest.fixture(scope="function")
def db_connections(mysql_client, clickhouse_client, config):
    """Setup database connections and initialize notifications (function-scoped)"""
    initialize_notifications(
        config.get("notifications", {}),
        config.get("environment", "test")
    )
    yield mysql_client, clickhouse_client, config


@pytest.fixture(scope="function")
def migres_process(config):
    """Start migres process and yield it, stopping on teardown"""
    print("▶️ Starting migres process...")
    python_exe = sys.executable
    cfg_path = _test_config_path()

    process = subprocess.Popen(
        [python_exe, "migres.py", "--config", cfg_path],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    time.sleep(5)

    if process.poll() is not None:
        output = process.stdout.read().decode() if process.stdout else ""
        print(f"   ⚠️ Migres process exited with code {process.returncode}")
        print(f"   Output: {output[:2000]}")
        raise RuntimeError(f"Migres failed to start: {output[:500] if output else 'Unknown error'}")

    print("   ✅ Migres process running")
    yield process

    print("🛑 Stopping migres process...")
    notify_cdc_shutdown("Test complete")
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
    if process.stdout:
        process.stdout.close()
    print("✅ Migres process stopped")


def wait_for_cdc_sync(timeout=60, cfg=None):
    """Wait for CDC to process all events"""
    print(f"⏳ Waiting for CDC sync (max {timeout}s)...")
    buf = BufferDB(cfg=cfg) if cfg else BufferDB()
    start = time.time()

    while time.time() - start < timeout:
        stats = buf.get_queue_stats()
        if stats['raw_events'] == 0 and stats['prepared_queries'] == 0:
            print("✅ CDC queues are empty")
            time.sleep(2)
            return True
        print(f"   Queue: raw={stats['raw_events']}, prepared={stats['prepared_queries']}")
        time.sleep(1)

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
        time.sleep(1)

    print(f"⚠️ Timeout waiting for table '{table_name}' in ClickHouse")
    return False


def wait_for_cdc(timeout=60, cfg=None):
    return wait_for_cdc_sync(timeout=timeout, cfg=cfg)


def wait_for_table(ch, table_name, timeout=60):
    return wait_for_table_in_clickhouse(ch, table_name, timeout)


@pytest.fixture(scope="function")
def clean_test_table(mysql_client, clickhouse_client):
    yield
    print("🧹 Cleaning up test tables...")
    time.sleep(1)


def get_batch_delay_seconds(config):
    """Producer flush interval (formerly batch_delay_seconds, removed in 3.0.0)."""
    return config.get("migration", {}).get("cdc", {}).get("producer_flush_interval", 5)


def wait_for_batch_delay(config, multiplier=1.5):
    delay = get_batch_delay_seconds(config)
    wait_time = max(delay * multiplier, 5)
    print(f"⏳ Waiting {wait_time:.1f}s for producer flush (producer_flush_interval={delay}s)...")
    time.sleep(wait_time)


def optimize_clickhouse_table(ch, table_name, wait_after=3):
    try:
        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table_name}` FINAL")
        if wait_after > 0:
            time.sleep(wait_after)
        return True
    except Exception as e:
        print(f"   Optimization warning: {e}")
        return False


def get_clickhouse_count_reliable(ch, table_name, timeout=60):
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
        except Exception:
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
