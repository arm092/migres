"""
End-to-end test: spin up MySQL + ClickHouse via docker compose, run migres
(snapshot + CDC), verify typed rows and DML/DDL replication.
"""

import os
import sys
import time
import subprocess
import shutil
from decimal import Decimal
from datetime import date, datetime

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

COMPOSE_FILE = os.path.join(ROOT, "docker-compose.test.yml")
TEST_CONFIG = os.path.join(ROOT, "test", "config.test.yml")
TMP_DIR = os.path.join(ROOT, "test", ".tmp")


def _docker_available():
    try:
        r = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=15,
        )
        return r.returncode == 0
    except Exception:
        return False


def _compose(*args, check=True):
    cmd = ["docker", "compose", "-f", COMPOSE_FILE, *args]
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=check)


def _wait_mysql(host, port, user, password, database, timeout=120):
    from mysql_client import MySQLClient
    cfg = {"host": host, "port": port, "user": user, "password": password, "database": database}
    start = time.time()
    last = None
    while time.time() - start < timeout:
        try:
            c = MySQLClient(cfg)
            c.connect()
            c.assert_cdc_binlog_settings()
            c.close()
            return
        except Exception as e:
            last = e
            time.sleep(2)
    raise RuntimeError(f"MySQL not ready: {last}")


def _wait_ch(host, port, timeout=120):
    from clickhouse_client import CHClient
    start = time.time()
    last = None
    while time.time() - start < timeout:
        try:
            ch = CHClient(
                {"host": host, "port": port, "user": "default", "password": "", "database": "migres_dst"},
                {"clickhouse_timezone": "UTC"},
            )
            ch.execute("SELECT 1")
            ch.close()
            return
        except Exception as e:
            last = e
            time.sleep(2)
    raise RuntimeError(f"ClickHouse not ready: {last}")


@pytest.fixture(scope="module")
def e2e_stack():
    if not _docker_available():
        pytest.skip("Docker is not available")

    # Clean local state
    if os.path.exists(TMP_DIR):
        shutil.rmtree(TMP_DIR, ignore_errors=True)
    os.makedirs(TMP_DIR, exist_ok=True)

    # Keep containers up across runs for faster iteration; recreate if missing
    _compose("up", "-d")
    try:
        _wait_mysql("127.0.0.1", 3307, "migres", "migrespass", "migres_src", timeout=180)
        subprocess.run(
            [
                "docker", "exec", "migres-mysql-test",
                "mysql", "-uroot", "-prootpass", "-e",
                "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'migres'@'%'; FLUSH PRIVILEGES;",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        _wait_ch("127.0.0.1", 9001, timeout=180)
        yield
    finally:
        # Do not tear down containers here — allows re-runs; CI can compose down separately
        pass


@pytest.mark.e2e
@pytest.mark.slow
def test_end_to_end_snapshot_and_cdc(e2e_stack):
    from config import load_config
    from mysql_client import MySQLClient
    from clickhouse_client import CHClient
    from buffer import BufferDB

    cfg = load_config(TEST_CONFIG)
    mysql = MySQLClient(cfg["mysql"])
    mysql.connect()
    ch = CHClient(cfg["clickhouse"], cfg.get("migration", {}))

    table = "e2e_all_types"
    table2 = "e2e_new_table"

    # Cleanup leftovers
    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"DROP TABLE IF EXISTS {table2}")
        mysql.cn.commit()
    try:
        ch.execute(f"DROP TABLE IF EXISTS `{ch.db}`.`{table}`")
        ch.execute(f"DROP TABLE IF EXISTS `{ch.db}`.`{table2}`")
    except Exception:
        pass

    # Create typed table and seed snapshot rows
    with mysql.cn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                ti TINYINT,
                si SMALLINT,
                bi BIGINT,
                uti TINYINT UNSIGNED,
                f FLOAT,
                d DOUBLE,
                dec_col DECIMAL(10,2),
                dt DATE,
                dtt DATETIME,
                vc VARCHAR(100),
                txt TEXT,
                en ENUM('a','b','c'),
                nn INT NULL
            ) ENGINE=InnoDB
        """)
        cur.execute(
            f"""
            INSERT INTO {table}
            (id, ti, si, bi, uti, f, d, dec_col, dt, dtt, vc, txt, en, nn)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                1, -5, -100, -9999999999, 200,
                1.5, 2.5, Decimal("123.45"),
                date(2024, 1, 15), datetime(2024, 1, 15, 12, 30, 45),
                "hello 🚀", "long text " + ("x" * 200),
                "b", None,
            ),
        )
        cur.execute(
            f"""
            INSERT INTO {table}
            (id, ti, si, bi, uti, f, d, dec_col, dt, dtt, vc, txt, en, nn)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                2, 0, 0, 0, 0,
                0.0, 0.0, Decimal("0.00"),
                date(1970, 1, 1), datetime(1970, 1, 1, 0, 0, 0),
                "", "",
                "a", 0,
            ),
        )
        mysql.cn.commit()

    # Start migres; log to a file (not PIPE) so a full stdout buffer cannot deadlock the child.
    log_path = os.path.join(TMP_DIR, "e2e_migres.log")
    log_fh = open(log_path, "w", encoding="utf-8", errors="replace")
    proc = subprocess.Popen(
        [sys.executable, "-u", "migres.py", "--config", TEST_CONFIG],
        cwd=ROOT,
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        text=True,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )

    def _tail_log(n=4000):
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                return f.read()[-n:]
        except Exception:
            return ""

    try:
        # Wait until snapshot has loaded both seed rows (not just table EXISTS)
        deadline = time.time() + 180
        rows = []
        while time.time() < deadline:
            if proc.poll() is not None:
                pytest.fail(f"migres exited early (code={proc.returncode}): {_tail_log()}")
            try:
                exists = ch.execute(f"EXISTS TABLE `{ch.db}`.`{table}`")
                if exists and exists[0][0] == 1:
                    ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
                    rows = ch.execute(
                        f"SELECT id, ti, si, bi, uti, dec_col, dt, vc, en, nn, "
                        f"__data_transfer_delete_time "
                        f"FROM `{ch.db}`.`{table}` FINAL ORDER BY id"
                    )
                    if len(rows) >= 2:
                        break
            except Exception:
                pass
            time.sleep(2)
        else:
            pytest.fail(f"Timed out waiting for snapshot rows in ClickHouse (have {len(rows)})")

        buf = BufferDB(cfg=cfg)
        # Wait until CDC producer is alive and idle
        time.sleep(2)
        for _ in range(30):
            if buf.get_queue_stats()["raw_events"] == 0 and buf.get_queue_stats()["prepared_queries"] == 0:
                break
            time.sleep(1)

        by_id = {r[0]: r for r in rows}
        assert by_id[1][1] == -5
        assert by_id[1][4] == 200
        assert Decimal(str(by_id[1][5])) == Decimal("123.45")
        assert by_id[1][7] == "hello 🚀"
        assert by_id[1][8] == "b"
        assert by_id[1][9] is None
        assert by_id[1][10] == 0

        def wait_until(pred, timeout=90, label="condition"):
            start = time.time()
            while time.time() - start < timeout:
                if proc.poll() is not None:
                    pytest.fail(f"migres died while waiting for {label}: {_tail_log(2000)}")
                try:
                    if pred():
                        return True
                except Exception:
                    pass
                time.sleep(1)
            try:
                print("Queue stats on timeout:", buf.get_queue_stats())
                print("Prepared:", buf.fetch_prepared_queries_batch(limit=5))
            except Exception as e:
                print("Could not dump buffer:", e)
            print("--- migres log (tail) ---")
            print(_tail_log(3000))
            pytest.fail(f"Timeout waiting for {label}")

        def queues_idle():
            stats = buf.get_queue_stats()
            return stats["raw_events"] == 0 and stats["prepared_queries"] == 0

        # CDC: INSERT (full column list so binlog row image is complete)
        with mysql.cn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {table}
                (id, ti, si, bi, uti, f, d, dec_col, dt, dtt, vc, txt, en, nn)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    3, 7, 1, 2, 3, 1.0, 2.0, Decimal("9.99"),
                    date(2024, 2, 1), datetime(2024, 2, 1, 10, 0, 0),
                    "cdc-insert", "txt", "c", None,
                ),
            )
            mysql.cn.commit()
        wait_until(
            lambda: bool(ch.execute(
                f"SELECT id FROM `{ch.db}`.`{table}` FINAL WHERE id=3 AND __data_transfer_delete_time=0"
            )),
            label="cdc insert id=3",
        )
        wait_until(queues_idle, label="queues after insert")


        # CDC: UPDATE
        with mysql.cn.cursor() as cur:
            cur.execute(f"UPDATE {table} SET vc=%s WHERE id=%s", ("updated", 1))
            mysql.cn.commit()
        wait_until(queues_idle, label="queues after update")
        wait_until(
            lambda: (ch.execute(
                f"SELECT vc FROM `{ch.db}`.`{table}` FINAL WHERE id=1 AND __data_transfer_delete_time=0"
            ) or [[None]])[0][0] == "updated",
            label="cdc update id=1",
        )

        # CDC: DELETE
        with mysql.cn.cursor() as cur:
            cur.execute(f"DELETE FROM {table} WHERE id=%s", (2,))
            mysql.cn.commit()
        wait_until(queues_idle, label="queues after delete")
        wait_until(
            lambda: bool(ch.execute(
                f"SELECT id FROM `{ch.db}`.`{table}` FINAL WHERE id=2 AND __data_transfer_is_deleted=1"
            )),
            label="cdc soft-delete id=2",
        )

        # CDC: ALTER ADD COLUMN
        with mysql.cn.cursor() as cur:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN extra VARCHAR(50) DEFAULT 'x'")
            mysql.cn.commit()
        wait_until(queues_idle, label="queues after alter")
        wait_until(
            lambda: "extra" in {row[0] for row in ch.execute(f"DESCRIBE TABLE `{ch.db}`.`{table}`")},
            label="cdc add column extra",
        )

        # CDC: CREATE + INSERT new table
        with mysql.cn.cursor() as cur:
            cur.execute(f"CREATE TABLE {table2} (id INT PRIMARY KEY, name VARCHAR(50))")
            cur.execute(f"INSERT INTO {table2} (id, name) VALUES (1, 'new')")
            mysql.cn.commit()
        wait_until(queues_idle, label="queues after create")
        wait_until(
            lambda: bool(ch.execute(
                f"SELECT name FROM `{ch.db}`.`{table2}` FINAL WHERE id=1 AND __data_transfer_delete_time=0"
            )),
            label="cdc new table row",
        )

        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        time.sleep(1)

        r1 = ch.execute(
            f"SELECT vc FROM `{ch.db}`.`{table}` FINAL WHERE id=1 AND __data_transfer_delete_time=0"
        )
        assert r1 and r1[0][0] == "updated"

        r3 = ch.execute(
            f"SELECT id, vc, dec_col FROM `{ch.db}`.`{table}` FINAL WHERE id=3 AND __data_transfer_delete_time=0"
        )
        assert r3 and r3[0][0] == 3 and r3[0][1] == "cdc-insert"

        r2 = ch.execute(
            f"SELECT __data_transfer_is_deleted FROM `{ch.db}`.`{table}` FINAL WHERE id=2"
        )
        assert r2 and int(r2[0][0]) == 1

        n2 = ch.execute(
            f"SELECT name FROM `{ch.db}`.`{table2}` FINAL WHERE id=1 AND __data_transfer_delete_time=0"
        )
        assert n2 and n2[0][0] == "new"

        cols = {row[0] for row in ch.execute(f"DESCRIBE TABLE `{ch.db}`.`{table}`")}
        assert "extra" in cols

        # Poison query: should go to failed_queries, process stays up
        buf.commit_prepared_queries(
            [{
                "sql": (
                    f"INSERT INTO `{ch.db}`.`{table}` "
                    f"(`id`, `vc`, `__data_transfer_commit_time`, `__data_transfer_delete_time`) VALUES"
                ),
                "params": [["bad-id", "poison", 1, 0]],
                "group_id": "poison",
                "schema": "migres_src",
                "table": table,
            }],
            [],
        )
        with mysql.cn.cursor() as cur:
            cur.execute(f"INSERT INTO {table} (id, vc, en) VALUES (100, 'after-poison', 'a')")
            mysql.cn.commit()

        for _ in range(60):
            stats = buf.get_queue_stats()
            if stats["prepared_queries"] == 0 and stats["raw_events"] == 0:
                break
            time.sleep(1)

        stats = buf.get_queue_stats()
        assert stats["failed_queries"] >= 1
        assert proc.poll() is None, "migres died after poison query"

        ch.execute(f"OPTIMIZE TABLE `{ch.db}`.`{table}` FINAL")
        r100 = ch.execute(
            f"SELECT vc FROM `{ch.db}`.`{table}` FINAL WHERE id=100 AND __data_transfer_delete_time=0"
        )
        assert r100 and r100[0][0] == "after-poison"

        # DROP TABLE
        with mysql.cn.cursor() as cur:
            cur.execute(f"DROP TABLE {table2}")
            mysql.cn.commit()
        for _ in range(60):
            exists2 = ch.execute(f"EXISTS TABLE `{ch.db}`.`{table2}`")
            if exists2 and exists2[0][0] == 0:
                break
            time.sleep(1)
        exists2 = ch.execute(f"EXISTS TABLE `{ch.db}`.`{table2}`")
        assert exists2 and exists2[0][0] == 0

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        try:
            log_fh.close()
        except Exception:
            pass
        print("--- migres log (tail) ---")
        print(_tail_log(3000))
        mysql.close()
        ch.close()
