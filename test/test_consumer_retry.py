#!/usr/bin/env python3
"""
Consumer failure handling tests.

Verifies that permanent (data/type) errors move queries to failed_queries
and do not stop the CDC pipeline, while good rows continue to replicate.
"""

import time
import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from migres.buffer import BufferDB
from conftest import wait_for_cdc_sync, wait_for_table_in_clickhouse, get_batch_delay_seconds


@pytest.mark.integration
@pytest.mark.unit
def test_move_to_failed_unit(tmp_path):
    """Unit: BufferDB.move_to_failed relocates poison queries."""
    buf_path = tmp_path / "buffer.db"
    cfg = {"buffer_file": str(buf_path)}
    buf = BufferDB(cfg=cfg)

    buf.commit_prepared_queries(
        [{
            "sql": "INSERT INTO `db`.`t` (`id`) VALUES",
            "params": [[1], ["bad"]],
            "group_id": "g1",
            "schema": "db",
            "table": "t",
        }],
        [],
    )
    queries = buf.fetch_prepared_queries_batch(limit=10)
    assert len(queries) == 1

    buf.move_to_failed(queries, "type conversion failed")
    assert buf.fetch_prepared_queries_batch(limit=10) == []
    stats = buf.get_queue_stats()
    assert stats["failed_queries"] == 1
    assert stats["prepared_queries"] == 0


@pytest.mark.integration
@pytest.mark.slow
def test_consumer_moves_poison_query_to_failed(db_connections, migres_process):
    """Integration: inject a poison prepared query; pipeline continues."""
    mysql, ch, cfg = db_connections
    table = "test_consumer_retry"
    batch_delay = get_batch_delay_seconds(cfg)

    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                id INT PRIMARY KEY,
                data VARCHAR(100)
            )
        """)
        mysql.cn.commit()

    time.sleep(max(batch_delay + 3, 8))
    assert wait_for_table_in_clickhouse(ch, table, timeout=90)

    with mysql.cn.cursor() as cur:
        for i in range(1, 11):
            cur.execute(f"INSERT INTO {table} (id, data) VALUES (%s, %s)", (i, f"data_{i}"))
        mysql.cn.commit()

    assert wait_for_cdc_sync(timeout=120, cfg=cfg)

    # Inject a poison query that will fail type conversion / CH insert
    buf = BufferDB(cfg=cfg)
    buf.commit_prepared_queries(
        [{
            "sql": f"INSERT INTO `{ch.db}`.`{table}` (`id`, `data`, `__data_transfer_commit_time`, `__data_transfer_delete_time`) VALUES",
            "params": [["not-an-int", "poison", 1, 0]],
            "group_id": "poison",
            "schema": cfg["mysql"]["database"],
            "table": table,
        }],
        [],
    )

    # Also insert a good row that should still replicate
    with mysql.cn.cursor() as cur:
        cur.execute(f"INSERT INTO {table} (id, data) VALUES (%s, %s)", (999, "after_poison"))
        mysql.cn.commit()

    time.sleep(max(batch_delay * 3, 8))
    assert wait_for_cdc_sync(timeout=120, cfg=cfg)

    stats = buf.get_queue_stats()
    assert stats["failed_queries"] >= 1, f"Expected failed_queries >= 1, got {stats}"

    # Pipeline should still be alive (fixture checks process)
    assert migres_process.poll() is None, "migres process died after poison query"

    # Good row should be present
    rows = ch.execute(
        f"SELECT id, data FROM `{ch.db}`.`{table}` FINAL "
        f"WHERE id = 999 AND __data_transfer_delete_time = 0"
    )
    assert rows and rows[0][0] == 999

    with mysql.cn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mysql.cn.commit()
