"""Unit tests for BufferDB (sqlite tempfile, no live DBs)."""

import os
import sqlite3
import tempfile

import pytest

from buffer import BufferDB


pytestmark = pytest.mark.unit


@pytest.fixture
def buffer_db():
    # ignore_cleanup_errors: Windows may keep the sqlite handle briefly after close
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        path = os.path.join(tmp, "buffer.db")
        buf = BufferDB(cfg={"buffer_file": path})
        yield buf
        conn = getattr(buf.local, "conn", None)
        if conn is not None:
            conn.close()
            del buf.local.conn


def test_insert_fetch_and_last_committed_pos(buffer_db):
    assert buffer_db.get_last_committed_pos() == (None, None)

    buffer_db.insert_raw_events(
        [
            {
                "binlog_file": "mysql-bin.000001",
                "binlog_pos": 100,
                "schema": "db",
                "table": "t",
                "event_type": "write",
                "event_data": {"id": 1},
            },
            {
                "binlog_file": "mysql-bin.000001",
                "binlog_pos": 200,
                "schema": "db",
                "table": "t",
                "event_type": "update",
                "event_data": {"id": 2},
            },
        ]
    )

    rows = buffer_db.fetch_raw_events_batch(limit=10)
    assert len(rows) == 2
    assert rows[0]["binlog_pos"] == 100
    assert rows[0]["event_data"] == {"id": 1}
    assert rows[1]["binlog_pos"] == 200

    file_name, pos = buffer_db.get_last_committed_pos()
    assert file_name == "mysql-bin.000001"
    assert pos == 200


def test_commit_prepared_queries_atomicity(buffer_db):
    buffer_db.insert_raw_events(
        [
            {
                "binlog_file": "mysql-bin.000001",
                "binlog_pos": 10,
                "schema": "db",
                "table": "t",
                "event_type": "write",
                "event_data": {},
            }
        ]
    )
    events = buffer_db.fetch_raw_events_batch()
    assert len(events) == 1
    event_id = events[0]["id"]

    buffer_db.commit_prepared_queries(
        [
            {
                "sql": "INSERT INTO t VALUES",
                "params": [[1]],
                "group_id": "g1",
                "schema": "db",
                "table": "t",
            }
        ],
        [event_id],
    )

    assert buffer_db.fetch_raw_events_batch() == []
    prepared = buffer_db.fetch_prepared_queries_batch()
    assert len(prepared) == 1
    assert prepared[0]["sql"] == "INSERT INTO t VALUES"
    assert prepared[0]["params"] == [[1]]

    # Failure path: raise during DELETE so the whole transaction rolls back
    buffer_db.insert_raw_events(
        [
            {
                "binlog_file": "mysql-bin.000001",
                "binlog_pos": 20,
                "schema": "db",
                "table": "t",
                "event_type": "write",
                "event_data": {},
            }
        ]
    )
    new_events = buffer_db.fetch_raw_events_batch()
    new_id = new_events[0]["id"]
    before_prepared = len(buffer_db.fetch_prepared_queries_batch(limit=1000))

    real_get_conn = buffer_db._get_conn

    class _CursorWrap:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, parameters=()):
            if isinstance(sql, str) and sql.startswith("DELETE FROM raw_events"):
                raise sqlite3.OperationalError("forced delete failure")
            return self._cur.execute(sql, parameters)

        def executemany(self, sql, seq_of_parameters):
            return self._cur.executemany(sql, seq_of_parameters)

        def __getattr__(self, name):
            return getattr(self._cur, name)

    class _ConnWrap:
        def __init__(self, conn):
            self._conn = conn

        def cursor(self):
            return _CursorWrap(self._conn.cursor())

        def __getattr__(self, name):
            return getattr(self._conn, name)

    buffer_db._get_conn = lambda: _ConnWrap(real_get_conn())
    try:
        with pytest.raises(sqlite3.OperationalError, match="forced delete failure"):
            buffer_db.commit_prepared_queries(
                [{"sql": "INSERT INTO t VALUES", "params": [[2]], "schema": "db", "table": "t"}],
                [new_id],
            )
    finally:
        buffer_db._get_conn = real_get_conn

    remaining = buffer_db.fetch_raw_events_batch()
    assert len(remaining) == 1
    assert remaining[0]["id"] == new_id
    assert len(buffer_db.fetch_prepared_queries_batch(limit=1000)) == before_prepared


def test_move_to_failed(buffer_db):
    buffer_db.commit_prepared_queries(
        [
            {
                "sql": "INSERT INTO bad VALUES",
                "params": [[1]],
                "group_id": "g",
                "schema": "db",
                "table": "bad",
            }
        ],
        [],
    )
    prepared = buffer_db.fetch_prepared_queries_batch()
    assert len(prepared) == 1

    buffer_db.move_to_failed(prepared, "clickhouse error")

    assert buffer_db.fetch_prepared_queries_batch() == []
    stats = buffer_db.get_queue_stats()
    assert stats["prepared_queries"] == 0
    assert stats["failed_queries"] == 1

    conn = buffer_db._get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, sql_query, error_reason FROM failed_queries")
    row = cur.fetchone()
    assert row[0] == prepared[0]["id"]
    assert row[1] == "INSERT INTO bad VALUES"
    assert "clickhouse error" in row[2]
