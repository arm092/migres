import json
import os
import tempfile
import threading
import logging

log = logging.getLogger(__name__)

class StateJson:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        self._data = {
            "binlog": None,
            "tables": {}
        }
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
                log.info("Loaded state from %s", self.path)
            except (IOError, json.JSONDecodeError) as e:
                log.exception("Failed to load state file; starting fresh: %s", e)
                self._data = {"binlog": None, "tables": {}}
        else:
            d = os.path.dirname(self.path)
            if d:
                os.makedirs(d, exist_ok=True)
            self._flush()

    def _flush(self):
        # atomic write
        dirpath = os.path.dirname(self.path) or "."
        fd, tmppath = tempfile.mkstemp(prefix="state_", suffix=".tmp", dir=dirpath)
        os.close(fd)
        with open(tmppath, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)
        os.replace(tmppath, self.path)

    def save(self):
        with self._lock:
            try:
                self._flush()
            except (IOError, TypeError) as e:
                log.exception("Failed to write state: %s", e)

    # binlog
    def set_binlog(self, file, pos):
        with self._lock:
            self._data["binlog"] = {"file": file, "pos": int(pos)}
            self._flush()

    def get_binlog(self):
        with self._lock:
            return self._data.get("binlog")

    # table state helpers
    def get_table(self, table):
        with self._lock:
            return self._data["tables"].get(table, {"status": "pending", "last_pk": None, "rows_processed": 0})

    def set_table(self, table, state_dict):
        with self._lock:
            self._data["tables"][table] = state_dict
            self._flush()

    def set_table_last_pk(self, table, last_pk):
        with self._lock:
            t = self._data["tables"].get(table, {"status": "pending", "last_pk": None, "rows_processed": 0})
            t["last_pk"] = last_pk
            t["status"] = "in_progress"
            self._data["tables"][table] = t
            self._flush()

    def incr_table_rows(self, table, inc):
        with self._lock:
            t = self._data["tables"].get(table, {"status": "pending", "last_pk": None, "rows_processed": 0})
            t["rows_processed"] = t.get("rows_processed", 0) + int(inc)
            t["status"] = "in_progress"
            self._data["tables"][table] = t
            self._flush()

    def mark_table_done(self, table):
        with self._lock:
            t = self._data["tables"].get(table, {"status": "pending", "last_pk": None, "rows_processed": 0})
            t["status"] = "done"
            self._data["tables"][table] = t
            self._flush()
