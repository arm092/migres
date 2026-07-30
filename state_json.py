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
        self._ensure_parent_dir()
        # Ensure state file exists, but do not cache whole content in memory.
        if not os.path.exists(self.path):
            with self._lock:
                self._flush_unlocked(self._default_state())

    @staticmethod
    def _default_state():
        return {"binlog": None, "tables": {}}

    @staticmethod
    def _normalize_state(data):
        if not isinstance(data, dict):
            return StateJson._default_state()
        if "binlog" not in data:
            data["binlog"] = None
        if "tables" not in data or not isinstance(data.get("tables"), dict):
            data["tables"] = {}
        return data

    def _ensure_parent_dir(self):
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)

    def _load_unlocked(self):
        if not os.path.exists(self.path):
            return self._default_state()
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return self._normalize_state(json.load(f))
        except (IOError, json.JSONDecodeError) as e:
            log.exception("Failed to load state file; starting fresh: %s", e)
            return self._default_state()

    def _flush_unlocked(self, data):
        # atomic write
        self._ensure_parent_dir()
        dirpath = os.path.dirname(self.path) or "."
        fd, tmppath = tempfile.mkstemp(prefix="state_", suffix=".tmp", dir=dirpath)
        os.close(fd)
        with open(tmppath, "w", encoding="utf-8") as f:
            json.dump(self._normalize_state(data), f, indent=2)
        os.replace(tmppath, self.path)

    def save(self):
        with self._lock:
            try:
                self._flush_unlocked(self._load_unlocked())
            except (IOError, TypeError) as e:
                log.exception("Failed to write state: %s", e)

    # binlog
    def set_binlog(self, file, pos):
        with self._lock:
            data = self._load_unlocked()
            data["binlog"] = {"file": file, "pos": int(pos)}
            self._flush_unlocked(data)

    def get_binlog(self):
        with self._lock:
            return self._load_unlocked().get("binlog")

    # table state helpers
    def get_table(self, table):
        with self._lock:
            data = self._load_unlocked()
            return data["tables"].get(table, {"status": "pending", "last_pk": None, "rows_processed": 0})

    def set_table(self, table, state_dict):
        with self._lock:
            data = self._load_unlocked()
            data["tables"][table] = state_dict
            self._flush_unlocked(data)

    def set_table_last_pk(self, table, last_pk):
        with self._lock:
            data = self._load_unlocked()
            t = data["tables"].get(table, {"status": "pending", "last_pk": None, "rows_processed": 0})
            t["last_pk"] = last_pk
            t["status"] = "in_progress"
            data["tables"][table] = t
            self._flush_unlocked(data)

    def incr_table_rows(self, table, inc):
        with self._lock:
            data = self._load_unlocked()
            t = data["tables"].get(table, {"status": "pending", "last_pk": None, "rows_processed": 0})
            t["rows_processed"] = t.get("rows_processed", 0) + int(inc)
            t["status"] = "in_progress"
            data["tables"][table] = t
            self._flush_unlocked(data)

    def mark_table_done(self, table):
        with self._lock:
            data = self._load_unlocked()
            t = data["tables"].get(table, {"status": "pending", "last_pk": None, "rows_processed": 0})
            t["status"] = "done"
            data["tables"][table] = t
            self._flush_unlocked(data)
