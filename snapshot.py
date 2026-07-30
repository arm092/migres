import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from decimal import Decimal
import mysql.connector
from mysql.connector import Error as MySQLError
from clickhouse_driver.errors import Error as CHError
from mysql_client import MySQLClient
from clickhouse_client import CHClient
from schema_and_ddl import build_table_ddl, quote_ident
from state_json import StateJson

log = logging.getLogger(__name__)


class SnapshotError(Exception):
    """Raised when one or more snapshot table workers fail."""
    pass


def _normalize_mysql_value(v):
    """Convert MySQL driver values to ClickHouse-friendly Python types."""
    import json
    if v is None:
        return None
    if isinstance(v, timedelta):
        total = int(v.total_seconds())
        sign = "-" if total < 0 else ""
        total = abs(total)
        h, rem = divmod(total, 3600)
        m, s = divmod(rem, 60)
        return f"{sign}{h:02d}:{m:02d}:{s:02d}"
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8")
        except UnicodeDecodeError:
            return v.decode("latin-1", errors="replace")
    if isinstance(v, set):
        return ",".join(sorted(str(x) for x in v))
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, Decimal):
        return v
    return v


def _normalize_row(row):
    return tuple(_normalize_mysql_value(v) for v in row)


def _process_table_worker(table, cfg, state: StateJson):
    """
    Worker function executed in parallel for a single table.
    Each worker creates its own DB connections and starts a REPEATABLE READ transaction.
    """
    mysql_cfg = cfg["mysql"]
    ch_cfg = cfg["clickhouse"]
    mig_cfg = cfg.get("migration", {})
    batch = int(mig_cfg.get("batch_rows", 5000))

    mysql_client = MySQLClient(mysql_cfg)
    cn = mysql_client.connect()
    mysql_client.start_repeatable_snapshot()
    ch = CHClient(ch_cfg, mig_cfg)

    try:
        tstate = state.get_table(table)
        if tstate.get("status") == "done":
            log.info("Worker: skipping %s (already done)", table)
            return
        resume_in_progress = (
            tstate.get("status") == "in_progress" and
            (tstate.get("last_pk") is not None or int(tstate.get("rows_processed", 0)) > 0)
        )

        cols_meta, pk_cols = mysql_client.get_table_columns_and_pk(table)
        mysql_cols = [c["COLUMN_NAME"] for c in cols_meta]
        log.info("Worker: %s columns: %s; pk: %s", table, mysql_cols, pk_cols)

        ddl, insert_cols = build_table_ddl(table, cols_meta, pk_cols, mig_cfg)
        qtable = quote_ident(table)
        qdb = quote_ident(ch.db)

        if resume_in_progress:
            exists = False
            try:
                exists_res = ch.execute(f"EXISTS TABLE {qdb}.{qtable}")
                exists = bool(exists_res and exists_res[0][0] == 1)
            except CHError:
                exists = False

            if exists:
                log.info("Worker: resuming in-progress table %s without dropping ClickHouse table", table)
            else:
                log.warning("Worker: in-progress table %s missing in ClickHouse, restarting table from scratch", table)
                state.set_table(table, {"status": "in_progress", "last_pk": None, "rows_processed": 0})
                try:
                    ch.execute(f"DROP TABLE IF EXISTS {qtable}")
                except CHError as e:
                    log.warning("Worker: drop failed for %s: %s", table, e)
                log.info("Worker: creating ClickHouse table %s ...", table)
                ch.execute(ddl)
                log.info("Worker: ClickHouse table ensured %s", table)
        else:
            try:
                ch.execute(f"DROP TABLE IF EXISTS {qtable}")
                log.info("Worker: dropped existing ClickHouse table %s (if any)", table)
            except CHError as e:
                log.exception("Worker: failed to drop ClickHouse table %s (continuing): %s", table, e)
            log.info("Worker: creating ClickHouse table %s ...", table)
            ch.execute(ddl)
            log.info("Worker: ClickHouse table ensured %s", table)

        use_pk_method = False
        pk_col = None
        if pk_cols and len(pk_cols) == 1:
            pkname = pk_cols[0]
            pk_meta = next((c for c in cols_meta if c["COLUMN_NAME"] == pkname), None)
            raw_dtype = (pk_meta or {}).get("DATA_TYPE", "")
            if isinstance(raw_dtype, bytes):
                pk_dtype = raw_dtype.decode('utf-8').lower()
            else:
                pk_dtype = str(raw_dtype).lower()
            if pk_meta:
                log.info(
                    "Worker: detected PK %s data_type=%s column_type=%s",
                    pkname,
                    pk_dtype,
                    str(pk_meta.get("COLUMN_TYPE"))
                )
            if pk_meta and pk_dtype in ("tinyint", "smallint", "mediumint", "int", "integer", "bigint"):
                use_pk_method = True
                pk_col = pkname
            else:
                log.info(
                    "Worker: falling back to offset for table %s (pk dtype: %s)",
                    table,
                    pk_dtype or "<unknown>"
                )

        data_cols = insert_cols[:-2]
        if use_pk_method:
            log.info("Worker: using PK method for table %s on %s", table, pk_col)
            last_pk = state.get_table(table).get("last_pk", None)
            while True:
                rows = mysql_client.fetch_rows_by_pk(table, data_cols, pk_col, last_pk, batch)
                if not rows:
                    break
                out_rows = []
                for r in rows:
                    commit_ns = time.time_ns()
                    out_rows.append(tuple(list(_normalize_row(r)) + [commit_ns, 0]))
                ch.insert_rows(table, insert_cols, out_rows)
                last_row = rows[-1]
                try:
                    pk_index = data_cols.index(pk_col)
                    last_pk_value = last_row[pk_index]
                except ValueError:
                    last_pk_value = None
                state.set_table_last_pk(table, last_pk_value)
                last_pk = last_pk_value
                state.incr_table_rows(table, len(out_rows))
                log.info("Worker: table %s inserted %d rows, last_pk=%s", table, len(out_rows), str(last_pk_value))
        else:
            # Deterministic ORDER BY: PK columns if any, else all columns
            order_columns = list(pk_cols) if pk_cols else list(data_cols)
            if not pk_cols:
                log.warning(
                    "Worker: table %s has no PK; using ORDER BY all columns for offset pagination",
                    table,
                )
            log.info("Worker: using offset pagination for table %s (ORDER BY %s)", table, order_columns)
            offset = state.get_table(table).get("rows_processed", 0)
            while True:
                rows = mysql_client.fetch_stream_with_offset(
                    table, data_cols, offset, batch, order_columns=order_columns
                )
                if not rows:
                    break
                out_rows = []
                for r in rows:
                    commit_ns = time.time_ns()
                    out_rows.append(tuple(list(_normalize_row(r)) + [commit_ns, 0]))
                ch.insert_rows(table, insert_cols, out_rows)
                offset += len(out_rows)
                state.incr_table_rows(table, len(out_rows))
                log.info("Worker: table %s inserted %d rows (offset=%d)", table, len(out_rows), offset)

        state.mark_table_done(table)
        log.info("Worker: table %s migrated successfully", table)
    except (MySQLError, CHError, IOError, OSError, ValueError) as e:
        log.exception("Worker: error while processing table %s: %s", table, e)
        raise
    finally:
        try:
            cn.commit()
        except MySQLError as e:
            log.warning("Worker: failed to commit for table %s: %s", table, e)
        mysql_client.close()


def run_snapshot(cfg):
    mysql_cfg = cfg["mysql"]
    mig_cfg = cfg.get("migration", {})
    workers = int(mig_cfg.get("workers", 4))
    state_file = cfg.get("state_file")

    state = StateJson(state_file)

    master_mysql = MySQLClient(mysql_cfg)
    master_cn = master_mysql.connect()

    existing_binlog = state.get_binlog()
    if existing_binlog and existing_binlog.get("file") and existing_binlog.get("pos") is not None:
        file = existing_binlog["file"]
        pos = int(existing_binlog["pos"])
        log.info("Resuming snapshot. Keeping existing binlog position: %s:%d", file, pos)
    else:
        master_status = master_mysql.show_master_status()
        if not master_status:
            log.warning("Binary log status returned nothing. Is binlog enabled?")
        else:
            file, pos = master_status
            state.set_binlog(file, pos)
            log.info("Starting snapshot with binlog position: %s:%d", file, pos)

    include = mysql_cfg.get("include_tables") or []
    exclude = mysql_cfg.get("exclude_tables") or []
    tables = master_mysql.list_tables(include, exclude)
    log.info("Tables to snapshot (count=%d): %s", len(tables), tables)

    failures = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_process_table_worker, t, cfg, state): t for t in tables}
        for fut in as_completed(futures):
            tbl = futures[fut]
            try:
                fut.result()
            except Exception as e:
                log.exception("Table %s failed in worker: %s", tbl, e)
                failures.append((tbl, e))

    try:
        master_cn.commit()
    except MySQLError as e:
        log.warning("Failed to commit master connection: %s", e)
    master_mysql.close()

    if failures:
        names = ", ".join(t for t, _ in failures)
        raise SnapshotError(f"Snapshot failed for {len(failures)} table(s): {names}")

    log.info("Snapshot completed for all tables.")
