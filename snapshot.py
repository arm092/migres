import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from mysql_client import MySQLClient
from clickhouse_client import CHClient
from schema_and_ddl import build_table_ddl
from state_json import StateJson

log = logging.getLogger(__name__)

def _process_table_worker(table, cfg, state: StateJson):
    """
    Worker function executed in parallel for a single table.
    Each worker creates its own DB connections.
    """
    mysql_cfg = cfg["mysql"]
    ch_cfg = cfg["clickhouse"]
    mig_cfg = cfg.get("migration", {})
    batch = int(mig_cfg.get("batch_rows", 5000))

    # Per-thread clients
    mysql = MySQLClient(mysql_cfg)
    cn = mysql.connect()
    ch = CHClient(ch_cfg, mig_cfg)

    try:
        # If not in progress, mark as in_progress
        tstate = state.get_table(table)
        if tstate.get("status") == "done":
            log.info("Worker: skipping %s (already done)", table)
            return

        # introspect columns & pk
        cols_meta, pk_cols = mysql.get_table_columns_and_pk(table)
        mysql_cols = [c["COLUMN_NAME"] for c in cols_meta]
        log.info("Worker: %s columns: %s; pk: %s", table, mysql_cols, pk_cols)

        # To avoid duplicate data when snapshot is re-run without prior state,
        # drop the ClickHouse table if it already exists before recreating it.
        try:
            ch.execute(f"DROP TABLE IF EXISTS `{table}`")
            log.info("Worker: dropped existing ClickHouse table %s (if any)", table)
        except Exception:
            log.exception("Worker: failed to drop ClickHouse table %s (continuing)", table)

        # build DDL and insertable columns
        ddl, insert_cols = build_table_ddl(table, cols_meta, pk_cols, mig_cfg)
        log.info("Worker: creating ClickHouse table %s ...", table)
        ch.execute(ddl)
        log.info("Worker: ClickHouse table ensured %s", table)

        # choose method
        use_pk_method = False
        pk_col = None
        if pk_cols and len(pk_cols) == 1:
            pkname = pk_cols[0]
            pk_meta = next((c for c in cols_meta if c["COLUMN_NAME"] == pkname), None)
            # Normalize DATA_TYPE for robust comparison - handle bytes objects
            raw_dtype = (pk_meta or {}).get("DATA_TYPE", "")
            if isinstance(raw_dtype, bytes):
                pk_dtype = raw_dtype.decode('utf-8').lower()
            else:
                pk_dtype = str(raw_dtype).lower()
            # Helpful debug: log PK column and its types
            if pk_meta:
                log.info(
                    "Worker: detected PK %s data_type=%s column_type=%s",
                    pkname,
                    pk_dtype,
                    str(pk_meta.get("COLUMN_TYPE"))
                )
            if pk_meta and pk_dtype in ("tinyint","smallint","mediumint","int","integer","bigint"):
                use_pk_method = True
                pk_col = pkname
            else:
                log.info(
                    "Worker: falling back to offset for table %s (pk dtype: %s)",
                    table,
                    pk_dtype or "<unknown>"
                )

        if use_pk_method:
            log.info("Worker: using PK method for table %s on %s", table, pk_col)
            last_pk = state.get_table(table).get("last_pk", None)
            while True:
                rows = mysql.fetch_rows_by_pk(table, insert_cols[:-2], pk_col, last_pk, batch)
                if not rows:
                    break
                out_rows = []
                for r in rows:
                    commit_ns = time.time_ns()
                    out_rows.append(tuple(list(r) + [commit_ns, 0]))
                ch.insert_rows(table, insert_cols, out_rows)
                # update last_pk = value of pk in last row
                last_row = rows[-1]
                # find index of pk in returned columns: since we selected insert_cols[:-2] order equals insert_cols[:-2]
                try:
                    pk_index = insert_cols[:-2].index(pk_col)
                    last_pk_value = last_row[pk_index]
                except ValueError:
                    last_pk_value = None
                # Persist and advance in-memory cursor to avoid re-reading same batch
                state.set_table_last_pk(table, last_pk_value)
                last_pk = last_pk_value
                state.incr_table_rows(table, len(out_rows))
                log.info("Worker: table %s inserted %d rows, last_pk=%s", table, len(out_rows), str(last_pk_value))
        else:
            log.info("Worker: using offset pagination for table %s", table)
            offset = state.get_table(table).get("rows_processed", 0)
            while True:
                rows = mysql.fetch_stream_with_offset(table, insert_cols[:-2], offset, batch)
                if not rows:
                    break
                out_rows = []
                for r in rows:
                    commit_ns = time.time_ns()
                    out_rows.append(tuple(list(r) + [commit_ns, 0]))
                ch.insert_rows(table, insert_cols, out_rows)
                offset += len(out_rows)
                state.incr_table_rows(table, len(out_rows))
                log.info("Worker: table %s inserted %d rows (offset=%d)", table, len(out_rows), offset)

        state.mark_table_done(table)
        log.info("Worker: table %s migrated successfully", table)
    except Exception:
        log.exception("Worker: error while processing table %s", table)
        raise
    finally:
        try:
            cn.commit()
        except Exception:
            pass
        mysql.close()

def run_snapshot(cfg):
    mysql_cfg = cfg["mysql"]
    mig_cfg = cfg.get("migration", {})
    workers = int(mig_cfg.get("workers", 4))
    checkpoint_file = cfg.get("checkpoint_file")
    state_file = cfg.get("state_file")

    state = StateJson(state_file)

    # Use a dedicated connection to fetch table list & master status
    master_mysql = MySQLClient(mysql_cfg)
    master_cn = master_mysql.connect()

    # 1) record binlog pos
    master_status = master_mysql.show_master_status()
    if not master_status:
        log.warning("Binary log status returned nothing. Is binlog enabled?")
    else:
        file, pos = master_status
        state.set_binlog(file, pos)
        try:
            with open(checkpoint_file, "w", encoding="utf-8") as f:
                import json
                json.dump({"binlog_file": file, "binlog_pos": pos}, f, indent=2)
            log.info("Wrote binlog checkpoint to %s: %s:%s", checkpoint_file, file, pos)
        except Exception:
            log.exception("Failed to write checkpoint file")

    # 2) We will not hold a single global transaction when processing tables in parallel.
    #    Instead, each worker opens its own connection and starts a REPEATABLE READ transaction
    #    on that connection (so each table snapshot is consistent on its connection).
    #    NOTE: snapshots across connections are not a globally atomic snapshot. See caveats below.
    #    Nevertheless, recording binlog position before snapshot and then starting Transferia
    #    from that position ensures changes after the recorded pos are streamed by Transferia.
    #
    # 3) determine tables (include/exclude logic)
    include = mysql_cfg.get("include_tables") or []
    exclude = mysql_cfg.get("exclude_tables") or []
    tables = master_mysql.list_tables(include, exclude)
    log.info("Tables to snapshot (count=%d): %s", len(tables), tables)

    # 4) process tables in parallel using a thread pool
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_process_table_worker, t, cfg, state): t for t in tables}
        for fut in as_completed(futures):
            tbl = futures[fut]
            try:
                fut.result()
            except Exception:
                log.exception("Table %s failed in worker", tbl)

    # 5) finalize
    try:
        master_cn.commit()
    except Exception:
        pass
    master_mysql.close()
    log.info("Snapshot completed for all tables.")
