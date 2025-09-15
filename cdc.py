import logging
import time

from mysql_client import MySQLClient
from clickhouse_client import CHClient
from schema_and_ddl import build_table_ddl, ensure_clickhouse_columns
from state_json import StateJson

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from pymysqlreplication.event import QueryEvent

log = logging.getLogger(__name__)


def _serialize_event(event):
    try:
        etype = event.__class__.__name__
        base = {
            "type": etype,
            "schema": getattr(event, "schema", None),
            "table": getattr(event, "table", None),
        }
        if hasattr(event, "rows"):
            # rows is a list of dictionaries (values/after_values)
            base["rows"] = event.rows
        if hasattr(event, "query"):
            base["query"] = getattr(event, "query", None)
        # low-level position if available
        if hasattr(event, "packet"):
            try:
                base["log_pos"] = getattr(event.packet, "log_pos", None)
            except (AttributeError, KeyError):
                pass
        return base
    except (AttributeError, TypeError, ValueError):
        return {"type": str(type(event)), "repr": repr(event)}


def _build_insertable(cols_meta, pk_cols, mig_cfg):
    ddl, insert_cols = build_table_ddl("_dummy", cols_meta, pk_cols, mig_cfg)
    # We only need the column list, ddl will be rebuilt for real table
    return insert_cols


def _ensure_table_and_columns(mysql_client: MySQLClient, ch: CHClient, table: str, mig_cfg):
    cols_meta, pk_cols = mysql_client.get_table_columns_and_pk(table)
    ddl, insert_cols = build_table_ddl(table, cols_meta, pk_cols, mig_cfg)
    ch.execute(ddl)
    # ensure all columns exist (in case of ALTER ADD COLUMN)
    desired = []
    for col in cols_meta:
        name = col["COLUMN_NAME"]
        ch_type = _map_with_low_cardinality(col, mig_cfg)
        # Attach default expression when possible so ALTER ADD COLUMN sets defaults in CH
        from schema_and_ddl import _default_expr_for_column
        try:
            default_expr = _default_expr_for_column(col, ch_type)
        except (ValueError, TypeError):
            default_expr = None
        desired.append({"name": name, "type_sql": ch_type, "default_expr": default_expr})
    # transfer columns
    desired.extend([
        ("__data_transfer_commit_time", "UInt64"),
        ("__data_transfer_delete_time", "UInt64")
    ])
    ensure_clickhouse_columns(ch, table, desired)
    return insert_cols, cols_meta, pk_cols


def _wait_for_mutations(ch: CHClient, table: str, timeout_seconds: int = 180, poll_interval: float = 0.5):
    """
    Wait until all mutations for given table are finished, up to timeout.
    """
    import time as _t
    deadline = _t.time() + timeout_seconds
    dbname = ch.db
    while True:
        try:
            rows = ch.execute(
                "SELECT count() FROM system.mutations WHERE database = %(db)s AND table = %(tbl)s AND is_done = 0",
                {"db": dbname, "tbl": table}
            )
            pending = rows[0][0] if rows and rows[0] else 0
        except (AttributeError, TypeError, ValueError):
            pending = 0
        if pending == 0:
            return True
        if _t.time() >= deadline:
            log.warning("CDC: timeout waiting for mutations to finish on %s.%s (pending=%d)", dbname, table, pending)
            return False
        _t.sleep(poll_interval)


def _rebuild_entire_table_with_type_change(mysql_client: MySQLClient, ch: CHClient, table: str, col_name: str, target_ch_type: str, mig_cfg):
    """
    Create a new table with the desired schema and swap. Used as last resort when column type refuses to change.
    """
    cols_meta, pk_cols = mysql_client.get_table_columns_and_pk(table)
    ddl, insert_cols = build_table_ddl(f"{table}__migres_new", cols_meta, pk_cols, mig_cfg)
    ch.execute(ddl)
    db = ch.db
    # Build SELECT list mapping from old table
    select_exprs = []
    for c in insert_cols:
        if c in ("__data_transfer_commit_time", "__data_transfer_delete_time"):
            # Will set in SELECT from existing columns
            select_exprs.append(f"`{c}`")
        elif c == col_name:
            if target_ch_type.endswith("String)") or target_ch_type == "String":
                select_exprs.append(f"toString(`{c}`)")
            else:
                select_exprs.append(f"CAST(`{c}` AS {target_ch_type})")
        else:
            select_exprs.append(f"`{c}`")
    select_sql = ", ".join(select_exprs)
    ch.execute(
        f"INSERT INTO `{db}`.`{table}__migres_new` ({', '.join('`'+c+'`' for c in insert_cols)}) SELECT {select_sql} FROM `{db}`.`{table}`"
    )
    _wait_for_mutations(ch, table, timeout_seconds=300)
    # Swap tables
    ch.execute(f"RENAME TABLE `{db}`.`{table}` TO `{db}`.`{table}__migres_old`, `{db}`.`{table}__migres_new` TO `{db}`.`{table}`")
    # Drop old
    ch.execute(f"DROP TABLE IF EXISTS `{db}`.`{table}__migres_old`")


def _map_with_low_cardinality(col, mig_cfg):
    from schema_and_ddl import map_mysql_to_ch_type
    ch_type = map_mysql_to_ch_type(col, mig_cfg)
    if bool(mig_cfg.get("low_cardinality_strings", True)) and ch_type.endswith("String") and not ch_type.startswith("LowCardinality("):
        ch_type = ch_type.replace("Nullable(String)", "Nullable(LowCardinality(String))") if ch_type.startswith("Nullable(") else f"LowCardinality({ch_type})"
    return ch_type


def run_cdc(cfg):
    mysql_cfg = cfg["mysql"]
    ch_cfg = cfg["clickhouse"]
    mig_cfg = cfg.get("migration", {})
    cdc_cfg = mig_cfg.get("cdc", {})

    heartbeat_seconds = int(cdc_cfg.get("heartbeat_seconds", 5))
    checkpoint_interval_rows = int(cdc_cfg.get("checkpoint_interval_rows", 1000))
    checkpoint_interval_seconds = int(cdc_cfg.get("checkpoint_interval_seconds", 5))

    state_file = cfg.get("state_file")
    checkpoint_file = cfg.get("checkpoint_file")
    state = StateJson(state_file)

    mysql_client = MySQLClient(mysql_cfg)
    mysql_client.connect()
    ch = CHClient(ch_cfg, mig_cfg)

    # read start position
    binlog = state.get_binlog()
    log.info("Starting CDC from %s", binlog if binlog else "current master position")

    server_id = int(cdc_cfg.get("server_id", 4379))
    only_schemas = [mysql_cfg["database"]]
    included = set(mysql_cfg.get("include_tables") or [])
    excluded = set(mysql_cfg.get("exclude_tables") or [])
    only_tables = list(included) if included else None
    ignored_tables = list(excluded) if excluded else None

    # Map table -> insertable columns cache
    table_cache = {}

    stream = BinLogStreamReader(
        connection_settings={
            "host": mysql_cfg["host"],
            "port": mysql_cfg.get("port", 3306),
            "user": mysql_cfg["user"],
            "passwd": mysql_cfg["password"],
        },
        server_id=server_id,
        blocking=True,
        resume_stream=True,
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent],
        only_schemas=only_schemas,
        only_tables=only_tables,
        ignored_tables=ignored_tables,
        log_file=(binlog["file"] if binlog else None),
        log_pos=(binlog["pos"] if binlog else None),
        slave_heartbeat=heartbeat_seconds,
    )

    log.info("CDC stream configured: resume_stream=%s, blocking=%s, only_schemas=%s, only_tables=%s, ignored_tables=%s, server_id=%s, log_file=%s, log_pos=%s",
             True, True, only_schemas, only_tables, ignored_tables, server_id, (binlog["file"] if binlog else None), (binlog["pos"] if binlog else None))

    last_checkpoint_time = time.time()
    rows_since_checkpoint = 0

    try:
        for event in stream:
            try:
                schema = getattr(event, "schema", None)
                table = getattr(event, "table", None)
                # Dump full event payload before processing
                try:
                    import json
                    payload = _serialize_event(event)
                    log.info("CDC EVENT: %s", json.dumps(payload, default=str)[:20000])
                except (TypeError, ValueError, AttributeError):
                    log.exception("CDC: failed to serialize event for logging")
                log.info("CDC event: %s schema=%s table=%s", event.__class__.__name__, schema, table)
                if isinstance(event, QueryEvent):
                    # DDL: best-effort schema sync
                    query_text = (event.query or "")
                    query_lower = query_text.lower()
                    
                    # Extract table name and schema for any DDL operation
                    try:
                        import re
                        # Only process DDL operations (CREATE/ALTER/DROP TABLE), skip other queries like BEGIN, COMMIT, etc.
                        if not re.match(r"(?:alter|create|drop)\s+table\s+", query_lower):
                            continue  # Skip non-DDL queries
                        
                        # Match optional schema qualification: ALTER/CREATE/DROP TABLE [IF EXISTS] `db`.`tbl` ... or db.tbl
                        m = re.search(r"(?:alter|create|drop)\s+table\s+(?:if\s+exists\s+)?(?:`?([a-zA-Z0-9_]+)`?\.)?`?([a-zA-Z0-9_]+)`?", query_lower)
                        if m:
                            matched_schema = m.group(1) if m.group(1) else (schema.decode() if isinstance(schema, (bytes, bytearray)) else (schema or mysql_cfg["database"]))
                            affected = m.group(2)
                            log.info("CDC: regex matched - schema: %s, table: %s", matched_schema, affected)
                        else:
                            matched_schema = (schema.decode() if isinstance(schema, (bytes, bytearray)) else (schema or mysql_cfg["database"]))
                            affected = table
                            log.warning("CDC: regex did not match for DDL query: %s", query_lower[:100])
                    except Exception:
                        matched_schema = (schema.decode() if isinstance(schema, (bytes, bytearray)) else (schema or mysql_cfg["database"]))
                        affected = table
                        log.exception("CDC: error extracting table name from query: %s", query_lower[:100])
                    
                    # Only react if the DDL targets our database and table of interest
                    if matched_schema == mysql_cfg["database"] and affected and (not included or affected in included) and affected not in excluded:
                        # Handle different DDL operations separately
                        if query_lower.startswith("create table"):
                            # CREATE TABLE: Create the table in ClickHouse
                            try:
                                log.info("CDC: detected CREATE TABLE for %s, creating table in ClickHouse", affected)
                                insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, affected, mig_cfg)
                                table_cache[affected] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                                log.info("CDC: created table %s in ClickHouse", affected)
                            except Exception:
                                log.exception("CDC: failed to create table %s in ClickHouse", affected)
                        
                        elif query_lower.startswith("drop table"):
                            # DROP TABLE: Drop the table from ClickHouse
                            try:
                                log.info("CDC: detected DROP TABLE for %s, dropping table in ClickHouse", affected)
                                ch.execute(f"DROP TABLE IF EXISTS `{affected}`")
                                # Remove from cache if it exists
                                if affected in table_cache:
                                    del table_cache[affected]
                                log.info("CDC: dropped table %s in ClickHouse", affected)
                            except Exception:
                                log.exception("CDC: failed to drop table %s in ClickHouse", affected)
                        
                        elif query_lower.startswith("alter table"):
                            # ALTER TABLE: Handle column operations
                            # Try to directly parse ADD COLUMN clause(s) and apply ALTER(s) to ClickHouse immediately
                            try:
                                import re as _re
                                # Capture name and full type segment up to next comma or end
                                add_clauses = [(m.group(1), m.group(2)) for m in _re.finditer(r"add\s+column\s+`?([a-zA-Z0-9_]+)`?\s+([^,]+?)(?=,\s*add\s+column|$)", query_lower, flags=_re.IGNORECASE | _re.DOTALL)]
                            except Exception:
                                add_clauses = []

                            # Parse CHANGE COLUMN (rename) clauses and apply direct renames
                            try:
                                import re as _re
                                change_clauses = [(m.group(1), m.group(2)) for m in _re.finditer(r"change\s+column\s+`?([a-zA-Z0-9_]+)`?\s+`?([a-zA-Z0-9_]+)`?\s+", query_lower, flags=_re.IGNORECASE)]
                            except Exception:
                                change_clauses = []

                            if change_clauses:
                                for old_name, new_name in change_clauses:
                                    try:
                                        ch.execute(f"ALTER TABLE `{affected}` RENAME COLUMN `{old_name}` TO `{new_name}`")
                                        log.info("CDC: renamed column %s -> %s on %s (direct RENAME)", old_name, new_name, affected)
                                    except Exception:
                                        log.exception("CDC: direct RENAME COLUMN failed for %s -> %s on %s", old_name, new_name, affected)

                            # Parse DROP COLUMN clauses and apply direct drops
                            try:
                                import re as _re
                                drop_clauses = [m.group(1) for m in _re.finditer(r"drop\s+column\s+`?([a-zA-Z0-9_]+)`?", query_lower, flags=_re.IGNORECASE)]
                            except Exception:
                                drop_clauses = []

                            if drop_clauses:
                                for col_name in drop_clauses:
                                    try:
                                        ch.execute(f"ALTER TABLE `{affected}` DROP COLUMN IF EXISTS `{col_name}`")
                                        log.info("CDC: dropped column %s on %s (direct DROP)", col_name, affected)
                                    except Exception:
                                        log.exception("CDC: direct DROP COLUMN failed for %s on %s", col_name, affected)

                            # Parse MODIFY COLUMN clauses and apply direct type/default changes
                            try:
                                import re as _re
                                modify_clauses = [(m.group(1), m.group(2)) for m in _re.finditer(r"modify\s+column\s+`?([a-zA-Z0-9_]+)`?\s+([^,]+?)(?=,\s*modify\s+column|$)", query_lower, flags=_re.IGNORECASE | _re.DOTALL)]
                            except Exception:
                                modify_clauses = []

                            if modify_clauses:
                                from schema_and_ddl import _default_expr_for_column
                                for col_name, col_def in modify_clauses:
                                    import re as _re2
                                    try:
                                        dt_match = _re2.match(r"\s*([a-z0-9]+)", col_def.strip())
                                        data_type = dt_match.group(1) if dt_match else col_def.strip().split()[0]
                                        coltype_core = _re2.split(r"\s+not\s+null|\s+null|\s+default\s+|\s+after\s+", col_def, flags=_re2.IGNORECASE)[0].strip()
                                        is_nullable = (" not null" not in col_def.lower())
                                        default_val = None
                                        dmatch = _re2.search(r"default\s+('[^']*'|\"[^\"]*\"|[^\s,]+)", col_def, flags=_re2.IGNORECASE)
                                        if dmatch:
                                            default_token = dmatch.group(1)
                                            if (default_token.startswith("'") and default_token.endswith("'")) or (default_token.startswith('"') and default_token.endswith('"')):
                                                default_val = default_token[1:-1]
                                            else:
                                                default_val = default_token
                                        col_meta = {
                                            "COLUMN_NAME": col_name,
                                            "COLUMN_TYPE": coltype_core,
                                            "DATA_TYPE": data_type,
                                            "IS_NULLABLE": "YES" if is_nullable else "NO",
                                            "COLUMN_DEFAULT": default_val,
                                        }
                                        log.info("CDC: Column metadata: DATA_TYPE - %s; COL_DEFAULT - %s", data_type, default_val)
                                        # Map to CH type; force strings to String/LowCardinality(String)
                                        ch_type = _map_with_low_cardinality(col_meta, mig_cfg)
                                        if data_type in ("char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "set", "json"):
                                            if bool(mig_cfg.get("low_cardinality_strings", True)):
                                                ch_type = "LowCardinality(String)"
                                            else:
                                                ch_type = "String"
                                        try:
                                            default_expr = _default_expr_for_column(col_meta, ch_type)
                                        except Exception:
                                            default_expr = None

                                        try:
                                            log.info("CDC: MODIFY target type for %s.%s -> %s", affected, col_name, ch_type)
                                            _wait_for_mutations(ch, affected, timeout_seconds=180)
                                            ch.execute(f"ALTER TABLE `{affected}` MODIFY COLUMN IF EXISTS `{col_name}` {ch_type}")
                                        except Exception:
                                            log.exception("CDC: direct MODIFY failed for %s on %s; attempting to rebuild", col_name, affected)
                                            # Fallback: rebuild column with cast
                                            tmp_name = f"__migres_tmp_{col_name}"
                                            _wait_for_mutations(ch, affected, timeout_seconds=180)
                                            ch.execute(f"ALTER TABLE `{affected}` ADD COLUMN IF NOT EXISTS `{tmp_name}` {ch_type}")
                                            # Cast old to new using toString()/CAST depending on target
                                            if ch_type.endswith("String)") or ch_type == "String":
                                                cast_expr = f"toString(`{col_name}`)"
                                            else:
                                                cast_expr = "CAST(`%s` AS %s)" % (col_name, ch_type)
                                            # Use UPDATE to backfill
                                            ch.execute(f"ALTER TABLE `{affected}` UPDATE `{tmp_name}` = {cast_expr} WHERE 1")
                                            _wait_for_mutations(ch, affected, timeout_seconds=180)
                                            ch.execute(f"ALTER TABLE `{affected}` DROP COLUMN IF EXISTS `{col_name}`")
                                            _wait_for_mutations(ch, affected, timeout_seconds=180)
                                            ch.execute(f"ALTER TABLE `{affected}` RENAME COLUMN `{tmp_name}` TO `{col_name}`")
                                            log.info("CDC: rebuilt column %s on %s (ADD tmp + UPDATE + DROP + RENAME)", col_name, affected)
                                            # Verify; if still wrong type due to engine quirks, rebuild the entire table as last resort
                                            try:
                                                # Wait a bit for mutations to complete and metadata to refresh
                                                import time as _t
                                                _t.sleep(1)
                                                desc_after = ch.execute(f"DESCRIBE TABLE `{affected}`")
                                                log.info("CDC: full DESCRIBE after rebuild: %s", [(d[0], d[1]) for d in desc_after if d[0] == col_name])
                                                type_now = None
                                                for d in desc_after:
                                                    if d[0] == col_name:
                                                        type_now = d[1]
                                                        break
                                                def _norm(t: str) -> str:
                                                    return (t or '').replace('Nullable(', '').replace('LowCardinality(', '').replace(')', '').strip().lower()
                                                log.info("CDC: verification after rebuild - current type: %s, target type: %s, normalized current: %s, normalized target: %s", 
                                                        type_now, ch_type, _norm(type_now), _norm(ch_type))
                                                if type_now is None or _norm(type_now) != _norm(ch_type):
                                                    log.warning("CDC: type still %s after rebuild for %s.%s, performing full table rebuild", type_now, affected, col_name)
                                                    _rebuild_entire_table_with_type_change(mysql_client, ch, affected, col_name, ch_type, mig_cfg)
                                                    log.info("CDC: completed full table rebuild for %s.%s", affected, col_name)
                                                else:
                                                    log.info("CDC: type verification passed after rebuild for %s.%s", affected, col_name)
                                            except Exception:
                                                log.exception("CDC: verification after column rebuild failed for %s on %s", col_name, affected)
                                        # Try to set/remove DEFAULT afterward
                                        try:
                                            log.info("CDC: Final step %s", f"ALTER TABLE `{affected}` MODIFY COLUMN `{col_name}` SET DEFAULT {default_expr}")
                                            if default_expr is not None:
                                                ch.execute(f"ALTER TABLE `{affected}` MODIFY COLUMN `{col_name}` DEFAULT {default_expr}")
                                            else:
                                                ch.execute(f"ALTER TABLE `{affected}` MODIFY COLUMN `{col_name}` REMOVE DEFAULT")
                                        except Exception:
                                            log.exception("CDC: default change not applied for %s on %s (may not be supported)", col_name, affected)
                                    except Exception:
                                        log.exception("CDC: MODIFY handling failed for %s on %s", col_name, affected)

                            if add_clauses:
                                from schema_and_ddl import _default_expr_for_column
                                for col_name, col_def in add_clauses:
                                    try:
                                        import re as _re2
                                        # Determine base DATA_TYPE token
                                        dt_match = _re2.match(r"\s*([a-z0-9]+)", col_def.strip())
                                        data_type = dt_match.group(1) if dt_match else col_def.strip().split()[0]
                                        # Extract core COLUMN_TYPE (keep size/attrs), strip null/default/after
                                        coltype_core = _re2.split(r"\s+not\s+null|\s+null|\s+default\s+|\s+after\s+", col_def, flags=_re2.IGNORECASE)[0].strip()
                                        is_nullable = (" not null" not in col_def.lower())
                                        # Extract default value if present
                                        default_val = None
                                        dmatch = _re2.search(r"default\s+('[^']*'|\"[^\"]*\"|[^\s,]+)", col_def, flags=_re2.IGNORECASE)
                                        if dmatch:
                                            default_token = dmatch.group(1)
                                            if (default_token.startswith("'") and default_token.endswith("'")) or (default_token.startswith('"') and default_token.endswith('"')):
                                                default_val = default_token[1:-1]
                                            else:
                                                default_val = default_token
                                        # Build minimal MySQL column meta
                                        col_meta = {
                                            "COLUMN_NAME": col_name,
                                            "COLUMN_TYPE": coltype_core,
                                            "DATA_TYPE": data_type,
                                            "IS_NULLABLE": "YES" if is_nullable else "NO",
                                            "COLUMN_DEFAULT": default_val,
                                        }
                                        ch_type = _map_with_low_cardinality(col_meta, mig_cfg)
                                        try:
                                            default_expr = _default_expr_for_column(col_meta, ch_type)
                                        except Exception:
                                            default_expr = None
                                        # Apply ALTER ADD COLUMN immediately
                                        if default_expr:
                                            ch.execute(f"ALTER TABLE `{affected}` ADD COLUMN IF NOT EXISTS `{col_name}` {ch_type} DEFAULT {default_expr}")
                                        else:
                                            ch.execute(f"ALTER TABLE `{affected}` ADD COLUMN IF NOT EXISTS `{col_name}` {ch_type}")
                                        log.info("CDC: added column %s to %s (direct ALTER)", col_name, affected)
                                    except Exception:
                                        log.exception("CDC: direct ADD COLUMN failed for %s on %s", col_name, affected)

                            # Also run generic ensure to cover any other DDL effects
                            insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, affected, mig_cfg)
                            # refresh cache so new columns are used immediately
                            table_cache[affected] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                    continue

                if not table:
                    log.info("CDC: skipping event without table: %s", event.__class__.__name__)
                    continue
                if included and table not in included:
                    log.info("CDC: skipping table %s (not in include_tables)", table)
                    continue
                if table in excluded:
                    log.info("CDC: skipping table %s (in exclude_tables)", table)
                    continue

                # Ensure table exists in CH and columns are up-to-date
                if table not in table_cache:
                    insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                    table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])

                insert_cols, mysql_cols = table_cache[table]

                if isinstance(event, WriteRowsEvent):
                    # If event contains columns unknown to our cache, refresh schema
                    try:
                        sample = event.rows[0]["values"] if event.rows else {}
                        incoming_cols = set(sample.keys())
                        cached_cols = set(insert_cols[:-2])
                        if incoming_cols and not incoming_cols.issubset(cached_cols):
                            log.info("CDC: detected new columns for %s, refreshing (writing) schema...", table)
                            insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                            table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                            cached_cols = set(insert_cols[:-2])
                            # If still missing (information_schema lag), retry a few times
                            if incoming_cols and not incoming_cols.issubset(cached_cols):
                                import time as _t
                                for _ in range(10):
                                    insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                                    table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                                    cached_cols = set(insert_cols[:-2])
                                    if incoming_cols.issubset(cached_cols):
                                        break
                                    _t.sleep(0.1)
                    except (AttributeError, KeyError, TypeError):
                        log.exception("CDC: failed to auto-refresh schema on INSERT; proceeding with current cache")
                    rows = []
                    for row in event.rows:
                        vals = [row["values"].get(col) for col in insert_cols[:-2]]
                        commit_ns = time.time_ns()
                        rows.append(tuple(vals + [commit_ns, 0]))
                    try:
                        ch.insert_rows(table, insert_cols, rows)
                    except Exception:
                        # If insert fails due to missing column(s), refresh schema and retry once
                        log.exception("CDC: insert failed, refreshing schema and retrying once for %s", table)
                        insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                        table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                        # Retry the insert with updated schema
                    ch.insert_rows(table, insert_cols, rows)
                    rows_since_checkpoint += len(rows)
                    log.info("CDC: inserted %d row(s) into %s (INSERT)", len(rows), table)

                elif isinstance(event, UpdateRowsEvent):
                    # Refresh if new columns appear in after_values
                    try:
                        sample = event.rows[0]["after_values"] if event.rows else {}
                        incoming_cols = set(sample.keys())
                        cached_cols = set(insert_cols[:-2])
                        if incoming_cols and not incoming_cols.issubset(cached_cols):
                            log.info("CDC: detected new columns for %s, refreshing (updating) schema...", table)
                            insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                            table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                            cached_cols = set(insert_cols[:-2])
                            # If still missing (information_schema lag), retry a few times
                            if incoming_cols and not incoming_cols.issubset(cached_cols):
                                import time as _t
                                for _ in range(10):
                                    insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                                    table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                                    cached_cols = set(insert_cols[:-2])
                                    if incoming_cols.issubset(cached_cols):
                                        break
                                    _t.sleep(0.1)
                    except Exception:
                        log.exception("CDC: failed to auto-refresh schema on UPDATE; proceeding with current cache")
                    rows = []
                    try:
                        debug_after = event.rows[0]["after_values"] if event.rows else {}
                        # Ensure new columns are inserted by rebuilding the values list against the latest insert_cols
                        log.info("CDC: update collist size=%d includes=%s", len(insert_cols[:-2]), list(insert_cols[:-2])[:50])
                        changed = {k: v for k, v in debug_after.items() if k not in (event.rows[0].get("before_values") or {}) or (event.rows[0].get("before_values") or {}).get(k) != v}
                        log.info("CDC: changed fields in UPDATE: %s", list(changed.items())[:20])
                    except Exception:
                        pass
                    for row in event.rows:
                        # Build values with explicit None for missing columns (so CH uses DEFAULT or NULL)
                        after_vals = row["after_values"]
                        vals = [after_vals[col] if col in after_vals else None for col in insert_cols[:-2]]
                        commit_ns = time.time_ns()
                        rows.append(tuple(vals + [commit_ns, 0]))
                    try:
                        ch.insert_rows(table, insert_cols, rows)
                    except Exception:
                        log.exception("CDC: insert failed, refreshing schema and retrying once for %s", table)
                        insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                        table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                        log.info("CDC: retrying with collist size=%d includes=%s", len(insert_cols[:-2]), list(insert_cols[:-2])[:50])
                        # Retry the insert with updated schema
                    ch.insert_rows(table, insert_cols, rows)
                    rows_since_checkpoint += len(rows)
                    log.info("CDC: inserted %d row(s) into %s (UPDATE->upsert)", len(rows), table)

                elif isinstance(event, DeleteRowsEvent):
                    rows = []
                    for row in event.rows:
                        # We write a delete tombstone: copy values if possible, set delete_time
                        vals = [row["values"].get(col) for col in insert_cols[:-2]]
                        commit_ns = time.time_ns()
                        rows.append(tuple(vals + [commit_ns, commit_ns]))
                    try:
                        ch.insert_rows(table, insert_cols, rows)
                    except Exception:
                        log.exception("CDC: insert failed, refreshing (deleting) schema and retrying once for %s", table)
                        insert_cols, cols_meta, pk_cols = _ensure_table_and_columns(mysql_client, ch, table, mig_cfg)
                        table_cache[table] = (insert_cols, [c["COLUMN_NAME"] for c in cols_meta])
                        # Retry the insert with updated schema
                    ch.insert_rows(table, insert_cols, rows)
                    rows_since_checkpoint += len(rows)
                    log.info("CDC: inserted %d tombstone row(s) into %s (DELETE)", len(rows), table)

                # periodic checkpoint
                now = time.time()
                if rows_since_checkpoint >= checkpoint_interval_rows or (now - last_checkpoint_time) >= checkpoint_interval_seconds:
                    f, p = stream.log_file, stream.log_pos
                    state.set_binlog(f, p)
                    try:
                        import os, json
                        os.makedirs(os.path.dirname(checkpoint_file) or ".", exist_ok=True)
                        with open(checkpoint_file, "w", encoding="utf-8") as fp:
                            json.dump({"binlog_file": f, "binlog_pos": p}, fp, indent=2)
                    except Exception:
                        log.exception("Failed to write checkpoint file")
                    rows_since_checkpoint = 0
                    last_checkpoint_time = now

            except Exception:
                log.exception("Error handling binlog event")
                time.sleep(1)
    finally:
        try:
            stream.close()
        except Exception:
            pass
        try:
            ch.close()
        except Exception:
            pass
        try:
            mysql_client.close()
        except Exception:
            pass


