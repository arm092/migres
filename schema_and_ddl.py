import logging
log = logging.getLogger(__name__)

def map_mysql_to_ch_type(column, mig_cfg=None):
    """
    column: dict from information_schema (COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, ...)
    mig_cfg: migration config dict for timezone settings
    """
    def to_str(val):
        if isinstance(val, (bytes, bytearray)):
            return val.decode("utf-8")
        return str(val) if val is not None else ""

    mysql_type = to_str(column.get("COLUMN_TYPE")).lower()
    data_type = to_str(column.get("DATA_TYPE")).lower()
    nullable = (to_str(column.get("IS_NULLABLE")).upper() == "YES")

    def wrap(t):
        return f"Nullable({t})" if nullable else t

    # Numeric / int types
    if data_type in ("tinyint", "smallint", "mediumint", "int", "integer", "bigint"):
        unsigned = "unsigned" in mysql_type
        sizes = {"tinyint":8, "smallint":16, "mediumint":32, "int":32, "integer":32, "bigint":64}
        bits = sizes.get(data_type, 32)
        base = f"UInt{bits}" if unsigned else f"Int{bits}"
        return wrap(base)

    if data_type == "float":
        return wrap("Float32")
    if data_type in ("double", "real"):
        return wrap("Float64")

    if data_type in ("decimal", "numeric"):
        # parse precision/scale: column.COLUMN_TYPE looks like decimal(p,s)
        import re
        m = re.search(r"\((\d+),\s*(\d+)\)", mysql_type)
        if m:
            p, s = m.group(1), m.group(2)
            return wrap(f"Decimal({p},{s})")
        else:
            return wrap("Decimal(38,10)")

    if data_type == "date":
        return wrap("Date")
    if data_type in ("datetime", "timestamp"):
        # Use DateTime64 with timezone support for proper analytical queries
        if mig_cfg and mig_cfg.get("clickhouse_timezone"):
            timezone = mig_cfg["clickhouse_timezone"]
            return wrap(f"DateTime64(3, '{timezone}')")
        else:
            return wrap("DateTime64(3)")

    if data_type == "time":
        return wrap("String")

    # Strings / text / blobs / enum / json
    if data_type in ("char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "set", "json"):
        return wrap("String")
    if "blob" in data_type or "binary" in data_type:
        return wrap("String")

    # fallback
    return wrap("String")


def _default_expr_for_column(col, ch_type):
    # Map MySQL COLUMN_DEFAULT to ClickHouse DEFAULT expression when feasible.
    default_val = col.get("COLUMN_DEFAULT")
    if default_val is None:
        return None
    # For NULL defaults, ClickHouse default is implicit for Nullable types.
    # For numeric and strings, we can use a literal.
    dt = str(col.get("DATA_TYPE") or "").lower()
    if dt in ("tinyint","smallint","mediumint","int","integer","bigint","float","double","real","decimal","numeric"):
        return str(default_val)
    if dt in ("date","datetime","timestamp"):
        # For DateTime64, use parseDateTimeBestEffort to handle various formats
        if "DateTime64" in ch_type:
            return f"parseDateTimeBestEffort('{str(default_val)}')"
        # For Date/DateTime types, use toDate/parseDateTimeBestEffort
        return f"toDateTime('{str(default_val)}')" if "DateTime" in ch_type else f"toDate('{str(default_val)}')"
    # Treat others as strings
    # Ensure single quotes escaped
    s = str(default_val).replace("'", "\\'")
    return f"'{s}'"


def build_table_ddl(table, columns_meta, pk_columns, mig_cfg):
    """
    columns_meta: list of dicts from information_schema for this table
    pk_columns: list of pk column names
    Returns: DDL string and list of insertable columns (mysql columns order)
    """
    low = bool(mig_cfg.get("low_cardinality_strings", True))
    engine = mig_cfg.get("ddl_engine", "ReplacingMergeTree")

    col_defs = []
    mysql_col_names = []
    synthesized = None

    for col in columns_meta:
        name = col["COLUMN_NAME"]
        mysql_col_names.append(name)
        ch_type = map_mysql_to_ch_type(col, mig_cfg)
        default_expr = _default_expr_for_column(col, ch_type)
        if default_expr is not None:
            col_defs.append(f"`{name}` {ch_type} DEFAULT {default_expr}")
        else:
            col_defs.append(f"`{name}` {ch_type}")

    # Add transfer columns (insertable)
    col_defs.append("`__data_transfer_commit_time` UInt64")
    col_defs.append("`__data_transfer_delete_time` UInt64 DEFAULT 0")

    # Add generated is_deleted (MATERIALIZED)
    col_defs.append("`__data_transfer_is_deleted` UInt8 MATERIALIZED if(__data_transfer_delete_time != 0, 1, 0)")

    # If there is no primary key we will add a synthesized materialized PK __migres_pk
    if not pk_columns:
        # build concat of toString(col1),'|',toString(col2) ...
        parts = [f"toString(`{c}`)" for c in mysql_col_names]
        concat_expr = " || '|' || ".join(parts) if parts else "''"
        # materialized cityHash64(concat(...))
        col_defs.insert(0, f"`__migres_pk` UInt64 MATERIALIZED cityHash64({concat_expr})")
        synthesized = "__migres_pk"

    # decide ORDER BY key columns (sorting key)
    # For ReplacingMergeTree(version), ORDER BY must NOT include the version column;
    # it must be the natural primary key so replacements collapse correctly.
    if "id" in mysql_col_names:
        key_cols = ["id"]
    elif pk_columns:
        key_cols = list(pk_columns)
    elif synthesized:
        key_cols = [synthesized]
    else:
        # Fallback: if absolutely no identifier, use commit time as the sole key
        key_cols = ["__data_transfer_commit_time"]

    order_by = ", ".join([f"`{c}`" for c in key_cols])

    # engine
    if engine.lower().startswith("replacing"):
        engine_sql = f"ENGINE = ReplacingMergeTree(__data_transfer_commit_time)\nORDER BY ({order_by})"
    else:
        # For plain MergeTree we can include commit time to keep append order deterministic
        non_replacing_order_by = ", ".join([f"`{c}`" for c in (key_cols + (["__data_transfer_commit_time"] if "__data_transfer_commit_time" not in key_cols else []))])
        engine_sql = f"ENGINE = MergeTree()\nORDER BY ({non_replacing_order_by})"

    cols_sql = ",\n  ".join(col_defs)
    ddl = f"CREATE TABLE IF NOT EXISTS `{table}` (\n  {cols_sql}\n) {engine_sql}"
    insertable_columns = mysql_col_names + ["__data_transfer_commit_time", "__data_transfer_delete_time"]
    return ddl, insertable_columns


def ensure_clickhouse_columns(ch_client, table, desired_columns):
    """
    Ensure that ClickHouse table `table` has all columns in desired_columns.
    desired_columns: list of dicts {name, type_sql, default_expr(optional)} or tuples (name, type_sql)
    Adds missing columns with ALTER TABLE ... ADD COLUMN if needed.
    """
    try:
        existing = ch_client.execute(f"DESCRIBE TABLE `{table}`")
        existing_names = {row[0] for row in existing}
    except Exception:
        # If table doesn't exist, caller should create with DDL
        return
    for item in desired_columns:
        if isinstance(item, tuple):
            name, type_sql = item
            default_expr = None
        else:
            name = item["name"]
            type_sql = item["type_sql"]
            default_expr = item.get("default_expr")
        if name not in existing_names:
            try:
                if default_expr:
                    ch_client.execute(f"ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS `{name}` {type_sql} DEFAULT {default_expr}")
                else:
                    ch_client.execute(f"ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS `{name}` {type_sql}")
                log.info("Added missing column %s to %s", name, table)
            except Exception:
                log.exception("Failed to add column %s to %s", name, table)
