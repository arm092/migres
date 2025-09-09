import logging
log = logging.getLogger(__name__)

def map_mysql_to_ch_type(column):
    """
    column: dict from information_schema (COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, ...)
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
        # using DateTime; could be DateTime64(3) for ms precision
        return wrap("DateTime")

    if data_type == "time":
        return wrap("String")

    # Strings / text / blobs / enum / json
    if data_type in ("char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "set", "json"):
        return wrap("String")
    if "blob" in data_type or "binary" in data_type:
        return wrap("String")

    # fallback
    return wrap("String")


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
        ch_type = map_mysql_to_ch_type(col)
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

    # decide order_by: prefer 'id' column if exists, else primary key, else synthesized
    order_by_cols = []
    if "id" in mysql_col_names:
        order_by_cols = ["id", "__data_transfer_commit_time"]
    elif pk_columns:
        order_by_cols = pk_columns + ["__data_transfer_commit_time"]
    elif synthesized:
        order_by_cols = [synthesized, "__data_transfer_commit_time"]
    else:
        order_by_cols = ["__data_transfer_commit_time"]

    order_by = ", ".join([f"`{c}`" for c in order_by_cols])

    # engine
    if engine.lower().startswith("replacing"):
        engine_sql = f"ENGINE = ReplacingMergeTree(__data_transfer_commit_time)\nORDER BY ({order_by})"
    else:
        engine_sql = f"ENGINE = MergeTree()\nORDER BY ({order_by})"

    cols_sql = ",\n  ".join(col_defs)
    ddl = f"CREATE TABLE IF NOT EXISTS `{table}` (\n  {cols_sql}\n) {engine_sql}"
    insertable_columns = mysql_col_names + ["__data_transfer_commit_time", "__data_transfer_delete_time"]
    return ddl, insertable_columns
