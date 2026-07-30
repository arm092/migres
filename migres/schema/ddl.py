import logging
import re

log = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_NUMERIC_DEFAULT_RE = re.compile(r"^-?\d+(\.\d+)?$")


def quote_ident(name: str) -> str:
    """Quote a SQL identifier with backticks, escaping embedded backticks.

    Raises ValueError for empty/None names. Prefer simple identifiers matching
    ^[A-Za-z_][A-Za-z0-9_]*$; others are still quoted safely by doubling `.
    """
    if name is None:
        raise ValueError("identifier is None")
    s = str(name)
    if not s:
        raise ValueError("identifier is empty")
    if not _IDENT_RE.match(s):
        log.warning("Unusual SQL identifier (will be quoted): %r", s)
    return "`" + s.replace("`", "``") + "`"


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
        sizes = {"tinyint": 8, "smallint": 16, "mediumint": 32, "int": 32, "integer": 32, "bigint": 64}
        bits = sizes.get(data_type, 32)
        base = f"UInt{bits}" if unsigned else f"Int{bits}"
        return wrap(base)

    if data_type == "float":
        return wrap("Float32")
    if data_type in ("double", "real"):
        return wrap("Float64")

    if data_type in ("decimal", "numeric"):
        m = re.search(r"\((\d+),\s*(\d+)\)", mysql_type)
        if m:
            p, s = m.group(1), m.group(2)
            return wrap(f"Decimal({p},{s})")
        else:
            return wrap("Decimal(38,10)")

    if data_type == "date":
        return wrap("Date")
    if data_type in ("datetime", "timestamp"):
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

    return wrap("String")


def _escape_sql_string(s: str) -> str:
    """Escape a value for use inside a single-quoted ClickHouse string literal."""
    return str(s).replace("\\", "\\\\").replace("'", "\\'")


def _default_expr_for_column(col, ch_type):
    # Map MySQL COLUMN_DEFAULT to ClickHouse DEFAULT expression when feasible.
    default_val = col.get("COLUMN_DEFAULT")
    if default_val is None:
        return None
    dt = str(col.get("DATA_TYPE") or "").lower()
    if dt in ("tinyint", "smallint", "mediumint", "int", "integer", "bigint", "float", "double", "real", "decimal", "numeric"):
        s = str(default_val).strip()
        if _NUMERIC_DEFAULT_RE.match(s):
            return s
        # Non-numeric default for a numeric column — fall through to string path
        return f"'{_escape_sql_string(s)}'"
    if dt in ("date", "datetime", "timestamp"):
        escaped = _escape_sql_string(default_val)
        if "DateTime64" in ch_type:
            return f"parseDateTimeBestEffort('{escaped}')"
        return f"toDateTime('{escaped}')" if "DateTime" in ch_type else f"toDate('{escaped}')"
    return f"'{_escape_sql_string(default_val)}'"


def map_with_low_cardinality(col, mig_cfg):
    """Map MySQL type to ClickHouse type, wrapping String in LowCardinality when enabled."""
    ch_type = map_mysql_to_ch_type(col, mig_cfg)
    # Match bare String or Nullable(String); endswith("String") fails for Nullable(...).
    if bool(mig_cfg.get("low_cardinality_strings", True)) and ch_type in ("String", "Nullable(String)"):
        if ch_type.startswith("Nullable("):
            ch_type = "Nullable(LowCardinality(String))"
        else:
            ch_type = f"LowCardinality({ch_type})"
    return ch_type


def binlog_position_key(file_name, pos):
    """Return a comparable key for binlog (file, pos) with numeric suffix comparison."""
    if not file_name:
        return ("", 0, 0)
    base = str(file_name)
    suffix = 0
    m = re.search(r"(\d+)$", base)
    if m:
        prefix = base[: m.start()]
        suffix = int(m.group(1))
        return (prefix, suffix, int(pos or 0))
    return (base, 0, int(pos or 0))


def strip_sql_leading_comments(query: str) -> str:
    """Strip leading whitespace and SQL comments from a statement."""
    if not query:
        return ""
    s = query.strip()
    while True:
        if s.startswith("--"):
            nl = s.find("\n")
            if nl < 0:
                return ""
            s = s[nl + 1:].lstrip()
            continue
        if s.startswith("#"):
            nl = s.find("\n")
            if nl < 0:
                return ""
            s = s[nl + 1:].lstrip()
            continue
        if s.startswith("/*"):
            end = s.find("*/")
            if end < 0:
                return ""
            s = s[end + 2:].lstrip()
            continue
        break
    return s


# DDL patterns anchored to statement start after comment stripping
DDL_CREATE_TABLE_PATTERN = re.compile(
    r"^create\s+table\s+(?:if\s+not\s+exists\s+)?(?:`?(?P<db>\w+)`?\.)?`?(?P<table>\w+)`?",
    re.IGNORECASE,
)
DDL_DROP_TABLE_PATTERN = re.compile(
    r"^drop\s+table\s+(?:if\s+exists\s+)?(?P<body>.+)$",
    re.IGNORECASE | re.DOTALL,
)
DDL_ALTER_TABLE_PATTERN = re.compile(
    r"^alter\s+table\s+(?:`?(?P<db>\w+)`?\.)?`?(?P<table>\w+)`?",
    re.IGNORECASE,
)
DDL_TRUNCATE_TABLE_PATTERN = re.compile(
    r"^truncate\s+(?:table\s+)?(?:`?(?P<db>\w+)`?\.)?`?(?P<table>\w+)`?",
    re.IGNORECASE,
)
DDL_RENAME_TABLE_PATTERN = re.compile(
    r"^rename\s+table\s+(?P<body>.+)$",
    re.IGNORECASE | re.DOTALL,
)
_TABLE_REF_RE = re.compile(r"(?:`?(?P<db>\w+)`?\.)?`?(?P<table>\w+)`?", re.IGNORECASE)


def _sqlglot_table_name(expr):
    """Extract bare table name from a sqlglot Table / identifier expression."""
    if expr is None:
        return None
    try:
        name = getattr(expr, "name", None) or getattr(expr, "this", None)
        if name is None:
            return None
        if hasattr(name, "name"):
            return str(name.name)
        return str(name).strip("`\"")
    except Exception:
        return None


def _parse_with_sqlglot(query: str):
    """
    Try to parse DDL with sqlglot. Returns dict:
      {"kind": create|drop|alter|truncate|rename, "tables": [...], "pairs": [(old,new), ...]}
    or None on failure.
    """
    cleaned = strip_sql_leading_comments(query)
    if not cleaned:
        return None
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return None
    try:
        parsed = sqlglot.parse_one(cleaned, read="mysql")
    except Exception:
        return None
    if parsed is None:
        return None

    if isinstance(parsed, exp.Create) and parsed.kind and str(parsed.kind).upper() == "TABLE":
        table = _sqlglot_table_name(parsed.this)
        return {"kind": "create", "tables": [table] if table else [], "pairs": []}

    if isinstance(parsed, exp.Drop) and parsed.kind and str(parsed.kind).upper() == "TABLE":
        tables = []
        # sqlglot may put one table in .this; multi-drop may appear as Schema/Var
        t = _sqlglot_table_name(parsed.this)
        if t:
            tables.append(t)
        for node in parsed.find_all(exp.Table):
            n = _sqlglot_table_name(node)
            if n and n not in tables:
                tables.append(n)
        return {"kind": "drop", "tables": tables, "pairs": []}

    if isinstance(parsed, exp.Alter) or parsed.__class__.__name__ == "AlterTable":
        table = _sqlglot_table_name(getattr(parsed, "this", None))
        return {"kind": "alter", "tables": [table] if table else [], "pairs": []}

    if isinstance(parsed, exp.TruncateTable) or (
        parsed.__class__.__name__ in ("Truncate", "TruncateTable")
    ):
        tables = []
        for node in parsed.find_all(exp.Table):
            n = _sqlglot_table_name(node)
            if n:
                tables.append(n)
        if not tables:
            t = _sqlglot_table_name(getattr(parsed, "this", None))
            if t:
                tables.append(t)
        return {"kind": "truncate", "tables": tables, "pairs": []}

    # RENAME TABLE a TO b — dialect-specific; fall through to regex if not recognized
    sql_upper = cleaned.lstrip().upper()
    if sql_upper.startswith("RENAME"):
        return None
    return None


def detect_ddl_kind(query: str):
    """Return DDL kind string or None. Prefer sqlglot, fall back to regex."""
    parsed = _parse_with_sqlglot(query)
    if parsed and parsed.get("kind"):
        return parsed["kind"]
    cleaned = strip_sql_leading_comments(query)
    if DDL_CREATE_TABLE_PATTERN.match(cleaned):
        return "create"
    if DDL_DROP_TABLE_PATTERN.match(cleaned):
        return "drop"
    if DDL_ALTER_TABLE_PATTERN.match(cleaned):
        return "alter"
    if DDL_TRUNCATE_TABLE_PATTERN.match(cleaned):
        return "truncate"
    if DDL_RENAME_TABLE_PATTERN.match(cleaned):
        return "rename"
    return None


def parse_drop_table_names(query: str):
    """Return list of table names from a DROP TABLE statement (supports multi-table)."""
    parsed = _parse_with_sqlglot(query)
    if parsed and parsed.get("kind") == "drop" and parsed.get("tables"):
        return parsed["tables"]
    cleaned = strip_sql_leading_comments(query)
    m = DDL_DROP_TABLE_PATTERN.match(cleaned)
    if not m:
        return []
    body = m.group("body").split(";")[0]
    names = []
    for part in body.split(","):
        tm = _TABLE_REF_RE.search(part.strip())
        if tm:
            names.append(tm.group("table"))
    return names


def parse_rename_table_pairs(query: str):
    """Return list of (old_name, new_name) from RENAME TABLE statement."""
    cleaned = strip_sql_leading_comments(query)
    m = DDL_RENAME_TABLE_PATTERN.match(cleaned)
    if not m:
        return []
    body = m.group("body").split(";")[0]
    pairs = []
    for part in body.split(","):
        # old TO new
        parts = re.split(r"\s+to\s+", part.strip(), flags=re.IGNORECASE)
        if len(parts) != 2:
            continue
        old_m = _TABLE_REF_RE.search(parts[0].strip())
        new_m = _TABLE_REF_RE.search(parts[1].strip())
        if old_m and new_m:
            pairs.append((old_m.group("table"), new_m.group("table")))
    return pairs


def parse_ddl_table_name(query: str, kind: str):
    """Parse single-table DDL (create/alter/truncate). Returns table name or None."""
    parsed = _parse_with_sqlglot(query)
    if parsed and parsed.get("kind") == kind and parsed.get("tables"):
        return parsed["tables"][0]
    cleaned = strip_sql_leading_comments(query)
    pattern = {
        "create": DDL_CREATE_TABLE_PATTERN,
        "alter": DDL_ALTER_TABLE_PATTERN,
        "truncate": DDL_TRUNCATE_TABLE_PATTERN,
    }.get(kind)
    if not pattern:
        return None
    m = pattern.match(cleaned)
    if not m:
        return None
    return m.group("table")


def build_table_ddl(table, columns_meta, pk_columns, mig_cfg):
    """
    columns_meta: list of dicts from information_schema for this table
    pk_columns: list of pk column names
    Returns: DDL string and list of insertable columns (mysql columns order)
    """
    engine = mig_cfg.get("ddl_engine", "ReplacingMergeTree")

    col_defs = []
    mysql_col_names = []
    synthesized = None
    qtable = quote_ident(table)

    for col in columns_meta:
        name = col["COLUMN_NAME"]
        mysql_col_names.append(name)
        ch_type = map_mysql_to_ch_type(col, mig_cfg)
        default_expr = _default_expr_for_column(col, ch_type)
        qname = quote_ident(name)
        if default_expr is not None:
            col_defs.append(f"{qname} {ch_type} DEFAULT {default_expr}")
        else:
            col_defs.append(f"{qname} {ch_type}")

    col_defs.append("`__data_transfer_commit_time` UInt64")
    col_defs.append("`__data_transfer_delete_time` UInt64 DEFAULT 0")
    col_defs.append("`__data_transfer_is_deleted` UInt8 MATERIALIZED if(__data_transfer_delete_time != 0, 1, 0)")

    if not pk_columns:
        parts = [f"toString({quote_ident(c)})" for c in mysql_col_names]
        concat_expr = " || '|' || ".join(parts) if parts else "''"
        col_defs.insert(0, f"`__migres_pk` UInt64 MATERIALIZED cityHash64({concat_expr})")
        synthesized = "__migres_pk"

    if "id" in mysql_col_names:
        key_cols = ["id"]
    elif pk_columns:
        key_cols = list(pk_columns)
    elif synthesized:
        key_cols = [synthesized]
    else:
        key_cols = ["__data_transfer_commit_time"]

    order_by = ", ".join([quote_ident(c) for c in key_cols])

    if engine.lower().startswith("replacing"):
        engine_sql = f"ENGINE = ReplacingMergeTree(__data_transfer_commit_time)\nORDER BY ({order_by})"
    else:
        non_replacing_order_by = ", ".join([
            quote_ident(c) for c in (
                key_cols + (["__data_transfer_commit_time"] if "__data_transfer_commit_time" not in key_cols else [])
            )
        ])
        engine_sql = f"ENGINE = MergeTree()\nORDER BY ({non_replacing_order_by})"

    cols_sql = ",\n  ".join(col_defs)
    ddl = f"CREATE TABLE IF NOT EXISTS {qtable} (\n  {cols_sql}\n) {engine_sql}"
    insertable_columns = mysql_col_names + ["__data_transfer_commit_time", "__data_transfer_delete_time"]
    return ddl, insertable_columns


def ensure_clickhouse_columns(ch_client, table, desired_columns):
    """
    Ensure that ClickHouse table `table` has all columns in desired_columns.
    desired_columns: list of dicts {name, type_sql, default_expr(optional)} or tuples (name, type_sql)
    Adds missing columns with ALTER TABLE ... ADD COLUMN if needed.
    """
    qtable = quote_ident(table)
    try:
        existing = ch_client.execute(f"DESCRIBE TABLE {qtable}")
        existing_names = {row[0] for row in existing}
    except Exception:
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
                qname = quote_ident(name)
                if default_expr:
                    ch_client.execute(
                        f"ALTER TABLE {qtable} ADD COLUMN IF NOT EXISTS {qname} {type_sql} DEFAULT {default_expr}"
                    )
                else:
                    ch_client.execute(
                        f"ALTER TABLE {qtable} ADD COLUMN IF NOT EXISTS {qname} {type_sql}"
                    )
                log.info("Added missing column %s to %s", name, table)
            except Exception:
                log.exception("Failed to add column %s to %s", name, table)
