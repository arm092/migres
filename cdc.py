import logging
import time

log = logging.getLogger(__name__)

class CriticalCDCError(Exception):
    """
    Critical CDC error that should cause the process to exit with error code.
    This prevents binlog position advancement and ensures Kubernetes restarts the process.
    """
    pass

# Global query tracking pools - Used by *client.py wrappers
mysql_query_pool = []
clickhouse_query_pool = []

def _add_mysql_query(query: str, table: str = None):
    """Add a MySQL query to the tracking pool"""
    global mysql_query_pool
    mysql_query_pool.append({
        "query": query,
        "table": table,
        "timestamp": time.time()
    })
    # Keep only last 10 queries to avoid memory issues
    if len(mysql_query_pool) > 10:
        mysql_query_pool.pop(0)

def _add_clickhouse_query(query: str, table: str = None):
    """Add a ClickHouse query to the tracking pool"""
    global clickhouse_query_pool
    clickhouse_query_pool.append({
        "query": query,
        "table": table,
        "timestamp": time.time()
    })
    # Keep only last 10 queries to avoid memory issues
    if len(clickhouse_query_pool) > 10:
        clickhouse_query_pool.pop(0)

def _clear_query_pools():
    """Clear both query pools after successful operation"""
    global mysql_query_pool, clickhouse_query_pool
    mysql_query_pool.clear()
    clickhouse_query_pool.clear()

def _get_query_pools_for_error():
    """Get current query pools for error reporting"""
    global mysql_query_pool, clickhouse_query_pool
    return {
        "mysql_queries": mysql_query_pool.copy(),
        "clickhouse_queries": clickhouse_query_pool.copy()
    }

def _map_with_low_cardinality(col, mig_cfg):
    """Helper to map MySQL type to ClickHouse type with LowCardinality optimization"""
    from schema_and_ddl import map_mysql_to_ch_type
    ch_type = map_mysql_to_ch_type(col, mig_cfg)
    if bool(mig_cfg.get("low_cardinality_strings", True)) and ch_type.endswith("String") and not ch_type.startswith("LowCardinality("):
        ch_type = ch_type.replace("Nullable(String)", "Nullable(LowCardinality(String))") if ch_type.startswith("Nullable(") else f"LowCardinality({ch_type})"
    return ch_type
