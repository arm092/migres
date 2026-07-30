# Data Model and Schema

## Metadata columns added to every ClickHouse table

| Column | Type | Meaning |
|--------|------|---------|
| `__data_transfer_commit_time` | UInt64 | Commit timestamp in nanoseconds; ReplacingMergeTree version column |
| `__data_transfer_delete_time` | UInt64 DEFAULT 0 | Non-zero (== commit time) marks a soft delete |
| `__data_transfer_is_deleted` | UInt8 MATERIALIZED | `if(__data_transfer_delete_time != 0, 1, 0)` |
| `__migres_pk` | UInt64 MATERIALIZED | Only when the MySQL table has no PK: `cityHash64(toString(col1) \|\| '\|' \|\| ...)` |

Insertable column order is always: all MySQL columns in ORDINAL_POSITION order, then
`__data_transfer_commit_time`, then `__data_transfer_delete_time` (`insert_cols[:-2]` idiom
throughout the code refers to the plain MySQL columns).

## Table engine and ordering

Default: `ENGINE = ReplacingMergeTree(__data_transfer_commit_time)`.
ORDER BY key preference (build_table_ddl):
1. `id` column if present (even if it is not the real PK!)
2. MySQL PK columns
3. synthesized `__migres_pk`
4. `__data_transfer_commit_time` (last resort)

Reading deduplicated data requires `SELECT ... FINAL` or periodic `OPTIMIZE TABLE ... FINAL`.

## MySQL → ClickHouse type mapping (map_mysql_to_ch_type)

| MySQL | ClickHouse |
|-------|------------|
| tinyint/smallint/mediumint/int/bigint [unsigned] | Int8..64 / UInt8..64 (tinyint→8, smallint→16, mediumint→32, int→32, bigint→64) |
| float | Float32 |
| double/real | Float64 |
| decimal(p,s)/numeric | Decimal(p,s), fallback Decimal(38,10) |
| date | Date |
| datetime/timestamp | DateTime64(3[, '<migration.clickhouse_timezone>']) |
| time | String |
| char/varchar/text*/enum/set/json/blob/binary | String |
| anything else | String |

- `IS_NULLABLE=YES` → wrapped in `Nullable(...)`.
- `migration.low_cardinality_strings: true` (used in CDC schema sync via
  `cdc._map_with_low_cardinality`) → `LowCardinality(String)` / `Nullable(LowCardinality(String))`.
- MySQL `COLUMN_DEFAULT` becomes a ClickHouse `DEFAULT` expression where feasible
  (`_default_expr_for_column`).

## SQLite buffer schema (data/buffer.db)

```sql
raw_events(id INTEGER PK AUTOINCREMENT, binlog_file TEXT, binlog_pos INTEGER,
           schema_name TEXT, table_name TEXT, event_type TEXT,
           event_data JSON, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
-- index: (binlog_file, binlog_pos)

prepared_queries(id INTEGER PK AUTOINCREMENT, sql_query TEXT, params JSON,
                 group_id TEXT, schema_name TEXT, table_name TEXT,
                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
-- index: (created_at)

failed_queries(id INTEGER PK, sql_query TEXT, params JSON, schema_name TEXT,
               table_name TEXT, error_reason TEXT, failed_at TIMESTAMP)
-- NOTE: created but no code path writes to it (see known-issues.md)
```

`event_data` JSON shape (produced by `PipelineProducer._serialize_event`):

```json
{
  "type": "WriteRowsEvent | UpdateRowsEvent | DeleteRowsEvent | QueryEvent",
  "timestamp": 1712345678,
  "log_pos": 123456,
  "schema": "db",
  "table": "t",
  "rows": [{"values": {...}} | {"before_values": {...}, "after_values": {...}}],
  "query": "only for QueryEvent"
}
```

Serialization rules: bytes → utf-8 (base64 on decode failure), datetime/date → ISO string,
unknown types (Decimal, timedelta, set, …) → `str(v)`.

`prepared_queries.params` is a JSON array of row arrays; `sql_query` is
`INSERT INTO \`db\`.\`table\` (cols...) VALUES` (clickhouse-driver style, params passed
separately) or a plain DDL statement (deferred DROP).

## state.json

```json
{
  "binlog": {"file": "mysql-bin.000123", "pos": 6855245},
  "tables": {
    "users": {"status": "pending|in_progress|done", "last_pk": 12345, "rows_processed": 50000}
  }
}
```

Written atomically (tempfile + os.replace). Every accessor re-reads the whole file under a lock.

## ClickHouse debug tables (db_debug: true)

`debug_processed_events` and `debug_processed_queries` (MergeTree ORDER BY id) mirror consumed
buffer rows with `received_at` / `processed_at` timestamps rendered in hardcoded UTC+4
(`Asia/Yerevan`). Contiguous compatible query rows are merged before upload
(`_merge_debug_query_rows`).
