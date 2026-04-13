# Databricks System Tables — Query Reference

Queries for gathering metadata, volume, and query profiling data from Databricks.
Run these via Databricks MCP (DBSQL) or through the Databricks SQL Warehouse directly.

---

## 1. Table Metadata

### Row count, file count, size
```sql
DESCRIBE DETAIL <catalog>.<schema>.<table>;
```
Key columns: `num_files`, `size_in_bytes`, `num_rows` (if available).

### Table properties (clustering, auto-optimise)
```sql
SHOW TBLPROPERTIES <catalog>.<schema>.<table>;
```
Look for: `delta.autoOptimize.autoCompact`, `delta.autoOptimize.optimizeWrite`, `delta.columnMapping.mode`, `clusteringColumns`.

### Column details and statistics
```sql
DESCRIBE EXTENDED <catalog>.<schema>.<table>;
```

### Check if object is a view or table
```sql
SHOW CREATE TABLE <catalog>.<schema>.<table>;
```
If it starts with `CREATE VIEW`, the object is a view.

---

## 2. Volume Breakdown

### Row count by day (for date-partitioned fact tables)
```sql
SELECT
    <date_column> AS report_date,
    COUNT(*) AS row_count
FROM <catalog>.<schema>.<table>
GROUP BY <date_column>
ORDER BY <date_column> DESC
LIMIT 365;
```

### Row count by month
```sql
SELECT
    DATE_TRUNC('month', <date_column>) AS report_month,
    COUNT(*) AS row_count
FROM <catalog>.<schema>.<table>
GROUP BY DATE_TRUNC('month', <date_column>)
ORDER BY report_month DESC;
```

### Row count by year
```sql
SELECT
    YEAR(<date_column>) AS report_year,
    COUNT(*) AS row_count
FROM <catalog>.<schema>.<table>
GROUP BY YEAR(<date_column>)
ORDER BY report_year DESC;
```

### Total row count (fast approximation)
```sql
SELECT COUNT(*) AS total_rows FROM <catalog>.<schema>.<table>;
```

---

## 3. Query History (Performance Profiling)

### PBI Daily Stats Summary (for Executive Summary card)

Populates `databricksDailyStats` in `synthesis.json`. Includes session count, slow query percentages, and cache hit rate.

```sql
SELECT
    COUNT(*) AS total_queries,
    COUNT(DISTINCT session_id) AS distinct_sessions,
    ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT session_id), 0), 1) AS avg_queries_per_session,
    SUM(CASE WHEN total_duration_ms > 10000 THEN 1 ELSE 0 END) AS slow_10s,
    SUM(CASE WHEN total_duration_ms > 30000 THEN 1 ELSE 0 END) AS slow_30s,
    ROUND(SUM(CASE WHEN total_duration_ms > 10000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_slow_10s,
    ROUND(SUM(CASE WHEN total_duration_ms > 30000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_slow_30s,
    ROUND(AVG(total_duration_ms) / 1000, 1) AS avg_duration_s,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_duration_ms) / 1000, 1) AS p50_s,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_duration_ms) / 1000, 1) AS p95_s,
    ROUND(MAX(total_duration_ms) / 1000, 0) AS max_s,
    ROUND(SUM(read_bytes) / (1024.0*1024*1024*1024), 2) AS total_read_tb,
    ROUND(SUM(CASE WHEN from_result_cache = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_cached,
    DATEDIFF(DAY, MIN(start_time), CURRENT_TIMESTAMP()) AS period_days
FROM system.query.history
WHERE executed_as = '<spn-object-id>'
  AND statement_type = 'SELECT'
  AND client_application = 'PowerBI'
  AND start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP());
```

Map result columns to `synthesis.json` keys:
| SQL Column | synthesis.json Key | Notes |
|---|---|---|
| `total_queries` | `totalQueries` | |
| `distinct_sessions` | `distinctSessions` | `COUNT(DISTINCT session_id)` — each PBI report connection creates a Databricks session |
| `avg_queries_per_session` | `avgQueriesPerSession` | Indicates how many visuals/interactions per report open |
| `slow_10s` | `slow10s` | |
| `slow_30s` | `slow30s` | |
| `pct_slow_10s` | `pctSlow10s` | Percentage of total queries |
| `pct_slow_30s` | `pctSlow30s` | Percentage of total queries |
| `avg_duration_s` | `avgDurationS` | |
| `p50_s` | `p50s` | |
| `p95_s` | `p95s` | |
| `max_s` | `maxs` | |
| `total_read_tb` | `totalReadTb` | |
| `pct_cached` | `pctCached` | Percentage served from Databricks result cache |
| `period_days` | `periodDays` | Actual days covered |

### Top slowest queries in the last 30 days
```sql
SELECT
    query_id,
    query_text,
    start_time,
    end_time,
    total_duration_ms,
    rows_produced,
    read_bytes,
    statement_type,
    executed_as_user_name
FROM system.query.history
WHERE statement_type = 'SELECT'
  AND start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
ORDER BY total_duration_ms DESC
LIMIT 50;
```

### Most frequent queries (last 30 days)
```sql
SELECT
    SUBSTRING(query_text, 1, 200) AS query_prefix,
    COUNT(*) AS execution_count,
    AVG(total_duration_ms) AS avg_duration_ms,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_duration_ms) AS p50_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_duration_ms) AS p95_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_duration_ms) AS p99_ms,
    SUM(read_bytes) AS total_bytes_read
FROM system.query.history
WHERE statement_type = 'SELECT'
  AND start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY SUBSTRING(query_text, 1, 200)
ORDER BY execution_count DESC
LIMIT 50;
```

### Queries from Power BI (filter by service principal)
The PBI service principal name and object ID are defined in `input.md`. For ASOS, the service principal is `spn-ade-pbi` (object ID: `65978fad-bc17-4f5a-b134-25d299885855`).

```sql
SELECT
    query_id,
    SUBSTRING(query_text, 1, 500) AS query_prefix,
    total_duration_ms,
    rows_produced,
    read_rows,
    read_bytes,
    start_time,
    executed_as_user_name
FROM system.query.history
WHERE statement_type = 'SELECT'
  AND start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
  AND executed_as_user_name LIKE '%spn-ade-pbi%'
ORDER BY total_duration_ms DESC
LIMIT 100;
```

### PBI queries with scan amplification (rows read >> rows returned)
```sql
SELECT
    query_id,
    SUBSTRING(query_text, 1, 300) AS query_prefix,
    total_duration_ms,
    rows_produced,
    read_rows,
    read_bytes,
    CASE WHEN rows_produced > 0
         THEN read_rows / rows_produced
         ELSE read_rows END AS scan_amplification_ratio
FROM system.query.history
WHERE statement_type = 'SELECT'
  AND start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
  AND executed_as_user_name LIKE '%spn-ade-pbi%'
  AND read_rows > 1000000
ORDER BY scan_amplification_ratio DESC
LIMIT 50;
```

### PBI query duration percentiles by table
```sql
SELECT
    CASE
        WHEN LOWER(query_text) LIKE '%fact_order_line%' THEN 'fact_order_line_v1'
        WHEN LOWER(query_text) LIKE '%fact_product_option_trade%' THEN 'fact_product_option_trade_daily_snapshot_v1'
        WHEN LOWER(query_text) LIKE '%fact_wholesale%' THEN 'fact_wholesale_sale_v1'
        ELSE 'other'
    END AS primary_fact_table,
    COUNT(*) AS query_count,
    AVG(total_duration_ms) AS avg_ms,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_duration_ms) AS p50_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_duration_ms) AS p95_ms,
    AVG(read_rows) AS avg_rows_read,
    AVG(read_bytes) / (1024*1024*1024) AS avg_gb_read
FROM system.query.history
WHERE statement_type = 'SELECT'
  AND start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
  AND executed_as_user_name LIKE '%spn-ade-pbi%'
GROUP BY 1
ORDER BY avg_ms DESC;
```

### Queries touching a specific table
```sql
SELECT
    query_id,
    SUBSTRING(query_text, 1, 500) AS query_prefix,
    total_duration_ms,
    rows_produced,
    start_time
FROM system.query.history
WHERE statement_type = 'SELECT'
  AND start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
  AND LOWER(query_text) LIKE '%<table_name>%'
ORDER BY total_duration_ms DESC
LIMIT 50;
```

---

## 4. Information Schema

### List all tables in a schema
```sql
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type,
    created,
    last_altered
FROM system.information_schema.tables
WHERE table_catalog = '<catalog>'
  AND table_schema = '<schema>'
ORDER BY table_name;
```

### List columns for a table
```sql
SELECT
    column_name,
    data_type,
    is_nullable,
    ordinal_position
FROM system.information_schema.columns
WHERE table_catalog = '<catalog>'
  AND table_schema = '<schema>'
  AND table_name = '<table>'
ORDER BY ordinal_position;
```

---

## 5. Clustering and Optimisation

### Check liquid clustering columns
```sql
DESCRIBE DETAIL <catalog>.<schema>.<table>;
```
Look for `clusteringColumns` in the output.

### Check table history (recent operations)
```sql
DESCRIBE HISTORY <catalog>.<schema>.<table> LIMIT 20;
```
Shows recent OPTIMIZE, VACUUM, WRITE operations with timestamps and metrics.

### Check file statistics
```sql
SELECT
    COUNT(*) AS num_files,
    SUM(size) / (1024*1024*1024) AS total_size_gb,
    AVG(size) / (1024*1024) AS avg_file_size_mb,
    MIN(size) / (1024*1024) AS min_file_size_mb,
    MAX(size) / (1024*1024) AS max_file_size_mb
FROM (
    SELECT input_file_name() AS file_path, 1 AS size
    FROM <catalog>.<schema>.<table>
    LIMIT 1
);
```
Note: For accurate file stats, prefer `DESCRIBE DETAIL`.

---

## 6. Per-Table PBI Query Statistics (for Report Enrichment)

Run once per model analysis. Results feed into `databricks-profile.json` under `tableQueryStats`.
Replace the `WHEN` clauses with actual table names from `model-taxonomy.json` Databricks source mappings.

### Query count, avg duration, and P95 per table (last 30 days)
```sql
SELECT
    table_ref,
    COUNT(*) AS total_queries,
    CAST(COUNT(*) / GREATEST(DATEDIFF(DAY, MIN(start_time), CURRENT_TIMESTAMP()), 1) AS INT) AS daily_queries,
    CAST(AVG(total_duration_ms) AS INT) AS avg_duration_ms,
    CAST(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_duration_ms) AS INT) AS p95_duration_ms
FROM (
    SELECT
        query_id,
        total_duration_ms,
        start_time,
        CASE
            WHEN LOWER(query_text) LIKE '%<table1>%' THEN '<table1>'
            WHEN LOWER(query_text) LIKE '%<table2>%' THEN '<table2>'
            -- Add one WHEN per Databricks source table from model-taxonomy.json
            ELSE NULL
        END AS table_ref
    FROM system.query.history
    WHERE executed_as_user_name LIKE '%<spn-name>%'
      AND start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
      AND statement_type = 'SELECT'
) sub
WHERE table_ref IS NOT NULL
GROUP BY table_ref
ORDER BY avg_duration_ms DESC;
```

Output schema for `databricks-profile.json`:
```json
{
  "tables": [ ... ],
  "tableQueryStats": [
    {
      "tableName": "fact_order_line_v1",
      "dailyQueries": 450,
      "avgDurationMs": 3200,
      "p95DurationMs": 8500
    }
  ]
}
```

---

## 7. Per-User PBI Query Attribution (last 30 days)

For the Query Attribution Dashboard. Groups queries by user to identify top consumers and training candidates.

```sql
SELECT
    executed_as_user_name AS username,
    COUNT(*) AS total_queries,
    SUM(total_duration_ms) AS total_duration_ms,
    CAST(AVG(total_duration_ms) AS INT) AS avg_duration_ms,
    CAST(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_duration_ms) AS INT) AS p95_duration_ms,
    MAX(total_duration_ms) AS max_duration_ms,
    SUM(CASE WHEN total_duration_ms > 10000 THEN 1 ELSE 0 END) AS queries_over_10s,
    SUM(CASE WHEN total_duration_ms > 30000 THEN 1 ELSE 0 END) AS queries_over_30s,
    SUM(read_bytes) / (1024*1024*1024) AS total_gb_read
FROM system.query.history
WHERE statement_type = 'SELECT'
  AND start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY executed_as_user_name
ORDER BY total_duration_ms DESC
LIMIT 50;
```

---

## 8. Query Duration Distribution (for Timeout Impact Simulation)

For the Capacity Settings Analysis. Shows how many queries fall into each duration bucket to simulate timeout threshold impact.

```sql
SELECT
    CASE
        WHEN total_duration_ms <= 10000 THEN '0-10s'
        WHEN total_duration_ms <= 30000 THEN '10-30s'
        WHEN total_duration_ms <= 60000 THEN '30-60s'
        WHEN total_duration_ms <= 120000 THEN '1-2min'
        WHEN total_duration_ms <= 225000 THEN '2-3.75min'
        ELSE '>3.75min'
    END AS duration_bucket,
    COUNT(*) AS query_count,
    SUM(read_bytes) / (1024*1024*1024) AS total_gb_read
FROM system.query.history
WHERE statement_type = 'SELECT'
  AND start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
  AND executed_as_user_name LIKE '%spn-ade-pbi%'
GROUP BY 1
ORDER BY MIN(total_duration_ms);
```

---

## 9. Row Count Distribution (for Row Set Limit Impact Simulation)

For the Capacity Settings Analysis. Shows distribution of rows produced per query.

```sql
SELECT
    CASE
        WHEN rows_produced <= 100000 THEN '0-100K'
        WHEN rows_produced <= 1000000 THEN '100K-1M'
        WHEN rows_produced <= 10000000 THEN '1M-10M'
        ELSE '>10M'
    END AS rows_bucket,
    COUNT(*) AS query_count,
    CAST(AVG(total_duration_ms) AS INT) AS avg_duration_ms
FROM system.query.history
WHERE statement_type = 'SELECT'
  AND start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
  AND executed_as_user_name LIKE '%spn-ade-pbi%'
GROUP BY 1
ORDER BY MIN(rows_produced);
```

---

## 10. CU Consumption by Hour (for Surge Protection Analysis)

For the Workload Analysis. Shows query distribution by hour of day to identify peak hours and recommend surge protection thresholds.

```sql
SELECT
    HOUR(start_time) AS hour_of_day,
    COUNT(*) AS query_count,
    SUM(total_duration_ms) / 1000 AS total_duration_s,
    CAST(AVG(total_duration_ms) AS INT) AS avg_duration_ms
FROM system.query.history
WHERE statement_type = 'SELECT'
  AND start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
  AND executed_as_user_name LIKE '%spn-ade-pbi%'
GROUP BY HOUR(start_time)
ORDER BY hour_of_day;
```
