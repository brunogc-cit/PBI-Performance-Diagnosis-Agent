# Example Slow Query — ADE - Sales Report

## Query Metadata

| Field | Value |
|-------|-------|
| **Query ID** | `01f13405-bcb8-186b-bdae-2c48257344ec` |
| **Report** | ADE - Sales |
| **Visual** | Product hierarchy division breakdown (9 measures) |
| **Duration** | 15s 604ms |
| **Rows Read** | 4,441,277,865 (4.4 billion) |
| **Rows Returned** | 2 |
| **Bytes Read** | 19.60 GB |
| **Files Read** | 2,092 |
| **Photon** | 99% of task time |
| **Executed By** | spn-ade-pbi (65978fad-bc17-4f5a-b134-25d299885855) |
| **Compute** | Technology SQL Warehouse |
| **Source** | PowerBI |
| **Start Time** | Apr 09, 2026, 01:17:47 PM GMT+02:00 |

## Query Pattern Analysis

### Problem Summary

This single visual reads **4.4 billion rows** to return just **2 rows**. The query:
1. Selects **90+ columns** from the fact table when only **9 aggregated values** are needed
2. JOINs across **3 different catalogs** (`sales`, `technology`, `product`)
3. All serve-layer objects are **views** (no materialisation), so every query hits the raw curated Delta tables
4. The date filter spans **2 years** of data, with no predicate pushdown to the fact table
5. The `dim_product_option_v1` filter (`product_buyrarchy_division = 'ASOS Design'`) is applied after the massive fact table scan

### Tables Involved

| Table | Catalog | Type | Role |
|-------|---------|------|------|
| `fact_product_option_trade_daily_snapshot_v1` | sales.serve | View (DQ) | Main fact table — 90+ columns selected |
| `dim_date_v2` | technology.serve | View (DQ/Dual) | Date dimension — 47 columns selected, 2-year filter |
| `dim_product_option_v1` | product.serve | View (DQ) | Product dimension — 73 columns selected, division filter |
| `dim_financial_time_period_v1` | technology.serve | View (DQ/Dual) | Time period — 'Last Week' filter |

### Key Performance Issues

1. **Massive over-selection**: The innermost subquery selects 90+ columns from the fact table, but the outer GROUP BY only needs `product_hierarchy_division` and 9 aggregate columns. DirectQuery does not perform column pruning at the M layer.

2. **Cross-catalog JOINs**: The query joins tables from `sales`, `technology`, and `product` catalogs in a single query. Each serve view is a view-over-view chain that must be resolved at query time.

3. **No date predicate on fact table**: The date filter (`calendar_date >= '2024-04-09'`) is applied to `dim_date_v2`, then JOINed to the fact table via `dim_date_sk`. The fact table scan reads ALL rows, not just the 2-year window, because the clustering key may not align with the join predicate.

4. **Redundant columns from dimension tables**: The query selects ALL columns from `dim_date_v2` (47 columns) and `dim_product_option_v1` (73 columns) even though the final aggregation only groups by `product_hierarchy_division`.

5. **Complex JOIN pattern for time period**: The `dim_financial_time_period_v1` join uses a LEFT OUTER JOIN + GROUP BY + complex NULL-handling ON clause, which prevents simple predicate pushdown.

6. **4.4B rows read for 2 rows returned**: The ratio of 2,200,000,000:1 (rows read : rows returned) indicates catastrophic scan amplification.

### Root Causes

| Root Cause | Layer | Impact |
|------------|-------|--------|
| All serve models are views | dbt | No materialisation = full scan every query |
| No aggregation tables | PBI model | Every visual hits the raw fact table |
| DirectQuery with no M-level filtering | PBI expressions | `_fn_GetDataFromDBX` passes through entire table |
| Column over-selection | PBI DirectQuery engine | Selects all columns in the model, not just what the visual needs |
| Cross-catalog joins | Databricks | Queries span 3+ catalogs, increasing complexity |
| Date filter via JOIN not WHERE | PBI DirectQuery engine | Date predicate not pushed to fact table scan |

### Potential Improvements

| Strategy | Effort | Impact | Risk |
|----------|--------|--------|------|
| Create Databricks aggregated materialized views for common groupings | Medium | Very High | Low |
| Materialise serve views as tables/incremental models | Medium | High | Medium (data freshness) |
| Add Incremental Refresh + Hybrid mode in PBI | Medium | Very High | Medium (requires Premium) |
| Add date parameters to `_fn_GetDataFromDBX` for M-level filtering | Low-Medium | High | Low |
| Reduce column count in serve views to only what PBI needs | Low | Medium | Low |
| Add clustering on fact table by `dim_product_option_sk` (if not already) | Low | Medium | Low |
| Create pre-aggregated "daily snapshot by division" table | Medium | Very High | Low |

## Full Query SQL

```sql
/*ActivityId: 1e9e3592-5a45-4471-a357-c51d2cb62559 CorrelationId: {...} */
select
  `product_hierarchy_division`,
  `C1`, `C2`, `C3`, `C4`, `C5`, `C6`, `C7`, `C8`, `C9`
from (
    select
      `product_hierarchy_division`,
      sum(`retail_return_value`) as `C1`,
      sum(`retail_sale_margin`) as `C2`,
      sum(cast(`view_count_weighted_by_available_sizes` as DOUBLE)) as `C3`,
      sum(cast(`view_count_of_available_sizes` as DOUBLE)) as `C4`,
      sum(cast(`retail_sale_quantity` as DOUBLE)) as `C5`,
      sum(cast(`view_count` as DOUBLE)) as `C6`,
      sum(`retail_sale_value`) as `C7`,
      sum(`retail_sale_total_realised_markdown`) as `C8`,
      sum(cast(`intake_quantity` as DOUBLE)) as `C9`
    from (
        -- Inner: selects 90+ columns from fact table
        -- JOINs dim_date_v2 (47 cols), dim_product_option_v1 (73 cols),
        -- dim_financial_time_period_v1
        -- from sales.serve, technology.serve, product.serve
        -- Full query omitted for brevity — see input from user
    ) as `ITBL`
    group by `product_hierarchy_division`
) as `ITBL`
where (/* NULL checks on C1..C9 */)
limit 1000001
```

The full unabridged query is 300+ lines of SQL with deeply nested subqueries — a hallmark of DirectQuery-generated SQL from Power BI when models have many relationships and wide tables.
