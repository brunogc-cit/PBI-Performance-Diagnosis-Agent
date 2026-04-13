# BPA Rules Reference — Power BI Performance Analyst

This document catalogues all Best Practice Analyser (BPA) rules checked by the `run_bpa.py` script. Rules are grouped by severity and include anti-pattern examples with recommended fixes.

---

## High Severity

### 1. AVOID_FLOATING_POINT_DATA_TYPES

| Field | Value |
|-------|-------|
| **ID** | `AVOID_FLOATING_POINT_DATA_TYPES` |
| **Name** | Avoid Floating-Point Data Types |
| **Severity** | High |
| **Category** | Data Model / Column Types |

**What it checks:**
Scans every column in the model for `dataType: "double"`. Flags any column using a floating-point type that could be replaced by `decimal` (fixed-point) or `int64`.

**Why it matters:**
- `double` columns consume more memory than `int64` and compress poorly because of floating-point representation noise.
- Floating-point arithmetic introduces rounding errors (e.g., `0.1 + 0.2 ≠ 0.3`), which cause silent discrepancies in financial and quantity measures.
- The VertiPaq engine achieves significantly better compression ratios on `int64` and `decimal` columns because their value distributions are more uniform.

**Example of violation (model.bim excerpt):**
```json
{
  "name": "Revenue",
  "dataType": "double",
  "sourceColumn": "Revenue"
}
```

**Recommended fix:**
```json
{
  "name": "Revenue",
  "dataType": "decimal",
  "sourceColumn": "Revenue"
}
```

For whole-number columns (counts, IDs, quantities), use `int64` instead:
```json
{
  "name": "OrderQty",
  "dataType": "int64",
  "sourceColumn": "OrderQty"
}
```

> **Tip:** After changing the data type, validate that downstream measures still produce correct results — particularly those involving division or percentage calculations.

---

### 2. AVOID_BIDIRECTIONAL_RELATIONSHIPS

| Field | Value |
|-------|-------|
| **ID** | `AVOID_BIDIRECTIONAL_RELATIONSHIPS` |
| **Name** | Avoid Bidirectional Cross-Filtering |
| **Severity** | High |
| **Category** | Relationships |

**What it checks:**
Scans model relationships for `crossFilteringBehavior: "bothDirections"`.

**Why it matters:**
- Bidirectional relationships create ambiguous filter paths when multiple fact tables share dimension tables, leading to incorrect or unexpected query results.
- The engine must evaluate additional filter propagation paths, increasing query execution time — especially in models with many relationships.
- They can expose row-level security (RLS) bypass vectors in certain topologies.

**Example of violation (model.bim excerpt):**
```json
{
  "name": "DimProduct-FactSales",
  "fromTable": "DimProduct",
  "fromColumn": "ProductKey",
  "toTable": "FactSales",
  "toColumn": "ProductKey",
  "crossFilteringBehavior": "bothDirections"
}
```

**Recommended fix:**
Remove the bidirectional setting and use single-direction filtering (the default):
```json
{
  "name": "DimProduct-FactSales",
  "fromTable": "DimProduct",
  "fromColumn": "ProductKey",
  "toTable": "FactSales",
  "toColumn": "ProductKey"
}
```

If you need the "reverse" filter direction in a specific measure, use `CROSSFILTER()` within that measure only:
```dax
Sales by Product =
CALCULATE(
    [Total Sales],
    CROSSFILTER( DimProduct[ProductKey], FactSales[ProductKey], Both )
)
```

This confines the bidirectional behaviour to a single calculation context rather than polluting every query against the model.

---

### 3. DUAL_MODE_TABLES

| Field | Value |
|-------|-------|
| **ID** | `DUAL_MODE_TABLES` |
| **Name** | Avoid Dual Storage Mode on Hidden/Standalone Tables |
| **Severity** | High |
| **Category** | Storage Mode |

**What it checks:**
Looks for table partitions where `mode: "dual"` is set, particularly on tables that are hidden or not directly consumed by report visuals.

**Why it matters:**
- Dual-mode tables are processed in both Import and DirectQuery during refresh, doubling processing time and memory usage.
- Hidden tables in Dual mode are almost always unintentional — they typically result from the storage-mode cascade logic in Power BI Desktop.
- They increase dataset refresh duration without providing any end-user benefit.

**Example of violation (model.bim excerpt):**
```json
{
  "name": "Bridge_ProductCategory",
  "isHidden": true,
  "partitions": [
    {
      "name": "Bridge_ProductCategory",
      "mode": "dual",
      "source": { "type": "m", "expression": "..." }
    }
  ]
}
```

**Recommended fix:**
Set the partition mode explicitly to `import` for hidden/dimension tables:
```json
{
  "name": "Bridge_ProductCategory",
  "isHidden": true,
  "partitions": [
    {
      "name": "Bridge_ProductCategory",
      "mode": "import",
      "source": { "type": "m", "expression": "..." }
    }
  ]
}
```

> **Note:** Changing a table from Dual to Import may break DirectQuery composability for downstream composite models. Validate the consumption pattern before changing.

---

## Medium Severity

### 4. FILTER_ALL_ANTIPATTERN

| Field | Value |
|-------|-------|
| **ID** | `FILTER_ALL_ANTIPATTERN` |
| **Name** | Replace FILTER(ALL(...)) with REMOVEFILTERS |
| **Severity** | Medium |
| **Category** | DAX Patterns |

**What it checks:**
Searches measure expressions for the pattern `FILTER(ALL(` — a common anti-pattern where the entire table is materialised in memory before filtering.

**Why it matters:**
- `FILTER(ALL(Table))` materialises the entire table as a data table in memory, then iterates row-by-row to apply the predicate. This is extremely expensive for large tables.
- The optimised equivalent uses `REMOVEFILTERS` (or `KEEPFILTERS`) with a direct predicate inside `CALCULATE`, which leverages the storage engine's native filtering.

**Example of violation:**
```dax
Sales UK =
CALCULATE(
    SUM( FactSales[Amount] ),
    FILTER( ALL( DimGeography ), DimGeography[Country] = "UK" )
)
```

**Recommended fix:**
```dax
Sales UK =
CALCULATE(
    SUM( FactSales[Amount] ),
    REMOVEFILTERS( DimGeography ),
    DimGeography[Country] = "UK"
)
```

Or, if the intent is to keep existing filters except on one column:
```dax
Sales UK =
CALCULATE(
    SUM( FactSales[Amount] ),
    REMOVEFILTERS( DimGeography[Country] ),
    DimGeography[Country] = "UK"
)
```

---

### 5. AVOID_IFERROR

| Field | Value |
|-------|-------|
| **ID** | `AVOID_IFERROR` |
| **Name** | Avoid IFERROR / ISERROR in DAX |
| **Severity** | Medium |
| **Category** | DAX Patterns |

**What it checks:**
Searches measure expressions for `IFERROR(` or `ISERROR(` function calls.

**Why it matters:**
- `IFERROR` evaluates the expression twice — once to check for error, once to return the result — effectively doubling the computation cost.
- The most common use case (division by zero) is far better handled by `DIVIDE()`, which has a built-in alternate-result parameter.
- `IFERROR` masks genuine errors that should surface during development, making debugging harder.

**Example of violation:**
```dax
Margin % =
IFERROR(
    SUM( FactSales[Margin] ) / SUM( FactSales[Revenue] ),
    0
)
```

**Recommended fix:**
```dax
Margin % =
DIVIDE(
    SUM( FactSales[Margin] ),
    SUM( FactSales[Revenue] ),
    0
)
```

For non-division errors, use explicit conditional checks:
```dax
Safe Lookup =
VAR _val = LOOKUPVALUE( DimProduct[Name], DimProduct[Key], [CurrentKey] )
RETURN
    IF( ISBLANK( _val ), "Unknown", _val )
```

---

### 6. WIDE_TABLES

| Field | Value |
|-------|-------|
| **ID** | `WIDE_TABLES` |
| **Name** | Avoid Wide Tables (>50 Columns) |
| **Severity** | Medium |
| **Category** | Data Model / Table Design |

**What it checks:**
Counts the number of visible and hidden columns per table. Flags any table exceeding 50 columns.

**Why it matters:**
- Wide tables almost always contain columns that are not referenced by any measure, relationship, or visual — dead weight that consumes memory and increases refresh time.
- They indicate a flat/denormalised design that would benefit from star-schema refactoring.
- VertiPaq processes each column independently; fewer columns = faster refresh.

**Example of violation:**
A table `FactTransactions` with 78 columns, of which 30 are never referenced in any measure or visual.

**Recommended fix:**
1. Cross-reference columns against measure expressions and visual field usage.
2. Remove or hide columns that are not consumed.
3. Consider splitting wide dimension tables into role-playing dimensions or sub-dimensions.
4. Move calculated columns to measures where possible.

---

### 7. MISSING_FORMAT_STRING

| Field | Value |
|-------|-------|
| **ID** | `MISSING_FORMAT_STRING` |
| **Name** | Measures Missing Format String |
| **Severity** | Medium |
| **Category** | Measures |

**What it checks:**
Identifies measures where the `formatString` property is absent or empty.

**Why it matters:**
- Without an explicit format string, Power BI falls back to the default General format, displaying raw floating-point values (e.g., `1234567.89` instead of `£1,234,568`).
- Report authors end up applying formatting in each visual individually, leading to inconsistencies across pages.
- Explicitly formatted measures are self-documenting and reduce visual-level configuration overhead.

**Example of violation (model.bim excerpt):**
```json
{
  "name": "Total Revenue",
  "expression": "SUM( FactSales[Revenue] )"
}
```

**Recommended fix:**
```json
{
  "name": "Total Revenue",
  "expression": "SUM( FactSales[Revenue] )",
  "formatString": "£#,##0"
}
```

Common format strings:
| Type | Format String |
|------|--------------|
| Currency (GBP) | `£#,##0` or `£#,##0.00` |
| Percentage | `0.0%` |
| Integer | `#,##0` |
| Decimal (2dp) | `#,##0.00` |

---

### 8. MANY_TO_MANY_RELATIONSHIPS

| Field | Value |
|-------|-------|
| **ID** | `MANY_TO_MANY_RELATIONSHIPS` |
| **Name** | Many-to-Many Relationships |
| **Severity** | Medium |
| **Category** | Relationships |

**What it checks:**
Identifies relationships where both `fromCardinality` and `toCardinality` are set to `"many"`.

**Why it matters:**
- Many-to-many relationships produce expanded (cross-joined) intermediate tables during query evaluation, which can cause significant performance degradation on large datasets.
- They frequently lead to unexpected aggregation results if the analyst is not aware of the relationship type.
- In most cases, a bridge table or restructured star schema eliminates the need for M:M relationships.

**Example of violation (model.bim excerpt):**
```json
{
  "name": "FactSales-FactBudget",
  "fromTable": "FactSales",
  "fromColumn": "DateKey",
  "toTable": "FactBudget",
  "toColumn": "DateKey",
  "fromCardinality": "many",
  "toCardinality": "many"
}
```

**Recommended fix:**
Introduce a shared dimension (bridge) table:
```
DimDate (1) ──── (*) FactSales
DimDate (1) ──── (*) FactBudget
```

Both fact tables relate to `DimDate` via standard many-to-one relationships, eliminating the M:M entirely.

---

## Low Severity

### 9. BARE_DIVISION

| Field | Value |
|-------|-------|
| **ID** | `BARE_DIVISION` |
| **Name** | Use DIVIDE() Instead of / Operator |
| **Severity** | Low |
| **Category** | DAX Patterns |

**What it checks:**
Identifies division operations using the `/` operator in measure expressions.

**Why it matters:**
- The `/` operator throws an error on division by zero, requiring explicit error handling.
- `DIVIDE()` handles division by zero gracefully with an optional alternate result (defaults to `BLANK()`).
- Using `DIVIDE()` consistently makes intent clearer and eliminates an entire class of runtime errors.

**Example of violation:**
```dax
Avg Order Value = SUM( FactSales[Revenue] ) / COUNTROWS( FactSales )
```

**Recommended fix:**
```dax
Avg Order Value =
DIVIDE(
    SUM( FactSales[Revenue] ),
    COUNTROWS( FactSales ),
    0
)
```

> **Note:** This rule is Low severity because `/` is not inherently wrong — it is acceptable when the denominator is guaranteed non-zero (e.g., dividing by a constant). The rule flags it as a best-practice recommendation.

---

### 10. AUTO_DATE_TABLES

| Field | Value |
|-------|-------|
| **ID** | `AUTO_DATE_TABLES` |
| **Name** | Disable Auto Date/Time Tables |
| **Severity** | Low |
| **Category** | Model Settings |

**What it checks:**
Looks for the model-level property `__PBI_TimeIntelligenceEnabled` set to `true` (or absent, as `true` is the default).

**Why it matters:**
- When enabled, Power BI automatically creates a hidden date table for every date/datetime column in the model.
- Each auto-generated table adds ~5,400 rows (covering a wide date range) and several calculated columns.
- In a model with 20 date columns, this creates 20 hidden tables — adding significant memory overhead and refresh time for functionality that is almost never used when a proper date dimension exists.

**Example of violation:**
Auto date/time is enabled (default) with no explicit override in the model.

**Recommended fix:**
Add or set the model annotation:
```json
{
  "annotations": [
    {
      "name": "__PBI_TimeIntelligenceEnabled",
      "value": "0"
    }
  ]
}
```

In Power BI Desktop: **File > Options > Current File > Data Load > Auto date/time** — untick the checkbox.

---

### 11. DAX_COLUMNS_NOT_FULLY_QUALIFIED

| Field | Value |
|-------|-------|
| **ID** | `DAX_COLUMNS_NOT_FULLY_QUALIFIED` |
| **Name** | Fully Qualify Column References in DAX |
| **Severity** | Low |
| **Category** | DAX Patterns |

**What it checks:**
Scans measure expressions for column references that are not prefixed with their table name (e.g., `[Amount]` instead of `FactSales[Amount]`).

**Why it matters:**
- Unqualified column references are ambiguous — if multiple tables contain a column with the same name, DAX may resolve to the wrong one.
- Fully qualified references are self-documenting: any reader can immediately identify which table a column belongs to.
- Measures should use `[MeasureName]` (no table prefix) and columns should always use `TableName[ColumnName]`.

**Example of violation:**
```dax
Total Revenue = SUM( [Revenue] )
```

**Recommended fix:**
```dax
Total Revenue = SUM( FactSales[Revenue] )
```

> **Convention:** Measures are referenced without a table prefix (`[Total Revenue]`); columns are always fully qualified (`FactSales[Revenue]`). This distinction makes it immediately clear whether a reference is a measure or a column.

---

### 12. UNUSED_COLUMNS_CANDIDATE

| Field | Value |
|-------|-------|
| **ID** | `UNUSED_COLUMNS_CANDIDATE` |
| **Name** | Candidate Unused Columns |
| **Severity** | Low |
| **Category** | Data Model / Column Usage |

**What it checks:**
Cross-references every column in the model against all measure expressions, calculated column expressions, calculated table expressions, and relationship definitions. Flags columns that are not referenced anywhere.

**Why it matters:**
- Unused columns consume memory (VertiPaq stores every column) and increase refresh time without contributing to any calculation or visual.
- Removing unused columns is one of the simplest and most impactful optimisations for large models.
- Even hidden columns are stored and processed — hiding is not the same as removing.

**Example of violation:**
Column `FactSales[InternalBatchId]` exists in the model but is not referenced by any measure, relationship, hierarchy, or calculated expression.

**Recommended fix:**
1. Verify the column is genuinely unused (check report visuals, RLS rules, and any downstream composite models).
2. Remove the column from the Power Query step or add a `Table.RemoveColumns` step.
3. If removal is too aggressive, hide the column and add a documentation annotation for future review.

```m
// Power Query — remove unused columns
= Table.RemoveColumns( PreviousStep, { "InternalBatchId", "LegacyCode", "DebugFlag" } )
```

---

## Rule Summary Table

| # | Rule ID | Severity | Category |
|---|---------|----------|----------|
| 1 | `AVOID_FLOATING_POINT_DATA_TYPES` | High | Column Types |
| 2 | `AVOID_BIDIRECTIONAL_RELATIONSHIPS` | High | Relationships |
| 3 | `DUAL_MODE_TABLES` | High/Medium | Storage Mode |
| 4 | `FILTER_ALL_ANTIPATTERN` | Medium | DAX Patterns |
| 5 | `AVOID_IFERROR` | Medium | DAX Patterns |
| 6 | `WIDE_TABLES` | Medium | Table Design |
| 7 | `MISSING_FORMAT_STRING` | Medium | Measures |
| 8 | `MANY_TO_MANY_RELATIONSHIPS` | Medium | Relationships |
| 9 | `BARE_DIVISION` | Low | DAX Patterns |
| 10 | `AUTO_DATE_TABLES` | Low | Model Settings |
| 11 | `DAX_COLUMNS_NOT_FULLY_QUALIFIED` | Low | DAX Patterns |
| 12 | `UNUSED_COLUMNS_CANDIDATE` | Low | Column Usage |
| 13 | `IS_AVAILABLE_IN_MDX` | High | Performance |
| 14 | `TIME_INTEL_ON_DQ` | High | Performance |
| 15 | `CALCULATED_TABLES` | High | Performance |
| 16 | `SNOWFLAKE_DQ_CHAINS` | Medium | Performance |
| 17 | `DATE_TABLE_NOT_MARKED` | Medium | Performance |
| 18 | `REDUNDANT_COLUMNS_IN_RELATED` | Medium | Performance |
| 19 | `EXCESSIVE_CALCULATED_COLUMNS` | Low | Performance |
| 20 | `M_FOLDING_BLOCKERS` | Low | Performance |

---

## New Rules (v3)

### 13. IS_AVAILABLE_IN_MDX

| Field | Value |
|-------|-------|
| **ID** | `IS_AVAILABLE_IN_MDX` |
| **Severity** | High |
| **Impact** | Memory |

**What it checks:** Flags non-attribute columns (keys, hidden columns, IDs) in DirectQuery/Dual tables that have `isAvailableInMdx = true` (the default).

**Why it's bad:** MDX exposure forces the engine to maintain additional metadata and memory structures. On large DirectQuery models this increases model load time and query overhead.

**Required action:** Bulk-select all non-attribute columns in Tabular Editor. Set `isAvailableInMdx = false`. Leave only display attributes (names, codes, descriptions) enabled.

### 14. TIME_INTEL_ON_DQ

| Field | Value |
|-------|-------|
| **ID** | `TIME_INTEL_ON_DQ` |
| **Severity** | High |
| **Impact** | Latency |

**What it checks:** Flags DAX time intelligence functions (DATEADD, SAMEPERIODLASTYEAR, DATESINPERIOD, etc.) in measures that reference DirectQuery or Dual tables.

**Why it's bad:** Time intelligence functions generate multiple DQ queries per evaluation. On large fact tables this triggers full scans and severe latency.

**Required action:** Rewrite measures to use pre-computed date bridge tables. Replace DAX time intelligence with joins to pre-shifted date keys.

### 15. CALCULATED_TABLES

| Field | Value |
|-------|-------|
| **ID** | `CALCULATED_TABLES` |
| **Severity** | High |
| **Impact** | Latency |

**What it checks:** Detects tables with `source.type = "calculated"`.

**Why it's bad:** Calculated tables re-evaluate on every refresh, block query caching, and increase model maintenance overhead.

**Required action:** Replace with static import tables or parameters. Move logic upstream into SQL/dbt.

### 16. SNOWFLAKE_DQ_CHAINS

| Field | Value |
|-------|-------|
| **ID** | `SNOWFLAKE_DQ_CHAINS` |
| **Severity** | Medium |
| **Impact** | Latency |

**What it checks:** Detects chains of 3+ DirectQuery/Dual tables connected via relationships (snowflake architecture in DQ).

**Why it's bad:** Chained joins force multi-hop DQ queries and prevent aggregation push-down. Each hop adds a nested SQL subquery.

**Required action:** Flatten snowflake tables into the import layer. Ensure attributes are resolved in a single hop.

### 17. DATE_TABLE_NOT_MARKED

| Field | Value |
|-------|-------|
| **ID** | `DATE_TABLE_NOT_MARKED` |
| **Severity** | Medium |
| **Impact** | Latency |

**What it checks:** Detects tables that look like date/calendar tables (by name or column types) but are not marked as Date Tables.

**Why it's bad:** Disables time intelligence optimisations. Prevents efficient date filter caching.

**Required action:** Mark the table as a Date Table in Power BI Desktop or Tabular Editor. Ensure it has a contiguous date column.

### 18. REDUNDANT_COLUMNS_IN_RELATED

| Field | Value |
|-------|-------|
| **ID** | `REDUNDANT_COLUMNS_IN_RELATED` |
| **Severity** | Medium |
| **Impact** | Cost |

**What it checks:** Detects key columns (ending in `_sk`, `_id`, `_key`) that exist in both sides of a relationship.

**Why it's bad:** Duplicated keys inflate DirectQuery payload. The same data is read and transferred multiple times per query.

**Required action:** Remove redundant columns from the fact/child table. Use dimension tables as the single source of truth.

### 19. EXCESSIVE_CALCULATED_COLUMNS

| Field | Value |
|-------|-------|
| **ID** | `EXCESSIVE_CALCULATED_COLUMNS` |
| **Severity** | Low |
| **Impact** | Memory |

**What it checks:** Flags tables with more than 5 calculated columns.

**Why it's bad:** Calculated columns are computed at refresh and stored in memory, increasing VertiPaq footprint and refresh time.

**Required action:** Move logic to SQL/dbt upstream. Convert to measures where appropriate.

### 20. M_FOLDING_BLOCKERS

| Field | Value |
|-------|-------|
| **ID** | `M_FOLDING_BLOCKERS` |
| **Severity** | Low |
| **Impact** | Latency |

**What it checks:** Detects M/Power Query functions in partition expressions that are known to block query folding (e.g., `Table.AddColumn`, `Table.Buffer`, `Table.Sort`, `List.Generate`).

**Why it's bad:** Complex M steps prevent the PBI engine from pushing transformations to the source, forcing full data pulls.

**Required action:** Rewrite transformations to be foldable or push logic upstream to SQL/dbt.

---

## Composite Model Awareness

### Source Groups

Tables in a Power BI composite model belong to **source groups** determined by their data source connection. Tables sharing the same Databricks catalog+schema in DirectQuery mode are in the same source group.

### Limited Relationships

When tables from different source groups are related (e.g., an Import dimension to a DirectQuery fact), the relationship becomes **limited**:
- `RELATED()` and `RELATEDTABLE()` may not work or produce incorrect results
- Bidirectional cross-filtering is disabled across the boundary
- Row-level security filters may not propagate

### Dual-to-Import Decision Tree

1. **No relationships to DQ tables → Safe to Import** (Quick Win)
2. **Has DQ relationships but no RELATED() usage → Cautiously switchable** (verify with DAX Studio)
3. **Has DQ relationships AND RELATED() usage → Keep Dual** (refactor RELATED() first)
4. **Hidden and no relationships → Safe to Import** (no functional impact)
