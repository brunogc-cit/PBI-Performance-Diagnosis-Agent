# DAX Patterns & Anti-Patterns Reference

A comprehensive reference of common DAX patterns and anti-patterns for Power BI semantic model development.

---

## 1. Anti-Patterns (What to Avoid)

### 1.1 FILTER(ALL()) Instead of Direct Predicate

**Why it is bad:** `FILTER(ALL(...))` forces the engine to materialise the entire column into memory before filtering row by row. A direct predicate allows the storage engine to apply the filter natively, which is dramatically faster.

```dax
-- BAD: Iterates every row of the table
Sales in UK =
CALCULATE(
    [Total Sales],
    FILTER(ALL(Geography[Country]), Geography[Country] = "UK")
)

-- GOOD: Direct predicate — storage engine optimised
Sales in UK =
CALCULATE(
    [Total Sales],
    Geography[Country] = "UK"
)
```

### 1.2 IFERROR / ISERROR Instead of DIVIDE or Conditional

**Why it is bad:** `IFERROR` evaluates the expression twice — once to check for the error and once to return the result. It also masks genuine errors that should surface during development.

```dax
-- BAD: Double evaluation + hides real errors
Margin % =
IFERROR([Gross Profit] / [Revenue], 0)

-- GOOD: Safe division with explicit alternate result
Margin % =
DIVIDE([Gross Profit], [Revenue], 0)
```

### 1.3 Bare Division (/) Instead of DIVIDE()

**Why it is bad:** The `/` operator raises a divide-by-zero error when the denominator is zero or BLANK. `DIVIDE` handles this gracefully and returns an alternate result.

```dax
-- BAD: Crashes on zero denominator
Conversion Rate = [Orders] / [Sessions]

-- GOOD: Returns BLANK (or a specified alternate) when denominator is zero
Conversion Rate =
DIVIDE([Orders], [Sessions])
```

### 1.4 COUNT(column) Instead of COUNTROWS(table)

**Why it is bad:** `COUNT` only counts non-BLANK values in a single column and requires a column reference. `COUNTROWS` counts rows in the table regardless of BLANK values and is semantically clearer.

```dax
-- BAD: Counts non-blank values only — misleading when BLANKs exist
Order Count = COUNT(Orders[OrderID])

-- GOOD: Counts all rows in the filtered table
Order Count = COUNTROWS(Orders)
```

### 1.5 Repeated Subexpressions Without VAR

**Why it is bad:** Without `VAR`, the same subexpression is recalculated every time it appears. The engine may or may not optimise this away. Using `VAR` guarantees single evaluation and improves readability.

```dax
-- BAD: CALCULATE evaluated twice
Sales Growth =
([Total Sales] - CALCULATE([Total Sales], SAMEPERIODLASTYEAR('Date'[Date])))
    / CALCULATE([Total Sales], SAMEPERIODLASTYEAR('Date'[Date]))

-- GOOD: Single evaluation via VAR
Sales Growth =
VAR _CurrentSales = [Total Sales]
VAR _PriorYearSales =
    CALCULATE([Total Sales], SAMEPERIODLASTYEAR('Date'[Date]))
RETURN
    DIVIDE(_CurrentSales - _PriorYearSales, _PriorYearSales)
```

### 1.6 Nested CALCULATE (More Than 2 Levels)

**Why it is bad:** Deeply nested `CALCULATE` calls make filter context extremely difficult to reason about. Each level modifies context in ways that compound unpredictably. Refactor into separate measures or use `VAR`.

```dax
-- BAD: Three levels of nesting — very hard to debug
Complex Measure =
CALCULATE(
    CALCULATE(
        CALCULATE([Base Measure], Filter1),
        Filter2
    ),
    Filter3
)

-- GOOD: Flatten with VAR or decompose into sub-measures
Complex Measure =
VAR _Step1 = CALCULATE([Base Measure], Filter1, Filter2, Filter3)
RETURN
    _Step1
```

### 1.7 VALUES() in Filter Context Instead of SELECTEDVALUE

**Why it is bad:** `VALUES` returns a table. Using it where a scalar is expected causes errors when multiple values are in context. The common workaround of `IF(HASONEVALUE(...), VALUES(...))` is verbose and error-prone.

```dax
-- BAD: Fails when multiple values are in context
Selected Country =
IF(
    HASONEVALUE(Geography[Country]),
    VALUES(Geography[Country]),
    "Multiple"
)

-- GOOD: Single function, clean alternate result
Selected Country =
SELECTEDVALUE(Geography[Country], "Multiple")
```

### 1.8 SUMX Over Entire Table Without Filter (Unnecessary Iterator)

**Why it is bad:** `SUMX` iterates row by row. If you are simply summing a column with no row-level calculation, `SUM` is more efficient because the storage engine can use batch aggregation.

```dax
-- BAD: Row-by-row iteration for a simple column sum
Total Sales = SUMX(Sales, Sales[Amount])

-- GOOD: Batch aggregation — no iterator needed
Total Sales = SUM(Sales[Amount])

-- LEGITIMATE use of SUMX: row-level calculation required
Total Line Value = SUMX(Sales, Sales[Quantity] * Sales[UnitPrice])
```

### 1.9 Context Transition in Iterators (Row Context to Filter Context Overhead)

**Why it is bad:** When a measure is referenced inside an iterator (e.g., `SUMX`, `FILTER`), DAX performs context transition — converting row context to filter context via an implicit `CALCULATE`. This is expensive when iterating large tables.

```dax
-- BAD: Context transition on every row of a large fact table
Weighted Score =
SUMX(
    Sales,
    [Average Score] * Sales[Weight]
)

-- GOOD: Pre-calculate or use column references to avoid context transition
Weighted Score =
SUMX(
    Sales,
    RELATED(Products[AverageScore]) * Sales[Weight]
)
```

### 1.10 Unqualified Column References

**Why it is bad:** Unqualified column names (without the table prefix) can resolve ambiguously when tables have columns with the same name. Always qualify column references with the table name.

```dax
-- BAD: Ambiguous — which table does [Date] come from?
Filtered Sales =
CALCULATE([Total Sales], [Date] >= DATE(2024, 1, 1))

-- GOOD: Explicit table reference
Filtered Sales =
CALCULATE([Total Sales], 'Date'[Date] >= DATE(2024, 1, 1))
```

---

## 2. Performance Patterns (Recommended)

### 2.1 VAR / RETURN for Calculation Reuse

**When to use:** Any time a subexpression appears more than once, or when you want to improve readability by naming intermediate results.

```dax
YoY Growth % =
VAR _CurrentSales = [Total Sales]
VAR _PriorYearSales =
    CALCULATE([Total Sales], SAMEPERIODLASTYEAR('Date'[Date]))
VAR _Difference = _CurrentSales - _PriorYearSales
RETURN
    DIVIDE(_Difference, _PriorYearSales)
```

**Why it is efficient:** Each `VAR` is evaluated exactly once, regardless of how many times it is referenced in the `RETURN` expression. This also makes debugging easier — you can inspect individual variables in DAX Studio.

### 2.2 REMOVEFILTERS + Direct Predicate (Replaces FILTER(ALL))

**When to use:** When you need to override existing filter context with a specific value.

```dax
Sales for UK Regardless of Slicer =
CALCULATE(
    [Total Sales],
    REMOVEFILTERS(Geography[Country]),
    Geography[Country] = "UK"
)
```

**Why it is efficient:** `REMOVEFILTERS` explicitly clears the filter on the specified column, and the direct predicate is applied natively by the storage engine. No row-by-row iteration required.

### 2.3 DIVIDE() for Safe Division

**When to use:** Every division operation. There is no performance penalty and it prevents runtime errors.

```dax
Conversion Rate =
DIVIDE([Completed Orders], [Total Sessions], 0)
```

**Why it is efficient:** `DIVIDE` is internally optimised and avoids the overhead of `IFERROR` error trapping. The third argument specifies the alternate result when the denominator is zero or BLANK.

### 2.4 COUNTROWS Instead of COUNT

**When to use:** When counting rows in a table rather than non-BLANK values in a specific column.

```dax
Number of Transactions = COUNTROWS(Sales)
```

**Why it is efficient:** `COUNTROWS` operates on the table's row count directly, without needing to scan a column for BLANK values.

### 2.5 SELECTEDVALUE Instead of VALUES / HASONEVALUE Combo

**When to use:** When you need the single selected value from a slicer or filter, with a fallback for multi-select.

```dax
Current Region =
SELECTEDVALUE(Geography[Region], "All Regions")
```

**Why it is efficient:** Single function call replacing a two-function pattern. Cleaner, faster, and less error-prone.

### 2.6 Pre-filtering with CALCULATETABLE for Complex Scenarios

**When to use:** When you need to iterate over a filtered subset of a table and the filter logic is complex.

```dax
High Value Customer Sales =
SUMX(
    CALCULATETABLE(
        Customers,
        Customers[LifetimeValue] > 10000,
        Customers[Status] = "Active"
    ),
    [Total Sales]
)
```

**Why it is efficient:** The filtered table is materialised once and then iterated. Without `CALCULATETABLE`, the filter would be evaluated on every iteration.

### 2.7 TREATAS for Virtual Relationships

**When to use:** When you need to apply a filter across tables that do not have a physical relationship in the model, or when you want to override an existing relationship.

```dax
Budget by Product =
CALCULATE(
    SUM(Budget[Amount]),
    TREATAS(VALUES(Products[ProductKey]), Budget[ProductKey])
)
```

**Why it is efficient:** `TREATAS` applies the filter without creating a physical relationship, avoiding model bloat. It is more efficient than `LOOKUPVALUE` or `FILTER` for this purpose.

### 2.8 KEEPFILTERS for AND Filter Semantics

**When to use:** When you want your `CALCULATE` filter to intersect with (rather than replace) the existing filter context.

```dax
-- Without KEEPFILTERS: replaces slicer selection
-- With KEEPFILTERS: intersects with slicer selection
Red Product Sales =
CALCULATE(
    [Total Sales],
    KEEPFILTERS(Products[Colour] = "Red")
)
```

**Why it is efficient:** `KEEPFILTERS` preserves the existing filter context and adds the new condition as an intersection. This is essential when you want slicers to continue working alongside programmatic filters.

---

## 3. Time Intelligence Patterns

### 3.1 Year-to-Date (YTD)

```dax
Sales YTD =
CALCULATE(
    [Total Sales],
    DATESYTD('Date'[Date])
)
```

### 3.2 Month-to-Date (MTD)

```dax
Sales MTD =
CALCULATE(
    [Total Sales],
    DATESMTD('Date'[Date])
)
```

### 3.3 Previous Year

```dax
Sales Previous Year =
CALCULATE(
    [Total Sales],
    SAMEPERIODLASTYEAR('Date'[Date])
)
```

### 3.4 Year-over-Year Growth (VAR + DIVIDE Pattern)

```dax
Sales YoY Growth % =
VAR _CurrentSales = [Total Sales]
VAR _PriorYearSales =
    CALCULATE(
        [Total Sales],
        SAMEPERIODLASTYEAR('Date'[Date])
    )
RETURN
    DIVIDE(
        _CurrentSales - _PriorYearSales,
        _PriorYearSales
    )
```

### 3.5 Rolling 12 Months (DATESINPERIOD)

```dax
Sales Rolling 12M =
CALCULATE(
    [Total Sales],
    DATESINPERIOD(
        'Date'[Date],
        MAX('Date'[Date]),
        -12,
        MONTH
    )
)
```

Rolling average that only counts months with data:

```dax
Sales Rolling 12M Average =
VAR _Period =
    DATESINPERIOD(
        'Date'[Date],
        MAX('Date'[Date]),
        -12,
        MONTH
    )
VAR _TotalSales =
    CALCULATE([Total Sales], _Period)
VAR _MonthsWithData =
    CALCULATE(
        COUNTROWS(
            FILTER(
                VALUES('Date'[YearMonth]),
                NOT ISBLANK([Total Sales])
            )
        ),
        _Period
    )
RETURN
    DIVIDE(_TotalSales, _MonthsWithData)
```

### 3.6 Custom Fiscal Calendar (DATESYTD with year_end_date)

For a fiscal year ending 31 August:

```dax
Sales Fiscal YTD =
CALCULATE(
    [Total Sales],
    DATESYTD('Date'[Date], "08-31")
)
```

### 3.7 Parallel Period: DATEADD vs PARALLELPERIOD

`DATEADD` shifts a set of dates by a fixed interval — it respects the current filter context exactly.

`PARALLELPERIOD` shifts to the full period (complete months/quarters/years) regardless of the current day-level filter.

```dax
-- DATEADD: Shifts the exact filtered dates back by 1 year
-- If the filter is 1-15 Jan 2024, this returns 1-15 Jan 2023
Sales Same Days Last Year =
CALCULATE(
    [Total Sales],
    DATEADD('Date'[Date], -1, YEAR)
)

-- PARALLELPERIOD: Shifts to the complete parallel period
-- If the filter is 1-15 Jan 2024, this returns ALL of Jan 2023
Sales Full Month Last Year =
CALCULATE(
    [Total Sales],
    PARALLELPERIOD('Date'[Date], -12, MONTH)
)
```

**Rule of thumb:** Use `DATEADD` for like-for-like day comparisons. Use `PARALLELPERIOD` when you need the full period regardless of the day-level filter.

---

## 4. Ranking & Top N Patterns

### 4.1 RANKX with ALLSELECTED

```dax
Product Sales Rank =
RANKX(
    ALLSELECTED(Products[ProductName]),
    [Total Sales],
    ,
    DESC,
    DENSE
)
```

`ALLSELECTED` ensures the ranking respects slicer selections but ignores the current row's filter. The `DENSE` parameter avoids gaps in rank numbers when ties occur.

### 4.2 Dynamic Top N with Parameter Table

Create a disconnected parameter table:

```dax
Top N Parameter = GENERATESERIES(3, 20, 1)
```

Then use it in a measure:

```dax
Top N Sales =
VAR _TopN = SELECTEDVALUE('Top N Parameter'[Top N Parameter], 10)
VAR _CurrentRank =
    RANKX(
        ALLSELECTED(Products[ProductName]),
        [Total Sales],
        ,
        DESC,
        DENSE
    )
RETURN
    IF(_CurrentRank <= _TopN, [Total Sales])
```

### 4.3 TOPN for Filtered Subsets

```dax
Top 5 Product Sales =
SUMX(
    TOPN(
        5,
        Products,
        [Total Sales],
        DESC
    ),
    [Total Sales]
)
```

**Note:** `TOPN` returns a table, so wrap it in an aggregation function (`SUMX`, `MAXX`, etc.) when a scalar result is needed.

---

## 5. Semi-Additive Measures

### 5.1 LASTNONBLANK for Balance at Date

For measures where you want the last known value (e.g., stock levels, account balances):

```dax
Closing Stock =
CALCULATE(
    SUM(Inventory[StockLevel]),
    LASTNONBLANK(
        'Date'[Date],
        CALCULATE(COUNTROWS(Inventory))
    )
)
```

### 5.2 OPENINGBALANCEMONTH / CLOSINGBALANCEMONTH

```dax
Opening Balance =
OPENINGBALANCEMONTH(
    SUM(Accounts[Balance]),
    'Date'[Date]
)

Closing Balance =
CLOSINGBALANCEMONTH(
    SUM(Accounts[Balance]),
    'Date'[Date]
)
```

### 5.3 Custom Semi-Additive with CALCULATE + LASTDATE

When the built-in functions do not fit your requirements:

```dax
Latest Stock Value =
CALCULATE(
    SUM(Inventory[StockValue]),
    LASTDATE('Date'[Date])
)
```

For a custom period (e.g., last date in the current quarter):

```dax
Quarter End Stock =
VAR _LastDateInQuarter =
    CALCULATE(
        MAX('Date'[Date]),
        DATESQTD('Date'[Date])
    )
RETURN
    CALCULATE(
        SUM(Inventory[StockValue]),
        'Date'[Date] = _LastDateInQuarter
    )
```

---

## 6. Dynamic Segmentation

### 6.1 SWITCH(TRUE(), ...) for Multi-Condition

```dax
Customer Segment =
SWITCH(
    TRUE(),
    [Lifetime Value] >= 10000, "Platinum",
    [Lifetime Value] >= 5000, "Gold",
    [Lifetime Value] >= 1000, "Silver",
    "Bronze"
)
```

**Note:** Conditions are evaluated top to bottom. Place the most restrictive condition first.

### 6.2 Disconnected Slicer Tables

Create a table that is not related to any other table in the model:

```dax
Metric Selector =
DATATABLE(
    "Metric", STRING,
    {
        {"Revenue"},
        {"Gross Profit"},
        {"Net Profit"},
        {"Units Sold"}
    }
)
```

Then use it to drive dynamic measure selection:

```dax
Selected Metric Value =
VAR _Selection = SELECTEDVALUE('Metric Selector'[Metric], "Revenue")
RETURN
    SWITCH(
        _Selection,
        "Revenue", [Total Revenue],
        "Gross Profit", [Gross Profit],
        "Net Profit", [Net Profit],
        "Units Sold", [Units Sold],
        BLANK()
    )
```

### 6.3 What-If Parameters with GENERATESERIES

```dax
Price Adjustment % =
GENERATESERIES(-0.20, 0.20, 0.01)
```

Selected value measure:

```dax
Price Adjustment Value =
SELECTEDVALUE('Price Adjustment %'[Price Adjustment %], 0)
```

Applied in a scenario measure:

```dax
Adjusted Revenue =
VAR _Adjustment = [Price Adjustment Value]
RETURN
    [Total Revenue] * (1 + _Adjustment)
```

---

## Quick Reference: Function Substitutions

| Avoid | Prefer | Reason |
|---|---|---|
| `FILTER(ALL(T[Col]), ...)` | Direct predicate in `CALCULATE` | Storage engine optimisation |
| `IFERROR(x/y, 0)` | `DIVIDE(x, y, 0)` | Single evaluation, no error masking |
| `x / y` | `DIVIDE(x, y)` | Divide-by-zero safety |
| `COUNT(T[Col])` | `COUNTROWS(T)` | Counts rows, not non-blanks |
| `IF(HASONEVALUE(...), VALUES(...))` | `SELECTEDVALUE(...)` | Cleaner, single function |
| `FILTER(T, ...)` in `CALCULATE` | Direct predicate or `KEEPFILTERS` | Avoids materialisation |
| Nested `CALCULATE` (3+ levels) | `VAR` + single `CALCULATE` | Readability and correctness |

---

## Compound Anti-Pattern Tiers

The anti-pattern tier system scores each measure by how many distinct anti-pattern flags it triggers. Multiple patterns compound cost because the engine cannot reuse cached results.

### 9-Flag Taxonomy

| Flag | Functions | Why Expensive |
|------|-----------|---------------|
| ITERATOR | SUMX, AVERAGEX, RANKX, COUNTX | Row-by-row evaluation; multiplies sub-queries |
| ALL_FILTER | ALL, ALLEXCEPT, ALLSELECTED | Clears filter context; forces full re-aggregation |
| ROW_FILTER | FILTER() on a table | Full table scan; cannot use dictionary indexes |
| SWITCH_IF | SWITCH(TRUE()), IF() | All branches evaluated; no short-circuiting |
| TIME_INTEL | DATEADD, DATESINPERIOD, etc. | Creates virtual date tables; adds joins |
| NESTED_CALC | CALCULATE inside CALCULATE | Multiple context transitions; expensive |
| USERELATIONSHIP | USERELATIONSHIP() | Forces alternate join path; prevents caching |
| CROSSJOIN | CROSSJOIN, GENERATE | Cartesian explosion; huge virtual tables |
| DIVIDE_CALC | DIVIDE(CALCULATE(...)) | Prevents single-pass aggregation |

### Severity Tiers

| Tier | Flag Count | Description |
|------|-----------|-------------|
| Critical | 4+ | Worst patterns combined; extremely slow |
| High Risk | 3 | Heavy compound cost; significant latency |
| Medium | 2 | Noticeable latency on high-cardinality visuals |
| Low Risk | 1 | Minor individually; expensive as dependencies |
| Clean | 0 | No anti-patterns detected |

### Pattern Families

Measures are grouped into **pattern families** based on naming conventions and structural DAX fingerprint. Common families:

- **WTD / YD-1 Window Measures**: SWITCH(TRUE()) + SUMX day-by-day + FILTER(ALL(Date))
- **Average Weekly Cover Measures**: ADDCOLUMNS virtual tables + AVERAGEX + ALL(Date)
- **LY / LM Comparison Measures**: DATE(YEAR()-1) inside SUMX + FILTER(ALL(Date))
- **L7D Rolling Window**: DATESINPERIOD + FILTER(ALL(Date))
- **Opening Balance**: Bridge table lookups via CALCULATETABLE + FILTER(ALL(Date))

Each family has a consolidated "Why it's slow" and "Required actions" block that applies to all measures in the group.

### Dependency Chain Amplification

When a high-tier measure (e.g., Critical with 4+ flags) calls a lower-tier measure inside an iterator (SUMX, COUNTX, etc.), the cost is multiplied. The dependency chain analysis identifies these amplification patterns and prioritises fixing them.
