# PBI Performance Diagnosis - Input

## Company Context
- **Company/Project Name**: ASOS ADE
- **Business Domain**: Retail / E-commerce
- **Brief Description**: Analytics Data Ecosystem (ADE) for ASOS, serving Power BI dashboards for sales, supply chain, customer, product, and sourcing domains. Semantic models use DirectQuery against Databricks SQL Warehouse via Unity Catalog.

## Reports to Analyse
<!-- List specific reports with performance issues. Write "Full" for complete analysis. -->
- ADE - OrderLine (critical - slowest report, fact table has 6B+ rows, 73 tables, 14 DirectQuery)
- ADE - Sales (slow page loads on billed sale visuals, 52 tables)
- ADE - Trade (daily trade snapshot slow on peak hours)

## Semantic Model Repository
- **Path**: /Users/brunogcmartins/environment/cit/ASOS/asos-data-workflow/asos-data-ade-powerbi
- **Models directory**: powerbi/models
- **Reports directory**: powerbi/reports (34 PBIX files — report layouts, visuals, filters, bookmarks)
- **BPA rules path**: powerbi/bestpracticeanalyser/BPARules.json

## dbt Repository
- **Path**: /Users/brunogcmartins/environment/cit/ASOS/asos-data-workflow/asos-data-ade-dbt
- **Serve layer pattern**: bundles/core_data/models/<domain>/serve/
- **Contracts pattern**: bundles/core_data/models/<domain>/serve/_contracts/
- **manifest.json path**: (not committed - run `dbt compile` to generate, or check `.dbt_manifest/`)

## Databricks Connection
- **Connection method**: MCP (Databricks MCP server via Claude Code)
- **Workspace URL**: https://adb-2762816844316267.7.azuredatabricks.net
- **SQL Warehouse (Production)**: Technology SQL Warehouse (`f0bdb929e2c1cf2d`) — used by PBI SPN
- **SQL Warehouse (Analysis)**: Engineering SQL Warehouse (`80bad8a5778c2e98`) — use for agent queries
- **PAT**: `<REDACTED — set DATABRICKS_TOKEN env var>`
- **Catalogs to analyse**: sales, product, customer, supplychain, sourcingandbuying, technology
- **PBI SPN executed_as filter**: `65978fad-bc17-4f5a-b134-25d299885855`

## Power BI Service Principal
- **Service Principal Name**: spn-ade-pbi
- **Object ID**: 65978fad-bc17-4f5a-b134-25d299885855
- **Query Source**: PowerBI
- **Note**: All queries from Power BI reports appear in Databricks Query History executed by this service principal. Filter `system.query.history` with `executed_as = '65978fad-bc17-4f5a-b134-25d299885855'` to isolate PBI traffic. The column is `executed_as` (NOT `executed_as_user_name`).

## MCP Servers Available
- **Flow MicroStrategy MCP** (`flow-microstrategy-prd-http`): Read-only access to Neo4j database with MicroStrategy metadata. Tools: `get-schema`, `read-cypher`, `search-metrics`, `search-attributes`, `trace-metric`, `trace-attribute`. Useful for cross-referencing PBI measures with MicroStrategy lineage.

## Power BI Report URLs
<!-- Optional: URLs for Playwright MCP browser analysis -->
- <!-- https://app.powerbi.com/groups/xxx/reports/yyy (ADE - OrderLine) -->

## Additional Context
- The main fact table `fact_order_line_v1` has over 6 billion rows
- `fact_product_option_trade_daily_snapshot_v1` also generates very heavy queries (4.4B rows read, 19.6 GB, 15s+ per query)
- Most reports use DirectQuery against Databricks SQL Warehouse (serverless)
- The serve layer in dbt is entirely views over curated Delta tables
- Curated tables use liquid clustering (e.g. fact_order_line_v1 clustered by date surrogate keys)
- Power BI Premium capacity is available (Fabric capacity: `fabcapdataservicesprod01`)
- The `_fn_GetDataFromDBX` function in expressions returns full tables with no M-level filtering
- All partition expressions are simple pass-through: `let DatabricksTable = _fn_GetDataFromDBX(...) in DatabricksTable`
- No aggregation tables exist in the current models
- Photon is enabled on the SQL Warehouse (99% of task time runs on Photon)
- Queries from PBI cross multiple catalogs in a single query (e.g. `sales.serve.*` JOIN `technology.serve.*` JOIN `product.serve.*`)
- PBI generates extremely wide column selections (90+ columns from fact tables, 47+ columns from dim_date), far more than the visual actually needs
- Date filtering is applied via JOIN against `dim_date_v2` with `calendar_date >= '2024-04-09' AND calendar_date < '2026-04-09'` (2-year window)
- See `references/example-slow-query.md` for a full annotated example of a 15s query from the ADE - Sales report
