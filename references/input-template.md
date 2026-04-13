# PBI Performance Diagnosis - Input

## Company Context
- **Company/Project Name**: <!-- REQUIRED: e.g. ASOS ADE -->
- **Business Domain**: <!-- e.g. Retail / E-commerce -->
- **Brief Description**: <!-- Brief description of the analytics platform and its purpose -->

## Reports to Analyse
<!-- List specific reports with performance issues, one per line.
     Leave empty or write "Full" for complete analysis of all models. -->
- <!-- Report name or model name (add notes about severity if known) -->

## Semantic Model Repository
<!-- Path to the Power BI semantic model source code (Tabular Editor / PBIR format) -->
- **Path**: <!-- REQUIRED (at least one data source): absolute or relative path -->
- **Models directory**: <!-- e.g. powerbi/models -->
- **Reports directory**: <!-- e.g. powerbi/reports (contains .pbix files with report layouts, visuals, filters) -->
- **BPA rules path**: <!-- e.g. powerbi/bestpracticeanalyser/BPARules.json (optional) -->

## dbt Repository
<!-- Path to the dbt project. Leave empty if not available. -->
- **Path**: <!-- absolute or relative path, or leave empty -->
- **Serve layer pattern**: <!-- e.g. bundles/core_data/models/<domain>/serve/ -->
- **Contracts pattern**: <!-- e.g. bundles/core_data/models/<domain>/serve/_contracts/ -->
- **manifest.json path**: <!-- path to compiled manifest, or leave empty -->

## Databricks Connection
<!-- How the agent should connect to Databricks. Fill one option or write "Skip". -->
- **Connection method**: <!-- MCP | PAT | Skip -->
- **Workspace URL**: <!-- e.g. https://adb-xxxx.azuredatabricks.net -->
- **SQL Warehouse HTTP Path**: <!-- e.g. /sql/1.0/warehouses/xxxx -->
- **PAT**: <!-- Set as env var DATABRICKS_TOKEN - do NOT paste tokens here -->
- **Catalogs to analyse**: <!-- comma-separated list, e.g. sales, product, customer -->

## Power BI Report URLs
<!-- Optional: URLs for Playwright MCP browser analysis.
     User will need to authenticate manually in the browser. -->
- <!-- https://app.powerbi.com/groups/xxx/reports/yyy (Report Name) -->

## Additional Context
<!-- Any extra information that may help the analysis.
     Examples: known bottlenecks, recent changes, table sizes, refresh schedules. -->
- <!-- Add context here -->
