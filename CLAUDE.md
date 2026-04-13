# CLAUDE.md

This file provides guidance to AI agents (Claude Code, Cursor, Copilot, or any MCP-compatible runtime) when working with the **PBI Performance Diagnosis Agent**.

## Project Overview

A **standalone AI agent** that performs end-to-end Power BI performance diagnosis across the full stack: semantic models, DAX measures, dbt data pipelines, and Databricks infrastructure. It produces comprehensive HTML reports with prioritised recommendations.

**Key characteristics:**
- Fully self-contained — no dependency on any web app or extension
- Input-driven via `input.md` — all project-specific config lives there
- British English mandatory (analyse, optimise, behaviour, colour)
- No git operations — the human handles all git work
- All output written to `output/` only
- Python scripts use stdlib only (no pip install needed)
- Reusable across any Databricks + Power BI project

## Directory Structure

```
PBI-Performance-Diagnosis-Agent/
├── CLAUDE.md                          # This file — agent context
├── SKILL.md                           # Main agent prompt + 8-step workflow
├── input.md                           # User-provided project config (ASOS pre-filled)
├── .gitignore                         # Excludes output artefacts
│
├── scripts/                           # Python analysis scripts (stdlib only)
│   ├── analyse_semantic_model.py      # Step 1: parse PBI model → model-taxonomy.json
│   ├── analyse_dax_complexity.py      # Step 2: score DAX complexity → dax-complexity.json
│   ├── audit_dax.py                   # Step 2: DAX anti-pattern scan → dax-audit.json
│   ├── analyse_dbt_lineage.py         # Step 3: dbt serve-layer lineage → dbt-lineage.json
│   ├── run_bpa.py                     # Step 6: BPA rule checks → bpa-results.json
│   ├── parse_perf_analyzer.py         # Step 5: parse PBI Perf Analyzer JSON → perf-summary.json
│   ├── generate_report.py             # Step 8: produce HTML report from intermediate JSONs
│   └── requirements.txt               # Empty — no external deps
│
├── references/                        # Knowledge base (read-only)
│   ├── input-template.md              # Blank input.md for new projects
│   ├── report-template.html           # HTML/CSS template for report styling
│   ├── bpa-rules-reference.md         # 12 BPA rules with examples and fixes
│   ├── dax-patterns.md                # DAX anti-patterns + recommended alternatives
│   └── system-tables-queries.md       # Databricks SQL queries for metadata/volume/profiling
│
├── plan/                              # Design documentation
│   ├── pbi-performance-diagnosis-agent.plan.md   # Full agent architecture plan
│   └── pbi-directquery-performance.plan.md       # DirectQuery performance strategy plan
│
└── output/                            # Agent writes all results here (gitignored)
    └── .gitkeep
```

## How the Agent Works

### Execution Flow

The agent follows an 8-step workflow defined in `SKILL.md`:

| Step | Name | Script | Output | Required |
|------|------|--------|--------|----------|
| 0 | Input Validation | — (reads `input.md`) | Human confirmation | Always |
| 1 | Semantic Model Analysis | `analyse_semantic_model.py` | `model-taxonomy.json` | Needs PBI repo |
| 2 | DAX Complexity Analysis | `audit_dax.py` + `analyse_dax_complexity.py` | `dax-audit.json` + `dax-complexity.json` | Needs PBI repo |
| 3 | dbt Source Code Analysis | `analyse_dbt_lineage.py` | `dbt-lineage.json` | Optional (needs dbt repo) |
| 4 | Databricks Metadata Profiling | — (MCP/manual queries) | `databricks-profile.json` | Optional (needs Databricks) |
| 5 | Query Profiling | `parse_perf_analyzer.py` | `perf-summary.json` / `query-profile.json` | Optional |
| 6 | Best Practice Analyser | `run_bpa.py` | `bpa-results.json` | Needs PBI repo |
| 7 | Cross-Reference + Synthesis | — (agent reasoning) | `synthesis.json` | Always |
| 8 | Report Generation | `generate_report.py` | `*_Performance_Diagnosis.html` | Always |

### Step 0 is Mandatory

The agent **must read `input.md` first**, validate all paths and config, present a checklist to the user, and wait for explicit confirmation before running any analysis. If `input.md` is missing, copy `references/input-template.md` and ask the user to fill it in.

### Graceful Degradation

The agent adapts to whatever data sources are available:

| Available | Steps Enabled |
|-----------|---------------|
| PBI semantic model only | 1, 2, 6, 7, 8 |
| PBI + dbt | 1, 2, 3, 6, 7, 8 |
| PBI + Databricks | 1, 2, 4, 5, 6, 7, 8 |
| PBI + dbt + Databricks | All (1–8) |
| Only Databricks | 4, 5, 7, 8 |
| Only report URL (Playwright) | 5, 7, 8 |

Steps that cannot run due to missing data are marked as SKIPPED in the checklist and noted as limitations in the final report.

## Scripts

All scripts are Python 3.10+ using **only the standard library** (json, re, os, argparse, pathlib, sys, datetime, html, statistics). No `pip install` is needed.

### Script CLI Reference

```bash
# Step 1: Analyse semantic model structure
python3 scripts/analyse_semantic_model.py \
  --model-path <path-to-PBI-model-dir> \
  --output output/ \
  --volumetry-file output/databricks-profile.json  # optional: enriches with row counts + sizes

# Step 2a: DAX anti-pattern audit
python3 scripts/audit_dax.py \
  --model-path <path-to-PBI-model-dir> \
  --output output/

# Step 2b: DAX complexity scoring + hot tables
python3 scripts/analyse_dax_complexity.py \
  --model-path <path-to-PBI-model-dir> \
  --output output/ \
  --taxonomy-file output/model-taxonomy.json  # optional: enriches hot tables with volumetry + degree

# Step 3: dbt lineage analysis
python3 scripts/analyse_dbt_lineage.py \
  --dbt-path <path-to-dbt-project-root> \
  --output output/

# Step 5: Parse Performance Analyzer export
python3 scripts/parse_perf_analyzer.py \
  --input <path-to-perf-analyzer.json> \
  --output output/

# Step 6: BPA rule checks
python3 scripts/run_bpa.py \
  --model-path <path-to-PBI-model-dir> \
  --output output/

# Step 8: Generate HTML report from intermediate JSONs
# --run-label is a brief LLM-generated description for the execution
# The script creates output/YYYY-MM-DD_HHMM_<run-label>/ and moves all files there
python3 scripts/generate_report.py \
  --input output/ \
  --output output/ \
  --model-name "Model Name" \
  --run-label "brief-description"
```

### Script Details

**`analyse_semantic_model.py`** — Parses PBI model directories (Tabular Editor JSON serialisation). Reads `database.json`, `expressions/*.json`, `tables/*/partitions/*.json`, `tables/*/<Table>.json` (extended properties), `relationships/*.json`. Extracts table storage modes, column counts, measure counts, Databricks source mapping. **New in v2**: auto-classifies tables as fact/dimension/bridge/metadata from relationship structure. Computes graph analysis (degree centrality, hub tables, snowflake depth). Accepts `--volumetry-file` for Databricks row count/size enrichment. Outputs `model-taxonomy.json`.

**`analyse_dax_complexity.py`** — Two-pass analysis: first collects table storage modes, then scores each measure's complexity. **New in v2**: complexity scoring prioritises context transitions (3 pts each), relationship hops (2 pts each), and FILTER(ALL) count (4 pts each) over LOC (1 pt for >30 lines). Each measure includes `contextTransitions`, `relationshipHops`, `filterAllCount`, and `estimatedSQLSubqueries`. Accepts `--taxonomy-file` for hot table enrichment with volumetry, degree, and computed `optimizationPriority` (critical/high/medium/low). Outputs `dax-complexity.json`.

**`audit_dax.py`** — Scans all DAX measure expressions for 10 anti-pattern rules (FILTER_ALL, IFERROR/ISERROR, nested CALCULATE, repeated subexpression, bare division, COUNT vs COUNTROWS, missing format string, unqualified columns, no VAR, hardcoded values). Strips comments and string literals before pattern matching. Outputs `dax-audit.json`.

**`analyse_dbt_lineage.py`** — Parses dbt serve-layer SQL for `ref()` calls, WHERE filters, UNION ALL, and column counts. Reads contract YAML files for materialisation config (view/table/incremental), liquid clustering, unique keys. **New in v2**: includes a value gate that identifies `actionableFindings` (wide-serve-view, missing-filter, should-materialise, missing-clustering) and sets `hasActionableFindings` boolean. The report collapses this section when no actionable findings exist. Outputs `dbt-lineage.json`.

**`run_bpa.py`** — Runs 12 Best Practice Analyser rules against PBI semantic model files (supports both JSON/TE2 and TMDL formats). Rules cover floating-point types, bidirectional relationships, dual mode tables, FILTER(ALL), IFERROR, wide tables, missing format strings, many-to-many, bare division, auto date tables, unqualified columns, unused columns. **New in v2**: each rule includes `performanceImpact` metadata (latency/cost/quality/memory) with descriptions. Output separates `ruleResults` (violations only) from `passingRules` (collapsed list). Outputs `bpa-results.json`.

**`parse_perf_analyzer.py`** — Parses Power BI Desktop Performance Analyzer JSON exports. Analyses visual load times, query durations, render costs. Groups visuals under User Actions. Computes summary statistics (avg, median, P95, page load, bottleneck classification). Outputs `perf-summary.json`.

**`generate_report.py`** — Reads all intermediate JSONs from the output directory, reads `synthesis.json` for root cause findings with **Where** classification (Engineering / Semantic Model / Power BI), and produces a single self-contained HTML file with inline CSS. Creates a timestamped subdirectory (`output/YYYY-MM-DD_HHMM_<run-label>/`) and moves all intermediate files there. **New in v2**: Model taxonomy shows classification (fact/dim), volumetry (rows, GB), and relationship topology (hub tables, snowflake depth). DAX complexity shows context transitions as primary metric. Hot tables include volumetry, degree, and optimisation priority. BPA shows impact type per rule and hides passing rules. dbt section is conditional (collapsed when no actionable findings). RCA includes scope badges (model-wide/report-specific). Detailed recommendations include evidence blocks, impact breakdowns, connection mode comparisons, dependency chains, sub-findings, and deep-dive flags.

## PBI Semantic Model Format

The agent works with Power BI semantic models serialised via **Tabular Editor** in JSON format (also known as PBIR/TE2 format). The directory structure is:

```
<Model Name>/
├── database.json                # Model metadata, annotations, BPA config
├── expressions/                 # M language expressions (data source functions)
│   ├── _fn_GetDataFromDBX.json  # Main Databricks connector function
│   ├── _DbxServer.json          # Server URL parameter
│   ├── _DbxEndpoint.json        # SQL Warehouse endpoint
│   └── ...
├── tables/
│   ├── <TableName>/
│   │   ├── <TableName>.json     # Table metadata, extendedProperties (source mapping)
│   │   ├── columns/             # Column definitions (name, dataType, isHidden)
│   │   ├── measures/            # DAX measure expressions
│   │   └── partitions/          # Source config + storage mode (import/directQuery/dual)
│   └── ...
└── relationships/               # Relationship definitions (from/to, cardinality, cross-filter)
```

The scripts also support **TMDL format** (`.tmdl` files) — both `run_bpa.py` and `audit_dax.py` auto-detect the format.

## PBIX Report Files

The PBI repository also contains `.pbix` report files at `powerbi/reports/<ReportName>/<ReportName>.pbix` (34 reports). A `.pbix` is a ZIP containing:

- `Layout` — JSON with pages, visuals, filters, bookmarks, and slicer configs
- `DataModelSchema` — JSON with the embedded semantic model

Extract with: `unzip -o "<path>.pbix" Layout DataModelSchema -d output/pbix_extracted`

Use PBIX analysis when:
- Correlating `VisualId` from Databricks query history to specific report visuals
- Checking which page-level/report-level filters exist (e.g., detecting missing date slicers)
- Understanding which measures are bound to which visuals
- The user asks about a specific report's layout or behaviour

**Important**: Prefer the Tabular Editor JSON in `powerbi/models/` for model analysis. Use PBIX only for report layout/visual/filter information.

## dbt Project Format

The dbt project follows ASOS conventions but the scripts are generic:

```
<dbt-root>/
├── dbt_project.yml
└── bundles/core_data/models/
    └── <domain>/                    # e.g. sales, customer, product
        ├── serve/                   # Serve layer (views consumed by PBI)
        │   ├── serve_*.sql          # SQL with {{ ref('curated_*') }}
        │   └── _contracts/
        │       └── serve_*.yml      # Materialisation, alias, lifecycle
        └── curated/                 # Curated layer (Delta tables)
            └── _contracts/
                └── curated_*.yml    # liquid_clustered_by, incremental_strategy
```

The lineage script looks for `bundles/core_data/models/<domain>/serve/` first, then falls back to `models/<domain>/serve/` for non-ASOS projects.

## Databricks Connectivity

Three options (configured in `input.md`):

1. **Databricks MCP** (recommended) — `npx databricks-mcp-server` via Claude Code MCP, uses Databricks CLI OAuth tokens
2. **PAT** — Personal Access Token in `DATABRICKS_TOKEN` env var, used by Python scripts or REST API
3. **Skip** — Databricks analysis steps are skipped; the agent works with PBI + dbt data only

### Databricks MCP Setup (Recommended)

The Databricks MCP server provides direct SQL execution and Unity Catalog browsing from within Claude Code or any MCP-compatible runtime.

#### Prerequisites

1. **Install Databricks CLI** (if not already installed):
   ```bash
   brew install databricks/tap/databricks
   ```

2. **Authenticate with Databricks** (OAuth via browser — creates a local token):
   ```bash
   databricks auth login --host https://adb-2762816844316267.7.azuredatabricks.net
   ```
   This opens a browser for Azure AD login. Once complete, a profile named `adb-2762816844316267` is created in `~/.databrickscfg`.

3. **Verify authentication**:
   ```bash
   databricks auth env --profile adb-2762816844316267
   ```
   You should see `DATABRICKS_AUTH_TYPE: databricks-cli` and the correct host in the output.

#### MCP Configuration

Add the following to your `.mcp.json` (project root or `~/.claude/.mcp.json`):

```json
{
  "mcpServers": {
    "databricks": {
      "command": "npx",
      "args": ["-y", "databricks-mcp-server"],
      "env": {
        "DATABRICKS_HOST": "https://adb-2762816844316267.7.azuredatabricks.net",
        "DATABRICKS_CONFIG_PROFILE": "adb-2762816844316267",
        "DATABRICKS_WAREHOUSE_ID": "f0bdb929e2c1cf2d"
      }
    }
  }
}
```

> **Note**: `DATABRICKS_WAREHOUSE_ID` is the warehouse ID only (not the full HTTP path). The MCP server uses Databricks CLI OAuth tokens automatically — no PAT needed.

#### Available MCP Tools

| Tool | Description |
|------|-------------|
| `execute_sql` | Execute SQL statements on a Databricks warehouse (supports `warehouse_id`, `max_rows`, `execution_timeout_seconds`) |
| `get_table` | Get detailed table info by full name (`catalog.schema.table`) |
| `list_catalogs` | List all catalogs in the workspace |
| `list_schemas` | List schemas in a catalog |
| `list_tables` | List tables in a schema (supports `table_name_pattern` regex filter) |
| `list_warehouses` | List all SQL warehouses and their status |

#### Warehouse Access

The user running the agent needs `CAN USE` permission on the target SQL Warehouse. The available warehouses are:

| Warehouse | ID | Purpose |
|-----------|-----|---------|
| Technology SQL Warehouse | `f0bdb929e2c1cf2d` | Production PBI queries (used by `spn-ade-pbi`) |
| Engineering SQL Warehouse | `80bad8a5778c2e98` | Engineering / ad-hoc queries |
| DBT ETL SQL Warehouse | `206c7a32b698f960` | dbt ETL runs |
| DBT DEV SQL Warehouse | `b0e1490aee9df380` | dbt development |
| DBT CI SQL Warehouse | `fab1ac2d4644fa14` | dbt CI pipelines |

If `execute_sql` returns "You do not have permission to use the SQL Warehouse", either:
- Ask your Databricks admin to grant `CAN USE` on the target warehouse
- Override the warehouse at query time: `execute_sql` accepts an optional `warehouse_id` parameter — try `80bad8a5778c2e98` (Engineering) as a fallback

#### Troubleshooting

| Problem | Solution |
|---------|----------|
| `npx` not found | Install Node.js: `brew install node` |
| Auth token expired | Re-run `databricks auth login --host https://adb-2762816844316267.7.azuredatabricks.net` |
| MCP tools not appearing | Restart Claude Code after creating/editing `.mcp.json` — MCP servers load at startup |
| Profile not found | Check `~/.databrickscfg` contains a `[adb-2762816844316267]` section |
| Permission denied on warehouse | Use `list_warehouses` to find an accessible warehouse, or request `CAN USE` from admin |

### Current ASOS Databricks Configuration

| Field | Value |
|-------|-------|
| Workspace URL | `https://adb-2762816844316267.7.azuredatabricks.net` |
| SQL Warehouse (Production) | Technology SQL Warehouse (`f0bdb929e2c1cf2d`) |
| SQL Warehouse (Fallback) | Engineering SQL Warehouse (`80bad8a5778c2e98`) |
| HTTP Path | `/sql/1.0/warehouses/f0bdb929e2c1cf2d` |
| Auth Method | Databricks CLI OAuth (`databricks-cli` profile) |
| PBI Service Principal | `spn-ade-pbi` (object ID: `65978fad-bc17-4f5a-b134-25d299885855`) |
| Query Source | PowerBI |
| Photon | Enabled (99% of task time) |
| Catalogs | sales, product, customer, supplychain, sourcingandbuying, technology |

To filter PBI queries in `system.query.history`:
```sql
WHERE executed_as_user_name LIKE '%spn-ade-pbi%'
```

All Databricks queries the agent needs are documented in `references/system-tables-queries.md`.

## MCP Servers

### Databricks MCP (`databricks`)

Provides direct access to Databricks SQL Warehouse and Unity Catalog from Claude Code. See [Databricks MCP Setup](#databricks-mcp-setup-recommended) above for installation instructions.

**Tools:** `execute_sql`, `get_table`, `list_catalogs`, `list_schemas`, `list_tables`, `list_warehouses`

**Usage**: Available natively in Claude Code when `.mcp.json` is configured. The agent uses `execute_sql` for Steps 4 (Databricks Metadata Profiling) and 5 (Query Profiling) to run queries from `references/system-tables-queries.md` directly against the warehouse.

### Flow MicroStrategy MCP (`user-flow-microstrategy-prd-http`)

Available via Cursor's MCP configuration. Provides read-only access to a Neo4j database containing MicroStrategy metadata.

**Tools:**
| Tool | Purpose |
|------|---------|
| `get-schema` | Retrieve Neo4j node labels, relationship types, property keys |
| `read-cypher` | Execute read-only Cypher queries against Neo4j |
| `search-metrics` | Find MicroStrategy metrics by GUID or name |
| `search-attributes` | Find MicroStrategy attributes by GUID or name |
| `trace-metric` | Trace metric lineage (upstream/downstream) |
| `trace-attribute` | Trace attribute lineage |

**Usage**: Call via `CallMcpTool` with `server: "user-flow-microstrategy-prd-http"`. Useful for cross-referencing PBI measures with their MicroStrategy origins and understanding lineage.

## DirectQuery Performance Patterns (Observed)

Real Databricks Query History from PBI reports reveals these patterns:

1. **Column over-selection**: PBI selects ALL columns from every table in the model (90+ from fact, 47+ from dim_date, 73+ from dim_product_option) even when the visual only needs a few aggregated values.

2. **Scan amplification**: A single visual returning 2 rows reads 4.4 billion rows and 19.6 GB of data. Ratio of rows_read:rows_returned exceeds 2,000,000,000:1.

3. **Cross-catalog JOINs**: One PBI visual generates SQL that JOINs tables from `sales.serve`, `technology.serve`, and `product.serve` in a single query.

4. **Date filter via JOIN, not WHERE**: Date predicates are applied to `dim_date_v2`, then JOINed to the fact table via surrogate key. The fact table itself receives no direct date filter, preventing partition/cluster pruning.

5. **Deeply nested subqueries**: PBI DirectQuery generates 4-5 levels of nested subqueries (one per relationship hop), with full column projections at each level.

6. **All serve views are pass-through**: Every serve model is `SELECT * FROM curated_table` — no filtering, no column reduction. The full curated table is scanned on every query.

See `references/example-slow-query.md` for a fully annotated 15s query example with root cause analysis.

## Current ASOS Configuration

The `input.md` is pre-filled for ASOS ADE with:
- **3 target reports**: ADE - OrderLine (6B+ row fact table), ADE - Sales, ADE - Trade
- **PBI repo**: `asos-data-ade-powerbi/powerbi/models/`
- **dbt repo**: `asos-data-ade-dbt/` (10 domains, 144 serve models, all views)
- **Databricks**: MCP via `databricks-mcp-server` (Databricks CLI OAuth), workspace `adb-2762816844316267.7.azuredatabricks.net`, SQL Warehouse `f0bdb929e2c1cf2d`
- **PBI service principal**: `spn-ade-pbi` (65978fad-bc17-4f5a-b134-25d299885855) — all PBI queries appear under this identity
- **Key facts**: fact_order_line_v1 has 6B+ rows, fact_product_option_trade_daily_snapshot_v1 generates 15s+ queries reading 4.4B rows, all serve models are views, liquid clustering used, DirectQuery dominant, no aggregation tables, `_fn_GetDataFromDBX` is a simple pass-through with no M-level filtering, Photon enabled (99%), queries cross 3+ catalogs

## Tested Results (ADE - OrderLine)

The scripts were validated against the real ASOS model:
- **73 tables**: 14 DirectQuery, 55 Dual, 4 Import
- **1,525 columns** | **1,009 measures** | **69 relationships** (2 bidirectional, 6 inactive)
- **69 Databricks source mappings**
- **BPA findings**: 2 High (bidirectional relationships), 52 Medium (42 FILTER(ALL), 4 wide tables, 6 missing format strings), 1,882 Low
- **DAX complexity**: 18 critical, 22 high, 229 medium, 740 low (avg score 2.2)
- **Hot table**: `Order Line` (directQuery) with 400 measure references
- **dbt**: 144 serve models across 10 domains, all materialised as views, 19 wide (>50 cols)

## Key Constraints

- **Never execute git operations** — agents create files only, humans handle git
- **British English** in all output (analyse, optimise, behaviour, prioritise)
- **Always validate input.md first** — never skip Step 0
- **Use scripts for parsing** — never read raw JSON files >10K lines directly
- **Output directory only** — never write files outside `output/`
- **Human-in-the-loop** — always present the validation checklist and wait for confirmation before running analysis

## Reuse in Another Project

1. Copy the entire `PBI-Performance-Diagnosis-Agent/` directory
2. Delete `input.md`
3. Copy `references/input-template.md` to `input.md`
4. Fill in company context, paths, reports
5. Run the agent — it validates and presents the checklist before starting

No hardcoded paths exist in any script or reference file. Everything is parameterised through `input.md` and CLI arguments.
