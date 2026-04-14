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
│   ├── analyse_dax_antipatterns.py    # Step 6b: compound anti-pattern tiers → dax-antipattern-tiers.json
│   ├── parse_perf_analyzer.py         # Step 5: parse PBI Perf Analyzer JSON → perf-summary.json
│   ├── generate_report.py             # Step 8: produce HTML report from intermediate JSONs
│   ├── analyse_user_queries.py        # Step 4b: per-user query attribution
│   ├── analyse_capacity_settings.py   # Step 5c: capacity settings impact simulation
│   ├── analyse_workload.py            # Step 5d: workload & surge protection analysis
│   ├── analyse_column_memory.py       # Step 1c: column-level memory estimation
│   ├── extract_pbix_layouts.py         # Step 1b: extract Layout from .pbix → pbix_extracted/
│   ├── analyse_report_visuals.py      # Step 1b: PBI Inspector-style visual rules → visual-analysis.json
│   ├── run_engineering_bpa.py         # Step 3b: engineering BPA (15 dbt rules)
│   └── requirements.txt               # Empty — no external deps
│
├── references/                        # Knowledge base (read-only)
│   ├── input-template.md              # Blank input.md for new projects
│   ├── report-template.html           # HTML/CSS template for report styling
│   ├── bpa-rules-reference.md         # 20 BPA rules with examples, fixes, and composite model docs
│   ├── dax-patterns.md                # DAX anti-patterns, compound tiers, pattern families
│   ├── finding-suppression-rules.md   # Learned suppression rules from architect feedback
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
| 1b | PBIX Report Analysis | `extract_pbix_layouts.py` + `analyse_report_visuals.py` | `visual-analysis.json` | When reports listed |
| 2 | DAX Complexity Analysis | `audit_dax.py` + `analyse_dax_complexity.py` | `dax-audit.json` + `dax-complexity.json` | Needs PBI repo |
| 3 | dbt Source Code Analysis | `analyse_dbt_lineage.py` | `dbt-lineage.json` | Optional (needs dbt repo) |
| 4 | Databricks Metadata Profiling | — (MCP/manual queries) | `databricks-profile.json` | Optional (needs Databricks) |
| 5 | Query Profiling | `parse_perf_analyzer.py` | `perf-summary.json` / `query-profile.json` | Optional |
| 6 | Best Practice Analyser | `run_bpa.py` | `bpa-results.json` | Needs PBI repo |
| 6b | DAX Anti-Pattern Tiers | `analyse_dax_antipatterns.py` | `dax-antipattern-tiers.json` | Needs PBI repo |
| 3b | Engineering BPA | `run_engineering_bpa.py` | `engineering-bpa-results.json` | Optional (needs dbt repo) |
| 4b | User Query Attribution | `analyse_user_queries.py` | `user-query-profile.json` | Optional (needs query data) |
| 5c | Capacity Settings Simulation | `analyse_capacity_settings.py` | `capacity-settings-analysis.json` | Optional (needs query data) |
| 5d | Workload Analysis | `analyse_workload.py` | `workload-analysis.json` | Optional (needs query data) |
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

# Step 6b: DAX anti-pattern tier analysis (compound severity + pattern families + dependency chains)
python3 scripts/analyse_dax_antipatterns.py \
  --model-path <path-to-PBI-model-dir> \
  --output output/

# Step 3b: Engineering BPA
python3 scripts/run_engineering_bpa.py \
  --dbt-path <path-to-dbt-project> \
  --taxonomy output/model-taxonomy.json \
  --output output/

# Step 4b: User query attribution
python3 scripts/analyse_user_queries.py \
  --query-data output/query-history-export.json \
  --output output/

# Step 5c: Capacity settings simulation
python3 scripts/analyse_capacity_settings.py \
  --query-data output/query-history-export.json \
  --taxonomy output/model-taxonomy.json \
  --output output/

# Step 5d: Workload & capacity analysis
python3 scripts/analyse_workload.py \
  --query-data output/query-history-export.json \
  --capacity-config output/capacity-config.json \
  --output output/

# Column memory analysis
python3 scripts/analyse_column_memory.py \
  --model-path <path-to-PBI-model-dir> \
  --taxonomy output/model-taxonomy.json \
  --dax-complexity output/dax-complexity.json \
  --output output/

# Visual layer rules
python3 scripts/analyse_report_visuals.py \
  --layout-path output/pbix_extracted/Layout \
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

**`analyse_semantic_model.py`** — Parses PBI model directories (Tabular Editor JSON serialisation). Reads `database.json`, `expressions/*.json`, `tables/*/partitions/*.json`, `tables/*/<Table>.json` (extended properties), `relationships/*.json`. Extracts table storage modes, column counts, measure counts, Databricks source mapping. **New in v2**: auto-classifies tables as fact/dimension/bridge/metadata from relationship structure. Computes graph analysis (degree centrality, hub tables, snowflake depth). **New in v3**: builds `sourceGroups` (grouping tables by effective Databricks source) and `relatedUsage` (which tables are referenced via RELATED/RELATEDTABLE) for composite model analysis. **New in v4**: `dimensionConsolidation` — detects semantically similar dimension tables via token-based name similarity (Jaccard ≥ 0.4 + shared non-generic tokens). For each candidate group, analyses column overlap, shared Databricks sources, relationship savings, and storage modes to score consolidation benefit (high/medium/low). Produces actionable recommendations rendered in the report under the Snowflake Branching section. Accepts `--volumetry-file` for Databricks row count/size enrichment. Outputs `model-taxonomy.json`.

**`analyse_dax_complexity.py`** — Two-pass analysis: first collects table storage modes, then scores each measure's complexity. **New in v2**: complexity scoring prioritises context transitions (3 pts each), relationship hops (2 pts each), and FILTER(ALL) count (4 pts each) over LOC (1 pt for >30 lines). Each measure includes `contextTransitions`, `relationshipHops`, `filterAllCount`, and `estimatedSQLSubqueries`. Accepts `--taxonomy-file` for hot table enrichment with volumetry, degree, and computed `optimizationPriority` (critical/high/medium/low). Outputs `dax-complexity.json`.

**`audit_dax.py`** — Scans all DAX measure expressions for 13 anti-pattern rules (FILTER_ALL, IFERROR/ISERROR, nested CALCULATE, CROSSJOIN, repeated subexpression, bare division, COUNT vs COUNTROWS, missing format string, USERELATIONSHIP, DIVIDE_CALC, unqualified columns, no VAR, hardcoded values). Each issue includes `whyItsBad` (engine-level explanation) and `requiredActions` (list of specific fix steps). Strips comments and string literals before pattern matching. Three non-performance rules (MISSING_FORMAT_STRING, UNQUALIFIED_COLUMNS, HARDCODED_VALUES) are classified as severity `Info` and suppressed from the Action Register — they appear in detail tables only. Outputs `dax-audit.json`.

**`analyse_dbt_lineage.py`** — Parses dbt serve-layer SQL for `ref()` calls, WHERE filters, UNION ALL, and column counts. Reads contract YAML files for materialisation config (view/table/incremental), liquid clustering, unique keys. **New in v2**: includes a value gate that identifies `actionableFindings` (wide-serve-view, missing-filter, should-materialise, missing-clustering) and sets `hasActionableFindings` boolean. The report collapses this section when no actionable findings exist. Outputs `dbt-lineage.json`.

**`run_bpa.py`** — Runs 20 Best Practice Analyser rules against PBI semantic model files (supports both JSON/TE2 and TMDL formats). Original 12 rules plus 8 new rules: IS_AVAILABLE_IN_MDX, TIME_INTEL_ON_DQ, CALCULATED_TABLES, SNOWFLAKE_DQ_CHAINS, DATE_TABLE_NOT_MARKED, REDUNDANT_COLUMNS_IN_RELATED, EXCESSIVE_CALCULATED_COLUMNS, M_FOLDING_BLOCKERS. The DUAL_MODE_TABLES rule is now **source-group-aware**: checks for DirectQuery neighbours and RELATED() usage before recommending Dual→Import, adding warnings about limited relationships in composite models. Each rule includes `performanceImpact` metadata (latency/cost/quality/memory) with descriptions. Outputs `bpa-results.json`.

**`parse_perf_analyzer.py`** — Parses Power BI Desktop Performance Analyzer JSON exports. Analyses visual load times, query durations, render costs. Groups visuals under User Actions. Computes summary statistics (avg, median, P95, page load, bottleneck classification). Outputs `perf-summary.json`.

**`analyse_dax_antipatterns.py`** — Compound anti-pattern tier analysis. Detects 9 anti-pattern flags (ITERATOR, ALL_FILTER, ROW_FILTER, SWITCH_IF, TIME_INTEL, NESTED_CALC, USERELATIONSHIP, CROSSJOIN, DIVIDE_CALC) per measure. Assigns severity tiers (Critical ≥4, High Risk 3, Medium 2, Low Risk 1, Clean 0). Groups measures into semantic pattern families (WTD, LY, Cover, etc.) with consolidated "Why it's slow" and "Required actions". Builds measure-to-measure dependency call graph and detects amplification chains (expensive measures called inside iterators). Produces priority fix order. Outputs `dax-antipattern-tiers.json`.

**`generate_report.py`** — Reads all intermediate JSONs from the output directory (including `dax-antipattern-tiers.json`), reads `synthesis.json` for root cause findings with **Where** classification (dbt Models / Semantic Model / PBI Report / PBI Visual), and produces a single self-contained HTML file with inline CSS. Applies finding suppression rules (`SUPPRESSED_VISUAL_RULES`, `SUPPRESSED_DAX_AUDIT_RULES`, `SUPPRESSED_ENGINEERING_RULES`) to exclude non-performance findings from the Action Register CSV while keeping them in detail section tables. Creates a timestamped subdirectory (`output/YYYY-MM-DD_HHMM_<run-label>/`) and moves all intermediate files there. **Key features**: Model taxonomy shows classification (fact/dim), volumetry (rows, GB), and relationship topology (hub tables, snowflake depth). DAX complexity shows context transitions as primary metric. **New in v3**: DAX Anti-Pattern Tier Analysis section renders tier summary, 9-flag catalog, pattern family cards with "Why it's slow" / "Required actions", priority fix order, and dependency chain amplification. Detailed recommendations use "Why it's bad" / "Required action" format with structured action lists (from `whyItsBad`/`requiredActions` fields). BPA detailed findings include "Why it's bad" explanation from impact descriptions. Action-Priority Matrix shows inter-finding dependencies in a "Depends on" column. Hot tables include volumetry, degree, and optimisation priority. BPA and Engineering BPA sections show ALL rules (performance + quality) with interactive filter buttons. **New in v4**: Dimension Consolidation Opportunities card in the Snowflake Branching section — renders actionable groups of semantically similar dimension tables with column overlap bars, benefit scoring, evidence bullets, and recommended consolidation actions. No health score — removed in favour of detailed per-section findings.

**`run_engineering_bpa.py`** — Runs 15 engineering best practice rules against dbt SQL and contract YAMLs. Checks SELECT *, missing clustering, wide serve views, missing WHERE filters, functions on filter columns, OR in JOINs, ROW_NUMBER vs QUALIFY, non-atomic materialisation, and more. Cross-references with model-taxonomy.json to only check dbt models consumed by PBI. The report shows ALL 15 rules with interactive filter buttons by impact type (Latency/Cost/Quality). Displayed under section "dbt Best Practices". Outputs `engineering-bpa-results.json`.

**`analyse_user_queries.py`** — Processes Databricks system.query.history exports (JSON or CSV) to build per-user query profiles. Computes stats (avg/p50/p95/max duration, GB read, slow query counts), identifies training candidates via 3 heuristics, and produces hourly distribution. Outputs `user-query-profile.json`.

**`analyse_capacity_settings.py`** — Simulates the impact of Fabric capacity management settings (query timeout, memory limit, row set counts, dataset size) at various thresholds. Follows Microsoft specialist methodology. Outputs `capacity-settings-analysis.json`.

**`analyse_workload.py`** — Analyses CU consumption patterns by time-of-day and user. Recommends surge protection thresholds, workload isolation, capacity scaling pros/cons, and semantic model settings (Large Storage Format, Query Scale-Out). Outputs `workload-analysis.json`.

**`analyse_column_memory.py`** — Estimates column-level memory consumption from PBI model metadata and Databricks row counts. Identifies hidden/unreferenced columns as removal candidates with estimated savings. Outputs `column-memory-analysis.json`.

**`analyse_report_visuals.py`** — Parses PBIX Layout JSON and applies 8 PBI Inspector-style rules: too many visuals, missing date slicers, excessive filters, hidden pages with queries, auto-refresh, wide tables, measure-heavy cards, embedded images. Outputs `visual-analysis.json`.

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

Extract with:
```bash
python3 scripts/extract_pbix_layouts.py \
  --reports-dir <pbi-repo>/powerbi/reports \
  --report-names "Report Name 1,Report Name 2" \
  --output output/
```

The script handles both legacy PBIX format (`Report/Layout` as UTF-16-LE encoded JSON) and PBIR format (`Report/definition/pages/` structure) by reconstructing a compatible Layout JSON. Resolves report name mismatches (e.g., "ADE - Trade" → "Trade/") using prefix-stripping and fuzzy matching.

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
**IMPORTANT**: Always use the Technology SQL Warehouse (`f0bdb929e2c1cf2d`) for all queries. Do NOT use the Engineering warehouse or any other warehouse. If `execute_sql` returns a permission error, ask the user to request `CAN USE` access — do not fall back to another warehouse.

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
| SQL Warehouse | Technology SQL Warehouse (`f0bdb929e2c1cf2d`) — production only, no fallback |
| HTTP Path | `/sql/1.0/warehouses/f0bdb929e2c1cf2d` |
| Auth Method | Databricks CLI OAuth (`databricks-cli` profile) |
| PBI Service Principal | `spn-ade-pbi` (object ID: `65978fad-bc17-4f5a-b134-25d299885855`) |
| Query Source | PowerBI |
| Photon | Enabled (99% of task time) |
| Catalogs | sales, product, customer, supplychain, sourcingandbuying, technology (production — NEVER use `_dev` suffixed catalogs) |

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

- **Production only** — ALL Databricks queries MUST use production catalogs (`sales`, `product`, `customer`, `supplychain`, `sourcingandbuying`, `technology`) without `_dev` suffix. Always use the Technology SQL Warehouse (`f0bdb929e2c1cf2d`). NEVER use the Engineering warehouse (`80bad8a5778c2e98`) or any other warehouse. The PBI model repo contains `_dev` suffixed catalogs in `_DbxEnvironmentSuffix` parameters — ignore those for Databricks queries; they are dev-time settings, not production references.
- **Never execute git operations** — agents create files only, humans handle git
- **British English** in all output (analyse, optimise, behaviour, prioritise)
- **Always validate input.md first** — never skip Step 0
- **Use scripts for parsing** — never read raw JSON files >10K lines directly
- **Output directory only** — never write files outside `output/`
- **Human-in-the-loop** — always present the validation checklist and wait for confirmation before running analysis
- **No personal dev schemas in reports** — The PBI model references developer workspace schemas (e.g., `dbt_dev.rafael_diassantos.*`). These MUST be excluded from all report output. The scripts (`analyse_semantic_model.py`, `generate_report.py`) filter them out automatically. Never mention `rafael_diassantos` or any personal dev schema in synthesis, findings, or recommendations.

## Finding Suppression Rules

The agent applies learned suppression rules during Step 7 synthesis to avoid non-actionable recommendations. These rules are defined in `references/finding-suppression-rules.md` and encode feedback from human data architects about which findings are noise.

Rules are categorised as:
- **SUPPRESS** — never include in Action Register or synthesis findings (S1-S5)
- **DOWNGRADE** — keep in detail tables, don't promote to actions (D1)

### Suppressed Rules Summary

| ID | Rule(s) | Reason |
|----|---------|--------|
| S1 | V05 (Query reduction settings) | Org-level policy decisions, not actionable perf fixes |
| S2 | V08 (Embedded images) | Negligible perf impact in DirectQuery models |
| S3 | MISSING_FORMAT_STRING, UNQUALIFIED_COLUMNS, HARDCODED_VALUES | Code style/display only, no engine-level impact |
| S4 | E13 (Hardcoded magic numbers) | SQL style/maintainability only |
| S5 | New dbt model creation | Increases pipeline complexity; prefer optimising existing models |

### Script-Level Enforcement

The `generate_report.py` script enforces suppression via `SUPPRESSED_*_RULES` constants that filter findings from the Action Register CSV. Suppressed findings still appear in their respective HTML section tables (Visual Analysis, DAX Audit, Engineering BPA) for informational reference.

The `audit_dax.py` script reclassifies non-performance checks (MISSING_FORMAT_STRING, UNQUALIFIED_COLUMNS, HARDCODED_VALUES) to severity `Info` instead of `Medium`/`Low`, clearly distinguishing them from performance-impacting anti-patterns.

### Action Classification

The agent classifies actions by confidence level:
- **Accept** — clear perf impact, well-defined implementation
- **Validate** — needs runtime investigation (e.g., storage mode switches, clustering choices)
- **Propose** — report design changes requiring business stakeholder input

## Reuse in Another Project

1. Copy the entire `PBI-Performance-Diagnosis-Agent/` directory
2. Delete `input.md`
3. Copy `references/input-template.md` to `input.md`
4. Fill in company context, paths, reports
5. Run the agent — it validates and presents the checklist before starting

No hardcoded paths exist in any script or reference file. Everything is parameterised through `input.md` and CLI arguments.
