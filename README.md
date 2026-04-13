# PBI Performance Diagnosis Agent

A standalone AI agent that performs **end-to-end Power BI performance diagnosis** across the full stack: semantic models, DAX measures, dbt data pipelines, and Databricks infrastructure. It produces comprehensive, self-contained HTML reports with prioritised recommendations.

## What It Does

Given a Power BI semantic model (and optionally a dbt project and Databricks connection), the agent:

1. **Parses** the semantic model structure — tables, storage modes, relationships, source mappings, graph topology
2. **Analyses** every DAX measure for complexity, context transitions, anti-patterns, and estimated SQL subquery count
3. **Traces** dbt lineage from serve views to curated Delta tables, flagging wide views, missing filters, and materialisation gaps
4. **Profiles** Databricks table volumes (row counts, sizes, clustering) and query history (slow queries, scan amplification, cross-catalog joins)
5. **Runs** 12 Best Practice Analyser rules against the model
6. **Synthesises** all findings into a ranked root cause analysis with effort-vs-impact quadrant
7. **Generates** a single self-contained HTML report with executive summary, detailed recommendations, and implementation roadmap

## Architecture

```
input.md ──> Step 0: Validate ──> Human confirms
                                      │
              ┌───────────────────────┘
              ▼
         Step 1: Semantic Model ──> model-taxonomy.json
         Step 2: DAX Complexity ──> dax-audit.json + dax-complexity.json
         Step 3: dbt Lineage    ──> dbt-lineage.json
         Step 4: Databricks     ──> databricks-profile.json
         Step 5: Query History  ──> query-profile.json
         Step 6: BPA Rules      ──> bpa-results.json
         Step 7: Synthesis      ──> synthesis.json
         Step 8: Report         ──> *_Performance_Diagnosis.html
```

The agent adapts to whatever data sources are available:

| Available Sources | Enabled Steps |
|---|---|
| PBI semantic model only | 1, 2, 6, 7, 8 |
| PBI + dbt | 1, 2, 3, 6, 7, 8 |
| PBI + Databricks | 1, 2, 4, 5, 6, 7, 8 |
| PBI + dbt + Databricks | All (1-8) |
| Only Databricks | 4, 5, 7, 8 |

Steps that cannot run are marked as SKIPPED and noted as limitations in the report.

## Directory Structure

```
PBI-Performance-Diagnosis-Agent/
├── CLAUDE.md               # Agent context for AI runtimes
├── SKILL.md                # Main agent prompt — 8-step workflow
├── input.md                # Project-specific configuration
│
├── scripts/                # Python analysis scripts (stdlib only, no pip install)
│   ├── analyse_semantic_model.py    # Step 1: parse PBI model → model-taxonomy.json
│   ├── analyse_dax_complexity.py    # Step 2: score DAX complexity → dax-complexity.json
│   ├── audit_dax.py                 # Step 2: anti-pattern scan → dax-audit.json
│   ├── analyse_dbt_lineage.py       # Step 3: dbt lineage → dbt-lineage.json
│   ├── run_bpa.py                   # Step 6: BPA rule checks → bpa-results.json
│   ├── parse_perf_analyzer.py       # Step 5: parse PBI Perf Analyzer → perf-summary.json
│   └── generate_report.py           # Step 8: produce HTML report
│
├── references/             # Knowledge base (read-only)
│   ├── input-template.md            # Blank input.md for new projects
│   ├── report-template.html         # HTML/CSS template for reports
│   ├── bpa-rules-reference.md       # 12 BPA rules with examples and fixes
│   ├── dax-patterns.md              # DAX anti-patterns + alternatives
│   ├── example-slow-query.md        # Annotated real 15s query with root cause
│   └── system-tables-queries.md     # Databricks SQL for metadata/volume/profiling
│
├── plan/                   # Design documentation
│   ├── pbi-performance-diagnosis-agent.plan.md
│   └── pbi-directquery-performance.plan.md
│
└── output/                 # All analysis results (timestamped subdirectories)
    ├── 2026-04-10_1659_orderline-model-wide/
    ├── 2026-04-10_1659_sales-model-wide/
    ├── 2026-04-10_1659_trade-model-wide/
    └── ...
```

## Quick Start

### 1. Configure `input.md`

Copy the template and fill in your project details:

```bash
cp references/input-template.md input.md
```

Required fields:
- **Company/Project Name** and **Business Domain**
- **Semantic Model Path** — directory containing Tabular Editor JSON serialisation (`database.json`, `tables/`, `relationships/`)

Optional fields:
- **dbt repository path** — enables serve-layer lineage analysis
- **Databricks connection** — enables volume profiling and query history
- **Report URLs** — enables Performance Analyser capture via Playwright

### 2. Run the Agent

Open this directory in [Claude Code](https://docs.anthropic.com/en/docs/claude-code), [Cursor](https://cursor.com), or any MCP-compatible AI runtime. The agent reads `SKILL.md` as its prompt and begins with Step 0: input validation.

```
# In Claude Code
cd PBI-Performance-Diagnosis-Agent
claude

# The agent will:
# 1. Read and validate input.md
# 2. Present a checklist for confirmation
# 3. Run all enabled analysis steps
# 4. Produce the HTML report in output/
```

### 3. Review the Report

Each run creates a timestamped subdirectory in `output/`:

```
output/2026-04-10_1659_sales-model-wide/
├── ADE_-_Sales_Performance_Diagnosis.html   # Final report (open in browser)
├── model-taxonomy.json                       # Intermediate: model structure
├── dax-complexity.json                       # Intermediate: DAX scoring
├── dax-audit.json                            # Intermediate: anti-pattern scan
├── dbt-lineage.json                          # Intermediate: dbt analysis
├── bpa-results.json                          # Intermediate: BPA findings
└── synthesis.json                            # Intermediate: ranked findings
```

Open the `.html` file in any browser. The report is fully self-contained with inline CSS.

## Scripts

All scripts use **Python 3.10+ standard library only** — no `pip install` needed.

### CLI Reference

```bash
# Step 1: Semantic model structure
python3 scripts/analyse_semantic_model.py \
  --model-path <path-to-pbi-model-dir> \
  --output output/ \
  --volumetry-file output/databricks-profile.json  # optional

# Step 2a: DAX anti-pattern audit
python3 scripts/audit_dax.py \
  --model-path <path-to-pbi-model-dir> \
  --output output/

# Step 2b: DAX complexity scoring
python3 scripts/analyse_dax_complexity.py \
  --model-path <path-to-pbi-model-dir> \
  --output output/ \
  --taxonomy-file output/model-taxonomy.json  # optional: enriches hot tables

# Step 3: dbt lineage
python3 scripts/analyse_dbt_lineage.py \
  --dbt-path <path-to-dbt-project> \
  --output output/

# Step 5: Parse Performance Analyser export
python3 scripts/parse_perf_analyzer.py \
  --input <perf-analyzer.json> \
  --output output/

# Step 6: Best Practice Analyser
python3 scripts/run_bpa.py \
  --model-path <path-to-pbi-model-dir> \
  --output output/

# Step 8: Generate HTML report
python3 scripts/generate_report.py \
  --input output/ \
  --output output/ \
  --model-name "Model Name" \
  --run-label "brief-description"
```

## Report Sections

The generated HTML report includes:

1. **Executive Summary** — health score, key numbers, top recommendations with severity badges
2. **Model Taxonomy** — tables classified as fact/dimension/bridge, storage modes, volumetry, relationship topology
3. **Data Volume Profile** — row counts, sizes in GB, volume distribution across tables
4. **Storage Mode Analysis** — DirectQuery/Dual/Import breakdown with implications
5. **DAX Complexity Report** — measures ranked by context transitions and estimated SQL subqueries
6. **Query Performance Profile** — slowest queries, scan amplification ratios, cross-catalog joins
7. **Best Practice Findings** — BPA violations with performance impact type (latency/cost/memory)
8. **Root Cause Analysis** — per-issue deep dive with evidence, tagged by scope (model-wide/report-specific)
9. **Recommendation Quadrant** — effort vs impact matrix
10. **Detailed Recommendations** — implementation guides with evidence, trade-offs, and alternative options
11. **Implementation Roadmap** — phased action list with Where badges (Engineering / Semantic Model / Power BI)

## Key Features

**Input-driven** — all configuration lives in `input.md`. No hardcoded paths in scripts or references.

**Human-in-the-loop** — always validates input and presents a checklist before running analysis.

**Graceful degradation** — works with whatever data sources are available, from PBI-only to full stack.

**Where classification** — every finding and recommendation is tagged with the team/skillset needed: Engineering, Semantic Model, or Power BI.

**Evidence-based** — every factual claim links to supporting evidence (query IDs, SQL snippets, metrics).

**Zero dependencies** — all Python scripts use the standard library only.

**Supports both formats** — works with Tabular Editor JSON (TE2/PBIR) and TMDL model formats.

## Databricks Integration

Three connection options (configured in `input.md`):

| Method | Setup | Capabilities |
|---|---|---|
| **Databricks MCP** (recommended) | `npx databricks-mcp-server` + OAuth via `databricks auth login` | SQL queries, Unity Catalog browsing |
| **PAT** | `DATABRICKS_TOKEN` env var | SQL queries via Python scripts |
| **Skip** | No configuration needed | Agent works with PBI + dbt data only |

When Databricks is available, the agent queries `system.query.history` to identify slow PBI queries, scan amplification, column over-selection, and cross-catalog join patterns.

## DirectQuery Performance Patterns

The agent is designed to detect these common PBI DirectQuery anti-patterns:

- **Column over-selection** — PBI selects ALL columns from every table, even when the visual needs few
- **Date filter via JOIN** — date predicates applied through `dim_date` JOIN instead of direct WHERE, preventing partition pruning
- **Cross-catalog JOINs** — single visuals generating queries across 3+ Databricks catalogs
- **Deeply nested subqueries** — 4-5 levels of nesting, one per relationship hop
- **Scan amplification** — billions of rows read to return fewer than 10 results
- **Pass-through serve views** — `SELECT *` views providing no filtering or column reduction

## Reuse in Another Project

1. Copy the entire `PBI-Performance-Diagnosis-Agent/` directory
2. Delete `input.md`
3. Copy `references/input-template.md` to `input.md`
4. Fill in your project's paths, connection details, and reports
5. Run the agent — it validates and presents the checklist before starting

## Sample Output

This repository includes 14 completed diagnosis runs across ASOS ADE models:

| Model | Tables | Measures | Key Findings |
|---|---|---|---|
| OrderLine | 73 | 1,009 | 6B+ row fact table, 18 critical DAX measures, 2 bidirectional relationships |
| Sales | 52 | — | Slow billed sale visuals, column over-selection, scan amplification |
| Trade | — | — | 4.4B rows read per query, 15s+ peak hour queries |
| Customer, Loyalty, Wholesale, Parcel Tracking, PF Stock, PCSI, SCH Origin, In Day Sales | — | — | Model-wide analysis with BPA, DAX audit, and dbt lineage |

## License

Internal use.
