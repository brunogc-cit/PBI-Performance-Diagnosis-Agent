#!/usr/bin/env python3
"""
Engineering Best Practice Analyser (Engineering BPA)

Runs 15 engineering rules against a dbt codebase, focusing on patterns that
impact Power BI DirectQuery performance in Databricks.  Only checks dbt models
that are consumed by the PBI semantic model (via sourceTable mapping from
model-taxonomy.json).

Usage:
    python3 scripts/run_engineering_bpa.py \\
      --dbt-path <path-to-dbt-project> \\
      --taxonomy <path-to-model-taxonomy.json> \\
      --output <output-dir>
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DOMAINS = [
    "sales", "customer", "product", "supply_chain", "sourcing_and_buying",
    "technology", "finance", "digital", "marketing", "dbt", "template",
]

MAX_EXAMPLES_PER_RULE = 5

# ---------------------------------------------------------------------------
# Minimal YAML parser (same approach as analyse_dbt_lineage.py)
# ---------------------------------------------------------------------------

def _read_yaml_simple(path: Path) -> dict | None:
    """Minimal YAML-like parser for dbt contract files (key: value pairs).

    Avoids a PyYAML dependency.  Handles the subset of YAML used in dbt
    contract files: models list with name, config block (materialized, alias,
    liquid_clustered_by, incremental_strategy, incremental_predicates, etc.).
    """
    try:
        text = path.read_text(encoding="utf-8-sig")
    except OSError as exc:
        print(f"  WARNING: Could not read {path}: {exc}", file=sys.stderr)
        return None

    result: dict = {"models": []}
    current_model: dict | None = None

    for line in text.splitlines():
        stripped = line.strip()

        if stripped.startswith("- name:"):
            if current_model:
                result["models"].append(current_model)
            current_model = {
                "name": stripped.split(":", 1)[1].strip(),
                "config": {},
            }

        elif current_model and stripped.startswith("alias:"):
            current_model["config"]["alias"] = stripped.split(":", 1)[1].strip()

        elif current_model and stripped.startswith("materialized:"):
            current_model["config"]["materialized"] = stripped.split(":", 1)[1].strip()

        elif current_model and stripped.startswith("incremental_strategy:"):
            current_model["config"]["incremental_strategy"] = stripped.split(":", 1)[1].strip()

        elif current_model and stripped.startswith("liquid_clustered_by:"):
            val = stripped.split(":", 1)[1].strip()
            if val.startswith("["):
                cols = re.findall(r"'([^']+)'", val)
                current_model["config"]["liquid_clustered_by"] = cols
            else:
                current_model["config"]["liquid_clustered_by"] = [val]

        elif current_model and stripped.startswith("incremental_predicates:"):
            val = stripped.split(":", 1)[1].strip()
            if val.startswith("["):
                preds = re.findall(r"'([^']+)'", val)
                current_model["config"]["incremental_predicates"] = preds
            else:
                current_model["config"]["incremental_predicates"] = [val] if val else []

        elif current_model and stripped.startswith("owner:"):
            current_model["config"]["owner"] = stripped.split(":", 1)[1].strip()

        elif current_model and stripped.startswith("lifecycle:"):
            current_model["config"]["lifecycle"] = stripped.split(":", 1)[1].strip()

    if current_model:
        result["models"].append(current_model)

    return result


# ---------------------------------------------------------------------------
# SQL cleaning helpers
# ---------------------------------------------------------------------------

def _strip_comments(sql: str) -> str:
    """Remove Jinja comments {# ... #} and SQL line comments -- ... from SQL."""
    # Jinja block comments (may span multiple lines)
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.DOTALL)
    # SQL line comments
    sql = re.sub(r"--[^\n]*", "", sql)
    return sql


def _extract_refs(sql: str) -> list[str]:
    """Extract dbt ref() model names from SQL."""
    return re.findall(r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)", sql)


# ---------------------------------------------------------------------------
# Column counting for serve SQL
# ---------------------------------------------------------------------------

def _count_select_columns(sql: str) -> int:
    """Count comma-separated items in the top-level SELECT clause.

    Handles multiline SELECTs.  Stops at FROM or a closing parenthesis that
    would end a subquery.  Skips lines that are comments or blank.
    """
    clean = _strip_comments(sql)
    lines = clean.splitlines()

    in_select = False
    select_text_parts: list[str] = []
    paren_depth = 0

    for line in lines:
        stripped = line.strip().lower()

        if not in_select:
            # Look for SELECT keyword (not inside a subquery)
            m = re.match(r"^\s*select\s+(distinct\s+)?(.*)", line, re.IGNORECASE)
            if m and paren_depth == 0:
                in_select = True
                remainder = m.group(2)
                if remainder.strip():
                    select_text_parts.append(remainder)
            # Track parentheses outside SELECT
            paren_depth += line.count("(") - line.count(")")
            continue

        # Already inside SELECT — check for FROM or end
        if re.match(r"^\s*from\b", stripped):
            break

        # Track paren depth to handle subqueries in the SELECT list
        local_open = line.count("(")
        local_close = line.count(")")
        paren_depth += local_open - local_close

        if stripped and not stripped.startswith("--"):
            select_text_parts.append(line)

    if not select_text_parts:
        return 0

    select_block = "\n".join(select_text_parts)
    # Remove string literals to avoid false comma counts
    select_block = re.sub(r"'[^']*'", "''", select_block)

    # Count top-level commas (not inside parentheses)
    count = 1  # at least one column if we have any text
    depth = 0
    for ch in select_block:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            count += 1

    # Handle SELECT * case — that's just 1 expression
    stripped_block = select_block.strip()
    if stripped_block == "*" or stripped_block.endswith(".*"):
        return 0  # unknown/star — callers treat 0 as "cannot determine"

    return count


# ---------------------------------------------------------------------------
# Taxonomy loader
# ---------------------------------------------------------------------------

def _load_taxonomy(taxonomy_path: Path) -> dict:
    """Load model-taxonomy.json and build lookup structures."""
    try:
        with open(taxonomy_path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"ERROR: Could not load taxonomy file: {exc}", file=sys.stderr)
        sys.exit(1)

    # Build sourceTable -> PBI table info mapping
    source_to_pbi: dict[str, dict] = {}

    # From sourceMapping array
    for sm in data.get("sourceMapping", []):
        st = sm.get("databricksTable", "")
        if st:
            source_to_pbi[st] = {
                "pbiTable": sm.get("pbiTable", ""),
                "storageMode": sm.get("storageMode", ""),
                "catalog": sm.get("databricksCatalog", ""),
                "schema": sm.get("databricksSchema", ""),
            }

    # Enrich from tables array (has volumetry, columnCount, etc.)
    tables_by_name: dict[str, dict] = {}
    for t in data.get("tables", []):
        name = t.get("name", "")
        st = t.get("sourceTable", "")
        tables_by_name[name] = t
        if st and st in source_to_pbi:
            source_to_pbi[st]["columnCount"] = t.get("columnCount", 0)
            vol = t.get("volumetry", {})
            source_to_pbi[st]["rowCount"] = vol.get("rowCount", 0)
            source_to_pbi[st]["sizeGB"] = vol.get("sizeGB", 0)
            source_to_pbi[st]["classification"] = t.get("classification", "")
            source_to_pbi[st]["tableQueryStats"] = t.get("tableQueryStats", {})
        elif st:
            source_to_pbi[st] = {
                "pbiTable": name,
                "storageMode": t.get("storageMode", ""),
                "columnCount": t.get("columnCount", 0),
                "rowCount": vol.get("rowCount", 0) if (vol := t.get("volumetry", {})) else 0,
                "sizeGB": vol.get("sizeGB", 0) if (vol := t.get("volumetry", {})) else 0,
                "classification": t.get("classification", ""),
                "tableQueryStats": t.get("tableQueryStats", {}),
            }

    return {
        "sourceMapping": source_to_pbi,
        "tables": tables_by_name,
    }


# ---------------------------------------------------------------------------
# dbt model discovery
# ---------------------------------------------------------------------------

def _discover_models(dbt_path: Path, taxonomy: dict) -> list[dict]:
    """Discover dbt models that are consumed by the PBI semantic model.

    Returns a list of model dicts with SQL content, contract config, layer,
    domain, and PBI mapping info.
    """
    models_dir = dbt_path / "bundles" / "core_data" / "models"
    if not models_dir.is_dir():
        models_dir = dbt_path / "models"
        if not models_dir.is_dir():
            print(f"WARNING: No models directory found in {dbt_path}", file=sys.stderr)
            return []

    source_map = taxonomy["sourceMapping"]

    # Build alias -> sourceTable lookup from taxonomy
    # The dbt model name is typically serve_<sourceTable>
    pbi_aliases: set[str] = set(source_map.keys())

    discovered: list[dict] = []
    layers = ["serve", "curated", "enriched"]

    for domain_dir in sorted(models_dir.iterdir()):
        if not domain_dir.is_dir():
            continue
        domain_name = domain_dir.name

        for layer in layers:
            layer_dir = domain_dir / layer
            if not layer_dir.is_dir():
                continue

            for sql_file in sorted(layer_dir.glob("*.sql")):
                model_name = sql_file.stem
                # Determine alias from contract or derive from name
                alias = model_name
                if layer == "serve" and model_name.startswith("serve_"):
                    alias = model_name[len("serve_"):]
                elif layer == "curated" and model_name.startswith("curated_"):
                    alias = model_name[len("curated_"):]
                elif layer == "enriched" and model_name.startswith("enriched_"):
                    alias = model_name[len("enriched_"):]

                # Read SQL
                try:
                    sql_raw = sql_file.read_text(encoding="utf-8-sig")
                except OSError:
                    continue

                sql_clean = _strip_comments(sql_raw)
                refs = _extract_refs(sql_raw)

                # Read contract
                contract_config: dict = {}
                contracts_dir = layer_dir / "_contracts"
                if contracts_dir.is_dir():
                    contract_file = contracts_dir / f"{model_name}.yml"
                    if contract_file.is_file():
                        yaml_data = _read_yaml_simple(contract_file)
                        if yaml_data and yaml_data.get("models"):
                            cm = yaml_data["models"][0]
                            contract_config = cm.get("config", {})
                            # Override alias from contract if available
                            if contract_config.get("alias"):
                                alias = contract_config["alias"]

                has_contract = bool(contract_config)

                # Check if this model is PBI-relevant
                # A serve model is relevant if its alias is in the taxonomy
                # A curated/enriched model is relevant if any serve model that
                # references it is PBI-relevant
                pbi_info: dict | None = None
                is_pbi_relevant = False

                if layer == "serve" and alias in pbi_aliases:
                    is_pbi_relevant = True
                    pbi_info = source_map.get(alias, {})

                model = {
                    "modelName": model_name,
                    "alias": alias,
                    "domain": domain_name,
                    "layer": layer,
                    "sqlFile": str(sql_file.relative_to(dbt_path)),
                    "sqlFilePath": sql_file,
                    "sqlRaw": sql_raw,
                    "sqlClean": sql_clean,
                    "refs": refs,
                    "contractConfig": contract_config,
                    "hasContract": has_contract,
                    "isPbiRelevant": is_pbi_relevant,
                    "pbiInfo": pbi_info,
                }
                discovered.append(model)

    # Second pass: mark curated/enriched models as PBI-relevant if referenced
    # by a PBI-relevant serve model
    serve_refs: set[str] = set()
    for m in discovered:
        if m["layer"] == "serve" and m["isPbiRelevant"]:
            for ref in m["refs"]:
                serve_refs.add(ref)

    # Also gather indirect references (enriched referenced by curated)
    curated_refs: set[str] = set()
    for m in discovered:
        if m["modelName"] in serve_refs and m["layer"] == "curated":
            m["isPbiRelevant"] = True
            for ref in m["refs"]:
                curated_refs.add(ref)

    for m in discovered:
        if m["modelName"] in curated_refs and m["layer"] == "enriched":
            m["isPbiRelevant"] = True

    # Also propagate PBI info to curated/enriched models from the serve model
    # that references them
    serve_alias_to_pbi: dict[str, dict] = {}
    for m in discovered:
        if m["layer"] == "serve" and m["isPbiRelevant"] and m["pbiInfo"]:
            for ref in m["refs"]:
                serve_alias_to_pbi[ref] = m["pbiInfo"]

    for m in discovered:
        if m["isPbiRelevant"] and m["pbiInfo"] is None:
            # Inherit from the serve model that references this
            m["pbiInfo"] = serve_alias_to_pbi.get(m["modelName"])

    pbi_relevant = [m for m in discovered if m["isPbiRelevant"]]
    return pbi_relevant


# ---------------------------------------------------------------------------
# Helper: is model a _prepare model?
# ---------------------------------------------------------------------------

def _is_prepare(model_name: str) -> bool:
    return "_prepare" in model_name.lower()


def _is_fact_table(model: dict) -> bool:
    """Check if the PBI table associated with a model is a fact table."""
    pbi = model.get("pbiInfo") or {}
    classification = pbi.get("classification", "").lower()
    if classification == "fact":
        return True
    # Fallback: check if the name contains fact_
    alias = model.get("alias", "")
    return "fact_" in alias.lower()


def _get_row_count(model: dict) -> int:
    """Get the row count for the PBI table associated with a model."""
    pbi = model.get("pbiInfo") or {}
    return pbi.get("rowCount", 0)


def _get_pbi_column_count(model: dict) -> int:
    """Get the PBI column count for the table associated with a model."""
    pbi = model.get("pbiInfo") or {}
    return pbi.get("columnCount", 0)


def _get_pbi_table_name(model: dict) -> str:
    """Get the PBI table name for the model."""
    pbi = model.get("pbiInfo") or {}
    return pbi.get("pbiTable", model.get("alias", ""))


# ---------------------------------------------------------------------------
# Rule definitions
# ---------------------------------------------------------------------------

RuleFinding = dict


def rule_e01_select_star(models: list[dict]) -> list[RuleFinding]:
    """E01: SELECT * in non-prepare models."""
    findings: list[RuleFinding] = []
    for m in models:
        if _is_prepare(m["modelName"]):
            continue
        sql = m["sqlClean"].lower()
        if re.search(r"\bselect\s+\*", sql):
            findings.append({
                "model": m["modelName"],
                "file": m["sqlFile"],
                "detail": f"SELECT * found in {m['layer']} model (not a _prepare model)",
                "pbiTable": _get_pbi_table_name(m),
            })
    return findings


def rule_e02_missing_liquid_clustering(models: list[dict]) -> list[RuleFinding]:
    """E02: Missing liquid clustering on large curated tables."""
    findings: list[RuleFinding] = []
    for m in models:
        if m["layer"] != "curated":
            continue
        config = m["contractConfig"]
        materialised = config.get("materialized", "")
        if materialised != "incremental":
            continue
        lc = config.get("liquid_clustered_by", [])
        if lc:
            continue
        row_count = _get_row_count(m)
        if row_count > 1_000_000:
            findings.append({
                "model": m["modelName"],
                "file": m["sqlFile"],
                "detail": (
                    f"Curated model is incremental but has no liquid_clustered_by. "
                    f"PBI table has {row_count:,} rows."
                ),
                "pbiTable": _get_pbi_table_name(m),
            })
    return findings


def rule_e03_wide_serve_view(models: list[dict]) -> list[RuleFinding]:
    """E03: Serve view wider than PBI model needs."""
    findings: list[RuleFinding] = []
    for m in models:
        if m["layer"] != "serve":
            continue
        serve_cols = _count_select_columns(m["sqlRaw"])
        if serve_cols == 0:
            continue  # Could not determine (e.g. SELECT *)
        pbi_cols = _get_pbi_column_count(m)
        if pbi_cols == 0:
            continue
        if serve_cols > pbi_cols * 1.5:
            findings.append({
                "model": m["modelName"],
                "file": m["sqlFile"],
                "detail": (
                    f"Serve view has {serve_cols} columns but PBI model only uses "
                    f"{pbi_cols} columns ({serve_cols / pbi_cols:.1f}x wider)"
                ),
                "pbiTable": _get_pbi_table_name(m),
            })
    return findings


def rule_e04_no_where_large_fact(models: list[dict]) -> list[RuleFinding]:
    """E04: No WHERE filter on serve view over large fact table."""
    findings: list[RuleFinding] = []
    for m in models:
        if m["layer"] != "serve":
            continue
        sql_lower = m["sqlClean"].lower()
        has_where = bool(re.search(r"\bwhere\b", sql_lower))
        if has_where:
            continue
        row_count = _get_row_count(m)
        if row_count <= 100_000_000:
            continue
        if not _is_fact_table(m):
            continue
        findings.append({
            "model": m["modelName"],
            "file": m["sqlFile"],
            "detail": (
                f"No WHERE clause on serve view over curated table "
                f"with {row_count:,.0f} rows"
            ),
            "pbiTable": _get_pbi_table_name(m),
        })
    return findings


def rule_e05_functions_on_filter_columns(models: list[dict]) -> list[RuleFinding]:
    """E05: Functions on filter columns in WHERE clause."""
    # Functions that prevent predicate pushdown / index usage
    problem_funcs = [
        r"upper\s*\(", r"lower\s*\(", r"date_format\s*\(", r"cast\s*\(",
        r"trim\s*\(", r"ltrim\s*\(", r"rtrim\s*\(", r"substring\s*\(",
        r"concat\s*\(", r"coalesce\s*\(", r"to_date\s*\(", r"to_timestamp\s*\(",
    ]
    findings: list[RuleFinding] = []
    for m in models:
        sql_lower = m["sqlClean"].lower()
        # Extract WHERE clause text
        where_match = re.search(r"\bwhere\b(.*?)(?:\bgroup\b|\border\b|\bhaving\b|\blimit\b|\bunion\b|\)?\s*$)", sql_lower, re.DOTALL)
        if not where_match:
            continue
        where_text = where_match.group(1)
        found_funcs: list[str] = []
        for pat in problem_funcs:
            if re.search(pat, where_text):
                func_name = pat.split(r"\s*")[0].replace("\\", "")
                found_funcs.append(func_name)
        if found_funcs:
            findings.append({
                "model": m["modelName"],
                "file": m["sqlFile"],
                "detail": (
                    f"Functions on filter columns prevent predicate pushdown: "
                    f"{', '.join(found_funcs)}"
                ),
                "pbiTable": _get_pbi_table_name(m),
            })
    return findings


def rule_e06_or_in_join(models: list[dict]) -> list[RuleFinding]:
    """E06: OR in JOIN conditions."""
    findings: list[RuleFinding] = []
    for m in models:
        sql_lower = m["sqlClean"].lower()
        # Find JOIN ... ON ... OR pattern
        # Match from ON keyword to the next JOIN, WHERE, GROUP, ORDER, or end
        on_blocks = re.finditer(
            r"\bon\b(.*?)(?:\bjoin\b|\bwhere\b|\bgroup\b|\border\b|\bhaving\b|\blimit\b|$)",
            sql_lower,
            re.DOTALL,
        )
        for block in on_blocks:
            on_text = block.group(1)
            if re.search(r"\bor\b", on_text):
                findings.append({
                    "model": m["modelName"],
                    "file": m["sqlFile"],
                    "detail": "OR condition found in JOIN ON clause — prevents hash join optimisation",
                    "pbiTable": _get_pbi_table_name(m),
                })
                break  # One finding per model
    return findings


def rule_e07_row_number_no_qualify(models: list[dict]) -> list[RuleFinding]:
    """E07: ROW_NUMBER subselect instead of QUALIFY."""
    findings: list[RuleFinding] = []
    for m in models:
        sql_lower = m["sqlClean"].lower()
        has_row_number = bool(re.search(r"\brow_number\s*\(", sql_lower))
        has_qualify = bool(re.search(r"\bqualify\b", sql_lower))
        if has_row_number and not has_qualify:
            findings.append({
                "model": m["modelName"],
                "file": m["sqlFile"],
                "detail": (
                    "ROW_NUMBER() used in subquery/CTE without QUALIFY. "
                    "QUALIFY avoids the extra subquery layer."
                ),
                "pbiTable": _get_pbi_table_name(m),
            })
    return findings


def rule_e08_materialized_table(models: list[dict]) -> list[RuleFinding]:
    """E08: materialized: table in enriched/curated (non-prepare models)."""
    findings: list[RuleFinding] = []
    for m in models:
        if m["layer"] not in ("enriched", "curated"):
            continue
        if _is_prepare(m["modelName"]):
            continue
        config = m["contractConfig"]
        materialised = config.get("materialized", "")
        if materialised == "table":
            findings.append({
                "model": m["modelName"],
                "file": m["sqlFile"],
                "detail": (
                    f"Model is materialised as 'table' in {m['layer']} layer. "
                    "This causes full rebuilds on every dbt run. Consider 'incremental'."
                ),
                "pbiTable": _get_pbi_table_name(m),
            })
    return findings


def rule_e09_missing_incremental_predicates(models: list[dict]) -> list[RuleFinding]:
    """E09: Missing incremental_predicates on large merge."""
    findings: list[RuleFinding] = []
    for m in models:
        if m["layer"] != "curated":
            continue
        config = m["contractConfig"]
        if config.get("incremental_strategy") != "merge":
            continue
        row_count = _get_row_count(m)
        if row_count <= 100_000_000:
            continue
        preds = config.get("incremental_predicates", [])
        if preds:
            continue
        findings.append({
            "model": m["modelName"],
            "file": m["sqlFile"],
            "detail": (
                f"Merge strategy on table with {row_count:,} rows but no "
                "incremental_predicates. This causes a full table scan on merge."
            ),
            "pbiTable": _get_pbi_table_name(m),
        })
    return findings


def rule_e10_union_without_all(models: list[dict]) -> list[RuleFinding]:
    """E10: UNION instead of UNION ALL."""
    findings: list[RuleFinding] = []
    for m in models:
        sql_lower = m["sqlClean"].lower()
        # Find UNION that is NOT followed by ALL or DISTINCT
        # Matches: union\n, union<space>, but NOT union all, union distinct
        matches = re.finditer(r"\bunion\b", sql_lower)
        for match in matches:
            after = sql_lower[match.end():match.end() + 20].strip()
            if not after.startswith("all") and not after.startswith("distinct"):
                findings.append({
                    "model": m["modelName"],
                    "file": m["sqlFile"],
                    "detail": (
                        "UNION without ALL causes an unnecessary DISTINCT sort. "
                        "Use UNION ALL unless deduplication is required."
                    ),
                    "pbiTable": _get_pbi_table_name(m),
                })
                break  # One finding per model
    return findings


def rule_e11_nested_subqueries(models: list[dict]) -> list[RuleFinding]:
    """E11: Nested subqueries >2 levels deep."""
    findings: list[RuleFinding] = []
    for m in models:
        sql_lower = m["sqlClean"].lower()
        # Count maximum nested SELECT depth
        max_depth = 0
        current_depth = 0
        i = 0
        while i < len(sql_lower):
            if sql_lower[i] == "(":
                # Check if this parenthesis is followed by SELECT
                remainder = sql_lower[i + 1:i + 30].lstrip()
                if remainder.startswith("select"):
                    current_depth += 1
                    if current_depth > max_depth:
                        max_depth = current_depth
            elif sql_lower[i] == ")":
                if current_depth > 0:
                    current_depth -= 1
            i += 1

        if max_depth > 2:
            findings.append({
                "model": m["modelName"],
                "file": m["sqlFile"],
                "detail": (
                    f"Nested subqueries at {max_depth} levels deep. "
                    "Refactor to CTEs for readability and potential query plan improvement."
                ),
                "pbiTable": _get_pbi_table_name(m),
            })
    return findings


def rule_e12_python_udfs(models: list[dict]) -> list[RuleFinding]:
    """E12: Plain Python UDFs in SQL."""
    findings: list[RuleFinding] = []
    for m in models:
        sql_lower = m["sqlClean"].lower()
        if re.search(r"\bpython_udf\b", sql_lower) or re.search(r"\budf\s*\(", sql_lower):
            findings.append({
                "model": m["modelName"],
                "file": m["sqlFile"],
                "detail": (
                    "Python UDF reference found. Python UDFs disable Photon and "
                    "force row-at-a-time processing."
                ),
                "pbiTable": _get_pbi_table_name(m),
            })
    return findings


def rule_e13_magic_numbers(models: list[dict]) -> list[RuleFinding]:
    """E13: Hardcoded magic numbers in WHERE clause."""
    findings: list[RuleFinding] = []
    # Common acceptable values
    acceptable = {"0", "1", "-1", "0.0", "1.0", "100", "1000", "null", "true", "false"}

    for m in models:
        sql_lower = m["sqlClean"].lower()
        where_match = re.search(
            r"\bwhere\b(.*?)(?:\bgroup\b|\border\b|\bhaving\b|\blimit\b|\bunion\b|\)?\s*$)",
            sql_lower,
            re.DOTALL,
        )
        if not where_match:
            continue
        where_text = where_match.group(1)
        # Find bare numeric literals (not in quotes, not part of column names)
        numbers = re.findall(r"(?<![a-z_])(\d+\.?\d*)(?![a-z_])", where_text)
        magic = [n for n in numbers if n not in acceptable]
        if magic:
            unique_magic = sorted(set(magic))[:5]
            findings.append({
                "model": m["modelName"],
                "file": m["sqlFile"],
                "detail": (
                    f"Hardcoded magic numbers in WHERE clause: {', '.join(unique_magic)}. "
                    "Consider using dbt variables or named constants."
                ),
                "pbiTable": _get_pbi_table_name(m),
            })
    return findings


def rule_e14_missing_contract(models: list[dict]) -> list[RuleFinding]:
    """E14: Missing contract on public (serve) model."""
    findings: list[RuleFinding] = []
    for m in models:
        if m["layer"] != "serve":
            continue
        if not m["hasContract"]:
            findings.append({
                "model": m["modelName"],
                "file": m["sqlFile"],
                "detail": (
                    "Serve model has no matching contract YAML file in _contracts/. "
                    "Public models should have explicit contracts for governance."
                ),
                "pbiTable": _get_pbi_table_name(m),
            })
    return findings


def rule_e15_should_be_materialized_view(models: list[dict]) -> list[RuleFinding]:
    """E15: Serve view that should be materialized_view."""
    findings: list[RuleFinding] = []
    for m in models:
        if m["layer"] != "serve":
            continue
        config = m["contractConfig"]
        materialised = config.get("materialized", "view")
        if materialised != "view":
            continue
        pbi = m.get("pbiInfo") or {}
        storage_mode = pbi.get("storageMode", "").lower()
        if storage_mode != "directquery":
            continue
        # Check for high query frequency from tableQueryStats
        query_stats = pbi.get("tableQueryStats", {})
        daily_queries = query_stats.get("dailyQueryCount", 0)
        if daily_queries > 0:
            findings.append({
                "model": m["modelName"],
                "file": m["sqlFile"],
                "detail": (
                    f"Serve model is a view but PBI table is DirectQuery with "
                    f"{daily_queries:,} daily queries. A materialised view would "
                    "pre-compute the result and reduce query latency."
                ),
                "pbiTable": _get_pbi_table_name(m),
            })
        else:
            # No query stats — flag if it's a large DirectQuery fact table
            row_count = _get_row_count(m)
            if row_count > 100_000_000 and _is_fact_table(m):
                findings.append({
                    "model": m["modelName"],
                    "file": m["sqlFile"],
                    "detail": (
                        f"Serve model is a view over a DirectQuery fact table with "
                        f"{row_count:,} rows. Consider materialised_view to reduce "
                        "repeated full-table scans."
                    ),
                    "pbiTable": _get_pbi_table_name(m),
                })
    return findings


# ---------------------------------------------------------------------------
# Rule registry
# ---------------------------------------------------------------------------

RULES: list[dict] = [
    {
        "ruleId": "E01",
        "title": "SELECT * in non-prepare models",
        "severity": "high",
        "performanceImpact": "cost",
        "description": (
            "SELECT * in curated/enriched models passes all columns downstream, "
            "increasing read_bytes for every PBI query."
        ),
        "recommendation": "Explicitly list only the columns needed by downstream consumers.",
        "fn": rule_e01_select_star,
        "impact": "cost",
        "effort": "low",
    },
    {
        "ruleId": "E02",
        "title": "Missing liquid clustering on large curated tables",
        "severity": "high",
        "performanceImpact": "latency",
        "description": (
            "Large incremental curated tables without liquid clustering force full "
            "file scans.  Clustering on common filter columns (e.g. date_key) "
            "enables data skipping."
        ),
        "recommendation": "Add liquid_clustered_by to the contract YAML with the primary filter columns.",
        "fn": rule_e02_missing_liquid_clustering,
        "impact": "latency",
        "effort": "low",
    },
    {
        "ruleId": "E03",
        "title": "Serve view wider than PBI model needs",
        "severity": "high",
        "performanceImpact": "cost",
        "description": (
            "Serve views that expose significantly more columns than the PBI model "
            "consumes increase bytes read per query.  PBI DirectQuery selects from "
            "the serve view, so extra columns are scanned and discarded."
        ),
        "recommendation": "Reduce the serve view SELECT to only columns used in the PBI model.",
        "fn": rule_e03_wide_serve_view,
        "impact": "cost",
        "effort": "medium",
    },
    {
        "ruleId": "E04",
        "title": "No WHERE filter on serve view over large fact table",
        "severity": "high",
        "performanceImpact": "latency",
        "description": (
            "Serve views over large fact tables with no WHERE clause expose the "
            "entire table to every PBI DirectQuery.  Even with liquid clustering, "
            "the absence of a date filter means PBI queries must scan the full "
            "history."
        ),
        "recommendation": "Add date-based WHERE filter to restrict the scan window.",
        "fn": rule_e04_no_where_large_fact,
        "impact": "latency",
        "effort": "medium",
    },
    {
        "ruleId": "E05",
        "title": "Functions on filter columns",
        "severity": "high",
        "performanceImpact": "latency",
        "description": (
            "Wrapping filter columns in functions (UPPER, CAST, DATE_FORMAT, etc.) "
            "prevents predicate pushdown to the storage layer, disabling data "
            "skipping and cluster pruning."
        ),
        "recommendation": "Remove functions from filter columns; pre-compute values in the model instead.",
        "fn": rule_e05_functions_on_filter_columns,
        "impact": "latency",
        "effort": "medium",
    },
    {
        "ruleId": "E06",
        "title": "OR in JOIN conditions",
        "severity": "medium",
        "performanceImpact": "latency",
        "description": (
            "OR conditions in JOIN ON clauses prevent the optimiser from using "
            "hash joins, falling back to nested-loop joins which are significantly "
            "slower on large tables."
        ),
        "recommendation": "Refactor to separate JOINs or use UNION ALL of individual equality joins.",
        "fn": rule_e06_or_in_join,
        "impact": "latency",
        "effort": "medium",
    },
    {
        "ruleId": "E07",
        "title": "ROW_NUMBER subselect instead of QUALIFY",
        "severity": "medium",
        "performanceImpact": "latency",
        "description": (
            "Using ROW_NUMBER() in a subquery to filter rows adds an extra "
            "materialisation step.  Databricks supports QUALIFY, which applies "
            "the window filter inline without the extra subquery."
        ),
        "recommendation": "Replace the ROW_NUMBER subquery pattern with QUALIFY ROW_NUMBER() OVER(...) = 1.",
        "fn": rule_e07_row_number_no_qualify,
        "impact": "latency",
        "effort": "low",
    },
    {
        "ruleId": "E08",
        "title": "materialized: table in enriched/curated",
        "severity": "medium",
        "performanceImpact": "cost",
        "description": (
            "Models materialised as 'table' are fully rebuilt on every dbt run, "
            "which is expensive for large datasets.  'incremental' materialisation "
            "only processes new/changed rows."
        ),
        "recommendation": "Switch to incremental materialisation with an appropriate strategy (merge/append).",
        "fn": rule_e08_materialized_table,
        "impact": "cost",
        "effort": "high",
    },
    {
        "ruleId": "E09",
        "title": "Missing incremental_predicates on large merge",
        "severity": "medium",
        "performanceImpact": "latency",
        "description": (
            "Merge operations without incremental_predicates scan the entire "
            "target table to find matching rows.  For tables with >100M rows, "
            "this causes slow dbt runs and warehouse contention."
        ),
        "recommendation": "Add incremental_predicates with a date-based predicate to limit the merge scan.",
        "fn": rule_e09_missing_incremental_predicates,
        "impact": "latency",
        "effort": "low",
    },
    {
        "ruleId": "E10",
        "title": "UNION instead of UNION ALL",
        "severity": "medium",
        "performanceImpact": "latency",
        "description": (
            "UNION performs an implicit DISTINCT, adding a sort/hash aggregate "
            "step.  If deduplication is not required, UNION ALL avoids this overhead."
        ),
        "recommendation": "Replace UNION with UNION ALL unless deduplication is explicitly needed.",
        "fn": rule_e10_union_without_all,
        "impact": "latency",
        "effort": "low",
    },
    {
        "ruleId": "E11",
        "title": "Nested subqueries >2 levels",
        "severity": "medium",
        "performanceImpact": "latency",
        "description": (
            "Deeply nested subqueries increase query plan complexity and can "
            "prevent the Databricks optimiser from applying join reordering and "
            "predicate pushdown."
        ),
        "recommendation": "Refactor nested subqueries into CTEs (WITH clauses) for clarity and optimisation.",
        "fn": rule_e11_nested_subqueries,
        "impact": "latency",
        "effort": "medium",
    },
    {
        "ruleId": "E12",
        "title": "Plain Python UDFs",
        "severity": "low",
        "performanceImpact": "latency",
        "description": (
            "Python UDFs disable Photon vectorised execution and force "
            "row-at-a-time processing through the Python interpreter, dramatically "
            "increasing query latency."
        ),
        "recommendation": "Replace Python UDFs with native SQL/Spark functions where possible.",
        "fn": rule_e12_python_udfs,
        "impact": "latency",
        "effort": "high",
    },
    {
        "ruleId": "E13",
        "title": "Hardcoded magic numbers in SQL",
        "severity": "low",
        "performanceImpact": "quality",
        "performanceRelevant": False,  # Style/maintainability only (suppressed from Action Register)
        "description": (
            "Magic numbers in WHERE clauses reduce readability and increase the "
            "risk of errors when business rules change.  They do not directly "
            "impact performance."
        ),
        "recommendation": "Use dbt variables or named constants for business-rule thresholds.",
        "fn": rule_e13_magic_numbers,
        "impact": "quality",
        "effort": "low",
    },
    {
        "ruleId": "E14",
        "title": "Missing contract on public model",
        "severity": "low",
        "performanceImpact": "quality",
        "description": (
            "Serve-layer models without contracts lack governance metadata "
            "(owner, lifecycle, alias).  This makes it harder to track ownership "
            "and manage breaking changes."
        ),
        "recommendation": "Create a contract YAML in _contracts/ with materialisation, alias, owner, and lifecycle.",
        "fn": rule_e14_missing_contract,
        "impact": "quality",
        "effort": "low",
    },
    {
        "ruleId": "E15",
        "title": "Serve view that should be materialised_view",
        "severity": "medium",
        "performanceImpact": "latency",
        "description": (
            "Serve views over large DirectQuery tables are re-evaluated on "
            "every PBI query.  A materialised view pre-computes the result, "
            "reducing latency for frequently queried tables."
        ),
        "recommendation": "Change materialisation from 'view' to 'materialized_view' in the contract YAML.",
        "fn": rule_e15_should_be_materialized_view,
        "impact": "latency",
        "effort": "medium",
    },
]


# ---------------------------------------------------------------------------
# Main engine
# ---------------------------------------------------------------------------

def run_engineering_bpa(dbt_path: Path, taxonomy_path: Path, output_dir: Path) -> int:
    """Run all engineering BPA rules and write results.  Returns exit code."""

    print(f"\n=== Engineering Best Practice Analyser ===", file=sys.stderr)
    print(f"dbt project:  {dbt_path}", file=sys.stderr)
    print(f"Taxonomy:     {taxonomy_path}", file=sys.stderr)

    # Load taxonomy
    print("Loading model taxonomy...", file=sys.stderr)
    taxonomy = _load_taxonomy(taxonomy_path)
    source_count = len(taxonomy["sourceMapping"])
    print(f"  {source_count} PBI source mappings loaded", file=sys.stderr)

    # Discover PBI-relevant models
    print("Discovering PBI-relevant dbt models...", file=sys.stderr)
    models = _discover_models(dbt_path, taxonomy)
    print(f"  {len(models)} PBI-relevant models found", file=sys.stderr)

    if not models:
        print(
            "WARNING: No PBI-relevant models found. Check that --taxonomy "
            "contains sourceMapping entries matching dbt serve model aliases.",
            file=sys.stderr,
        )

    # Layer breakdown
    serve_count = sum(1 for m in models if m["layer"] == "serve")
    curated_count = sum(1 for m in models if m["layer"] == "curated")
    enriched_count = sum(1 for m in models if m["layer"] == "enriched")
    print(
        f"  Layers: {serve_count} serve, {curated_count} curated, "
        f"{enriched_count} enriched",
        file=sys.stderr,
    )

    # Run rules
    print("Running 15 engineering rules...", file=sys.stderr)
    all_rule_results: list[dict] = []
    passing_rules: list[dict] = []
    total_findings = 0

    for rule_def in RULES:
        rule_id = rule_def["ruleId"]
        rule_fn = rule_def["fn"]
        print(f"  Checking {rule_id}: {rule_def['title']}...", file=sys.stderr)

        findings = rule_fn(models)
        count = len(findings)
        total_findings += count

        if count > 0:
            # Cap examples
            examples = findings[:MAX_EXAMPLES_PER_RULE]
            all_rule_results.append({
                "rule": rule_id,
                "ruleId": rule_id,
                "title": rule_def["title"],
                "severity": rule_def["severity"],
                "count": count,
                "impact": rule_def["impact"],
                "effort": rule_def["effort"],
                "recommendation": rule_def["recommendation"],
                "examples": examples,
            })
            print(f"    FAIL — {count} finding(s)", file=sys.stderr)
        else:
            passing_rules.append({
                "ruleId": rule_id,
                "title": rule_def["title"],
            })
            print(f"    PASS", file=sys.stderr)

    # Summary
    high = sum(r["count"] for r in all_rule_results if r["severity"] == "high")
    medium = sum(r["count"] for r in all_rule_results if r["severity"] == "medium")
    low = sum(r["count"] for r in all_rule_results if r["severity"] == "low")

    summary = {
        "high": high,
        "medium": medium,
        "low": low,
        "totalFindings": total_findings,
        "totalRules": len(RULES),
    }

    # Build rules metadata (without fn)
    rules_meta = []
    for r in RULES:
        rules_meta.append({
            "ruleId": r["ruleId"],
            "title": r["title"],
            "severity": r["severity"],
            "performanceImpact": r["performanceImpact"],
            "description": r["description"],
            "recommendation": r["recommendation"],
        })

    # Build output
    analysis_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    result = {
        "analysisDate": analysis_date,
        "dbtPath": str(dbt_path),
        "modelsAnalysed": len(models),
        "rules": rules_meta,
        "ruleResults": all_rule_results,
        "passingRules": passing_rules,
        "summary": summary,
    }

    # Write output
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "engineering-bpa-results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    # Print summary to stderr
    print(f"\n--- Summary ---", file=sys.stderr)
    print(f"Models analysed:  {len(models)}", file=sys.stderr)
    print(f"Rules checked:    {len(RULES)}", file=sys.stderr)
    print(f"Rules passing:    {len(passing_rules)}", file=sys.stderr)
    print(f"Total findings:   {total_findings}", file=sys.stderr)
    print(f"  HIGH:   {high}", file=sys.stderr)
    print(f"  MEDIUM: {medium}", file=sys.stderr)
    print(f"  LOW:    {low}", file=sys.stderr)
    print(f"\nResults written to: {output_file}", file=sys.stderr)

    return 0


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Engineering Best Practice Analyser — checks dbt models consumed "
            "by Power BI for patterns that impact DirectQuery performance"
        )
    )
    parser.add_argument(
        "--dbt-path",
        required=True,
        type=Path,
        help="Path to the dbt project root",
    )
    parser.add_argument(
        "--taxonomy",
        required=True,
        type=Path,
        help="Path to model-taxonomy.json (output of analyse_semantic_model.py)",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Output directory for engineering-bpa-results.json",
    )
    args = parser.parse_args()

    dbt_path = args.dbt_path.resolve()
    taxonomy_path = args.taxonomy.resolve()
    output_dir = args.output.resolve()

    if not dbt_path.is_dir():
        print(f"ERROR: dbt path does not exist: {dbt_path}", file=sys.stderr)
        sys.exit(1)

    if not taxonomy_path.is_file():
        print(f"ERROR: Taxonomy file does not exist: {taxonomy_path}", file=sys.stderr)
        sys.exit(1)

    sys.exit(run_engineering_bpa(dbt_path, taxonomy_path, output_dir))


if __name__ == "__main__":
    main()
