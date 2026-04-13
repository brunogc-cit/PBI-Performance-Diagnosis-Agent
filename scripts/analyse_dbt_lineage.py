#!/usr/bin/env python3
"""
Analyse dbt project for serve-layer lineage and materialisation strategy.

Parses serve SQL files, contract YAMLs, and dbt_project.yml to build a
lineage map from Power BI tables to dbt models to Databricks tables.

Usage:
    python3 analyse_dbt_lineage.py --dbt-path <path> --output <output-dir>
"""

import argparse
import json
import re
import sys
from pathlib import Path
from datetime import datetime, timezone


def _read_yaml_simple(path: Path) -> dict | None:
    """Minimal YAML-like parser for dbt contract files (key: value pairs).

    This avoids a PyYAML dependency. It handles the subset of YAML used in
    dbt contract files: models list with name, config, columns, data_tests.
    Returns a simplified dict for the fields we care about.
    """
    try:
        text = path.read_text(encoding="utf-8-sig")
    except OSError as exc:
        print(f"  WARNING: Could not read {path}: {exc}", file=sys.stderr)
        return None

    result: dict = {"models": []}
    current_model: dict | None = None
    in_config = False

    for line in text.splitlines():
        stripped = line.strip()

        if stripped.startswith("- name:"):
            if current_model:
                result["models"].append(current_model)
            current_model = {
                "name": stripped.split(":", 1)[1].strip(),
                "config": {},
            }
            in_config = False

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

        elif current_model and stripped.startswith("auto_liquid_cluster:"):
            current_model["config"]["auto_liquid_cluster"] = stripped.split(":", 1)[1].strip().lower() == "true"

        elif current_model and stripped.startswith("unique_key:"):
            val = stripped.split(":", 1)[1].strip()
            if val.startswith("["):
                keys = re.findall(r"'([^']+)'", val)
                current_model["config"]["unique_key"] = keys
            else:
                current_model["config"]["unique_key"] = [val.strip("'\"")]

        elif current_model and stripped.startswith("owner:"):
            current_model["config"]["owner"] = stripped.split(":", 1)[1].strip()

        elif current_model and stripped.startswith("lifecycle:"):
            current_model["config"]["lifecycle"] = stripped.split(":", 1)[1].strip()

    if current_model:
        result["models"].append(current_model)

    return result


def _parse_serve_sql(sql_path: Path) -> dict:
    """Parse a serve-layer SQL file for refs, filters, and column count."""
    try:
        text = sql_path.read_text(encoding="utf-8-sig")
    except OSError:
        return {"refs": [], "hasFilter": False, "hasUnion": False, "columnCount": 0}

    refs = re.findall(r"\{\{\s*ref\(\s*'([^']+)'\s*\)\s*\}\}", text)
    has_filter = bool(re.search(r"\bWHERE\b", text, re.IGNORECASE))
    has_union = bool(re.search(r"\bUNION\s+ALL\b", text, re.IGNORECASE))

    select_cols = 0
    in_select = False
    for line in text.splitlines():
        stripped = line.strip().lower()
        if stripped.startswith("select"):
            in_select = True
            continue
        if in_select and (stripped.startswith("from") or stripped.startswith(")")):
            break
        if in_select and stripped and not stripped.startswith("--"):
            select_cols += 1

    return {
        "refs": refs,
        "hasFilter": has_filter,
        "hasUnion": has_union,
        "columnCount": select_cols,
    }


def analyse_dbt(dbt_path: Path) -> dict:
    """Analyse the dbt project for serve-layer lineage."""
    models_dir = dbt_path / "bundles" / "core_data" / "models"

    if not models_dir.is_dir():
        models_dir = dbt_path / "models"
        if not models_dir.is_dir():
            print(f"WARNING: No models directory found in {dbt_path}", file=sys.stderr)
            return {"domains": [], "serveModels": [], "statistics": {}}

    # Discover domains
    domains: list[str] = []
    serve_models: list[dict] = []

    for domain_dir in sorted(models_dir.iterdir()):
        if not domain_dir.is_dir():
            continue
        domain_name = domain_dir.name

        serve_dir = domain_dir / "serve"
        if not serve_dir.is_dir():
            continue

        domains.append(domain_name)

        # Parse serve SQL files
        for sql_file in sorted(serve_dir.glob("*.sql")):
            sql_analysis = _parse_serve_sql(sql_file)

            # Find matching contract
            contract: dict = {}
            contracts_dir = serve_dir / "_contracts"
            if contracts_dir.is_dir():
                contract_file = contracts_dir / f"{sql_file.stem}.yml"
                if contract_file.is_file():
                    yaml_data = _read_yaml_simple(contract_file)
                    if yaml_data and yaml_data.get("models"):
                        contract = yaml_data["models"][0]

            config = contract.get("config", {})

            # Check curated-layer contracts for the referenced models
            curated_info: list[dict] = []
            for ref_name in sql_analysis["refs"]:
                curated_dir = domain_dir / "curated" / "_contracts"
                if curated_dir.is_dir():
                    curated_contract = curated_dir / f"{ref_name}.yml"
                    if curated_contract.is_file():
                        curated_yaml = _read_yaml_simple(curated_contract)
                        if curated_yaml and curated_yaml.get("models"):
                            cm = curated_yaml["models"][0]
                            curated_info.append({
                                "name": ref_name,
                                "materialized": cm.get("config", {}).get("materialized", ""),
                                "incrementalStrategy": cm.get("config", {}).get("incremental_strategy", ""),
                                "liquidClusteredBy": cm.get("config", {}).get("liquid_clustered_by", []),
                                "autoLiquidCluster": cm.get("config", {}).get("auto_liquid_cluster", False),
                                "uniqueKey": cm.get("config", {}).get("unique_key", []),
                            })

            serve_models.append({
                "domain": domain_name,
                "sqlFile": sql_file.name,
                "modelName": sql_file.stem,
                "alias": config.get("alias", sql_file.stem),
                "materialized": config.get("materialized", "view"),
                "owner": config.get("owner", ""),
                "lifecycle": config.get("lifecycle", ""),
                "refs": sql_analysis["refs"],
                "hasFilter": sql_analysis["hasFilter"],
                "hasUnion": sql_analysis["hasUnion"],
                "selectColumnCount": sql_analysis["columnCount"],
                "curatedSources": curated_info,
            })

    # Statistics
    total_serve = len(serve_models)
    views = sum(1 for m in serve_models if m["materialized"] == "view")
    tables = sum(1 for m in serve_models if m["materialized"] == "table")
    mat_views = sum(1 for m in serve_models if m["materialized"] == "materialized_view")
    incremental = sum(1 for m in serve_models if m["materialized"] == "incremental")
    with_filter = sum(1 for m in serve_models if m["hasFilter"])
    with_union = sum(1 for m in serve_models if m["hasUnion"])
    wide_models = sum(1 for m in serve_models if m["selectColumnCount"] > 50)

    # ── Value gate: identify actionable performance findings ──
    actionable_findings: list[dict] = []

    for sm in serve_models:
        # Wide serve views (>50 columns and no filter = pass-through)
        if sm["selectColumnCount"] > 50 and not sm["hasFilter"]:
            actionable_findings.append({
                "type": "wide-serve-view",
                "model": sm["alias"],
                "domain": sm["domain"],
                "detail": f"{sm['selectColumnCount']} columns exposed with no WHERE filter. Pass-through view over curated table.",
                "columnCount": sm["selectColumnCount"],
            })
        # Views that could benefit from a WHERE filter
        elif not sm["hasFilter"] and sm["selectColumnCount"] > 0:
            # Check if curated source is large (incremental = likely large)
            for cs in sm.get("curatedSources", []):
                if cs.get("materialized") == "incremental":
                    actionable_findings.append({
                        "type": "missing-filter",
                        "model": sm["alias"],
                        "domain": sm["domain"],
                        "detail": f"No WHERE clause on serve view over incremental curated table '{cs['name']}'. Date filtering happens at query time instead.",
                    })
                    break

        # Views that should potentially be materialised
        if sm["materialized"] == "view" and sm["selectColumnCount"] > 50:
            for cs in sm.get("curatedSources", []):
                if cs.get("materialized") == "incremental" and cs.get("liquidClusteredBy"):
                    actionable_findings.append({
                        "type": "should-materialise",
                        "model": sm["alias"],
                        "domain": sm["domain"],
                        "detail": f"View over large incremental table with liquid clustering. Materialising as a narrow table could enable better pruning.",
                    })
                    break

        # Missing clustering on curated sources
        for cs in sm.get("curatedSources", []):
            if cs.get("materialized") == "incremental" and not cs.get("liquidClusteredBy") and not cs.get("autoLiquidCluster"):
                actionable_findings.append({
                    "type": "missing-clustering",
                    "model": sm["alias"],
                    "domain": sm["domain"],
                    "detail": f"Curated table '{cs['name']}' is incremental but has no liquid clustering configured.",
                })

    has_actionable = len(actionable_findings) > 0

    return {
        "dbtPath": str(dbt_path),
        "analysedAt": datetime.now(timezone.utc).isoformat(),
        "domains": domains,
        "serveModels": serve_models,
        "actionableFindings": actionable_findings,
        "hasActionableFindings": has_actionable,
        "statistics": {
            "totalServeModels": total_serve,
            "materializations": {
                "view": views,
                "table": tables,
                "materializedView": mat_views,
                "incremental": incremental,
            },
            "withFilter": with_filter,
            "withUnion": with_union,
            "wideModels": wide_models,
            "domainCount": len(domains),
            "actionableFindingsCount": len(actionable_findings),
        },
    }


def main():
    parser = argparse.ArgumentParser(
        description="Analyse dbt project for serve-layer lineage"
    )
    parser.add_argument(
        "--dbt-path", required=True, type=Path,
        help="Path to the dbt project root",
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output directory for dbt-lineage.json",
    )
    args = parser.parse_args()

    dbt_path = args.dbt_path.resolve()
    output_dir = args.output.resolve()

    if not dbt_path.is_dir():
        print(f"ERROR: dbt path does not exist: {dbt_path}", file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    result = analyse_dbt(dbt_path)

    output_file = output_dir / "dbt-lineage.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    stats = result["statistics"]
    print(f"\n=== dbt Lineage Analysis ===")
    print(f"Domains: {', '.join(result['domains'])}")
    print(f"Serve models: {stats['totalServeModels']}")
    print(f"Materializations: {stats['materializations']}")
    print(f"With WHERE filter: {stats['withFilter']} | With UNION ALL: {stats['withUnion']}")
    print(f"Wide models (>50 cols): {stats['wideModels']}")
    print(f"Actionable findings: {stats['actionableFindingsCount']}")
    if result["actionableFindings"]:
        for af in result["actionableFindings"][:5]:
            print(f"  [{af['type']}] {af['model']}: {af['detail'][:80]}")
    print(f"\nWritten: {output_file}")


if __name__ == "__main__":
    main()
