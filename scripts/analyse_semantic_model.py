#!/usr/bin/env python3
"""
Analyse a Power BI semantic model (Tabular Editor JSON format).

Parses the model directory structure and produces a structured taxonomy
including tables, storage modes, relationships, expressions, and source mappings.

Usage:
    python3 analyse_semantic_model.py --model-path <path> --output <output-dir>
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timezone


def _read_json(path: Path) -> dict | list | None:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  WARNING: Could not parse {path}: {exc}", file=sys.stderr)
        return None


def _expr_to_str(expr) -> str:
    if isinstance(expr, list):
        return "\n".join(str(e) for e in expr)
    return str(expr) if expr else ""


def _collect_items(directory: Path) -> list[dict]:
    items: list[dict] = []
    if not directory.is_dir():
        return items
    for f in sorted(directory.iterdir()):
        if f.suffix == ".json":
            data = _read_json(f)
            if data:
                items.append(data)
    return items


def _classify_tables(tables: list[dict], relationships: list[dict]) -> dict[str, str]:
    """Derive fact/dimension/bridge/metadata classification per table.

    Logic:
      - Tables starting with '@' or with no Databricks source → 'metadata'
      - Tables on the "many" side of many-to-one relationships → 'fact' candidate
      - Tables on the "one" side → 'dimension' candidate
      - Tables on both sides of many-to-many → 'bridge'
      - Default: 'dimension'
    """
    many_side: set[str] = set()
    one_side: set[str] = set()
    m2m_tables: set[str] = set()

    for r in relationships:
        fc = r.get("fromCardinality", "many")
        tc = r.get("toCardinality", "one")
        ft = r.get("fromTable", "")
        tt = r.get("toTable", "")
        if fc == "many" and tc == "many":
            m2m_tables.add(ft)
            m2m_tables.add(tt)
        elif fc == "many":
            many_side.add(ft)
            one_side.add(tt)
        elif tc == "many":
            many_side.add(tt)
            one_side.add(ft)

    result: dict[str, str] = {}
    for t in tables:
        name = t["name"]
        if name.startswith("@") or (not t.get("sourceTable") and t["storageMode"] == "import"):
            result[name] = "metadata"
        elif name in m2m_tables:
            result[name] = "bridge"
        elif name in many_side and name not in one_side:
            result[name] = "fact"
        elif name in one_side:
            result[name] = "dimension"
        elif t["storageMode"] == "directQuery":
            result[name] = "fact"
        else:
            result[name] = "dimension"
    return result


def _build_relationship_tree(
    root: str,
    adj_detail: dict[str, list[dict]],
    table_names: set[str],
    table_modes: dict[str, str],
    classifications: dict[str, str],
    volumetry_map: dict[str, dict],
    max_tree_depth: int = 6,
) -> dict:
    """Build a hierarchical relationship tree via BFS from a root table.

    Returns a tree structure with branches showing every reachable table,
    the relationship metadata at each edge, and depth-level distribution.
    """
    tree_children: list[dict] = []
    visited: set[str] = {root}
    # BFS queue: (parent_children_list, current_table, depth)
    queue: list[tuple[list, str, int]] = []

    # Seed with direct neighbours
    for edge in adj_detail.get(root, []):
        neighbour = edge["target"]
        if neighbour in table_names and neighbour not in visited:
            visited.add(neighbour)
            vol = volumetry_map.get(neighbour, {})
            child: dict = {
                "table": neighbour,
                "classification": classifications.get(neighbour, "unknown"),
                "storageMode": table_modes.get(neighbour, "unknown"),
                "cardinality": edge["cardinality"],
                "crossFilter": edge["crossFilter"],
                "direction": edge["direction"],
                "depth": 1,
                "rowCount": vol.get("rowCount"),
                "sizeGB": vol.get("sizeGB"),
                "children": [],
            }
            tree_children.append(child)
            queue.append((child["children"], neighbour, 1))

    while queue:
        parent_list, node, depth = queue.pop(0)
        if depth >= max_tree_depth:
            continue
        for edge in adj_detail.get(node, []):
            neighbour = edge["target"]
            if neighbour in table_names and neighbour not in visited:
                visited.add(neighbour)
                vol = volumetry_map.get(neighbour, {})
                child = {
                    "table": neighbour,
                    "classification": classifications.get(neighbour, "unknown"),
                    "storageMode": table_modes.get(neighbour, "unknown"),
                    "cardinality": edge["cardinality"],
                    "crossFilter": edge["crossFilter"],
                    "direction": edge["direction"],
                    "depth": depth + 1,
                    "rowCount": vol.get("rowCount"),
                    "sizeGB": vol.get("sizeGB"),
                    "children": [],
                }
                parent_list.append(child)
                queue.append((child["children"], neighbour, depth + 1))

    # Compute depth distribution and cascade metrics
    depth_dist: dict[int, int] = defaultdict(int)
    storage_at_depth: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    def _walk(nodes: list[dict]) -> None:
        for n in nodes:
            d = n["depth"]
            depth_dist[d] += 1
            storage_at_depth[d][n["storageMode"]] += 1
            _walk(n["children"])

    _walk(tree_children)

    total_reachable = sum(depth_dist.values())
    max_depth = max(depth_dist.keys()) if depth_dist else 0

    # Build depth summary list
    depth_summary: list[dict] = []
    for d in sorted(depth_dist.keys()):
        depth_summary.append({
            "depth": d,
            "tableCount": depth_dist[d],
            "storageModes": dict(storage_at_depth[d]),
        })

    return {
        "branches": tree_children,
        "totalReachableTables": total_reachable,
        "maxDepth": max_depth,
        "depthDistribution": depth_summary,
        "branchingFactor": round(total_reachable / max(len(tree_children), 1), 1),
    }


def _compute_join_cascade(
    table_names: set[str],
    adj: dict[str, set[str]],
    table_modes: dict[str, str],
    classifications: dict[str, str],
) -> list[dict]:
    """For every table, compute how many tables are transitively pulled in
    when a query touches it (the 'join cascade')."""
    cascades: list[dict] = []
    for tbl in sorted(table_names):
        visited: set[str] = {tbl}
        queue: list[str] = [tbl]
        dq_count = 0
        max_depth = 0
        depth_map: dict[str, int] = {tbl: 0}
        while queue:
            node = queue.pop(0)
            nd = depth_map[node]
            for neighbour in adj.get(node, set()):
                if neighbour not in visited and neighbour in table_names:
                    visited.add(neighbour)
                    depth_map[neighbour] = nd + 1
                    max_depth = max(max_depth, nd + 1)
                    if table_modes.get(neighbour) == "directQuery":
                        dq_count += 1
                    queue.append(neighbour)
        total = len(visited) - 1  # exclude self
        if total > 0:
            cascades.append({
                "table": tbl,
                "classification": classifications.get(tbl, "unknown"),
                "storageMode": table_modes.get(tbl, "unknown"),
                "cascadeTables": total,
                "cascadeDirectQuery": dq_count,
                "cascadeMaxDepth": max_depth,
            })
    cascades.sort(key=lambda x: x["cascadeTables"], reverse=True)
    return cascades


def _compute_graph_analysis(
    tables: list[dict],
    relationships: list[dict],
    classifications: dict[str, str],
    volumetry_map: dict[str, dict],
) -> dict:
    """Compute relationship graph metrics: degree centrality, hub detection,
    snowflake depth, and detailed branching analysis."""
    # Build adjacency (simple for degree counting)
    adj: dict[str, set[str]] = defaultdict(set)
    in_degree: dict[str, int] = defaultdict(int)
    out_degree: dict[str, int] = defaultdict(int)

    # Build detailed adjacency with relationship metadata
    adj_detail: dict[str, list[dict]] = defaultdict(list)

    for r in relationships:
        ft = r.get("fromTable", "")
        tt = r.get("toTable", "")
        if ft and tt:
            adj[ft].add(tt)
            adj[tt].add(ft)
            out_degree[ft] += 1
            in_degree[tt] += 1

            card = f"{r.get('fromCardinality', '*')}:{r.get('toCardinality', '1')}"
            cross = r.get("crossFilteringBehavior", "oneDirection")
            adj_detail[ft].append({
                "target": tt,
                "cardinality": card,
                "crossFilter": cross,
                "direction": "outbound",
            })
            adj_detail[tt].append({
                "target": ft,
                "cardinality": card,
                "crossFilter": cross,
                "direction": "inbound",
            })

    table_names = {t["name"] for t in tables}
    table_modes = {t["name"]: t["storageMode"] for t in tables}

    # Per-table graph stats
    graph_tables: list[dict] = []
    for t in tables:
        name = t["name"]
        deg = in_degree.get(name, 0) + out_degree.get(name, 0)
        graph_tables.append({
            "name": name,
            "degree": deg,
            "inDegree": in_degree.get(name, 0),
            "outDegree": out_degree.get(name, 0),
            "isHub": deg >= 5,
            "classification": classifications.get(name, "unknown"),
            "storageMode": table_modes.get(name, "unknown"),
        })

    # Max snowflake depth via BFS from each fact table
    fact_tables = [name for name, cls in classifications.items() if cls == "fact"]
    max_depth = 0
    for ft in fact_tables:
        visited: set[str] = {ft}
        queue: list[tuple[str, int]] = [(ft, 0)]
        while queue:
            node, depth = queue.pop(0)
            max_depth = max(max_depth, depth)
            for neighbour in adj.get(node, set()):
                if neighbour not in visited and neighbour in table_names:
                    visited.add(neighbour)
                    queue.append((neighbour, depth + 1))

    # Identify hub tables with enrichment + branching trees
    hub_tables: list[dict] = []
    for gt in graph_tables:
        if gt["isHub"]:
            vol = volumetry_map.get(gt["name"], {})
            tree = _build_relationship_tree(
                gt["name"], adj_detail, table_names, table_modes,
                classifications, volumetry_map,
            )
            hub_tables.append({
                "name": gt["name"],
                "degree": gt["degree"],
                "classification": gt["classification"],
                "storageMode": gt["storageMode"],
                "sizeGB": vol.get("sizeGB", None),
                "rowCount": vol.get("rowCount", None),
                "branchTree": tree,
            })
    hub_tables.sort(key=lambda x: x["degree"], reverse=True)

    total_degree = sum(gt["degree"] for gt in graph_tables)
    avg_degree = round(total_degree / len(graph_tables), 1) if graph_tables else 0

    # Join cascade analysis for all tables
    join_cascades = _compute_join_cascade(
        table_names, adj, table_modes, classifications,
    )

    return {
        "maxSnowflakeDepth": max_depth,
        "avgDegree": avg_degree,
        "hubTables": hub_tables,
        "tables": graph_tables,
        "joinCascades": join_cascades,
    }


def _merge_volumetry(tables: list[dict], volumetry_file: Path | None) -> dict[str, dict]:
    """Load databricks-profile.json and merge volumetry into tables list.
    Returns a mapping of PBI table name → volumetry dict for cross-referencing."""
    vol_map: dict[str, dict] = {}
    if not volumetry_file or not volumetry_file.is_file():
        return vol_map

    vol_data = _read_json(volumetry_file)
    if not vol_data:
        return vol_map

    # Build lookup by Databricks full name
    dbx_lookup: dict[str, dict] = {}
    for entry in vol_data.get("tables", []):
        full_name = entry.get("fullName", "").lower()
        if full_name:
            dbx_lookup[full_name] = entry

    for t in tables:
        if t.get("sourceTable"):
            full_name = f"{t['sourceCatalog']}.{t['sourceDatabase']}.{t['sourceTable']}".lower()
            if full_name in dbx_lookup:
                entry = dbx_lookup[full_name]
                vol = {
                    "rowCount": entry.get("rowCount"),
                    "sizeGB": entry.get("sizeGB"),
                    "numFiles": entry.get("numFiles"),
                    "clusteringColumns": entry.get("clusteringColumns", []),
                }
                t["volumetry"] = vol
                vol_map[t["name"]] = vol

    return vol_map


def analyse_model(model_path: Path, volumetry_file: Path | None = None) -> dict:
    """Analyse a single semantic model directory."""
    db_json_path = model_path / "database.json"
    db_json = _read_json(db_json_path) if db_json_path.is_file() else {}
    db_json = db_json or {}

    model_section = db_json.get("model", {})
    model_name = model_section.get("name", model_path.name)

    # BPA config
    annotations = model_section.get("annotations", [])
    bpa_external_rules = ""
    bpa_ignore_rules: list[str] = []
    for ann in annotations:
        if ann.get("name") == "BestPracticeAnalyzer_ExternalRuleFiles":
            bpa_external_rules = ann.get("value", "")
        if ann.get("name") == "BestPracticeAnalyzer_IgnoreRules":
            try:
                ignore = json.loads(ann.get("value", "{}"))
                bpa_ignore_rules = ignore.get("RuleIDs", [])
            except json.JSONDecodeError:
                pass

    # Expressions
    expressions: list[dict] = []
    expr_dir = model_path / "expressions"
    if expr_dir.is_dir():
        for f in sorted(expr_dir.iterdir()):
            if f.suffix == ".json":
                data = _read_json(f)
                if data:
                    expressions.append({
                        "name": data.get("name", f.stem),
                        "kind": data.get("kind", ""),
                        "expression": _expr_to_str(data.get("expression", "")),
                    })

    # Tables
    tables: list[dict] = []
    tables_dir = model_path / "tables"
    if tables_dir.is_dir():
        for table_dir in sorted(tables_dir.iterdir()):
            if not table_dir.is_dir():
                continue

            table_json_path = table_dir / f"{table_dir.name}.json"
            table_json = _read_json(table_json_path) if table_json_path.is_file() else {}
            table_json = table_json or {}
            table_name = table_json.get("name", table_dir.name)

            # Extended properties (source info)
            ext_props = {}
            for ep in table_json.get("extendedProperties", []):
                ext_props[ep.get("name", "")] = ep.get("value", "")

            columns = _collect_items(table_dir / "columns")
            measures = _collect_items(table_dir / "measures")
            partitions = _collect_items(table_dir / "partitions")

            # Determine storage mode
            storage_modes = set()
            partition_expressions: list[str] = []
            for p in partitions:
                mode = str(p.get("mode", "import")).lower()
                storage_modes.add(mode)
                source = p.get("source", {})
                expr = _expr_to_str(source.get("expression", ""))
                if expr:
                    partition_expressions.append(expr)

            primary_mode = "import"
            if "directquery" in storage_modes:
                primary_mode = "directQuery"
            elif "dual" in storage_modes:
                primary_mode = "dual"

            # Source Databricks table
            source_catalog = ext_props.get("SourceCatalog", "")
            source_database = ext_props.get("SourceDatabase", "")
            source_table = ext_props.get("SourceTable", "")

            if not source_table and partition_expressions:
                for pexpr in partition_expressions:
                    if "_fn_GetDataFromDBX" in pexpr:
                        import re
                        match = re.search(
                            r'_fn_GetDataFromDBX\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)',
                            pexpr
                        )
                        if match:
                            source_catalog = source_catalog or match.group(1)
                            source_database = source_database or match.group(2)
                            source_table = source_table or match.group(3)

            tables.append({
                "name": table_name,
                "storageMode": primary_mode,
                "columnCount": len(columns),
                "measureCount": len(measures),
                "partitionCount": len(partitions),
                "isHidden": table_json.get("isHidden", False),
                "sourceCatalog": source_catalog,
                "sourceDatabase": source_database,
                "sourceTable": source_table,
                "partitionExpressions": partition_expressions,
                "columns": [
                    {
                        "name": c.get("name", ""),
                        "dataType": c.get("dataType", ""),
                        "isHidden": c.get("isHidden", False),
                    }
                    for c in columns
                ],
                "measures": [
                    {
                        "name": m.get("name", ""),
                        "expression": _expr_to_str(m.get("expression", "")),
                        "formatString": m.get("formatString", ""),
                        "displayFolder": m.get("displayFolder", ""),
                        "isHidden": m.get("isHidden", False),
                    }
                    for m in measures
                ],
            })

    # Relationships
    relationships: list[dict] = []
    rels_dir = model_path / "relationships"
    if rels_dir.is_dir():
        for f in sorted(rels_dir.iterdir()):
            if f.suffix == ".json":
                data = _read_json(f)
                if data:
                    relationships.append({
                        "name": data.get("name", ""),
                        "fromTable": data.get("fromTable", ""),
                        "fromColumn": data.get("fromColumn", ""),
                        "toTable": data.get("toTable", ""),
                        "toColumn": data.get("toColumn", ""),
                        "isActive": data.get("isActive", True),
                        "crossFilteringBehavior": data.get("crossFilteringBehavior", "oneDirection"),
                        "fromCardinality": data.get("fromCardinality", "many"),
                        "toCardinality": data.get("toCardinality", "one"),
                    })

    # Statistics
    total_tables = len(tables)
    dq_tables = sum(1 for t in tables if t["storageMode"] == "directQuery")
    dual_tables = sum(1 for t in tables if t["storageMode"] == "dual")
    import_tables = sum(1 for t in tables if t["storageMode"] == "import")
    total_columns = sum(t["columnCount"] for t in tables)
    total_measures = sum(t["measureCount"] for t in tables)
    total_relationships = len(relationships)
    bidi_rels = sum(1 for r in relationships if r["crossFilteringBehavior"] == "bothDirections")
    m2m_rels = sum(1 for r in relationships if r["fromCardinality"] == "many" and r["toCardinality"] == "many")
    inactive_rels = sum(1 for r in relationships if not r["isActive"])

    # Merge volumetry from databricks-profile.json (if available)
    vol_map = _merge_volumetry(tables, volumetry_file)

    # Classify tables as fact/dimension/bridge/metadata
    classifications = _classify_tables(tables, relationships)
    for t in tables:
        t["classification"] = classifications.get(t["name"], "unknown")

    # Graph analysis: degree centrality, hub detection, snowflake depth
    graph_analysis = _compute_graph_analysis(tables, relationships, classifications, vol_map)

    # Source table mapping
    source_mapping: list[dict] = []
    for t in tables:
        if t["sourceTable"]:
            source_mapping.append({
                "pbiTable": t["name"],
                "storageMode": t["storageMode"],
                "databricksCatalog": t["sourceCatalog"],
                "databricksSchema": t["sourceDatabase"],
                "databricksTable": t["sourceTable"],
            })

    return {
        "modelName": model_name,
        "modelPath": str(model_path),
        "statistics": {
            "totalTables": total_tables,
            "directQueryTables": dq_tables,
            "dualTables": dual_tables,
            "importTables": import_tables,
            "totalColumns": total_columns,
            "totalMeasures": total_measures,
            "totalRelationships": total_relationships,
            "bidirectionalRelationships": bidi_rels,
            "manyToManyRelationships": m2m_rels,
            "inactiveRelationships": inactive_rels,
        },
        "bpaConfig": {
            "externalRuleFiles": bpa_external_rules,
            "ignoredRules": bpa_ignore_rules,
        },
        "expressions": expressions,
        "tables": tables,
        "relationships": relationships,
        "sourceMapping": source_mapping,
        "graphAnalysis": graph_analysis,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Analyse Power BI semantic model structure"
    )
    parser.add_argument(
        "--model-path", required=True, type=Path,
        help="Path to the semantic model directory",
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output directory for model-taxonomy.json",
    )
    parser.add_argument(
        "--volumetry-file", required=False, type=Path, default=None,
        help="Path to databricks-profile.json for volumetry enrichment",
    )
    args = parser.parse_args()

    model_path = args.model_path.resolve()
    output_dir = args.output.resolve()
    vol_file = args.volumetry_file.resolve() if args.volumetry_file else None

    if not model_path.is_dir():
        print(f"ERROR: Model path does not exist: {model_path}", file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    result = analyse_model(model_path, volumetry_file=vol_file)

    output_file = output_dir / "model-taxonomy.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    stats = result["statistics"]
    ga = result.get("graphAnalysis", {})
    print(f"\n=== Semantic Model Analysis ===")
    print(f"Model: {result['modelName']}")
    print(f"Tables: {stats['totalTables']} ({stats['directQueryTables']} DQ, {stats['dualTables']} Dual, {stats['importTables']} Import)")
    print(f"Columns: {stats['totalColumns']} | Measures: {stats['totalMeasures']}")
    print(f"Relationships: {stats['totalRelationships']} ({stats['bidirectionalRelationships']} bidirectional, {stats['manyToManyRelationships']} M:M, {stats['inactiveRelationships']} inactive)")
    print(f"Source mappings: {len(result['sourceMapping'])} tables mapped to Databricks")
    if ga.get("hubTables"):
        print(f"Hub tables (degree >= 5): {', '.join(h['name'] for h in ga['hubTables'])}")
    print(f"Max snowflake depth: {ga.get('maxSnowflakeDepth', 0)}")
    vol_count = sum(1 for t in result["tables"] if t.get("volumetry"))
    if vol_count:
        print(f"Volumetry enriched: {vol_count} tables")
    print(f"\nWritten: {output_file}")


if __name__ == "__main__":
    main()
