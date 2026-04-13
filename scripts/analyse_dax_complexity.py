#!/usr/bin/env python3
"""
DAX Complexity Analyser for Power BI semantic models.

Extends the basic DAX audit by computing complexity scores for each measure
and building a measure-to-table dependency map. Identifies measures that
cross multiple DirectQuery tables (expensive cross-source joins).

Usage:
    python3 analyse_dax_complexity.py --model-path <path> --output <output-dir>
"""

import argparse
import json
import re
import sys
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


def _compute_complexity(expression: str) -> dict:
    """Compute a complexity score for a DAX expression.

    Scoring prioritises DirectQuery performance impact:
      1. Context transitions (CALCULATE + iterators) — 3 pts each
      2. Relationship hops (distinct table refs beyond 1) — 2 pts each
      3. FILTER(ALL) anti-pattern — 4 pts each
      4. Cross-DQ table references — 3 pts per additional DQ table (computed externally)
      5. LOC — secondary signal, 1 pt only for very long expressions
    """
    if not expression:
        return {
            "score": 0, "level": "none", "factors": [],
            "contextTransitions": 0, "relationshipHops": 0,
            "filterAllCount": 0, "estimatedSQLSubqueries": 0,
        }

    factors: list[str] = []
    score = 0
    upper = expression.upper()
    line_count = len(expression.strip().splitlines())

    # ── Context transitions (PRIMARY) ──
    calculate_count = len(re.findall(r"\bCALCULATE\s*\(", upper))
    iterators = re.findall(r"\b(SUMX|AVERAGEX|MAXX|MINX|COUNTX|RANKX|PRODUCTX)\s*\(", upper)
    iterator_count = len(iterators)
    context_transitions = calculate_count + iterator_count
    score += context_transitions * 3
    if calculate_count > 0:
        factors.append(f"{calculate_count} CALCULATE")
    if iterator_count > 0:
        factors.append(f"iterators: {', '.join(sorted(set(iterators)))}")

    # ── Relationship hops ──
    table_refs = set(re.findall(r"'([^']+)'\s*\[", expression))
    hop_count = len(table_refs)
    score += max(0, hop_count - 1) * 2
    if hop_count > 1:
        factors.append(f"{hop_count} table hops")

    # ── FILTER(ALL) anti-pattern ──
    filter_all_matches = re.findall(r"\bFILTER\s*\(\s*ALL\s*\(", upper)
    filter_all_count = len(filter_all_matches)
    score += filter_all_count * 4
    if filter_all_count:
        factors.append(f"{filter_all_count} FILTER(ALL)")

    # ── Time intelligence (moderate weight) ──
    time_intel = re.findall(
        r"\b(SAMEPERIODLASTYEAR|DATEADD|DATESYTD|DATESMTD|DATESQTD|DATESINPERIOD|"
        r"PARALLELPERIOD|PREVIOUSMONTH|PREVIOUSQUARTER|PREVIOUSYEAR|"
        r"OPENINGBALANCEMONTH|CLOSINGBALANCEMONTH|LASTNONBLANK)\s*\(",
        upper,
    )
    if time_intel:
        score += 2
        factors.append(f"time intelligence: {', '.join(sorted(set(time_intel)))}")

    # ── IFERROR / ISERROR ──
    if re.search(r"\b(IFERROR|ISERROR)\s*\(", upper):
        score += 1
        factors.append("IFERROR/ISERROR")

    # ── LOC (secondary — reduced weight) ──
    if line_count > 30:
        score += 1

    # ── VAR usage (good practice, slight reduction) ──
    if re.search(r"\bVAR\b", upper):
        score -= 1

    score = max(score, 0)
    estimated_subqueries = context_transitions + filter_all_count + (1 if context_transitions > 0 or filter_all_count > 0 else 0)

    if score <= 3:
        level = "low"
    elif score <= 8:
        level = "medium"
    elif score <= 15:
        level = "high"
    else:
        level = "critical"

    return {
        "score": score,
        "level": level,
        "factors": factors,
        "contextTransitions": context_transitions,
        "relationshipHops": hop_count,
        "filterAllCount": filter_all_count,
        "estimatedSQLSubqueries": estimated_subqueries,
    }


def _extract_table_references(expression: str) -> list[str]:
    """Extract table names referenced in a DAX expression."""
    return sorted(set(re.findall(r"'([^']+)'\s*\[", expression)))


def _load_taxonomy(taxonomy_file: Path | None) -> dict:
    """Load model-taxonomy.json for hot table enrichment."""
    if not taxonomy_file or not taxonomy_file.is_file():
        return {}
    try:
        with open(taxonomy_file, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _compute_optimization_priority(
    storage_mode: str, row_count: int | None, ref_count: int
) -> tuple[str, str]:
    """Compute optimization priority and reason for a hot table."""
    rc = row_count or 0
    if storage_mode == "directQuery" and rc > 1_000_000_000 and ref_count > 50:
        return "critical", f"DirectQuery with {rc / 1e9:.1f}B rows and {ref_count} measure references"
    if storage_mode == "directQuery" and (rc > 100_000_000 or ref_count > 20):
        return "high", f"DirectQuery with {rc / 1e6:.0f}M rows, {ref_count} measure references"
    if storage_mode == "dual" and ref_count > 50:
        return "medium", f"Dual mode with {ref_count} measure references"
    return "low", ""


def analyse_complexity(model_path: Path, taxonomy_file: Path | None = None) -> dict:
    """Analyse DAX complexity for all measures in a model."""
    tables_dir = model_path / "tables"
    if not tables_dir.is_dir():
        return {"measures": [], "statistics": {}, "hotTables": []}

    # First pass: collect table storage modes
    table_modes: dict[str, str] = {}
    for table_dir in sorted(tables_dir.iterdir()):
        if not table_dir.is_dir():
            continue
        table_json_path = table_dir / f"{table_dir.name}.json"
        table_json = _read_json(table_json_path) if table_json_path.is_file() else {}
        table_json = table_json or {}
        table_name = table_json.get("name", table_dir.name)

        partitions = _collect_items(table_dir / "partitions")
        modes = {str(p.get("mode", "import")).lower() for p in partitions}
        if "directquery" in modes:
            table_modes[table_name] = "directQuery"
        elif "dual" in modes:
            table_modes[table_name] = "dual"
        else:
            table_modes[table_name] = "import"

    # Second pass: analyse measures
    measure_results: list[dict] = []
    table_reference_counts: dict[str, int] = {}

    for table_dir in sorted(tables_dir.iterdir()):
        if not table_dir.is_dir():
            continue
        table_json_path = table_dir / f"{table_dir.name}.json"
        table_json = _read_json(table_json_path) if table_json_path.is_file() else {}
        table_json = table_json or {}
        host_table = table_json.get("name", table_dir.name)

        measures = _collect_items(table_dir / "measures")
        for m in measures:
            name = m.get("name", "")
            expr = _expr_to_str(m.get("expression", ""))
            if not name or not expr:
                continue

            complexity = _compute_complexity(expr)
            referenced_tables = _extract_table_references(expr)

            for t in referenced_tables:
                table_reference_counts[t] = table_reference_counts.get(t, 0) + 1

            dq_tables = [t for t in referenced_tables if table_modes.get(t) == "directQuery"]
            dual_tables = [t for t in referenced_tables if table_modes.get(t) == "dual"]
            crosses_multiple_dq = len(dq_tables) > 1

            measure_results.append({
                "name": name,
                "hostTable": host_table,
                "complexityScore": complexity["score"],
                "complexityLevel": complexity["level"],
                "complexityFactors": complexity["factors"],
                "contextTransitions": complexity["contextTransitions"],
                "relationshipHops": complexity["relationshipHops"],
                "filterAllCount": complexity["filterAllCount"],
                "estimatedSQLSubqueries": complexity["estimatedSQLSubqueries"],
                "lineCount": len(expr.strip().splitlines()),
                "referencedTables": referenced_tables,
                "directQueryTables": dq_tables,
                "dualTables": dual_tables,
                "crossesMultipleDQ": crosses_multiple_dq,
                "formatString": m.get("formatString", ""),
                "isHidden": m.get("isHidden", False),
            })

    measure_results.sort(key=lambda x: x["complexityScore"], reverse=True)

    # Load taxonomy for hot table enrichment
    taxonomy = _load_taxonomy(taxonomy_file)
    tax_tables = {t["name"]: t for t in taxonomy.get("tables", [])}
    tax_graph = {gt["name"]: gt for gt in taxonomy.get("graphAnalysis", {}).get("tables", [])}

    # Hot tables (most referenced by measures) — enriched with volumetry + degree
    hot_tables_raw = sorted(
        table_reference_counts.items(), key=lambda x: x[1], reverse=True
    )
    hot_tables: list[dict] = []
    for t_name, ref_count in hot_tables_raw[:20]:
        mode = table_modes.get(t_name, "unknown")
        tax_t = tax_tables.get(t_name, {})
        vol = tax_t.get("volumetry", {})
        graph_t = tax_graph.get(t_name, {})
        row_count = vol.get("rowCount")
        size_gb = vol.get("sizeGB")
        degree = graph_t.get("degree", 0)

        priority, reason = _compute_optimization_priority(mode, row_count, ref_count)
        ht: dict = {
            "table": t_name,
            "referenceCount": ref_count,
            "storageMode": mode,
            "rowCount": row_count,
            "sizeGB": size_gb,
            "degree": degree,
            "optimizationPriority": priority,
        }
        if reason:
            ht["reason"] = reason
        hot_tables.append(ht)

    # Statistics
    total = len(measure_results)
    by_level = {"low": 0, "medium": 0, "high": 0, "critical": 0}
    for m in measure_results:
        by_level[m["complexityLevel"]] = by_level.get(m["complexityLevel"], 0) + 1

    cross_dq = sum(1 for m in measure_results if m["crossesMultipleDQ"])
    total_ctx = sum(m["contextTransitions"] for m in measure_results)
    measures_with_filter_all = sum(1 for m in measure_results if m["filterAllCount"] > 0)
    measures_high_subq = sum(1 for m in measure_results if m["estimatedSQLSubqueries"] >= 5)

    return {
        "version": "2.0.0",
        "analysedAt": datetime.now(timezone.utc).isoformat(),
        "modelPath": str(model_path),
        "measures": measure_results,
        "hotTables": hot_tables,
        "statistics": {
            "totalMeasures": total,
            "byComplexity": by_level,
            "crossMultipleDQ": cross_dq,
            "avgComplexityScore": round(
                sum(m["complexityScore"] for m in measure_results) / total, 1
            ) if total > 0 else 0,
            "avgContextTransitions": round(
                total_ctx / total, 1
            ) if total > 0 else 0,
            "avgRelationshipHops": round(
                sum(m["relationshipHops"] for m in measure_results) / total, 1
            ) if total > 0 else 0,
            "measuresWithFilterAll": measures_with_filter_all,
            "measuresWithHighSubqueries": measures_high_subq,
        },
    }


def main():
    parser = argparse.ArgumentParser(
        description="Analyse DAX measure complexity in Power BI semantic models"
    )
    parser.add_argument(
        "--model-path", required=True, type=Path,
        help="Path to the semantic model directory",
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output directory for dax-complexity.json",
    )
    parser.add_argument(
        "--taxonomy-file", required=False, type=Path, default=None,
        help="Path to model-taxonomy.json for hot table enrichment with volumetry and degree",
    )
    args = parser.parse_args()

    model_path = args.model_path.resolve()
    output_dir = args.output.resolve()
    tax_file = args.taxonomy_file.resolve() if args.taxonomy_file else None

    if not model_path.is_dir():
        print(f"ERROR: Model path does not exist: {model_path}", file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    result = analyse_complexity(model_path, taxonomy_file=tax_file)

    output_file = output_dir / "dax-complexity.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    stats = result["statistics"]
    print(f"\n=== DAX Complexity Analysis ===")
    print(f"Measures: {stats['totalMeasures']}")
    print(f"Complexity: {stats['byComplexity']}")
    print(f"Avg score: {stats['avgComplexityScore']} | Avg context transitions: {stats['avgContextTransitions']}")
    print(f"Measures with FILTER(ALL): {stats['measuresWithFilterAll']} | With 5+ SQL subqueries: {stats['measuresWithHighSubqueries']}")
    print(f"Cross-DQ measures: {stats['crossMultipleDQ']}")
    if result["hotTables"]:
        print(f"\nTop 5 hot tables:")
        for ht in result["hotTables"][:5]:
            vol = f", {ht['sizeGB']:.1f} GB" if ht.get("sizeGB") else ""
            deg = f", degree {ht['degree']}" if ht.get("degree") else ""
            pri = f" [{ht['optimizationPriority']}]" if ht.get("optimizationPriority") != "low" else ""
            print(f"  {ht['table']} ({ht['storageMode']}{vol}{deg}): {ht['referenceCount']} refs{pri}")
    print(f"\nWritten: {output_file}")


if __name__ == "__main__":
    main()
