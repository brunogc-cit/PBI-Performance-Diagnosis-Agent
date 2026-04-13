#!/usr/bin/env python3
"""
Column Memory Analyser for Power BI semantic models.

Estimates column-level memory consumption from PBI semantic model metadata
(Tabular Editor JSON format) and identifies columns that are candidates for
removal to reduce memory footprint.

Usage:
    python3 analyse_column_memory.py \
      --model-path <path-to-PBI-model-dir> \
      --taxonomy output/model-taxonomy.json \
      --dax-complexity output/dax-complexity.json \
      --output output/
"""

import argparse
import json
import re
import sys
from pathlib import Path
from datetime import date, datetime, timezone


# ── Type base sizes (bytes per value) ──

TYPE_BASE_SIZES: dict[str, int] = {
    "int64": 8,
    "Int64": 8,
    "double": 8,
    "Double": 8,
    "decimal": 16,
    "Decimal": 16,
    "string": 20,
    "String": 20,
    "boolean": 1,
    "Boolean": 1,
    "dateTime": 8,
    "DateTime": 8,
    "binary": 100,
    "Binary": 100,
}

DEFAULT_BASE_SIZE = 8


def _read_json(path: Path) -> dict | list | None:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  WARNING: Could not parse {path}: {exc}", file=sys.stderr)
        return None


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


def _expr_to_str(expr) -> str:
    if isinstance(expr, list):
        return "\n".join(str(e) for e in expr)
    return str(expr) if expr else ""


def _compression_factor(data_type: str, column_name: str, classification: str) -> float:
    """Compute a heuristic compression factor based on data type and context."""
    dt_lower = data_type.lower()
    name_lower = column_name.lower()

    if dt_lower == "boolean":
        return 0.01

    if dt_lower in ("int64",):
        if any(tok in name_lower for tok in ("sk", "key")):
            return 0.1
        return 0.05

    if dt_lower in ("string",):
        # Dimension tables typically have lower cardinality strings
        if classification == "dimension":
            return 0.05
        return 0.15

    if dt_lower in ("datetime",):
        return 0.08

    if dt_lower in ("decimal", "double"):
        return 0.2

    return 0.1


def _is_key_column(name: str) -> bool:
    """Check whether a column name suggests it is a key / identifier column."""
    lower = name.lower()
    return any(tok in lower for tok in ("sk", "key", "id"))


def _extract_column_references(expression: str) -> set[tuple[str, str]]:
    """Extract (table, column) pairs from a DAX expression.

    Handles both qualified references like 'Table Name'[Column] and
    unqualified [Column] (returned with table='').
    """
    refs: set[tuple[str, str]] = set()

    # Qualified: 'Table Name'[Column Name]
    for match in re.finditer(r"'([^']+)'\s*\[([^\]]+)\]", expression):
        refs.add((match.group(1), match.group(2)))

    # Unqualified: [Column Name] (not preceded by a quote-bracket pair already captured)
    for match in re.finditer(r"(?<!')\[([^\]]+)\]", expression):
        col = match.group(1)
        # Skip if this is part of a qualified ref already
        start = match.start()
        preceding = expression[:start].rstrip()
        if preceding.endswith("'"):
            continue
        refs.add(("", col))

    return refs


def _load_taxonomy(taxonomy_path: Path) -> dict:
    """Load model-taxonomy.json and build a lookup by table name."""
    if not taxonomy_path.is_file():
        print(f"  WARNING: Taxonomy file not found: {taxonomy_path}", file=sys.stderr)
        return {}
    data = _read_json(taxonomy_path)
    if not data:
        return {}
    return data


def _load_dax_complexity(dax_path: Path) -> dict:
    """Load dax-complexity.json."""
    if not dax_path.is_file():
        print(f"  WARNING: DAX complexity file not found: {dax_path}", file=sys.stderr)
        return {}
    data = _read_json(dax_path)
    if not data:
        return {}
    return data


def analyse_column_memory(
    model_path: Path,
    taxonomy_path: Path,
    dax_complexity_path: Path,
) -> dict:
    """Analyse column-level memory consumption across all tables."""

    tables_dir = model_path / "tables"
    if not tables_dir.is_dir():
        print(f"ERROR: No tables directory found at {tables_dir}", file=sys.stderr)
        sys.exit(1)

    # ── Step 1: Load taxonomy for row counts and classifications ──
    print("  Step 1: Loading taxonomy for row counts...", file=sys.stderr)
    taxonomy = _load_taxonomy(taxonomy_path)
    tax_tables: dict[str, dict] = {}
    for t in taxonomy.get("tables", []):
        tax_tables[t.get("name", "")] = t

    # ── Step 2: Load DAX complexity for measure references ──
    print("  Step 2: Loading DAX complexity for measure references...", file=sys.stderr)
    dax_data = _load_dax_complexity(dax_complexity_path)

    # Build a set of tables referenced by measures (from hotTables / measures)
    tables_referenced_by_measures: set[str] = set()
    for m in dax_data.get("measures", []):
        for t_name in m.get("referencedTables", []):
            tables_referenced_by_measures.add(t_name)

    # Build precise (table, column) reference set from measure expressions
    print("  Step 3: Scanning measure expressions for column references...", file=sys.stderr)
    column_refs: dict[tuple[str, str], int] = {}  # (table, column) -> reference count

    # Collect all measure expressions from the model
    all_measures: list[dict] = []
    for table_dir in sorted(tables_dir.iterdir()):
        if not table_dir.is_dir():
            continue
        measures = _collect_items(table_dir / "measures")
        for m in measures:
            expr = _expr_to_str(m.get("expression", ""))
            if expr:
                all_measures.append({"expression": expr})

    # Scan all measure expressions for column references
    for m in all_measures:
        expr = m["expression"]
        refs = _extract_column_references(expr)
        for table_col in refs:
            column_refs[table_col] = column_refs.get(table_col, 0) + 1

    # ── Step 4: Analyse each table and column ──
    print("  Step 4: Estimating column memory consumption...", file=sys.stderr)

    # Read model name from database.json
    db_json_path = model_path / "database.json"
    db_json = _read_json(db_json_path) if db_json_path.is_file() else {}
    db_json = db_json or {}
    model_name = db_json.get("model", {}).get("name", model_path.name)

    table_results: list[dict] = []
    all_removal_candidates: list[dict] = []
    total_columns = 0
    total_estimated_memory = 0.0

    for table_dir in sorted(tables_dir.iterdir()):
        if not table_dir.is_dir():
            continue

        # Read table metadata
        table_json_path = table_dir / f"{table_dir.name}.json"
        table_json = _read_json(table_json_path) if table_json_path.is_file() else {}
        table_json = table_json or {}
        table_name = table_json.get("name", table_dir.name)

        # Get taxonomy info
        tax_entry = tax_tables.get(table_name, {})
        classification = tax_entry.get("classification", "unknown")
        volumetry = tax_entry.get("volumetry", {})
        row_count = volumetry.get("rowCount", 0) or 0

        # Determine storage mode from taxonomy or partitions
        storage_mode = tax_entry.get("storageMode", "import")

        # Read columns
        columns_data = _collect_items(table_dir / "columns")
        if not columns_data:
            continue

        column_results: list[dict] = []
        table_memory = 0.0

        for col in columns_data:
            col_name = col.get("name", "")
            data_type = col.get("dataType", "string")
            is_hidden = col.get("isHidden", False)

            if not col_name:
                continue

            total_columns += 1

            # Calculate estimated memory
            base_size = TYPE_BASE_SIZES.get(data_type, DEFAULT_BASE_SIZE)
            comp_factor = _compression_factor(data_type, col_name, classification)
            estimated_bytes = row_count * base_size * comp_factor
            estimated_mb = round(estimated_bytes / (1024 * 1024), 1)

            # Check if referenced by measures
            # Qualified match
            qualified_count = column_refs.get((table_name, col_name), 0)
            # Unqualified match
            unqualified_count = column_refs.get(("", col_name), 0)
            measure_ref_count = qualified_count + unqualified_count
            is_referenced = measure_ref_count > 0

            # Also check if the table itself is referenced (looser check)
            table_is_referenced = table_name in tables_referenced_by_measures

            recommendation = None
            removal_reason = None

            # ── Step 5: Identify removal candidates ──

            # Rule 1: Hidden and unreferenced
            if is_hidden and not is_referenced:
                removal_reason = "Hidden, unreferenced by measures"
                recommendation = (
                    f"Hidden and unreferenced by any measure "
                    f"— candidate for removal. Estimated savings: {estimated_mb} MB."
                )

            # Rule 2: Unreferenced and not a key column
            elif not is_referenced and not _is_key_column(col_name):
                removal_reason = "Unreferenced by measures, not a key column"
                recommendation = (
                    f"Not referenced by any measure and not a key column "
                    f"— candidate for removal. Estimated savings: {estimated_mb} MB."
                )

            # Rule 3: Metadata table with large footprint
            elif table_name.startswith("@") and estimated_mb > 1.0:
                removal_reason = "Metadata table with large memory footprint"
                recommendation = (
                    f"Column in metadata table '{table_name}' consuming {estimated_mb} MB "
                    f"— review whether this metadata table is necessary."
                )

            column_results.append({
                "name": col_name,
                "dataType": data_type,
                "isHidden": is_hidden,
                "estimatedMemoryMB": estimated_mb,
                "isReferencedByMeasures": is_referenced,
                "measureReferenceCount": measure_ref_count,
                "recommendation": recommendation,
            })

            table_memory += estimated_mb

            if removal_reason:
                all_removal_candidates.append({
                    "table": table_name,
                    "column": col_name,
                    "dataType": data_type,
                    "isHidden": is_hidden,
                    "reason": removal_reason,
                    "estimatedSavingsMB": estimated_mb,
                })

        # Sort columns within table by memory descending
        column_results.sort(key=lambda c: c["estimatedMemoryMB"], reverse=True)
        table_memory = round(table_memory, 1)
        total_estimated_memory += table_memory

        removal_count = sum(
            1 for c in column_results if c["recommendation"] is not None
        )

        table_results.append({
            "name": table_name,
            "storageMode": storage_mode,
            "classification": classification,
            "rowCount": row_count,
            "columnCount": len(column_results),
            "estimatedMemoryMB": table_memory,
            "columns": column_results,
            "_removalCandidateCount": removal_count,
        })

    # Sort tables by estimated memory descending
    table_results.sort(key=lambda t: t["estimatedMemoryMB"], reverse=True)

    # Sort removal candidates by savings descending
    all_removal_candidates.sort(key=lambda c: c["estimatedSavingsMB"], reverse=True)

    total_estimated_memory = round(total_estimated_memory, 1)
    total_savings = round(
        sum(c["estimatedSavingsMB"] for c in all_removal_candidates), 1
    )
    savings_pct = round(
        (total_savings / total_estimated_memory * 100) if total_estimated_memory > 0 else 0.0,
        1,
    )

    # Top 10 tables by memory
    top_tables = [
        {
            "name": t["name"],
            "estimatedMemoryMB": t["estimatedMemoryMB"],
            "columnCount": t["columnCount"],
            "removalCandidates": t["_removalCandidateCount"],
        }
        for t in table_results[:10]
    ]

    # Clean up internal fields
    for t in table_results:
        del t["_removalCandidateCount"]

    # Count summary stats
    hidden_columns = sum(
        1 for t in table_results for c in t["columns"] if c["isHidden"]
    )
    unreferenced_columns = sum(
        1 for t in table_results for c in t["columns"] if not c["isReferencedByMeasures"]
    )

    result = {
        "analysisDate": date.today().isoformat(),
        "modelName": model_name,
        "totalEstimatedMemoryMB": total_estimated_memory,
        "totalColumns": total_columns,
        "columnsAnalysed": total_columns,
        "tables": table_results,
        "removalCandidates": all_removal_candidates,
        "totalPotentialSavingsMB": total_savings,
        "topTablesByMemory": top_tables,
        "summary": {
            "totalTables": len(table_results),
            "totalColumns": total_columns,
            "hiddenColumns": hidden_columns,
            "unreferencedColumns": unreferenced_columns,
            "removalCandidateCount": len(all_removal_candidates),
            "estimatedTotalMemoryMB": total_estimated_memory,
            "potentialSavingsMB": total_savings,
            "potentialSavingsPct": savings_pct,
        },
    }

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Estimate column-level memory consumption in Power BI semantic models"
    )
    parser.add_argument(
        "--model-path", required=True, type=Path,
        help="Path to the semantic model directory",
    )
    parser.add_argument(
        "--taxonomy", required=True, type=Path,
        help="Path to model-taxonomy.json (provides row counts and classifications)",
    )
    parser.add_argument(
        "--dax-complexity", required=True, type=Path,
        help="Path to dax-complexity.json (provides measure references)",
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output directory for column-memory-analysis.json",
    )
    args = parser.parse_args()

    model_path = args.model_path.resolve()
    taxonomy_path = args.taxonomy.resolve()
    dax_complexity_path = args.dax_complexity.resolve()
    output_dir = args.output.resolve()

    if not model_path.is_dir():
        print(f"ERROR: Model path does not exist: {model_path}", file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n=== Column Memory Analysis ===", file=sys.stderr)
    print(f"Model: {model_path}", file=sys.stderr)

    result = analyse_column_memory(model_path, taxonomy_path, dax_complexity_path)

    output_file = output_dir / "column-memory-analysis.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    # Print summary to stdout
    summary = result["summary"]
    print(f"\n=== Column Memory Analysis ===")
    print(f"Model: {result['modelName']}")
    print(f"Tables: {summary['totalTables']} | Columns: {summary['totalColumns']}")
    print(f"Hidden columns: {summary['hiddenColumns']} | Unreferenced columns: {summary['unreferencedColumns']}")
    print(f"Estimated total memory: {summary['estimatedTotalMemoryMB']:.1f} MB")
    print(f"Removal candidates: {summary['removalCandidateCount']}")
    print(f"Potential savings: {summary['potentialSavingsMB']:.1f} MB ({summary['potentialSavingsPct']:.1f}%)")

    if result["topTablesByMemory"]:
        print(f"\nTop tables by estimated memory:")
        for t in result["topTablesByMemory"][:5]:
            print(
                f"  {t['name']}: {t['estimatedMemoryMB']:.1f} MB "
                f"({t['columnCount']} cols, {t['removalCandidates']} removal candidates)"
            )

    if result["removalCandidates"]:
        print(f"\nTop removal candidates by savings:")
        for c in result["removalCandidates"][:10]:
            hidden_tag = " [hidden]" if c["isHidden"] else ""
            print(
                f"  {c['table']}.{c['column']}{hidden_tag}: "
                f"{c['estimatedSavingsMB']:.1f} MB — {c['reason']}"
            )

    print(f"\nWritten: {output_file}")


if __name__ == "__main__":
    main()
