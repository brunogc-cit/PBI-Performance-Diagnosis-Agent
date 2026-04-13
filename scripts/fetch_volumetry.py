#!/usr/bin/env python3
"""
Fetch table volumetry (row counts, sizes) for all Databricks tables
referenced by a PBI semantic model.

Two modes:
  1. --sql-output  : Generates the SQL queries to run in Databricks (manual mode)
  2. --csv-file    : Reads pre-collected data from a CSV file (columns: source_table, row_count)

Both modes produce output/databricks-profile.json that can be fed to:
  python3 analyse_semantic_model.py --volumetry-file output/databricks-profile.json

Usage:
    # Generate SQL to run manually in Databricks
    python3 fetch_volumetry.py --taxonomy output/model-taxonomy.json --output output/ --sql-output

    # Import from CSV (e.g., exported from a spreadsheet)
    python3 fetch_volumetry.py --taxonomy output/model-taxonomy.json --output output/ --csv-file data.csv

    # Merge an existing databricks-profile.json with a CSV overlay
    python3 fetch_volumetry.py --taxonomy output/model-taxonomy.json --output output/ --csv-file data.csv --merge
"""

import argparse
import csv
import json
import sys
from pathlib import Path


def _read_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _extract_tables(taxonomy: dict) -> list[dict]:
    """Extract unique Databricks source tables from model-taxonomy.json."""
    seen = set()
    tables = []
    for t in taxonomy.get("tables", []):
        src = t.get("sourceTable")
        if not src:
            continue
        full_name = f"{t.get('sourceCatalog', '')}.{t.get('sourceDatabase', '')}.{src}"
        if full_name.lower() in seen:
            continue
        seen.add(full_name.lower())
        tables.append({
            "pbiName": t["name"],
            "fullName": full_name,
            "catalog": t.get("sourceCatalog", ""),
            "schema": t.get("sourceDatabase", ""),
            "table": src,
        })
    return tables


def generate_sql(tables: list[dict]) -> str:
    """Generate SQL to fetch row counts and sizes from Databricks."""
    lines = [
        "-- Volumetry queries for PBI Performance Diagnosis Agent",
        "-- Run each DESCRIBE DETAIL and collect results into databricks-profile.json",
        "-- Alternatively, run the UNION ALL query at the bottom for a single result set.",
        "",
    ]

    # Individual DESCRIBE DETAIL queries
    for t in tables:
        lines.append(f"-- {t['pbiName']}")
        lines.append(f"DESCRIBE DETAIL {t['fullName']};")
        lines.append("")

    # Combined count query (for row counts only, works for views too)
    lines.append("-- Combined row count query (works for views and tables):")
    union_parts = []
    for t in tables:
        union_parts.append(
            f"SELECT '{t['fullName']}' AS full_name, COUNT(*) AS row_count FROM {t['fullName']}"
        )
    lines.append("\n  UNION ALL\n".join(union_parts) + ";")

    return "\n".join(lines)


def from_csv(csv_path: Path, tables: list[dict]) -> list[dict]:
    """Read volumetry from a CSV file.

    Expected columns (flexible matching):
      - source_table / fullName / table_name: Databricks table identifier
      - row_count / rowCount / rows: Row count
      - size_gb / sizeGB / size (optional): Size in GB

    The CSV source_table is matched against the taxonomy's full Databricks name.
    """
    # Build lookup: various forms of the Databricks name → table entry
    dbx_lookup: dict[str, dict] = {}
    for t in tables:
        dbx_lookup[t["fullName"].lower()] = t
        # Also allow matching by just the table name (last part)
        dbx_lookup[t["table"].lower()] = t

    result = []
    matched = set()

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Find the source table column (flexible)
            src = (
                row.get("source_table", "")
                or row.get("fullName", "")
                or row.get("full_name", "")
                or ""
            ).strip()
            if not src:
                continue

            # Match against taxonomy
            entry = dbx_lookup.get(src.lower())
            if not entry:
                # Try partial match (just the table part after last dot)
                parts = src.split(".")
                entry = dbx_lookup.get(parts[-1].lower()) if parts else None
            if not entry:
                continue
            if entry["fullName"].lower() in matched:
                continue
            matched.add(entry["fullName"].lower())

            # Parse row count
            rc_str = (
                row.get("row_count", "")
                or row.get("rowCount", "")
                or row.get("rows", "")
                or ""
            ).strip()
            row_count = None
            if rc_str:
                try:
                    row_count = int(float(rc_str))
                except ValueError:
                    pass

            # Parse size
            sz_str = (
                row.get("size_gb", "")
                or row.get("sizeGB", "")
                or row.get("size", "")
                or ""
            ).strip()
            size_gb = None
            if sz_str:
                try:
                    size_gb = float(sz_str)
                except ValueError:
                    pass

            result.append({
                "fullName": entry["fullName"],
                "rowCount": row_count,
                "sizeGB": size_gb,
            })

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Fetch/generate Databricks table volumetry for PBI models"
    )
    parser.add_argument(
        "--taxonomy", required=True, type=Path,
        help="Path to model-taxonomy.json",
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output directory for databricks-profile.json",
    )
    parser.add_argument(
        "--sql-output", action="store_true",
        help="Print SQL queries to stdout (manual mode)",
    )
    parser.add_argument(
        "--csv-file", type=Path, default=None,
        help="CSV file with row counts (columns: source_table, row_count, [size_gb])",
    )
    parser.add_argument(
        "--merge", action="store_true",
        help="Merge with existing databricks-profile.json instead of overwriting",
    )
    args = parser.parse_args()

    taxonomy = _read_json(args.taxonomy.resolve())
    if not taxonomy:
        print(f"ERROR: Cannot read taxonomy: {args.taxonomy}", file=sys.stderr)
        sys.exit(1)

    tables = _extract_tables(taxonomy)
    if not tables:
        print("ERROR: No Databricks source tables found in taxonomy", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(tables)} unique Databricks source tables in taxonomy")

    if args.sql_output:
        print(generate_sql(tables))
        return

    if args.csv_file:
        csv_path = args.csv_file.resolve()
        if not csv_path.is_file():
            print(f"ERROR: CSV file not found: {csv_path}", file=sys.stderr)
            sys.exit(1)

        vol_entries = from_csv(csv_path, tables)
        print(f"Matched {len(vol_entries)} of {len(tables)} tables from CSV")
    else:
        print("ERROR: Specify --sql-output or --csv-file", file=sys.stderr)
        sys.exit(1)

    # Build or merge databricks-profile.json
    output_dir = args.output.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "databricks-profile.json"

    existing: dict = {}
    if args.merge and output_file.is_file():
        existing = _read_json(output_file) or {}

    # Merge: existing entries + new entries (new wins on conflict)
    existing_tables = {e["fullName"].lower(): e for e in existing.get("tables", [])}
    for entry in vol_entries:
        existing_tables[entry["fullName"].lower()] = entry
    merged_tables = list(existing_tables.values())

    # Preserve tableQueryStats if merging
    profile = {
        "tables": merged_tables,
    }
    if existing.get("tableQueryStats"):
        profile["tableQueryStats"] = existing["tableQueryStats"]

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(profile, f, indent=2, ensure_ascii=False)

    print(f"\nWritten: {output_file} ({len(merged_tables)} tables)")

    # Summary
    with_rows = sum(1 for t in merged_tables if t.get("rowCount"))
    with_size = sum(1 for t in merged_tables if t.get("sizeGB"))
    missing = len(tables) - len(merged_tables)
    print(f"  With row counts: {with_rows}")
    print(f"  With size data:  {with_size}")
    if missing > 0:
        matched_names = {e["fullName"].lower() for e in merged_tables}
        print(f"  Missing ({missing}):")
        for t in tables:
            if t["fullName"].lower() not in matched_names:
                print(f"    - {t['fullName']} ({t['pbiName']})")


if __name__ == "__main__":
    main()
