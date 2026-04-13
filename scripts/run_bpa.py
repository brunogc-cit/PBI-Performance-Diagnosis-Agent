#!/usr/bin/env python3
"""
Power BI Best Practice Analyser (BPA)

Runs BPA checks on Power BI semantic model files in PBIR / Tabular Editor 2
JSON format.  Stdlib only — no third-party dependencies.

Usage:
    python3 run_bpa.py --model-path <path-to-model-dir> --output <output-dir>
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_json(path: Path) -> dict | list | None:
    """Read and parse a JSON file.  Returns None on failure."""
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  WARNING: Could not parse {path}: {exc}", file=sys.stderr)
        return None


def _expr_to_str(expr) -> str:
    """Normalise an expression that may be a string or list of strings."""
    if isinstance(expr, list):
        return "\n".join(str(e) for e in expr)
    return str(expr) if expr else ""


def _model_name_from_path(model_path: Path) -> str:
    """Derive a human-readable model name from the directory path."""
    name = model_path.name
    for suffix in (".SemanticModel", ".Dataset", ".database"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    return name or "Unknown Model"

# ---------------------------------------------------------------------------
# Model discovery
# ---------------------------------------------------------------------------

def resolve_definition_dir(model_path: Path) -> Path | None:
    """
    Given a user-supplied path, figure out where the actual definition lives.
    Handles:
      - model_path/definition/
      - model_path IS the definition dir (contains tables/)
      - model_path already contains tables/, relationships/ directly
    """
    # Explicit definition/ subdirectory
    candidate = model_path / "definition"
    if candidate.is_dir():
        return candidate

    # The path itself is the definition dir
    if (model_path / "tables").is_dir():
        return model_path

    # One level deeper — maybe user pointed at parent of *.SemanticModel
    for child in model_path.iterdir():
        if child.is_dir():
            deep = child / "definition"
            if deep.is_dir():
                return deep
            if (child / "tables").is_dir():
                return child

    return None

# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

class ModelData:
    """Container for everything we read from the model on disk."""

    def __init__(self):
        self.tables: list[dict] = []          # (table_name, table_json, columns, measures, partitions)
        self.relationships: list[dict] = []
        self.model_json: dict | None = None
        self.all_measure_expressions: list[str] = []  # flat list of every DAX expression


def _detect_format(defn_dir: Path) -> str:
    """Detect whether the model uses JSON (TE2) or TMDL format."""
    tables_dir = defn_dir / "tables"
    if tables_dir.is_dir():
        for item in tables_dir.iterdir():
            if item.suffix == ".tmdl":
                return "tmdl"
            if item.is_dir():
                return "json"
    if (defn_dir / "model.tmdl").is_file():
        return "tmdl"
    return "json"


# ---------------------------------------------------------------------------
# TMDL parser
# ---------------------------------------------------------------------------

def _parse_tmdl_table(filepath: Path) -> dict:
    """Parse a single .tmdl table file into structured data."""
    text = filepath.read_text(encoding="utf-8-sig")
    lines = text.splitlines()

    table_name = ""
    columns: list[dict] = []
    measures: list[dict] = []
    partitions: list[dict] = []

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Table name
        if stripped.startswith("table "):
            table_name = stripped[6:].strip().strip("'")

        # Column
        elif stripped.startswith("column "):
            col = _parse_tmdl_column(lines, i)
            columns.append(col)

        # Measure
        elif stripped.startswith("measure "):
            meas, skip = _parse_tmdl_measure(lines, i)
            measures.append(meas)
            i = skip
            continue

        # Partition
        elif stripped.startswith("partition "):
            part = _parse_tmdl_partition(lines, i)
            partitions.append(part)

        i += 1

    return {
        "name": table_name or filepath.stem,
        "json": {"name": table_name or filepath.stem},
        "columns": columns,
        "measures": measures,
        "partitions": partitions,
        "dir": filepath.parent,
    }


def _parse_tmdl_column(lines: list[str], start: int) -> dict:
    """Parse a column block from TMDL."""
    header = lines[start].strip()
    # column 'Name' or column Name
    name = header[7:].strip().strip("'")
    col: dict = {"name": name}

    i = start + 1
    while i < len(lines):
        stripped = lines[i].strip()
        if not stripped or stripped.startswith("column ") or stripped.startswith("measure ") or stripped.startswith("partition ") or stripped.startswith("table "):
            break
        if stripped.startswith("dataType:"):
            col["dataType"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("sourceColumn:"):
            col["sourceColumn"] = stripped.split(":", 1)[1].strip()
        elif stripped == "isHidden":
            col["isHidden"] = True
        elif stripped.startswith("formatString:"):
            col["formatString"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("summarizeBy:"):
            col["summarizeBy"] = stripped.split(":", 1)[1].strip()
        i += 1

    return col


def _parse_tmdl_measure(lines: list[str], start: int) -> tuple[dict, int]:
    """Parse a measure block from TMDL. Returns (measure_dict, next_line_index)."""
    header = lines[start].strip()
    # measure 'Name' = <expression> OR measure 'Name' = ```
    eq_pos = header.find("=")
    name_part = header[8:eq_pos].strip().strip("'") if eq_pos > 0 else header[8:].strip().strip("'")
    expr_start = header[eq_pos + 1:].strip() if eq_pos > 0 else ""

    expression_lines: list[str] = []
    in_multiline = False

    if expr_start == "```" or expr_start.startswith("```"):
        in_multiline = True
    elif expr_start:
        expression_lines.append(expr_start)

    meas: dict = {"name": name_part, "expression": ""}
    i = start + 1

    while i < len(lines):
        stripped = lines[i].strip()

        if in_multiline:
            if stripped == "```":
                in_multiline = False
                i += 1
                continue
            expression_lines.append(lines[i].strip())
            i += 1
            continue

        # Non-expression properties
        if not stripped or (not stripped.startswith("\t") and not stripped.startswith(" ")):
            # Check if this is a new top-level block
            if stripped.startswith("column ") or stripped.startswith("measure ") or stripped.startswith("partition ") or stripped.startswith("table ") or stripped.startswith("relationship "):
                break
            if not stripped:
                # Empty line might end the measure block — peek ahead
                if i + 1 < len(lines):
                    next_s = lines[i + 1].strip()
                    if next_s.startswith("column ") or next_s.startswith("measure ") or next_s.startswith("partition ") or next_s.startswith("table "):
                        break
                    # Check for annotation or changedProperty (still part of measure)
                    if next_s.startswith("annotation ") or next_s.startswith("changedProperty"):
                        i += 1
                        continue
                i += 1
                continue

        if stripped.startswith("formatString:"):
            meas["formatString"] = stripped.split(":", 1)[1].strip()
        elif stripped == "isHidden":
            meas["isHidden"] = True
        i += 1

    meas["expression"] = "\n".join(expression_lines).strip()
    return meas, i


def _parse_tmdl_partition(lines: list[str], start: int) -> dict:
    """Parse a partition block from TMDL."""
    header = lines[start].strip()
    # partition Name = m  OR  partition Name = entity ...
    parts = header.split("=", 1)
    name = parts[0].replace("partition", "", 1).strip().strip("'")
    part: dict = {"name": name, "mode": "import"}

    i = start + 1
    while i < len(lines):
        stripped = lines[i].strip()
        if not stripped:
            i += 1
            # Check if next line is still in the partition
            if i < len(lines):
                ns = lines[i].strip()
                if ns.startswith("column ") or ns.startswith("measure ") or ns.startswith("partition ") or ns.startswith("table ") or ns.startswith("annotation ") and not lines[i].startswith("\t\t"):
                    break
            continue
        if stripped.startswith("mode:"):
            part["mode"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("column ") or stripped.startswith("measure ") or stripped.startswith("table "):
            break
        i += 1

    return part


def _parse_tmdl_relationships(filepath: Path) -> list[dict]:
    """Parse relationships.tmdl file."""
    if not filepath.is_file():
        return []

    text = filepath.read_text(encoding="utf-8-sig")
    lines = text.splitlines()
    relationships: list[dict] = []
    current: dict | None = None

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("relationship "):
            if current:
                relationships.append(current)
            current = {"name": stripped.split(" ", 1)[1] if " " in stripped else ""}

        elif current is not None:
            if stripped.startswith("fromColumn:"):
                val = stripped.split(":", 1)[1].strip()
                # Format: Table.Column or 'Table'.Column
                if "." in val:
                    parts = val.split(".", 1)
                    current["fromTable"] = parts[0].strip("'")
                    current["fromColumn"] = parts[1].strip("'")
            elif stripped.startswith("toColumn:"):
                val = stripped.split(":", 1)[1].strip()
                if "." in val:
                    parts = val.split(".", 1)
                    current["toTable"] = parts[0].strip("'")
                    current["toColumn"] = parts[1].strip("'")
            elif stripped.startswith("crossFilteringBehavior:"):
                current["crossFilteringBehavior"] = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("fromCardinality:"):
                current["fromCardinality"] = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("toCardinality:"):
                current["toCardinality"] = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("isActive:"):
                current["isActive"] = stripped.split(":", 1)[1].strip().lower() == "true"

    if current:
        relationships.append(current)

    return relationships


def _parse_tmdl_model(filepath: Path) -> dict | None:
    """Parse model.tmdl for model-level settings."""
    if not filepath.is_file():
        return None
    text = filepath.read_text(encoding="utf-8-sig")
    result: dict = {"annotations": []}
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("annotation "):
            parts = stripped[11:].split("=", 1)
            if len(parts) == 2:
                result["annotations"].append({
                    "name": parts[0].strip(),
                    "value": parts[1].strip(),
                })
    return result


def collect_model(defn_dir: Path) -> ModelData:
    md = ModelData()
    fmt = _detect_format(defn_dir)

    if fmt == "tmdl":
        return _collect_model_tmdl(defn_dir)

    # ---- JSON (TE2) format ----
    model_json_path = defn_dir / "model.json"
    if model_json_path.is_file():
        md.model_json = _read_json(model_json_path)

    # Tables
    tables_dir = defn_dir / "tables"
    if tables_dir.is_dir():
        for table_dir in sorted(tables_dir.iterdir()):
            if not table_dir.is_dir():
                continue
            table_json_path = table_dir / "table.json"
            table_json = _read_json(table_json_path) if table_json_path.is_file() else {}
            table_json = table_json or {}
            table_name = table_json.get("name", table_dir.name)

            columns = _collect_items(table_dir / "columns")
            measures = _collect_items(table_dir / "measures")
            partitions = _collect_items(table_dir / "partitions")

            for m in measures:
                expr = _expr_to_str(m.get("expression", ""))
                if expr:
                    md.all_measure_expressions.append(expr)

            md.tables.append({
                "name": table_name,
                "json": table_json,
                "columns": columns,
                "measures": measures,
                "partitions": partitions,
                "dir": table_dir,
            })

    # Relationships
    rels_dir = defn_dir / "relationships"
    if rels_dir.is_dir():
        for rfile in sorted(rels_dir.iterdir()):
            if rfile.suffix == ".json":
                data = _read_json(rfile)
                if data:
                    md.relationships.append(data)

    return md


def _collect_model_tmdl(defn_dir: Path) -> ModelData:
    """Collect model data from TMDL format files."""
    md = ModelData()

    # model.tmdl
    md.model_json = _parse_tmdl_model(defn_dir / "model.tmdl")

    # Tables — each .tmdl file in tables/ directory
    tables_dir = defn_dir / "tables"
    if tables_dir.is_dir():
        for tmdl_file in sorted(tables_dir.iterdir()):
            if tmdl_file.suffix != ".tmdl":
                continue
            try:
                table_data = _parse_tmdl_table(tmdl_file)
                for m in table_data["measures"]:
                    expr = m.get("expression", "")
                    if expr:
                        md.all_measure_expressions.append(expr)
                md.tables.append(table_data)
            except Exception as exc:
                print(f"  WARNING: Could not parse {tmdl_file}: {exc}", file=sys.stderr)

    # Relationships
    md.relationships = _parse_tmdl_relationships(defn_dir / "relationships.tmdl")

    return md


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

# ---------------------------------------------------------------------------
# BPA Rules
# ---------------------------------------------------------------------------

Finding = dict  # alias for readability


def rule_avoid_floating_point(md: ModelData) -> list[Finding]:
    findings: list[Finding] = []
    for t in md.tables:
        for col in t["columns"]:
            if col.get("dataType") == "double":
                findings.append({
                    "rule": "AVOID_FLOATING_POINT_DATA_TYPES",
                    "severity": "High",
                    "category": "Performance",
                    "table": t["name"],
                    "object": col.get("name", "?"),
                    "objectType": "column",
                    "message": (
                        f"Column '{col.get('name')}' uses 'double' data type. "
                        "Use 'decimal' for monetary values to avoid rounding and improve compression."
                    ),
                    "fix": "Change dataType from 'double' to 'decimal' in the column definition.",
                })
    return findings


def rule_avoid_bidirectional(md: ModelData) -> list[Finding]:
    findings: list[Finding] = []
    for r in md.relationships:
        if r.get("crossFilteringBehavior") == "bothDirections":
            label = f"{r.get('fromTable')}.{r.get('fromColumn')} -> {r.get('toTable')}.{r.get('toColumn')}"
            findings.append({
                "rule": "AVOID_BIDIRECTIONAL_RELATIONSHIPS",
                "severity": "High",
                "category": "Performance",
                "table": r.get("fromTable", "?"),
                "object": label,
                "objectType": "relationship",
                "message": (
                    f"Relationship '{label}' uses bidirectional cross-filtering. "
                    "This creates ambiguous filter paths and can degrade query performance."
                ),
                "fix": "Change crossFilteringBehavior to 'oneDirection' unless bidirectional is explicitly required.",
            })
    return findings


def rule_dual_mode_tables(md: ModelData) -> list[Finding]:
    """Flag tables in Dual mode with source-group-aware recommendations.

    Analyses whether switching Dual → Import would create limited (cross-source)
    relationships.  Checks for RELATED() usage that would break.
    """
    rel_tables: set[str] = set()
    for r in md.relationships:
        rel_tables.add(r.get("fromTable", ""))
        rel_tables.add(r.get("toTable", ""))

    # Build DQ neighbour set for each table
    dq_tables: set[str] = set()
    table_modes: dict[str, str] = {}
    for t in md.tables:
        modes = {str(p.get("mode", "")).lower() for p in t["partitions"]}
        if "directquery" in modes:
            dq_tables.add(t["name"])
            table_modes[t["name"]] = "directQuery"
        elif "dual" in modes:
            table_modes[t["name"]] = "dual"
        else:
            table_modes[t["name"]] = "import"

    # Build adjacency: which tables is each table directly related to
    adj: dict[str, set[str]] = {}
    for r in md.relationships:
        ft = r.get("fromTable", "")
        tt = r.get("toTable", "")
        if ft and tt:
            adj.setdefault(ft, set()).add(tt)
            adj.setdefault(tt, set()).add(ft)

    # Check RELATED/RELATEDTABLE usage in measure expressions
    related_pat = re.compile(r"\bRELATED(?:TABLE)?\s*\(\s*'?([^'\[\)]+)'?\s*\[", re.IGNORECASE)
    tables_used_via_related: set[str] = set()
    for t in md.tables:
        for m in t["measures"]:
            expr = _expr_to_str(m.get("expression", ""))
            if not expr:
                continue
            for match in related_pat.finditer(expr):
                tables_used_via_related.add(match.group(1).strip())

    findings: list[Finding] = []
    for t in md.tables:
        for p in t["partitions"]:
            if str(p.get("mode", "")).lower() != "dual":
                continue

            is_hidden = t["json"].get("isHidden", False)
            has_rels = t["name"] in rel_tables
            neighbours = adj.get(t["name"], set())
            has_dq_neighbour = bool(neighbours & dq_tables)
            used_via_related = t["name"] in tables_used_via_related

            if is_hidden and not has_rels:
                findings.append({
                    "rule": "DUAL_MODE_TABLES",
                    "severity": "High",
                    "category": "Performance",
                    "table": t["name"],
                    "object": p.get("name", "?"),
                    "objectType": "partition",
                    "message": (
                        f"Table '{t['name']}' uses Dual storage mode but is hidden and "
                        "has no relationships. Dual mode causes double processing during refresh."
                    ),
                    "fix": "Switch partition mode to 'import' — no risk of limited relationships.",
                })
            elif has_dq_neighbour and used_via_related:
                findings.append({
                    "rule": "DUAL_MODE_TABLES",
                    "severity": "Medium",
                    "category": "Performance",
                    "table": t["name"],
                    "object": p.get("name", "?"),
                    "objectType": "partition",
                    "message": (
                        f"Table '{t['name']}' is Dual mode and directly related to DirectQuery "
                        f"tables ({', '.join(sorted(neighbours & dq_tables)[:3])}). "
                        "DAX measures use RELATED() on this table's columns. "
                        "WARNING: Switching to Import would create a limited relationship — "
                        "RELATED() calls would break and bidirectional filtering would stop working."
                    ),
                    "fix": (
                        "Keep Dual mode unless RELATED() dependencies are first refactored "
                        "to use TREATAS or LOOKUPVALUE. Evaluate each RELATED() reference before switching."
                    ),
                })
            elif has_dq_neighbour:
                findings.append({
                    "rule": "DUAL_MODE_TABLES",
                    "severity": "Medium",
                    "category": "Performance",
                    "table": t["name"],
                    "object": p.get("name", "?"),
                    "objectType": "partition",
                    "message": (
                        f"Table '{t['name']}' is Dual mode and directly related to DirectQuery "
                        f"tables. Switching to Import would create a limited (cross-source) "
                        "relationship with reduced functionality."
                    ),
                    "fix": (
                        "If the table is small and changes infrequently, switching to Import "
                        "with scheduled refresh may be beneficial — but verify that no measures "
                        "use RELATED() on this table's columns first."
                    ),
                })
            elif not has_rels:
                findings.append({
                    "rule": "DUAL_MODE_TABLES",
                    "severity": "High",
                    "category": "Performance",
                    "table": t["name"],
                    "object": p.get("name", "?"),
                    "objectType": "partition",
                    "message": (
                        f"Table '{t['name']}' uses Dual storage mode but has no relationships. "
                        "Dual mode causes double processing during refresh."
                    ),
                    "fix": "Switch partition mode to 'import' — no risk of limited relationships.",
                })
            break
    return findings


def rule_filter_all_antipattern(md: ModelData) -> list[Finding]:
    pat = re.compile(r"FILTER\s*\(\s*ALL\s*\(", re.IGNORECASE)
    return _scan_measures(md, "FILTER_ALL_ANTIPATTERN", "Medium", pat,
                          "uses FILTER(ALL(…)) anti-pattern. Use REMOVEFILTERS or direct predicate pushdown.",
                          "Replace FILTER(ALL(…)) with REMOVEFILTERS or CALCULATE with predicates.")


def rule_avoid_iferror(md: ModelData) -> list[Finding]:
    pat = re.compile(r"\b(IFERROR|ISERROR)\s*\(", re.IGNORECASE)
    return _scan_measures(md, "AVOID_IFERROR", "Medium", pat,
                          "uses IFERROR/ISERROR which evaluates the expression twice. Use DIVIDE or conditional patterns.",
                          "Replace IFERROR/ISERROR with DIVIDE() or IF(ISBLANK(…), …) patterns.")


def rule_wide_tables(md: ModelData) -> list[Finding]:
    findings: list[Finding] = []
    for t in md.tables:
        n = len(t["columns"])
        if n > 50:
            findings.append({
                "rule": "WIDE_TABLES",
                "severity": "Medium",
                "category": "Performance",
                "table": t["name"],
                "object": f"{n} columns",
                "objectType": "table",
                "message": f"Table '{t['name']}' has {n} columns (>50). May contain unused columns slowing refresh.",
                "fix": "Review columns for unused ones and remove them from the model.",
            })
    return findings


def rule_missing_format_string(md: ModelData) -> list[Finding]:
    findings: list[Finding] = []
    for t in md.tables:
        for m in t["measures"]:
            fs = m.get("formatString", "")
            is_hidden = m.get("isHidden", False)
            if not fs and not is_hidden:
                findings.append({
                    "rule": "MISSING_FORMAT_STRING",
                    "severity": "Medium",
                    "category": "Formatting",
                    "table": t["name"],
                    "object": m.get("name", "?"),
                    "objectType": "measure",
                    "message": f"Measure '{m.get('name')}' has no formatString. Best practice requires format strings on visible measures.",
                    "fix": "Add a formatString property (e.g. '#,##0.00' for numbers, '0.0%' for percentages).",
                })
    return findings


def rule_many_to_many(md: ModelData) -> list[Finding]:
    findings: list[Finding] = []
    for r in md.relationships:
        fc = r.get("fromCardinality", "")
        tc = r.get("toCardinality", "")
        if fc == "many" and tc == "many":
            label = f"{r.get('fromTable')}.{r.get('fromColumn')} <-> {r.get('toTable')}.{r.get('toColumn')}"
            findings.append({
                "rule": "MANY_TO_MANY_RELATIONSHIPS",
                "severity": "Medium",
                "category": "Performance",
                "table": r.get("fromTable", "?"),
                "object": label,
                "objectType": "relationship",
                "message": f"Relationship '{label}' is many-to-many. Performance concern for large tables.",
                "fix": "Consider introducing a bridge table or restructuring the data model.",
            })
    return findings


def rule_bare_division(md: ModelData) -> list[Finding]:
    # Heuristic: look for ] / or ) / patterns (column/function result divided without DIVIDE)
    pat = re.compile(r"[\]\)]\s*/\s*(?![\*/])")
    return _scan_measures(md, "BARE_DIVISION", "Low", pat,
                          "uses bare '/' division without DIVIDE(). Risk of division-by-zero errors.",
                          "Wrap division in DIVIDE(numerator, denominator) for safe division.")


def rule_auto_date_tables(md: ModelData) -> list[Finding]:
    findings: list[Finding] = []
    if md.model_json is None:
        return findings
    annotations = md.model_json.get("annotations", [])
    for ann in annotations:
        if ann.get("name") == "__PBI_TimeIntelligenceEnabled":
            val = str(ann.get("value", "1"))
            if val != "0":
                findings.append({
                    "rule": "AUTO_DATE_TABLES",
                    "severity": "Low",
                    "category": "Performance",
                    "table": "(model)",
                    "object": "__PBI_TimeIntelligenceEnabled",
                    "objectType": "annotation",
                    "message": "Auto date/time tables are enabled. This adds hidden date tables per date column, increasing model size.",
                    "fix": "Set __PBI_TimeIntelligenceEnabled to '0' and manage date tables explicitly.",
                })
            break
    return findings


def rule_dax_columns_not_fully_qualified(md: ModelData) -> list[Finding]:
    # Heuristic: [ColumnName] without 'Table' prefix — i.e. not preceded by a closing single quote
    # Pattern: something other than ' directly before [
    pat = re.compile(r"(?<!')\[([A-Za-z_][\w ]*)\]")
    findings: list[Finding] = []
    for t in md.tables:
        for m in t["measures"]:
            expr = _expr_to_str(m.get("expression", ""))
            if not expr:
                continue
            matches = pat.findall(expr)
            if matches:
                # Deduplicate
                unique = sorted(set(matches))
                findings.append({
                    "rule": "DAX_COLUMNS_NOT_FULLY_QUALIFIED",
                    "severity": "Low",
                    "category": "Best Practice",
                    "table": t["name"],
                    "object": m.get("name", "?"),
                    "objectType": "measure",
                    "message": (
                        f"Measure '{m.get('name')}' references columns without table qualifier: "
                        f"{', '.join('[' + c + ']' for c in unique[:5])}"
                        f"{'…' if len(unique) > 5 else ''}. "
                        "Fully qualify column references for clarity."
                    ),
                    "fix": "Prefix column references with 'TableName'[ColumnName].",
                })
    return findings


def rule_unused_columns_candidate(md: ModelData) -> list[Finding]:
    # Build a combined text of all measure expressions for searching
    all_expr = "\n".join(md.all_measure_expressions).lower()

    findings: list[Finding] = []
    for t in md.tables:
        for col in t["columns"]:
            col_name = col.get("name", "")
            if not col_name:
                continue
            # Check if the column name appears in any measure expression
            # Use case-insensitive search — column names in DAX are case-insensitive
            if col_name.lower() not in all_expr:
                findings.append({
                    "rule": "UNUSED_COLUMNS_CANDIDATE",
                    "severity": "Low",
                    "category": "Maintenance",
                    "table": t["name"],
                    "object": col_name,
                    "objectType": "column",
                    "message": (
                        f"Column '{col_name}' in table '{t['name']}' is not referenced in any measure expression. "
                        "Candidate for removal (verify it is not used in visuals or relationships)."
                    ),
                    "fix": "If not used in visuals or relationships, remove the column to reduce model size.",
                })
    return findings


def _scan_measures(md: ModelData, rule: str, severity: str,
                   pattern: re.Pattern, message_suffix: str, fix: str) -> list[Finding]:
    """Helper: scan all measures for a regex pattern."""
    findings: list[Finding] = []
    for t in md.tables:
        for m in t["measures"]:
            expr = _expr_to_str(m.get("expression", ""))
            if expr and pattern.search(expr):
                findings.append({
                    "rule": rule,
                    "severity": severity,
                    "category": "Performance",
                    "table": t["name"],
                    "object": m.get("name", "?"),
                    "objectType": "measure",
                    "message": f"Measure '{m.get('name')}' {message_suffix}",
                    "fix": fix,
                })
    return findings


# ---------------------------------------------------------------------------
# New BPA Rules (v3)
# ---------------------------------------------------------------------------

def rule_is_available_in_mdx(md: ModelData) -> list[Finding]:
    """Flag non-attribute columns with isAvailableInMdx = true (default).

    On large DirectQuery models this forces extra metadata/memory overhead.
    Only display/attribute columns (names, codes, descriptions) should have
    this enabled.
    """
    _ATTRIBUTE_HINTS = {"name", "description", "label", "title", "code", "display",
                        "category", "type", "status", "group", "text"}
    findings: list[Finding] = []
    for t in md.tables:
        modes = {str(p.get("mode", "")).lower() for p in t["partitions"]}
        is_dq = "directquery" in modes or "dual" in modes
        if not is_dq:
            continue
        for col in t["columns"]:
            mdx = col.get("isAvailableInMdx", True)
            if mdx is False:
                continue
            col_name = col.get("name", "")
            col_lower = col_name.lower()
            is_attribute = any(hint in col_lower for hint in _ATTRIBUTE_HINTS)
            is_key = col_lower.endswith("_sk") or col_lower.endswith("_id") or col_lower.endswith("_key") or col_lower.endswith("id")
            is_hidden = col.get("isHidden", False)
            if (is_key or is_hidden) and not is_attribute:
                findings.append({
                    "rule": "IS_AVAILABLE_IN_MDX",
                    "severity": "High",
                    "category": "Performance",
                    "table": t["name"],
                    "object": col_name,
                    "objectType": "column",
                    "message": (
                        f"Column '{col_name}' in DirectQuery table '{t['name']}' has "
                        "isAvailableInMdx = true but appears to be a key/hidden column, "
                        "not a display attribute. This forces extra metadata and memory structures."
                    ),
                    "fix": "Set isAvailableInMdx = false in Tabular Editor for all non-attribute columns.",
                })
    return findings


def rule_time_intel_on_dq(md: ModelData) -> list[Finding]:
    """Flag time intelligence functions in measures that reference DirectQuery tables.

    Functions like DATEADD, SAMEPERIODLASTYEAR, DATESINPERIOD generate multiple
    DQ queries on large fact tables, triggering full scans and severe latency.
    """
    _TIME_INTEL_PAT = re.compile(
        r"\b(DATEADD|SAMEPERIODLASTYEAR|DATESINPERIOD|DATESYTD|DATESMTD|DATESQTD|"
        r"PARALLELPERIOD|PREVIOUSMONTH|PREVIOUSYEAR|PREVIOUSQUARTER|"
        r"NEXTMONTH|NEXTYEAR|NEXTQUARTER|OPENINGBALANCEMONTH|CLOSINGBALANCEMONTH|"
        r"TOTALMTD|TOTALYTD|TOTALQTD)\s*\(",
        re.IGNORECASE,
    )
    dq_tables = {
        t["name"] for t in md.tables
        if any(str(p.get("mode", "")).lower() in ("directquery", "dual") for p in t["partitions"])
    }
    if not dq_tables:
        return []

    findings: list[Finding] = []
    for t in md.tables:
        for m in t["measures"]:
            expr = _expr_to_str(m.get("expression", ""))
            if not expr:
                continue
            ti_match = _TIME_INTEL_PAT.search(expr)
            if not ti_match:
                continue
            measure_host_is_dq = t["name"] in dq_tables
            refs_dq_table = any(
                dq_name in expr for dq_name in dq_tables
            )
            if measure_host_is_dq or refs_dq_table:
                func_name = ti_match.group(1).upper()
                findings.append({
                    "rule": "TIME_INTEL_ON_DQ",
                    "severity": "High",
                    "category": "Performance",
                    "table": t["name"],
                    "object": m.get("name", "?"),
                    "objectType": "measure",
                    "message": (
                        f"Measure '{m.get('name')}' uses {func_name}() on a DirectQuery-connected table. "
                        "Time intelligence functions generate multiple DQ queries and can trigger full table scans."
                    ),
                    "fix": (
                        "Rewrite to use pre-computed date bridge tables or "
                        "replace DAX time intelligence with joins to pre-shifted date keys."
                    ),
                })
    return findings


def rule_calculated_tables(md: ModelData) -> list[Finding]:
    """Flag calculated tables in the model.

    Calculated tables re-evaluate on every refresh, block query caching,
    and increase maintenance overhead.
    """
    findings: list[Finding] = []
    for t in md.tables:
        for p in t["partitions"]:
            source = p.get("source", {})
            if source.get("type", "").lower() == "calculated":
                findings.append({
                    "rule": "CALCULATED_TABLES",
                    "severity": "High",
                    "category": "Performance",
                    "table": t["name"],
                    "object": t["name"],
                    "objectType": "table",
                    "message": (
                        f"Table '{t['name']}' is a calculated table. "
                        "Calculated tables re-evaluate on every refresh, block query caching, "
                        "and increase model maintenance overhead."
                    ),
                    "fix": "Replace with a static import table or move the logic upstream into SQL/dbt.",
                })
                break
    return findings


def rule_snowflake_dq_chains(md: ModelData) -> list[Finding]:
    """Flag snowflake (multi-hop) dimension chains in DirectQuery.

    Chained joins (e.g., Fact -> DimA -> DimB) force multi-hop DQ queries
    and prevent aggregation push-down.
    """
    dq_or_dual = set()
    table_modes: dict[str, str] = {}
    for t in md.tables:
        modes = {str(p.get("mode", "")).lower() for p in t["partitions"]}
        if "directquery" in modes:
            table_modes[t["name"]] = "directQuery"
            dq_or_dual.add(t["name"])
        elif "dual" in modes:
            table_modes[t["name"]] = "dual"
            dq_or_dual.add(t["name"])
        else:
            table_modes[t["name"]] = "import"

    adj: dict[str, set[str]] = {}
    for r in md.relationships:
        ft = r.get("fromTable", "")
        tt = r.get("toTable", "")
        if ft and tt:
            adj.setdefault(ft, set()).add(tt)
            adj.setdefault(tt, set()).add(ft)

    findings: list[Finding] = []
    reported_chains: set[str] = set()
    for start in dq_or_dual:
        visited = {start}
        queue: list[tuple[str, list[str]]] = [(start, [start])]
        while queue:
            node, path = queue.pop(0)
            for neighbour in adj.get(node, set()):
                if neighbour in visited:
                    continue
                if neighbour not in dq_or_dual:
                    continue
                new_path = path + [neighbour]
                visited.add(neighbour)
                if len(new_path) >= 3:
                    chain_key = " → ".join(sorted(new_path[:3]))
                    if chain_key not in reported_chains:
                        reported_chains.add(chain_key)
                        chain_str = " → ".join(new_path[:4])
                        findings.append({
                            "rule": "SNOWFLAKE_DQ_CHAINS",
                            "severity": "Medium",
                            "category": "Performance",
                            "table": start,
                            "object": chain_str,
                            "objectType": "relationship",
                            "message": (
                                f"DirectQuery snowflake chain detected: {chain_str}. "
                                "Multi-hop joins force chained DQ queries and prevent aggregation push-down."
                            ),
                            "fix": (
                                "Flatten snowflake tables into the import layer or "
                                "ensure attributes are resolved in a single hop."
                            ),
                        })
                if len(new_path) < 5:
                    queue.append((neighbour, new_path))
    return findings


def rule_date_table_not_marked(md: ModelData) -> list[Finding]:
    """Flag tables that look like date tables but are not marked as such.

    Not marking date tables disables time intelligence optimisations
    and prevents efficient date filter caching.
    """
    _DATE_HINTS = {"date", "calendar", "dim_date", "dimdate", "fiscal", "time_period"}
    _DATE_COL_HINTS = {"date", "calendar_date", "fulldate", "datekey", "date_key"}

    findings: list[Finding] = []
    for t in md.tables:
        tname_lower = t["json"].get("name", t["name"]).lower().replace(" ", "_")
        looks_like_date = any(hint in tname_lower for hint in _DATE_HINTS)
        has_date_col = any(
            c.get("name", "").lower().replace(" ", "_") in _DATE_COL_HINTS
            or c.get("dataType", "").lower() in ("dateTime", "datetime")
            for c in t["columns"]
        )
        if not (looks_like_date or has_date_col):
            continue
        data_category = t["json"].get("dataCategory", "")
        is_marked = data_category.lower() == "time" if data_category else False
        cols_marked = any(
            c.get("dataCategory", "").lower() == "time" or
            c.get("isKey", False)
            for c in t["columns"]
        )
        if not is_marked and not cols_marked:
            findings.append({
                "rule": "DATE_TABLE_NOT_MARKED",
                "severity": "Medium",
                "category": "Performance",
                "table": t["name"],
                "object": t["name"],
                "objectType": "table",
                "message": (
                    f"Table '{t['name']}' appears to be a date/calendar table but is not "
                    "marked as a Date Table. This disables time intelligence optimisations "
                    "and efficient date filter caching."
                ),
                "fix": (
                    "Mark the table as a Date Table in Power BI Desktop or Tabular Editor "
                    "and ensure it has a contiguous date column."
                ),
            })
    return findings


def rule_redundant_columns_in_related(md: ModelData) -> list[Finding]:
    """Flag the same key column appearing on both sides of a relationship.

    Duplicated keys inflate DirectQuery payload and increase query cost/memory.
    """
    findings: list[Finding] = []
    table_columns: dict[str, set[str]] = {}
    for t in md.tables:
        table_columns[t["name"]] = {c.get("name", "") for c in t["columns"]}

    for r in md.relationships:
        from_table = r.get("fromTable", "")
        to_table = r.get("toTable", "")
        from_col = r.get("fromColumn", "")
        to_col = r.get("toColumn", "")
        if from_col == to_col:
            continue
        from_cols = table_columns.get(from_table, set())
        to_cols = table_columns.get(to_table, set())
        dupes = from_cols & to_cols
        rel_keys = {from_col, to_col}
        redundant = dupes - rel_keys
        for col_name in sorted(redundant):
            if col_name.lower().endswith("_sk") or col_name.lower().endswith("_id") or col_name.lower().endswith("_key"):
                findings.append({
                    "rule": "REDUNDANT_COLUMNS_IN_RELATED",
                    "severity": "Medium",
                    "category": "Performance",
                    "table": from_table,
                    "object": f"{col_name} (in both {from_table} and {to_table})",
                    "objectType": "column",
                    "message": (
                        f"Key column '{col_name}' exists in both '{from_table}' and '{to_table}' "
                        "which are related. This duplicated key inflates DQ payload and increases query cost."
                    ),
                    "fix": (
                        "Remove the redundant column from the fact/child table and "
                        "use the dimension table as the single source of truth."
                    ),
                })
    return findings


def rule_excessive_calculated_columns(md: ModelData) -> list[Finding]:
    """Flag tables with more than 5 calculated columns.

    Calculated columns are computed at refresh and stored in memory,
    increasing VertiPaq footprint.
    """
    findings: list[Finding] = []
    for t in md.tables:
        calc_cols = [
            c for c in t["columns"]
            if c.get("type", "").lower() == "calculated"
            or c.get("expression")  # columns with expressions are calculated
        ]
        if len(calc_cols) > 5:
            col_names = [c.get("name", "?") for c in calc_cols[:5]]
            findings.append({
                "rule": "EXCESSIVE_CALCULATED_COLUMNS",
                "severity": "Low",
                "category": "Performance",
                "table": t["name"],
                "object": f"{len(calc_cols)} calculated columns",
                "objectType": "table",
                "message": (
                    f"Table '{t['name']}' has {len(calc_cols)} calculated columns "
                    f"(e.g. {', '.join(col_names)}). Calculated columns are computed at refresh "
                    "and stored in memory, increasing VertiPaq footprint."
                ),
                "fix": "Move logic to SQL/dbt upstream or convert to measures where appropriate.",
            })
    return findings


def rule_m_folding_blockers(md: ModelData) -> list[Finding]:
    """Flag M/Power Query expressions that block query folding.

    Complex M steps prevent the PBI engine from pushing transformations
    to the source, forcing full data pulls.
    """
    _BLOCKING_FUNS = [
        "Table.AddColumn", "Table.TransformColumns", "Table.TransformColumnTypes",
        "Table.FillDown", "Table.FillUp", "Table.Sort", "Table.Buffer",
        "List.Generate", "List.Accumulate", "Table.Group",
        "Table.Pivot", "Table.Unpivot", "Table.Combine",
    ]
    _BLOCKING_PAT = re.compile(
        r"\b(" + "|".join(re.escape(f) for f in _BLOCKING_FUNS) + r")\b",
        re.IGNORECASE,
    )

    findings: list[Finding] = []
    for t in md.tables:
        for p in t["partitions"]:
            source = p.get("source", {})
            expr = _expr_to_str(source.get("expression", ""))
            if not expr:
                continue
            matches = _BLOCKING_PAT.findall(expr)
            if matches:
                unique_fns = sorted(set(m for m in matches))
                findings.append({
                    "rule": "M_FOLDING_BLOCKERS",
                    "severity": "Low",
                    "category": "Performance",
                    "table": t["name"],
                    "object": ", ".join(unique_fns),
                    "objectType": "partition",
                    "message": (
                        f"Table '{t['name']}' uses M functions that block query folding: "
                        f"{', '.join(unique_fns)}. This prevents pushing transformations "
                        "to the data source, forcing full data pulls."
                    ),
                    "fix": "Rewrite transformations to be foldable or push logic upstream to SQL/dbt.",
                })
    return findings

# ---------------------------------------------------------------------------
# Statistics helpers
# ---------------------------------------------------------------------------

def _compute_table_stats(md: ModelData) -> dict:
    total = len(md.tables)
    import_mode = 0
    dual_mode = 0
    dq_mode = 0
    calculated = 0

    for t in md.tables:
        modes = {str(p.get("mode", "")).lower() for p in t["partitions"]}
        if "dual" in modes:
            dual_mode += 1
        elif "directquery" in modes:
            dq_mode += 1
        elif any(p.get("source", {}).get("type", "") == "calculated" for p in t["partitions"]):
            calculated += 1
        else:
            # Default to import
            import_mode += 1

    return {
        "total": total,
        "importMode": import_mode,
        "dualMode": dual_mode,
        "directQueryMode": dq_mode,
        "calculated": calculated,
    }


def _compute_measure_stats(md: ModelData) -> dict:
    total = 0
    with_fs = 0
    without_fs = 0
    for t in md.tables:
        for m in t["measures"]:
            total += 1
            if m.get("formatString"):
                with_fs += 1
            else:
                without_fs += 1
    return {"total": total, "withFormatString": with_fs, "withoutFormatString": without_fs}


def _compute_rel_stats(md: ModelData) -> dict:
    total = len(md.relationships)
    bidi = sum(1 for r in md.relationships if r.get("crossFilteringBehavior") == "bothDirections")
    m2m = sum(1 for r in md.relationships
              if r.get("fromCardinality") == "many" and r.get("toCardinality") == "many")
    return {"total": total, "bidirectional": bidi, "manyToMany": m2m}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# Performance impact metadata per rule — classifies HOW each rule
# affects Power BI performance (latency, cost, quality, memory).
RULE_PERFORMANCE_IMPACT: dict[str, dict] = {
    "AVOID_FLOATING_POINT_DATA_TYPES": {
        "impact": "memory",
        "description": "Higher memory consumption and worse VertiPaq compression. No direct query latency impact in DirectQuery mode.",
    },
    "AVOID_BIDIRECTIONAL_RELATIONSHIPS": {
        "impact": "latency",
        "description": "Generates additional SQL round-trips for bidirectional filter propagation. Can double the number of queries per visual.",
    },
    "DUAL_MODE_TABLES": {
        "impact": "latency",
        "description": "May trigger unnecessary DirectQuery when Import would suffice. Dual tables are processed twice during refresh.",
    },
    "FILTER_ALL_ANTIPATTERN": {
        "impact": "latency",
        "description": "Forces full table scan in DirectQuery mode. Each FILTER(ALL) adds an unfiltered subquery to the generated SQL.",
    },
    "AVOID_IFERROR": {
        "impact": "latency",
        "description": "IFERROR evaluates the expression twice. Use DIVIDE() or IF(ISBLANK()) for safe arithmetic.",
    },
    "WIDE_TABLES": {
        "impact": "cost",
        "description": "More columns increases bytes read per query. Primarily a cost issue; latency impact depends on whether I/O is the bottleneck.",
    },
    "MISSING_FORMAT_STRING": {
        "impact": "quality",
        "description": "No performance impact. Missing format strings affect display consistency and user experience.",
    },
    "MANY_TO_MANY_RELATIONSHIPS": {
        "impact": "latency",
        "description": "Many-to-many relationships generate complex SQL with intermediate materialisation. Significant performance concern for large tables.",
    },
    "BARE_DIVISION": {
        "impact": "quality",
        "description": "Risk of division-by-zero errors. No direct performance impact, but errors can cause visual rendering failures.",
    },
    "AUTO_DATE_TABLES": {
        "impact": "memory",
        "description": "Adds hidden auto date tables per date column, increasing model size and refresh time. No DirectQuery latency impact.",
    },
    "DAX_COLUMNS_NOT_FULLY_QUALIFIED": {
        "impact": "quality",
        "description": "Readability and maintenance issue. No performance impact but increases risk of errors when columns are renamed.",
    },
    "UNUSED_COLUMNS_CANDIDATE": {
        "impact": "cost",
        "description": "Hidden/unused columns are still included in DirectQuery SQL. Increases read bytes and cost per query.",
    },
    "IS_AVAILABLE_IN_MDX": {
        "impact": "memory",
        "description": "MDX exposure on non-attribute columns forces extra metadata and memory structures. On large DirectQuery models this increases model load time and query overhead.",
    },
    "TIME_INTEL_ON_DQ": {
        "impact": "latency",
        "description": "Time intelligence functions on DirectQuery tables generate multiple DQ queries per evaluation. On large fact tables this triggers full scans and severe latency.",
    },
    "CALCULATED_TABLES": {
        "impact": "latency",
        "description": "Calculated tables re-evaluate on every refresh, block query caching, and increase model maintenance overhead.",
    },
    "SNOWFLAKE_DQ_CHAINS": {
        "impact": "latency",
        "description": "Multi-hop DirectQuery joins prevent aggregation push-down and force chained subqueries. Each hop adds a nested SQL subquery.",
    },
    "DATE_TABLE_NOT_MARKED": {
        "impact": "latency",
        "description": "Unmarked date tables disable time intelligence optimisations and prevent efficient date filter caching in the storage engine.",
    },
    "REDUNDANT_COLUMNS_IN_RELATED": {
        "impact": "cost",
        "description": "Duplicated key columns across related tables inflate DirectQuery payload. The same data is read and transferred multiple times per query.",
    },
    "EXCESSIVE_CALCULATED_COLUMNS": {
        "impact": "memory",
        "description": "Calculated columns are computed at refresh and stored in memory, increasing VertiPaq footprint and refresh time.",
    },
    "M_FOLDING_BLOCKERS": {
        "impact": "latency",
        "description": "M functions that block query folding force full data pulls from the source instead of pushing transformations server-side.",
    },
}

ALL_RULES = [
    ("AVOID_FLOATING_POINT_DATA_TYPES", rule_avoid_floating_point),
    ("AVOID_BIDIRECTIONAL_RELATIONSHIPS", rule_avoid_bidirectional),
    ("DUAL_MODE_TABLES", rule_dual_mode_tables),
    ("FILTER_ALL_ANTIPATTERN", rule_filter_all_antipattern),
    ("AVOID_IFERROR", rule_avoid_iferror),
    ("WIDE_TABLES", rule_wide_tables),
    ("MISSING_FORMAT_STRING", rule_missing_format_string),
    ("MANY_TO_MANY_RELATIONSHIPS", rule_many_to_many),
    ("BARE_DIVISION", rule_bare_division),
    ("AUTO_DATE_TABLES", rule_auto_date_tables),
    ("DAX_COLUMNS_NOT_FULLY_QUALIFIED", rule_dax_columns_not_fully_qualified),
    ("UNUSED_COLUMNS_CANDIDATE", rule_unused_columns_candidate),
    ("IS_AVAILABLE_IN_MDX", rule_is_available_in_mdx),
    ("TIME_INTEL_ON_DQ", rule_time_intel_on_dq),
    ("CALCULATED_TABLES", rule_calculated_tables),
    ("SNOWFLAKE_DQ_CHAINS", rule_snowflake_dq_chains),
    ("DATE_TABLE_NOT_MARKED", rule_date_table_not_marked),
    ("REDUNDANT_COLUMNS_IN_RELATED", rule_redundant_columns_in_related),
    ("EXCESSIVE_CALCULATED_COLUMNS", rule_excessive_calculated_columns),
    ("M_FOLDING_BLOCKERS", rule_m_folding_blockers),
]


def run_bpa(model_path: Path, output_dir: Path) -> int:
    """Run all BPA rules and write results.  Returns exit code."""

    defn_dir = resolve_definition_dir(model_path)
    if defn_dir is None:
        print(
            f"ERROR: Could not find model definition in '{model_path}'.\n"
            "Expected structure: <model-dir>/definition/tables/  OR  <model-dir>/tables/\n"
            "Please pass --model-path pointing to the *.SemanticModel directory or the definition/ directory.",
            file=sys.stderr,
        )
        return 1

    model_name = _model_name_from_path(model_path)
    print(f"\n=== Power BI Best Practice Analyser ===")
    print(f"Scanning: {model_path}")

    md = collect_model(defn_dir)

    table_stats = _compute_table_stats(md)
    measure_stats = _compute_measure_stats(md)
    rel_stats = _compute_rel_stats(md)

    print(f"Model: {model_name}")
    print(
        f"Tables: {table_stats['total']} "
        f"({table_stats['importMode']} Import, {table_stats['dualMode']} Dual, "
        f"{table_stats['directQueryMode']} DQ, {table_stats['calculated']} Calculated)"
    )
    print(f"Measures: {measure_stats['total']} | Relationships: {rel_stats['total']}")

    # Run rules
    all_findings: list[Finding] = []
    rule_results: list[dict] = []
    passing_rules: list[str] = []

    for rule_name, rule_fn in ALL_RULES:
        findings = rule_fn(md)
        count = len(findings)
        all_findings.extend(findings)
        impact_meta = RULE_PERFORMANCE_IMPACT.get(rule_name, {})
        if count > 0:
            rule_results.append({
                "rule": rule_name,
                "status": "FAIL",
                "count": count,
                "performanceImpact": impact_meta.get("impact", "unknown"),
                "impactDescription": impact_meta.get("description", ""),
            })
        else:
            passing_rules.append(rule_name)

    # Summary counts
    high = sum(1 for f in all_findings if f["severity"] == "High")
    medium = sum(1 for f in all_findings if f["severity"] == "Medium")
    low = sum(1 for f in all_findings if f["severity"] == "Low")

    summary = {
        "high": high,
        "medium": medium,
        "low": low,
        "totalFindings": len(all_findings),
        "rulesChecked": len(ALL_RULES),
        "rulesPassed": len(passing_rules),
    }

    # Print summary
    print(f"\n--- Findings by Severity ---")
    print(f"  HIGH:   {high}")
    print(f"  MEDIUM: {medium}")
    print(f"  LOW:    {low}")

    print(f"\n--- Rule Results ---")
    for r in rule_results:
        status = r["status"]
        marker = "FAIL" if status == "FAIL" else "PASS"
        # Determine object type label for count
        count = r["count"]
        print(f"  {marker}  {r['rule']:<45s} {count}")

    # Build output
    result = {
        "version": "2.0.0",
        "analysedAt": datetime.now(timezone.utc).isoformat(),
        "modelPath": str(model_path),
        "modelName": model_name,
        "tables": table_stats,
        "measures": measure_stats,
        "relationships": rel_stats,
        "findings": all_findings,
        "summary": summary,
        "ruleResults": rule_results,
        "passingRules": passing_rules,
    }

    # Write output
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "bpa-results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\nResults written to: {output_file}")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Power BI Best Practice Analyser — checks PBIR / TE2 JSON semantic models"
    )
    parser.add_argument(
        "--model-path",
        required=True,
        type=Path,
        help="Path to the semantic model directory (contains definition/ or tables/ directly)",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Output directory for bpa-results.json",
    )
    args = parser.parse_args()

    model_path = args.model_path.resolve()
    output_dir = args.output.resolve()

    if not model_path.is_dir():
        print(f"ERROR: Model path does not exist or is not a directory: {model_path}", file=sys.stderr)
        sys.exit(1)

    sys.exit(run_bpa(model_path, output_dir))


if __name__ == "__main__":
    main()
