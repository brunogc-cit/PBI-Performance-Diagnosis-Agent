#!/usr/bin/env python3
"""
Power BI DAX Audit

Scans all DAX measures in a Power BI semantic model (JSON/TE2 or TMDL format)
and flags anti-patterns.  Stdlib only -- no third-party dependencies.

Usage:
    python3 audit_dax.py --model-path <path-to-model-dir> --output <output-dir>
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

    # One level deeper -- maybe user pointed at parent of *.SemanticModel
    for child in model_path.iterdir():
        if child.is_dir():
            deep = child / "definition"
            if deep.is_dir():
                return deep
            if (child / "tables").is_dir():
                return child

    return None


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

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
# Measure data structure
# ---------------------------------------------------------------------------

class MeasureInfo:
    """Container for a single measure extracted from the model."""

    __slots__ = ("name", "table", "expression", "format_string", "is_hidden")

    def __init__(
        self,
        name: str,
        table: str,
        expression: str,
        format_string: str = "",
        is_hidden: bool = False,
    ):
        self.name = name
        self.table = table
        self.expression = expression
        self.format_string = format_string
        self.is_hidden = is_hidden


# ---------------------------------------------------------------------------
# TMDL parser (reuses patterns from run_bpa.py)
# ---------------------------------------------------------------------------

def _parse_tmdl_measure(lines: list[str], start: int) -> tuple[dict, int]:
    """Parse a measure block from TMDL.  Returns (measure_dict, next_line_index)."""
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

    meas: dict = {"name": name_part, "expression": "", "formatString": "", "isHidden": False}
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
                # Empty line might end the measure block -- peek ahead
                if i + 1 < len(lines):
                    next_s = lines[i + 1].strip()
                    if next_s.startswith("column ") or next_s.startswith("measure ") or next_s.startswith("partition ") or next_s.startswith("table "):
                        break
                    if next_s.startswith("annotation ") or next_s.startswith("changedProperty"):
                        i += 1
                        continue
                i += 1
                continue

        if stripped.startswith("formatString:"):
            meas["formatString"] = stripped.split(":", 1)[1].strip()
        elif stripped == "isHidden":
            meas["isHidden"] = True
        elif stripped.startswith("displayFolder:"):
            pass  # skip, not needed
        elif stripped.startswith("lineageTag:"):
            pass  # skip
        i += 1

    meas["expression"] = "\n".join(expression_lines).strip()
    return meas, i


def _parse_tmdl_table_measures(filepath: Path) -> list[MeasureInfo]:
    """Parse all measures from a single .tmdl table file."""
    try:
        text = filepath.read_text(encoding="utf-8-sig")
    except OSError as exc:
        print(f"  WARNING: Could not read {filepath}: {exc}", file=sys.stderr)
        return []

    lines = text.splitlines()
    table_name = ""
    measures: list[MeasureInfo] = []

    i = 0
    while i < len(lines):
        stripped = lines[i].strip()

        if stripped.startswith("table "):
            table_name = stripped[6:].strip().strip("'")

        elif stripped.startswith("measure "):
            try:
                meas, skip = _parse_tmdl_measure(lines, i)
                measures.append(MeasureInfo(
                    name=meas.get("name", ""),
                    table=table_name or filepath.stem,
                    expression=meas.get("expression", ""),
                    format_string=meas.get("formatString", ""),
                    is_hidden=meas.get("isHidden", False),
                ))
                i = skip
                continue
            except Exception as exc:
                print(f"  WARNING: Could not parse measure at line {i} in {filepath}: {exc}", file=sys.stderr)

        i += 1

    return measures


# ---------------------------------------------------------------------------
# JSON (TE2) parser
# ---------------------------------------------------------------------------

def _collect_json_measures(defn_dir: Path) -> list[MeasureInfo]:
    """Collect measures from JSON (TE2) format model."""
    measures: list[MeasureInfo] = []
    tables_dir = defn_dir / "tables"
    if not tables_dir.is_dir():
        return measures

    for table_dir in sorted(tables_dir.iterdir()):
        if not table_dir.is_dir():
            continue

        # Determine table name
        table_json_path = table_dir / "table.json"
        table_json = _read_json(table_json_path) if table_json_path.is_file() else {}
        table_json = table_json or {}
        table_name = table_json.get("name", table_dir.name)

        # Measures directory
        measures_dir = table_dir / "measures"
        if not measures_dir.is_dir():
            continue

        for mfile in sorted(measures_dir.iterdir()):
            if mfile.suffix != ".json":
                continue
            data = _read_json(mfile)
            if not data or not isinstance(data, dict):
                continue
            measures.append(MeasureInfo(
                name=data.get("name", mfile.stem),
                table=table_name,
                expression=_expr_to_str(data.get("expression", "")),
                format_string=data.get("formatString", ""),
                is_hidden=data.get("isHidden", False),
            ))

    return measures


# ---------------------------------------------------------------------------
# Collect all measures
# ---------------------------------------------------------------------------

def collect_measures(defn_dir: Path, fmt: str) -> list[MeasureInfo]:
    """Collect all measures from the model, regardless of format."""
    if fmt == "tmdl":
        all_measures: list[MeasureInfo] = []
        tables_dir = defn_dir / "tables"
        if tables_dir.is_dir():
            for tmdl_file in sorted(tables_dir.iterdir()):
                if tmdl_file.suffix != ".tmdl":
                    continue
                try:
                    all_measures.extend(_parse_tmdl_table_measures(tmdl_file))
                except Exception as exc:
                    print(f"  WARNING: Could not parse {tmdl_file}: {exc}", file=sys.stderr)
        return all_measures
    else:
        return _collect_json_measures(defn_dir)


# ---------------------------------------------------------------------------
# Anti-pattern rules
# ---------------------------------------------------------------------------

# Each rule function takes a MeasureInfo and returns a list of issue dicts.
# Issue dict: { "rule", "severity", "message", "fix", "line" (optional, 0-based) }

def _strip_dax_comments_and_strings(expr: str) -> str:
    """
    Remove single-line comments (// ...), block comments (/* ... */),
    and string literals ("...") from DAX so pattern matching is cleaner.
    """
    # Block comments
    result = re.sub(r"/\*.*?\*/", " ", expr, flags=re.DOTALL)
    # Single-line comments
    result = re.sub(r"//[^\n]*", " ", result)
    # String literals
    result = re.sub(r'"[^"]*"', '""', result)
    return result


def _find_line(expr: str, pattern: re.Pattern) -> int:
    """Find the 1-based line number of the first match, or 0 if not found."""
    for idx, line in enumerate(expr.splitlines(), start=1):
        if pattern.search(line):
            return idx
    return 0


def check_filter_all(m: MeasureInfo) -> list[dict]:
    """HIGH: FILTER(ALL(...)) anti-pattern."""
    pat = re.compile(r"FILTER\s*\(\s*ALL\s*\(", re.IGNORECASE)
    cleaned = _strip_dax_comments_and_strings(m.expression)
    issues: list[dict] = []
    if pat.search(cleaned):
        issues.append({
            "rule": "FILTER_ALL",
            "severity": "High",
            "message": "Uses FILTER(ALL(...)) -- should use REMOVEFILTERS + direct predicate",
            "fix": "Replace FILTER(ALL(Table[Column]), ...) with REMOVEFILTERS(Table[Column]) and a direct predicate in CALCULATE",
            "whyItsBad": "FILTER(ALL()) materialises the entire column into memory before filtering row by row. In DirectQuery this generates a full table scan subquery.",
            "requiredActions": [
                "Replace FILTER(ALL(Table[Col]), ...) with REMOVEFILTERS(Table[Col]) + direct predicate.",
                "Or use KEEPFILTERS for AND filter semantics.",
                "Test functional equivalence in DAX Studio with Server Timings.",
            ],
            "line": _find_line(m.expression, pat),
        })
    return issues


def check_iferror_iserror(m: MeasureInfo) -> list[dict]:
    """HIGH: IFERROR / ISERROR usage."""
    pat = re.compile(r"\b(IFERROR|ISERROR)\s*\(", re.IGNORECASE)
    cleaned = _strip_dax_comments_and_strings(m.expression)
    issues: list[dict] = []
    if pat.search(cleaned):
        func = pat.search(cleaned).group(1).upper()
        issues.append({
            "rule": "IFERROR_ISERROR",
            "severity": "High",
            "message": f"Uses {func}() which evaluates the expression twice, harming performance",
            "fix": "Replace with DIVIDE() for division or IF(ISBLANK(...), ...) for null checks",
            "whyItsBad": f"{func}() evaluates the expression twice — once to check for error, once to return the result — doubling computation cost. It also masks genuine errors.",
            "requiredActions": [
                "Replace with DIVIDE(x, y, 0) for division-by-zero handling.",
                "Use IF(ISBLANK(...), ...) for null checks.",
                "Surface genuine errors during development instead of hiding them.",
            ],
            "line": _find_line(m.expression, pat),
        })
    return issues


def check_nested_calculate(m: MeasureInfo) -> list[dict]:
    """HIGH: More than 2 levels of nested CALCULATE."""
    cleaned = _strip_dax_comments_and_strings(m.expression)
    pat = re.compile(r"\bCALCULATE\s*\(", re.IGNORECASE)

    # Walk through and count nesting depth
    upper = cleaned.upper()
    max_depth = 0
    depth = 0
    i = 0
    while i < len(upper):
        if upper[i:i+9] == "CALCULATE" and (i + 9 < len(upper)) and upper[i+9:].lstrip().startswith("("):
            depth += 1
            max_depth = max(max_depth, depth)
            i += 9
        elif upper[i] == "(":
            i += 1
        elif upper[i] == ")":
            # We need a better heuristic: track CALCULATE-initiated parens
            i += 1
        else:
            i += 1

    # Simpler heuristic: count occurrences of CALCULATE(
    calc_count = len(pat.findall(cleaned))
    issues: list[dict] = []
    if calc_count >= 3:
        issues.append({
            "rule": "NESTED_CALCULATE",
            "severity": "High",
            "message": f"Contains {calc_count} CALCULATE calls -- likely deeply nested, harming readability and performance",
            "fix": "Extract inner calculations into VAR variables or separate measures to reduce nesting",
            "whyItsBad": f"Contains {calc_count} CALCULATE calls creating multiple context transitions. In DirectQuery, each nesting level generates an additional SQL subquery.",
            "requiredActions": [
                "Flatten nested CALCULATE using VAR/RETURN pattern.",
                "Use CALCULATE([Measure], filter1, filter2) instead of CALCULATE(CALCULATE(...)).",
                "Extract inner calculations into separate reusable measures.",
            ],
            "line": _find_line(m.expression, pat),
        })
    return issues


def check_repeated_subexpression(m: MeasureInfo) -> list[dict]:
    """MEDIUM: Same complex subexpression appears 2+ times without VAR."""
    cleaned = _strip_dax_comments_and_strings(m.expression)
    if not cleaned or len(cleaned) < 60:
        return []

    # Check if measure already uses VARs
    has_vars = bool(re.search(r"\bVAR\b", cleaned, re.IGNORECASE))

    # Find repeated substrings of length > 30
    min_len = 30
    found: dict[str, int] = {}
    normalised = re.sub(r"\s+", " ", cleaned).strip()

    for length in range(min_len, min(80, len(normalised) // 2 + 1)):
        for start in range(len(normalised) - length + 1):
            sub = normalised[start:start + length]
            # Must contain a function call or column reference to be meaningful
            if not re.search(r"[A-Z]+\s*\(|\[", sub, re.IGNORECASE):
                continue
            # Must not be purely whitespace/punctuation
            if not re.search(r"[A-Za-z]", sub):
                continue
            count = normalised.count(sub)
            if count >= 2:
                # Check this substring is not a subset of an already-found longer one
                already_covered = False
                for existing in list(found.keys()):
                    if sub in existing:
                        already_covered = True
                        break
                if not already_covered:
                    # Remove shorter subsets
                    for existing in list(found.keys()):
                        if existing in sub:
                            del found[existing]
                    found[sub] = count
        if found:
            break  # Stop at the first length that yields duplicates

    issues: list[dict] = []
    if found and not has_vars:
        sample = list(found.keys())[0]
        sample_display = sample[:50] + "..." if len(sample) > 50 else sample
        issues.append({
            "rule": "REPEATED_SUBEXPRESSION",
            "severity": "Medium",
            "message": f"Repeated subexpression found ({list(found.values())[0]}x): '{sample_display}' -- extract to a VAR",
            "fix": "Use VAR to capture the repeated expression and reference the variable instead",
            "whyItsBad": "Without VAR, the same subexpression is recalculated every time it appears, potentially doubling or tripling query cost.",
            "requiredActions": [
                "Wrap the repeated subexpression in a VAR statement.",
                "Reference the VAR in all places where the subexpression was used.",
            ],
            "line": 0,
        })
    return issues


def check_bare_division(m: MeasureInfo) -> list[dict]:
    """MEDIUM: Using / for division instead of DIVIDE()."""
    cleaned = _strip_dax_comments_and_strings(m.expression)
    pat = re.compile(r"[\]\)]\s*/\s*(?![\*/])")
    issues: list[dict] = []
    if pat.search(cleaned):
        # Find original line
        orig_pat = re.compile(r"[\]\)]\s*/\s*(?![\*/])")
        issues.append({
            "rule": "BARE_DIVISION",
            "severity": "Medium",
            "message": "Uses '/' operator instead of DIVIDE() -- risk of division by zero",
            "fix": "Wrap in DIVIDE(numerator, denominator) for safe division",
            "whyItsBad": "The '/' operator raises an error when the denominator is zero or BLANK. DIVIDE() handles this gracefully.",
            "requiredActions": [
                "Wrap division in DIVIDE(numerator, denominator, alternateResult).",
            ],
            "line": _find_line(m.expression, orig_pat),
        })
    return issues


def check_count_vs_countrows(m: MeasureInfo) -> list[dict]:
    """MEDIUM: COUNT('Table'[Column]) when COUNTROWS would be better."""
    cleaned = _strip_dax_comments_and_strings(m.expression)
    pat = re.compile(r"\bCOUNT\s*\(\s*'?[^)]+'\s*\[[^\]]+\]\s*\)", re.IGNORECASE)
    issues: list[dict] = []
    if pat.search(cleaned):
        # Exclude COUNTA, COUNTAX, COUNTBLANK, COUNTX
        # Only flag plain COUNT(
        plain_count = re.compile(r"\bCOUNT\s*\(", re.IGNORECASE)
        not_variants = re.compile(r"\bCOUNT(A|AX|BLANK|X|ROWS)\s*\(", re.IGNORECASE)
        for match in plain_count.finditer(cleaned):
            pos = match.start()
            prefix = cleaned[max(0, pos - 5):pos + 5]
            if not not_variants.search(prefix):
                issues.append({
                    "rule": "COUNT_VS_COUNTROWS",
                    "severity": "Medium",
                    "message": "Uses COUNT(Table[Column]) -- consider COUNTROWS(Table) if counting rows not specific column values",
                    "fix": "Replace COUNT('Table'[Column]) with COUNTROWS('Table') when counting all rows",
                    "whyItsBad": "COUNT only counts non-BLANK values in a single column. COUNTROWS counts all rows and is semantically clearer.",
                    "requiredActions": [
                        "Replace COUNT('Table'[Column]) with COUNTROWS('Table').",
                    ],
                    "line": _find_line(m.expression, plain_count),
                })
                break
    return issues


def check_missing_format_string(m: MeasureInfo) -> list[dict]:
    """MEDIUM: Measure has no formatString."""
    issues: list[dict] = []
    if not m.format_string:
        issues.append({
            "rule": "MISSING_FORMAT_STRING",
            "severity": "Medium",
            "message": "Measure has no formatString -- values may display with inconsistent formatting",
            "fix": "Add a formatString (e.g. '#,0.00' for numbers, '0.0%' for percentages, '#,0' for integers)",
            "whyItsBad": "Without an explicit format string, PBI uses raw floating-point display, causing inconsistencies across visuals.",
            "requiredActions": [
                "Add formatString property (e.g. '#,##0.00' for currency, '0.0%' for percentages).",
            ],
            "line": 0,
        })
    return issues


def check_unqualified_columns(m: MeasureInfo) -> list[dict]:
    """LOW: Column references like [Column] without table prefix."""
    cleaned = _strip_dax_comments_and_strings(m.expression)
    # [Name] NOT preceded by ' (closing quote of a table name)
    pat = re.compile(r"(?<!')\[([A-Za-z_][\w ]*)\]")
    matches = pat.findall(cleaned)
    issues: list[dict] = []
    if matches:
        unique = sorted(set(matches))
        display = ", ".join("[" + c + "]" for c in unique[:5])
        if len(unique) > 5:
            display += f" ... (+{len(unique) - 5} more)"
        issues.append({
            "rule": "UNQUALIFIED_COLUMNS",
            "severity": "Low",
            "message": f"Unqualified column references: {display} -- prefix with table name for clarity",
            "fix": "Use 'TableName'[ColumnName] instead of bare [ColumnName]",
            "whyItsBad": "Unqualified column names can resolve ambiguously when tables share column names.",
            "requiredActions": [
                "Prefix all column references with 'TableName'[ColumnName].",
            ],
            "line": _find_line(m.expression, pat),
        })
    return issues


def check_no_variables(m: MeasureInfo) -> list[dict]:
    """LOW: Measure has >3 lines of DAX but uses no VAR statements."""
    expr = m.expression
    if not expr:
        return []
    line_count = len(expr.strip().splitlines())
    has_var = bool(re.search(r"\bVAR\b", expr, re.IGNORECASE))
    issues: list[dict] = []
    if line_count > 3 and not has_var:
        issues.append({
            "rule": "NO_VARIABLES",
            "severity": "Low",
            "message": f"Measure has {line_count} lines of DAX but uses no VAR statements -- consider extracting intermediate calculations",
            "fix": "Use VAR/RETURN pattern to name intermediate calculations for readability and potential performance gains",
            "whyItsBad": "Without VAR, intermediate calculations may be re-evaluated multiple times. VARs guarantee single evaluation.",
            "requiredActions": [
                "Introduce VAR statements for intermediate calculations.",
                "Use RETURN to reference the final result.",
            ],
            "line": 0,
        })
    return issues


def check_hardcoded_values(m: MeasureInfo) -> list[dict]:
    """LOW: Literal strings or numbers in CALCULATE filter arguments."""
    cleaned = _strip_dax_comments_and_strings(m.expression)
    issues: list[dict] = []

    calc_pat = re.compile(r"\bCALCULATE\s*\(", re.IGNORECASE)
    if not calc_pat.search(cleaned):
        return []

    orig = m.expression
    hardcoded_str = re.compile(r'=\s*"[^"]+?"', re.IGNORECASE)
    hardcoded_num = re.compile(r'=\s*\d{2,}(?:\.\d+)?(?!\s*[\]\)])\b')

    found_strings = hardcoded_str.findall(orig)
    found_numbers = hardcoded_num.findall(orig)

    if found_strings or found_numbers:
        samples = []
        for s in found_strings[:3]:
            samples.append(s.strip())
        for n in found_numbers[:2]:
            samples.append(n.strip())
        display = ", ".join(samples)
        issues.append({
            "rule": "HARDCODED_VALUES",
            "severity": "Low",
            "message": f"Hardcoded literals in filter context: {display} -- consider parameterising",
            "fix": "Extract hardcoded values to a parameter table or separate measure for maintainability",
            "whyItsBad": "Hardcoded literals make measures fragile and hard to maintain. Changes require editing each measure individually.",
            "requiredActions": [
                "Extract hardcoded values to a parameter table.",
                "Reference the parameter table from the measure instead of literals.",
            ],
            "line": _find_line(orig, hardcoded_str) or _find_line(orig, hardcoded_num),
        })
    return issues


def check_userelationship(m: MeasureInfo) -> list[dict]:
    """MEDIUM: USERELATIONSHIP activates an inactive relationship."""
    pat = re.compile(r"\bUSERELATIONSHIP\s*\(", re.IGNORECASE)
    cleaned = _strip_dax_comments_and_strings(m.expression)
    issues: list[dict] = []
    if pat.search(cleaned):
        issues.append({
            "rule": "USERELATIONSHIP",
            "severity": "Medium",
            "message": "Uses USERELATIONSHIP() which forces an alternate join path and prevents query caching",
            "fix": "Evaluate whether the inactive relationship can be made active or the model restructured",
            "whyItsBad": "USERELATIONSHIP activates an inactive relationship, forcing Databricks to use an alternate join path. This prevents query plan caching and adds overhead.",
            "requiredActions": [
                "Evaluate if the inactive relationship can be made the active one.",
                "If multiple relationships are needed, consider role-playing dimensions.",
                "As a last resort, keep USERELATIONSHIP but ensure it is not inside iterators.",
            ],
            "line": _find_line(m.expression, pat),
        })
    return issues


def check_crossjoin(m: MeasureInfo) -> list[dict]:
    """HIGH: CROSSJOIN / GENERATE creates cartesian products."""
    pat = re.compile(r"\bCROSSJOIN\s*\(", re.IGNORECASE)
    cleaned = _strip_dax_comments_and_strings(m.expression)
    issues: list[dict] = []
    if pat.search(cleaned):
        issues.append({
            "rule": "CROSSJOIN",
            "severity": "High",
            "message": "Uses CROSSJOIN() which creates cartesian products — huge virtual tables that explode query cost",
            "fix": "Replace CROSSJOIN with SUMMARIZE or a physical bridge table",
            "whyItsBad": "CROSSJOIN creates a cartesian product of two tables, producing huge virtual tables. In DirectQuery this generates extremely expensive SQL with massive intermediate result sets.",
            "requiredActions": [
                "Replace CROSSJOIN with SUMMARIZE or NATURALINNERJOIN where possible.",
                "If a cross-product is genuinely needed, pre-compute it in SQL/dbt.",
                "Consider using a physical bridge table instead.",
            ],
            "line": _find_line(m.expression, pat),
        })
    return issues


def check_divide_calculate(m: MeasureInfo) -> list[dict]:
    """MEDIUM: DIVIDE(CALCULATE(...), ...) prevents single-pass aggregation."""
    pat = re.compile(r"\bDIVIDE\s*\(\s*CALCULATE\s*\(", re.IGNORECASE)
    cleaned = _strip_dax_comments_and_strings(m.expression)
    issues: list[dict] = []
    if pat.search(cleaned):
        issues.append({
            "rule": "DIVIDE_CALC",
            "severity": "Medium",
            "message": "Uses DIVIDE(CALCULATE(...), ...) which prevents single-pass aggregation",
            "fix": "Use VAR to pre-compute CALCULATE result, then DIVIDE the variable",
            "whyItsBad": "DIVIDE(CALCULATE(...), ...) forces the engine to evaluate the CALCULATE separately from the denominator, preventing single-pass aggregation. Each part generates its own DQ query.",
            "requiredActions": [
                "Extract CALCULATE into a VAR variable.",
                "Use DIVIDE(_var, denominator) in the RETURN clause.",
                "This allows the engine to potentially combine the aggregations.",
            ],
            "line": _find_line(m.expression, pat),
        })
    return issues


# Ordered list of all anti-pattern checkers
ALL_CHECKS: list[tuple[str, str, callable]] = [
    ("FILTER_ALL",              "High",   check_filter_all),
    ("IFERROR_ISERROR",         "High",   check_iferror_iserror),
    ("NESTED_CALCULATE",        "High",   check_nested_calculate),
    ("CROSSJOIN",               "High",   check_crossjoin),
    ("REPEATED_SUBEXPRESSION",  "Medium", check_repeated_subexpression),
    ("BARE_DIVISION",           "Medium", check_bare_division),
    ("COUNT_VS_COUNTROWS",      "Medium", check_count_vs_countrows),
    ("MISSING_FORMAT_STRING",   "Medium", check_missing_format_string),
    ("USERELATIONSHIP",         "Medium", check_userelationship),
    ("DIVIDE_CALC",             "Medium", check_divide_calculate),
    ("UNQUALIFIED_COLUMNS",     "Low",    check_unqualified_columns),
    ("NO_VARIABLES",            "Low",    check_no_variables),
    ("HARDCODED_VALUES",        "Low",    check_hardcoded_values),
]


# ---------------------------------------------------------------------------
# Main audit logic
# ---------------------------------------------------------------------------

def audit_measures(measures: list[MeasureInfo]) -> list[dict]:
    """Run all anti-pattern checks against every measure.  Returns measure result dicts."""
    results: list[dict] = []

    for m in measures:
        if not m.name:
            continue

        all_issues: list[dict] = []
        for _rule_name, _severity, check_fn in ALL_CHECKS:
            try:
                issues = check_fn(m)
                all_issues.extend(issues)
            except Exception as exc:
                print(f"  WARNING: Rule check failed on '{m.name}': {exc}", file=sys.stderr)

        expr_lines = m.expression.strip().splitlines() if m.expression else []
        results.append({
            "name": m.name,
            "table": m.table,
            "isHidden": m.is_hidden,
            "hasFormatString": bool(m.format_string),
            "expressionLength": len(m.expression),
            "lineCount": len(expr_lines),
            "issues": all_issues,
            "issueCount": len(all_issues),
        })

    # Sort by issue count descending
    results.sort(key=lambda r: r["issueCount"], reverse=True)
    return results


def _build_anti_pattern_summary(measure_results: list[dict]) -> list[dict]:
    """Build the per-rule summary with counts."""
    counts: dict[str, int] = {}
    for rule_name, severity, _ in ALL_CHECKS:
        counts[rule_name] = 0

    for mr in measure_results:
        for issue in mr["issues"]:
            rule = issue["rule"]
            if rule in counts:
                counts[rule] += 1

    summary: list[dict] = []
    for rule_name, severity, _ in ALL_CHECKS:
        summary.append({
            "rule": rule_name,
            "severity": severity,
            "count": counts[rule_name],
        })
    return summary


def _build_summary(measure_results: list[dict], ap_summary: list[dict]) -> dict:
    """Build the top-level summary stats."""
    total_issues = sum(mr["issueCount"] for mr in measure_results)
    measures_with_issues = sum(1 for mr in measure_results if mr["issueCount"] > 0)
    clean = len(measure_results) - measures_with_issues

    high = sum(s["count"] for s in ap_summary if s["severity"] == "High")
    medium = sum(s["count"] for s in ap_summary if s["severity"] == "Medium")
    low = sum(s["count"] for s in ap_summary if s["severity"] == "Low")

    return {
        "high": high,
        "medium": medium,
        "low": low,
        "totalIssues": total_issues,
        "rulesChecked": len(ALL_CHECKS),
        "cleanMeasures": clean,
    }


def _print_summary(
    model_name: str,
    fmt: str,
    measure_results: list[dict],
    ap_summary: list[dict],
    summary: dict,
    output_file: Path,
) -> None:
    """Print a human-readable summary to stdout."""
    total = len(measure_results)
    with_issues = total - summary["cleanMeasures"]

    print(f"\n=== Power BI DAX Audit ===")
    print(f"Model: {model_name} ({fmt.upper()} format)")
    print(f"Measures: {total} total, {with_issues} with issues, {summary['cleanMeasures']} clean")

    print(f"\n--- Anti-Pattern Summary ---")
    for s in ap_summary:
        sev = s["severity"].upper()
        print(f"  {sev:<8s}{s['rule']:<30s}{s['count']:>4d}")

    # Top offenders
    offenders = [mr for mr in measure_results if mr["issueCount"] > 0]
    if offenders:
        print(f"\n--- Top Offenders ---")
        print(f"{'#':<3s}{'Measure':<32s}{'Table':<15s}{'Issues'}")
        for idx, mr in enumerate(offenders[:10], start=1):
            # Count by severity
            h = sum(1 for iss in mr["issues"] if iss["severity"] == "High")
            m = sum(1 for iss in mr["issues"] if iss["severity"] == "Medium")
            lo = sum(1 for iss in mr["issues"] if iss["severity"] == "Low")
            parts = []
            if h:
                parts.append(f"{h}H")
            if m:
                parts.append(f"{m}M")
            if lo:
                parts.append(f"{lo}L")
            detail = f"{mr['issueCount']} ({', '.join(parts)})"
            name_display = mr["name"][:30] if len(mr["name"]) > 30 else mr["name"]
            table_display = mr["table"][:13] if len(mr["table"]) > 13 else mr["table"]
            print(f"{idx:<3d}{name_display:<32s}{table_display:<15s}{detail}")

    print(f"\nWritten: {output_file}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_audit(model_path: Path, output_dir: Path) -> int:
    """Run the DAX audit and write results.  Returns exit code."""

    defn_dir = resolve_definition_dir(model_path)
    if defn_dir is None:
        print(
            f"ERROR: Could not find model definition in '{model_path}'.\n"
            "Expected structure: <model-dir>/definition/tables/  OR  <model-dir>/tables/\n"
            "Please pass --model-path pointing to the *.SemanticModel directory or the definition/ directory.",
            file=sys.stderr,
        )
        return 1

    fmt = _detect_format(defn_dir)
    model_name = _model_name_from_path(model_path)

    # Collect measures
    measures = collect_measures(defn_dir, fmt)
    if not measures:
        print(f"WARNING: No measures found in '{model_path}'.", file=sys.stderr)

    # Run audit
    measure_results = audit_measures(measures)
    ap_summary = _build_anti_pattern_summary(measure_results)
    summary = _build_summary(measure_results, ap_summary)

    measures_with_issues = len(measure_results) - summary["cleanMeasures"]

    # Build output JSON
    result = {
        "version": "1.0.0",
        "analysedAt": datetime.now(timezone.utc).isoformat(),
        "modelPath": str(model_path),
        "modelName": model_name,
        "format": fmt,
        "totalMeasures": len(measure_results),
        "measuresWithIssues": measures_with_issues,
        "measures": measure_results,
        "antiPatternSummary": ap_summary,
        "summary": summary,
    }

    # Write output
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "dax-audit.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    # Print summary
    _print_summary(model_name, fmt, measure_results, ap_summary, summary, output_file)

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Power BI DAX Audit -- scans DAX measures and flags anti-patterns"
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
        help="Output directory for dax-audit.json",
    )
    args = parser.parse_args()

    model_path = args.model_path.resolve()
    output_dir = args.output.resolve()

    if not model_path.is_dir():
        print(f"ERROR: Model path does not exist or is not a directory: {model_path}", file=sys.stderr)
        sys.exit(1)

    sys.exit(run_audit(model_path, output_dir))


if __name__ == "__main__":
    main()
