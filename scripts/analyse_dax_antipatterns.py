#!/usr/bin/env python3
"""
DAX Anti-Pattern Tier Analysis

Analyses DAX measures for compound anti-pattern severity using a 9-flag
taxonomy.  Groups measures into semantic pattern families and builds a
measure-to-measure dependency call graph for amplification detection.

Reads the PBI semantic model directly (JSON/TE2 or TMDL) and optionally
enriches with dax-audit.json and model-taxonomy.json.

Stdlib only — no third-party dependencies.

Usage:
    python3 analyse_dax_antipatterns.py \
        --model-path <path-to-model-dir> \
        --output <output-dir> \
        [--taxonomy-file <model-taxonomy.json>]
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers (shared with audit_dax / run_bpa)
# ---------------------------------------------------------------------------

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


def _model_name_from_path(model_path: Path) -> str:
    name = model_path.name
    for suffix in (".SemanticModel", ".Dataset", ".database"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    return name or "Unknown Model"


def _strip_comments_and_strings(expr: str) -> str:
    result = re.sub(r"/\*.*?\*/", " ", expr, flags=re.DOTALL)
    result = re.sub(r"//[^\n]*", " ", result)
    result = re.sub(r'"[^"]*"', '""', result)
    return result


# ---------------------------------------------------------------------------
# Model discovery + measure collection (reused from audit_dax.py)
# ---------------------------------------------------------------------------

def _resolve_definition_dir(model_path: Path) -> Path | None:
    candidate = model_path / "definition"
    if candidate.is_dir():
        return candidate
    if (model_path / "tables").is_dir():
        return model_path
    for child in model_path.iterdir():
        if child.is_dir():
            deep = child / "definition"
            if deep.is_dir():
                return deep
            if (child / "tables").is_dir():
                return child
    return None


def _detect_format(defn_dir: Path) -> str:
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


class MeasureData:
    __slots__ = ("name", "table", "expression", "display_folder", "is_hidden")

    def __init__(self, name: str, table: str, expression: str,
                 display_folder: str = "", is_hidden: bool = False):
        self.name = name
        self.table = table
        self.expression = expression
        self.display_folder = display_folder
        self.is_hidden = is_hidden


def _collect_measures_json(defn_dir: Path) -> list[MeasureData]:
    measures: list[MeasureData] = []
    tables_dir = defn_dir / "tables"
    if not tables_dir.is_dir():
        return measures
    for table_dir in sorted(tables_dir.iterdir()):
        if not table_dir.is_dir():
            continue
        table_json_path = table_dir / "table.json"
        table_json = _read_json(table_json_path) if table_json_path.is_file() else {}
        table_json = table_json or {}
        table_name = table_json.get("name", table_dir.name)
        measures_dir = table_dir / "measures"
        if not measures_dir.is_dir():
            continue
        for mfile in sorted(measures_dir.iterdir()):
            if mfile.suffix != ".json":
                continue
            data = _read_json(mfile)
            if not data or not isinstance(data, dict):
                continue
            measures.append(MeasureData(
                name=data.get("name", mfile.stem),
                table=table_name,
                expression=_expr_to_str(data.get("expression", "")),
                display_folder=data.get("displayFolder", ""),
                is_hidden=data.get("isHidden", False),
            ))
    return measures


def _parse_tmdl_measure_block(lines: list[str], start: int) -> tuple[dict, int]:
    header = lines[start].strip()
    eq_pos = header.find("=")
    name_part = header[8:eq_pos].strip().strip("'") if eq_pos > 0 else header[8:].strip().strip("'")
    expr_start = header[eq_pos + 1:].strip() if eq_pos > 0 else ""
    expression_lines: list[str] = []
    in_multiline = False
    if expr_start == "```" or expr_start.startswith("```"):
        in_multiline = True
    elif expr_start:
        expression_lines.append(expr_start)
    meas: dict = {"name": name_part, "expression": "", "displayFolder": "", "isHidden": False}
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
        if not stripped or (not stripped.startswith("\t") and not stripped.startswith(" ")):
            if stripped.startswith(("column ", "measure ", "partition ", "table ", "relationship ")):
                break
            if not stripped:
                if i + 1 < len(lines):
                    ns = lines[i + 1].strip()
                    if ns.startswith(("column ", "measure ", "partition ", "table ")):
                        break
                    if ns.startswith(("annotation ", "changedProperty")):
                        i += 1
                        continue
                i += 1
                continue
        if stripped.startswith("formatString:"):
            pass
        elif stripped == "isHidden":
            meas["isHidden"] = True
        elif stripped.startswith("displayFolder:"):
            meas["displayFolder"] = stripped.split(":", 1)[1].strip()
        i += 1
    meas["expression"] = "\n".join(expression_lines).strip()
    return meas, i


def _collect_measures_tmdl(defn_dir: Path) -> list[MeasureData]:
    measures: list[MeasureData] = []
    tables_dir = defn_dir / "tables"
    if not tables_dir.is_dir():
        return measures
    for tmdl_file in sorted(tables_dir.iterdir()):
        if tmdl_file.suffix != ".tmdl":
            continue
        try:
            text = tmdl_file.read_text(encoding="utf-8-sig")
        except OSError:
            continue
        lines = text.splitlines()
        table_name = ""
        i = 0
        while i < len(lines):
            stripped = lines[i].strip()
            if stripped.startswith("table "):
                table_name = stripped[6:].strip().strip("'")
            elif stripped.startswith("measure "):
                try:
                    meas, skip = _parse_tmdl_measure_block(lines, i)
                    measures.append(MeasureData(
                        name=meas.get("name", ""),
                        table=table_name or tmdl_file.stem,
                        expression=meas.get("expression", ""),
                        display_folder=meas.get("displayFolder", ""),
                        is_hidden=meas.get("isHidden", False),
                    ))
                    i = skip
                    continue
                except Exception:
                    pass
            i += 1
    return measures


def collect_all_measures(model_path: Path) -> list[MeasureData]:
    defn_dir = _resolve_definition_dir(model_path)
    if defn_dir is None:
        print(f"ERROR: Could not find model definition in '{model_path}'.", file=sys.stderr)
        return []
    fmt = _detect_format(defn_dir)
    if fmt == "tmdl":
        return _collect_measures_tmdl(defn_dir)
    return _collect_measures_json(defn_dir)


# ---------------------------------------------------------------------------
# Anti-Pattern Flag Detection (9 flags)
# ---------------------------------------------------------------------------

_FLAG_PATTERNS: dict[str, dict] = {
    "ITERATOR": {
        "pattern": re.compile(
            r"\b(SUMX|AVERAGEX|RANKX|COUNTX|PRODUCTX|MAXX|MINX|ADDCOLUMNS|GENERATE)\s*\(",
            re.IGNORECASE,
        ),
        "functions": ["SUMX", "AVERAGEX", "RANKX", "COUNTX", "PRODUCTX", "MAXX", "MINX", "ADDCOLUMNS", "GENERATE"],
        "whyExpensive": "Row-by-row evaluation; multiplies sub-queries in DirectQuery.",
    },
    "ALL_FILTER": {
        "pattern": re.compile(
            r"\b(ALL|ALLEXCEPT|ALLSELECTED|REMOVEFILTERS)\s*\(",
            re.IGNORECASE,
        ),
        "functions": ["ALL", "ALLEXCEPT", "ALLSELECTED", "REMOVEFILTERS"],
        "whyExpensive": "Clears filter context; forces full re-aggregation of the table.",
    },
    "ROW_FILTER": {
        "pattern": re.compile(r"\bFILTER\s*\(", re.IGNORECASE),
        "functions": ["FILTER"],
        "whyExpensive": "Full table scan; cannot use dictionary indexes in the storage engine.",
    },
    "SWITCH_IF": {
        "pattern": re.compile(
            r"\b(SWITCH\s*\(\s*TRUE\s*\(\s*\)|IF\s*\()",
            re.IGNORECASE,
        ),
        "functions": ["SWITCH(TRUE())", "IF"],
        "whyExpensive": "All branches are evaluated by the storage engine; no short-circuiting.",
    },
    "TIME_INTEL": {
        "pattern": re.compile(
            r"\b(DATEADD|DATESINPERIOD|SAMEPERIODLASTYEAR|DATESYTD|DATESMTD|DATESQTD|"
            r"PARALLELPERIOD|PREVIOUSMONTH|PREVIOUSYEAR|PREVIOUSQUARTER|"
            r"TOTALMTD|TOTALYTD|TOTALQTD)\s*\(",
            re.IGNORECASE,
        ),
        "functions": ["DATEADD", "DATESINPERIOD", "SAMEPERIODLASTYEAR", "DATESYTD", "DATESMTD", "PARALLELPERIOD"],
        "whyExpensive": "Creates virtual date tables; adds joins and prevents caching.",
    },
    "NESTED_CALC": {
        "pattern": None,  # custom detection
        "functions": ["CALCULATE inside CALCULATE"],
        "whyExpensive": "Multiple context transitions; each nesting adds a SQL subquery level.",
    },
    "USERELATIONSHIP": {
        "pattern": re.compile(r"\bUSERELATIONSHIP\s*\(", re.IGNORECASE),
        "functions": ["USERELATIONSHIP"],
        "whyExpensive": "Activates inactive relationship; forces alternate join path and prevents caching.",
    },
    "CROSSJOIN": {
        "pattern": re.compile(r"\b(CROSSJOIN|GENERATE|GENERATEALL)\s*\(", re.IGNORECASE),
        "functions": ["CROSSJOIN", "GENERATE", "GENERATEALL"],
        "whyExpensive": "Cartesian explosion; creates huge virtual tables that multiply query cost.",
    },
    "DIVIDE_CALC": {
        "pattern": re.compile(r"\bDIVIDE\s*\(\s*CALCULATE\s*\(", re.IGNORECASE),
        "functions": ["DIVIDE(CALCULATE(...))"],
        "whyExpensive": "Prevents single-pass aggregation; forces separate evaluation of numerator.",
    },
}


def _detect_nested_calculate(cleaned: str) -> bool:
    """Detect CALCULATE inside CALCULATE (depth >= 2)."""
    pat = re.compile(r"\bCALCULATE\s*\(", re.IGNORECASE)
    count = len(pat.findall(cleaned))
    return count >= 2


def detect_flags(expression: str) -> list[str]:
    """Detect which anti-pattern flags are present in a DAX expression."""
    cleaned = _strip_comments_and_strings(expression)
    flags: list[str] = []
    for flag_name, flag_info in _FLAG_PATTERNS.items():
        if flag_name == "NESTED_CALC":
            if _detect_nested_calculate(cleaned):
                flags.append(flag_name)
        elif flag_name == "CROSSJOIN":
            pat = re.compile(r"\bCROSSJOIN\s*\(", re.IGNORECASE)
            gen_pat = re.compile(r"\bGENERATE(?:ALL)?\s*\(", re.IGNORECASE)
            if pat.search(cleaned) or gen_pat.search(cleaned):
                if "ITERATOR" not in flags:
                    flags.append(flag_name)
                elif pat.search(cleaned):
                    flags.append(flag_name)
        else:
            if flag_info["pattern"] and flag_info["pattern"].search(cleaned):
                flags.append(flag_name)
    return flags


def assign_tier(flag_count: int) -> str:
    if flag_count >= 4:
        return "critical"
    elif flag_count == 3:
        return "highRisk"
    elif flag_count == 2:
        return "medium"
    elif flag_count == 1:
        return "lowRisk"
    return "clean"


# ---------------------------------------------------------------------------
# Semantic Measure Grouping (Pattern Families)
# ---------------------------------------------------------------------------

_SUFFIX_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("WTD", re.compile(r"\bWTD\b", re.IGNORECASE)),
    ("MTD", re.compile(r"\bMTD\b", re.IGNORECASE)),
    ("HTD", re.compile(r"\bHTD\b", re.IGNORECASE)),
    ("YTD", re.compile(r"\bYTD\b", re.IGNORECASE)),
    ("LY", re.compile(r"\bLY\b", re.IGNORECASE)),
    ("LM", re.compile(r"\bLM\b", re.IGNORECASE)),
    ("LW", re.compile(r"\bLW\b", re.IGNORECASE)),
    ("L7D", re.compile(r"\bL7D\b", re.IGNORECASE)),
    ("Cover", re.compile(r"\b(Cover|Avg\s*Weekly)\b", re.IGNORECASE)),
    ("OB", re.compile(r"\bOB\b", re.IGNORECASE)),
    ("Ranking", re.compile(r"\b(Rank|Ranking)\b", re.IGNORECASE)),
    ("Variance", re.compile(r"\b(Var|Variance|Diff)\b", re.IGNORECASE)),
    ("Conversion", re.compile(r"\b(Conv|Conversion)\b", re.IGNORECASE)),
]

_FAMILY_DESCRIPTIONS: dict[str, dict] = {
    "WTD": {
        "name": "Week-to-Date Window Measures",
        "whySlow": (
            "SWITCH(TRUE()) evaluates all branches. SUMX iterates day-by-day. "
            "Nested CALCULATE inside SUMX. FILTER(ALL(Date)) forces full date table scans."
        ),
        "requiredActions": [
            "Add pre-computed flags to Date table: Is_WTD, Is_YD1, Is_LW, etc.",
            "Replace SUMX + FILTER with simple CALCULATE using boolean flags.",
            "Remove SWITCH(TRUE()) by splitting measures by grain.",
        ],
    },
    "MTD": {
        "name": "Month-to-Date Window Measures",
        "whySlow": "Same pattern as WTD but scoped to month boundaries. FILTER(ALL(Date)) forces full date scans.",
        "requiredActions": [
            "Add Is_MTD flag to Date table.",
            "Replace FILTER(ALL(Date)) with CALCULATE using the boolean flag.",
        ],
    },
    "HTD": {
        "name": "Half-to-Date Window Measures",
        "whySlow": "Same pattern as WTD/MTD but scoped to half-year boundaries.",
        "requiredActions": [
            "Add Is_HTD flag to Date table.",
            "Replace FILTER(ALL(Date)) with CALCULATE using the boolean flag.",
        ],
    },
    "YTD": {
        "name": "Year-to-Date Window Measures",
        "whySlow": "Same pattern as WTD/MTD but scoped to year boundaries.",
        "requiredActions": [
            "Add Is_YTD flag to Date table.",
            "Replace FILTER(ALL(Date)) with CALCULATE using the boolean flag.",
        ],
    },
    "LY": {
        "name": "Last Year Comparison Measures",
        "whySlow": (
            "DATE(YEAR()-1) inside SUMX is row-by-row scalar evaluation. "
            "FILTER(ALL(Date)) scans entire date table. IF() evaluates both branches."
        ),
        "requiredActions": [
            "Add LY_Date surrogate key column to Date table.",
            "Replace SUMX with direct CALCULATE using the LY key.",
            "Remove SWITCH(TRUE()) branching.",
        ],
    },
    "LM": {
        "name": "Last Month Comparison Measures",
        "whySlow": "Same pattern as LY but for month offset. Bridge table lookups via CALCULATETABLE add extra joins.",
        "requiredActions": [
            "Add LM surrogate key directly to Date table.",
            "Remove bridge-table lookups.",
            "Replace FILTER(ALL(Date)) with TREATAS or direct equality.",
        ],
    },
    "LW": {
        "name": "Last Week Comparison Measures",
        "whySlow": "Similar to LY/LM — bridge table lookup via CALCULATETABLE + FILTER(ALL(Date)).",
        "requiredActions": [
            "Add LW surrogate key directly to Date table.",
            "Remove bridge-table lookups.",
        ],
    },
    "L7D": {
        "name": "Last 7 Days Rolling Window Measures",
        "whySlow": "DATESINPERIOD creates virtual tables. FILTER(ALL(Date)) scans full date table.",
        "requiredActions": [
            "Add rolling 7-day window flags to Date table.",
            "Replace DATESINPERIOD with direct boolean filters.",
        ],
    },
    "Cover": {
        "name": "Average Weekly Cover Measures",
        "whySlow": (
            "ADDCOLUMNS materialises virtual tables repeatedly. "
            "FILTER over ADDCOLUMNS doubles the cost. "
            "AVERAGEX over virtual tables multiplies sub-queries."
        ),
        "requiredActions": [
            "Pre-aggregate weekly sales and stock into a weekly summary table.",
            "Replace ADDCOLUMNS + FILTER with SUM / COUNT from the summary table.",
            "Remove AVERAGEX by storing weekly averages directly.",
        ],
    },
    "OB": {
        "name": "Opening Balance Measures",
        "whySlow": "Uses bridge table lookups with FILTER(ALL(Date)) and IN operator.",
        "requiredActions": [
            "Add opening balance date keys directly to Date table.",
            "Replace FILTER(ALL(Date)) with TREATAS or direct equality.",
        ],
    },
    "Ranking": {
        "name": "Ranking Measures",
        "whySlow": "RANKX iterates over the entire table for every row context, multiplying sub-queries.",
        "requiredActions": [
            "Limit the ranking scope with TOPN or filtered ALLSELECTED.",
            "Consider pre-computing ranks in SQL/dbt for static rankings.",
        ],
    },
    "Variance": {
        "name": "Variance / Difference Measures",
        "whySlow": "Typically calls two expensive base measures (current + comparison period), doubling cost.",
        "requiredActions": [
            "Ensure base measures are optimised first.",
            "Use VAR/RETURN to avoid recalculating the base measure.",
        ],
    },
    "Conversion": {
        "name": "Conversion Rate Measures",
        "whySlow": "Typically divides two expensive aggregations, each generating separate DQ queries.",
        "requiredActions": [
            "Use DIVIDE with pre-computed VARs.",
            "Ensure numerator and denominator base measures share filter context where possible.",
        ],
    },
}


def classify_family(measure_name: str, display_folder: str) -> str | None:
    """Classify a measure into a pattern family based on naming convention."""
    for suffix, pat in _SUFFIX_PATTERNS:
        if pat.search(measure_name):
            return suffix
    return None


def _build_family_fingerprint(flags: list[str]) -> str:
    """Create a structural fingerprint from sorted flags for grouping."""
    return "+".join(sorted(flags))


# ---------------------------------------------------------------------------
# Measure Dependency Chain Analysis
# ---------------------------------------------------------------------------

def build_measure_call_graph(measures: list[MeasureData]) -> dict[str, list[str]]:
    """Parse each measure's DAX for references to other measures.

    Measure references in DAX are [MeasureName] NOT preceded by a table name
    (i.e., not preceded by a single-quote character).
    """
    all_names = {m.name for m in measures}
    call_graph: dict[str, list[str]] = {}

    for m in measures:
        if not m.expression:
            call_graph[m.name] = []
            continue
        cleaned = _strip_comments_and_strings(m.expression)
        refs: list[str] = []
        bracket_pat = re.compile(r"(?<!')\[([^\]]+)\]")
        for match in bracket_pat.finditer(cleaned):
            ref_name = match.group(1)
            if ref_name in all_names and ref_name != m.name:
                refs.append(ref_name)
        call_graph[m.name] = sorted(set(refs))

    return call_graph


def find_amplification_chains(
    call_graph: dict[str, list[str]],
    measure_tiers: dict[str, str],
    measure_flags: dict[str, list[str]],
) -> list[dict]:
    """Detect amplification chains where high-tier measures call lower-tier
    measures inside iterators, multiplying cost."""
    chains: list[dict] = []
    tier_rank = {"critical": 4, "highRisk": 3, "medium": 2, "lowRisk": 1, "clean": 0}

    for caller, callees in call_graph.items():
        if not callees:
            continue
        caller_tier = measure_tiers.get(caller, "clean")
        caller_rank = tier_rank.get(caller_tier, 0)
        if caller_rank < 2:
            continue
        caller_flags = set(measure_flags.get(caller, []))
        has_iterator = "ITERATOR" in caller_flags or "ROW_FILTER" in caller_flags

        for callee in callees:
            callee_tier = measure_tiers.get(callee, "clean")
            callee_rank = tier_rank.get(callee_tier, 0)
            if callee_rank >= 1 and has_iterator:
                chains.append({
                    "caller": caller,
                    "callerTier": caller_tier,
                    "callee": callee,
                    "calleeTier": callee_tier,
                    "amplification": (
                        f"{caller} ({caller_tier}) calls [{callee}] ({callee_tier}) "
                        f"inside an iterator — each dependency multiplies cost."
                    ),
                })

    chains.sort(key=lambda c: tier_rank.get(c["callerTier"], 0), reverse=True)
    return chains


# ---------------------------------------------------------------------------
# Main Analysis
# ---------------------------------------------------------------------------

def analyse(model_path: Path, taxonomy_file: Path | None = None) -> dict:
    measures = collect_all_measures(model_path)
    if not measures:
        print("WARNING: No measures found.", file=sys.stderr)

    model_name = _model_name_from_path(model_path)

    # Flag detection per measure
    measure_results: list[dict] = []
    measure_flags_map: dict[str, list[str]] = {}
    measure_tiers_map: dict[str, str] = {}
    flag_counts: dict[str, int] = defaultdict(int)

    for m in measures:
        flags = detect_flags(m.expression) if m.expression else []
        flag_count = len(flags)
        tier = assign_tier(flag_count)
        family = classify_family(m.name, m.display_folder)

        for f in flags:
            flag_counts[f] += 1

        measure_flags_map[m.name] = flags
        measure_tiers_map[m.name] = tier

        measure_results.append({
            "name": m.name,
            "table": m.table,
            "flags": flags,
            "flagCount": flag_count,
            "tier": tier,
            "patternFamily": family,
            "displayFolder": m.display_folder,
        })

    # Tier summary
    tier_counts = defaultdict(int)
    for mr in measure_results:
        tier_counts[mr["tier"]] += 1

    tier_summary = {
        "critical": {"count": tier_counts.get("critical", 0), "flags": "4+"},
        "highRisk": {"count": tier_counts.get("highRisk", 0), "flags": "3"},
        "medium": {"count": tier_counts.get("medium", 0), "flags": "2"},
        "lowRisk": {"count": tier_counts.get("lowRisk", 0), "flags": "1"},
        "clean": {"count": tier_counts.get("clean", 0), "flags": "0"},
    }

    flagged_count = sum(1 for mr in measure_results if mr["flagCount"] > 0)

    # Anti-pattern catalog
    catalog: list[dict] = []
    for flag_name, flag_info in _FLAG_PATTERNS.items():
        catalog.append({
            "flag": flag_name,
            "functions": flag_info["functions"],
            "whyExpensive": flag_info["whyExpensive"],
            "measureCount": flag_counts.get(flag_name, 0),
        })

    # Pattern families
    family_groups: dict[str, list[dict]] = defaultdict(list)
    for mr in measure_results:
        if mr["patternFamily"]:
            family_groups[mr["patternFamily"]].append(mr)

    pattern_families: list[dict] = []
    for family_key, members in sorted(family_groups.items(), key=lambda x: -len(x[1])):
        desc = _FAMILY_DESCRIPTIONS.get(family_key, {})
        all_flags: set[str] = set()
        for m in members:
            all_flags.update(m["flags"])
        max_tier = "clean"
        tier_order = ["clean", "lowRisk", "medium", "highRisk", "critical"]
        for m in members:
            if tier_order.index(m["tier"]) > tier_order.index(max_tier):
                max_tier = m["tier"]
        example_names = [m["name"] for m in members[:5]]
        pattern_families.append({
            "id": f"pattern-{family_key.lower()}",
            "name": desc.get("name", f"{family_key} Measures"),
            "measureCount": len(members),
            "tier": max_tier,
            "flags": sorted(all_flags),
            "exampleMeasures": example_names,
            "whySlow": desc.get("whySlow", "Multiple anti-patterns compound query cost."),
            "requiredActions": desc.get("requiredActions", [
                "Analyse the specific DAX pattern and simplify.",
                "Replace FILTER(ALL) with KEEPFILTERS or TREATAS.",
                "Use VAR/RETURN to avoid repeated sub-expression evaluation.",
            ]),
        })

    # Ungrouped measures with high flag counts
    ungrouped_critical = [
        mr for mr in measure_results
        if mr["patternFamily"] is None and mr["tier"] in ("critical", "highRisk")
    ]
    if ungrouped_critical:
        fingerprint_groups: dict[str, list[dict]] = defaultdict(list)
        for mr in ungrouped_critical:
            fp = _build_family_fingerprint(mr["flags"])
            fingerprint_groups[fp].append(mr)
        for fp, members in sorted(fingerprint_groups.items(), key=lambda x: -len(x[1])):
            if len(members) >= 2:
                example_names = [m["name"] for m in members[:5]]
                pattern_families.append({
                    "id": f"pattern-struct-{fp.lower().replace('+', '-')}",
                    "name": f"Structural Pattern ({fp})",
                    "measureCount": len(members),
                    "tier": members[0]["tier"],
                    "flags": sorted(set(f for m in members for f in m["flags"])),
                    "exampleMeasures": example_names,
                    "whySlow": "Multiple anti-patterns compound query cost.",
                    "requiredActions": [
                        "Analyse the specific DAX pattern and simplify.",
                        "Replace FILTER(ALL) with KEEPFILTERS or TREATAS.",
                        "Use VAR/RETURN to avoid repeated sub-expression evaluation.",
                    ],
                })

    # Dependency chain analysis
    call_graph = build_measure_call_graph(measures)
    amplification_chains = find_amplification_chains(
        call_graph, measure_tiers_map, measure_flags_map,
    )

    # Enrich measure results with dependencies
    for mr in measure_results:
        deps = call_graph.get(mr["name"], [])
        mr["dependencies"] = deps

    # Priority fix order
    priority_fixes: list[dict] = []
    priority_counter = 0
    pf_sorted = sorted(pattern_families, key=lambda x: (
        -["clean", "lowRisk", "medium", "highRisk", "critical"].index(x["tier"]),
        -x["measureCount"],
    ))
    for pf in pf_sorted:
        if pf["tier"] in ("critical", "highRisk", "medium"):
            priority_counter += 1
            impact = "Very High" if pf["tier"] == "critical" else "High" if pf["tier"] == "highRisk" else "Medium"
            priority_fixes.append({
                "priority": priority_counter,
                "action": f"Rewrite {pf['name']}",
                "measures": pf["measureCount"],
                "expectedImpact": impact,
                "patternFamily": pf["id"],
            })

    # Add generic fixes
    switch_only = sum(1 for mr in measure_results if mr["flags"] == ["SWITCH_IF"])
    if switch_only > 10:
        priority_counter += 1
        priority_fixes.append({
            "priority": priority_counter,
            "action": "Replace SWITCH(TRUE()) with grain-specific measures",
            "measures": switch_only,
            "expectedImpact": "Medium",
            "patternFamily": None,
        })

    filter_all_only = flag_counts.get("ALL_FILTER", 0)
    if filter_all_only > 5:
        priority_counter += 1
        priority_fixes.append({
            "priority": priority_counter,
            "action": "Replace FILTER(ALL(Date)) with KEEPFILTERS/TREATAS across all measures",
            "measures": filter_all_only,
            "expectedImpact": "High",
            "patternFamily": None,
        })

    return {
        "version": "1.0.0",
        "analysedAt": datetime.now(timezone.utc).isoformat(),
        "modelPath": str(model_path),
        "modelName": model_name,
        "totalMeasures": len(measure_results),
        "flaggedAsSlow": flagged_count,
        "tierSummary": tier_summary,
        "antiPatternCatalog": catalog,
        "patternFamilies": pattern_families,
        "measures": measure_results,
        "dependencyChains": amplification_chains[:50],
        "priorityFixOrder": priority_fixes,
    }


def main():
    parser = argparse.ArgumentParser(
        description="DAX Anti-Pattern Tier Analysis — compound severity + pattern families + dependency chains"
    )
    parser.add_argument(
        "--model-path", required=True, type=Path,
        help="Path to the semantic model directory",
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output directory for dax-antipattern-tiers.json",
    )
    parser.add_argument(
        "--taxonomy-file", required=False, type=Path, default=None,
        help="Path to model-taxonomy.json for enrichment",
    )
    args = parser.parse_args()

    model_path = args.model_path.resolve()
    output_dir = args.output.resolve()

    if not model_path.is_dir():
        print(f"ERROR: Model path does not exist: {model_path}", file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    result = analyse(model_path, taxonomy_file=args.taxonomy_file)

    output_file = output_dir / "dax-antipattern-tiers.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    ts = result["tierSummary"]
    print(f"\n=== DAX Anti-Pattern Tier Analysis ===")
    print(f"Model: {result['modelName']}")
    print(f"Measures: {result['totalMeasures']} total, {result['flaggedAsSlow']} flagged")
    print(f"\n--- Tier Summary ---")
    print(f"  Critical (4+ flags): {ts['critical']['count']}")
    print(f"  High Risk (3 flags): {ts['highRisk']['count']}")
    print(f"  Medium (2 flags):    {ts['medium']['count']}")
    print(f"  Low Risk (1 flag):   {ts['lowRisk']['count']}")
    print(f"  Clean (0 flags):     {ts['clean']['count']}")
    print(f"\n--- Pattern Families ---")
    for pf in result["patternFamilies"][:10]:
        print(f"  {pf['name']}: {pf['measureCount']} measures ({pf['tier']})")
    if result["dependencyChains"]:
        print(f"\n--- Amplification Chains ---")
        for chain in result["dependencyChains"][:5]:
            print(f"  {chain['amplification']}")
    print(f"\nPriority fixes: {len(result['priorityFixOrder'])}")
    print(f"\nWritten: {output_file}")


if __name__ == "__main__":
    main()
