#!/usr/bin/env python3
"""
Analyse PBIX Layout JSON files for visual-layer performance issues.

Parses extracted PBIX Layout files and applies PBI Inspector-style metadata
rules against the visual layer. Produces a structured JSON report with
findings, severities, and recommendations.

Usage:
    # Single Layout file
    python3 scripts/analyse_report_visuals.py \
      --layout-path output/pbix_extracted/Layout \
      --output output/

    # Directory containing Layout files in subdirectories
    python3 scripts/analyse_report_visuals.py \
      --layout-dir output/pbix_extracted/ \
      --output output/
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, timezone


# ── Rule definitions ──

RULES = [
    {
        "ruleId": "V01",
        "title": "Too many visuals per page",
        "severity": "medium",
        "performanceImpact": "latency",
        "description": (
            "Pages with more than 15 visuals generate many parallel queries on "
            "page load, competing for capacity CU and increasing total render time."
        ),
        "recommendation": (
            "Split into sub-pages, use bookmarks for conditional visibility, "
            "or reduce visual count."
        ),
    },
    {
        "ruleId": "V02",
        "title": "No date slicer on data-heavy pages",
        "severity": "medium",
        "performanceImpact": "latency",
        "description": (
            "Pages with table or matrix visuals but no date slicer force full "
            "table scans on every interaction. A date slicer enables partition "
            "pruning and drastically reduces data read."
        ),
        "recommendation": (
            "Add a date slicer (or relative date filter) to pages containing "
            "table or matrix visuals to enable date-based query pruning."
        ),
    },
    {
        "ruleId": "V03",
        "title": "Excessive report-level filters",
        "severity": "medium",
        "performanceImpact": "latency",
        "description": (
            "More than 10 report-level filters add WHERE clauses to every query "
            "on every page, increasing query complexity and reducing cache hit rates."
        ),
        "recommendation": (
            "Consolidate report-level filters. Move page-specific filters to "
            "page level. Remove unused or redundant filters."
        ),
    },
    {
        "ruleId": "V04",
        "title": "Hidden pages with active visuals",
        "severity": "high",
        "performanceImpact": "cost",
        "description": (
            "Hidden pages (visibility: 1) that contain visuals with queries still "
            "consume capacity when accessed via bookmarks, drillthrough, or the "
            "API. They also increase model refresh scope if they reference "
            "import tables."
        ),
        "recommendation": (
            "Remove hidden pages that are no longer needed, or strip their "
            "visuals if the page is kept for layout purposes only."
        ),
    },
    {
        "ruleId": "V05",
        "title": "Auto-refresh or query reduction settings",
        "severity": "medium",
        "performanceImpact": "cost",
        "description": (
            "Checks for automatic page refresh (which sends repeated queries "
            "at a fixed interval) and query reduction settings (cross-highlighting, "
            "slicer/filter Apply buttons) that affect DirectQuery query volume "
            "during user interaction."
        ),
        "recommendation": (
            "Disable auto-refresh for daily snapshot data. Enable query reduction "
            "settings (disable cross-highlighting, enable Apply buttons) to reduce "
            "interactive query load on DirectQuery models."
        ),
    },
    {
        "ruleId": "V06",
        "title": "Wide tables/matrices",
        "severity": "medium",
        "performanceImpact": "latency",
        "description": (
            "Table or matrix visuals with more than 10 columns generate wide "
            "SELECT statements with many columns, increasing data transfer and "
            "rendering time. Each additional column adds a GROUP BY dimension."
        ),
        "recommendation": (
            "Reduce column count to essential fields. Use drillthrough pages "
            "for detail views. Consider splitting into focused visuals."
        ),
    },
    {
        "ruleId": "V07",
        "title": "Measure-heavy card pages",
        "severity": "low",
        "performanceImpact": "latency",
        "description": (
            "Pages with more than 8 card or multi-row card visuals each fire a "
            "separate DAX query. On page load all queries execute in parallel, "
            "competing for capacity CU."
        ),
        "recommendation": (
            "Consolidate cards into a multi-row card or a single matrix visual. "
            "Use conditional formatting to highlight key values."
        ),
    },
    {
        "ruleId": "V08",
        "title": "Large embedded images",
        "severity": "low",
        "performanceImpact": "latency",
        "description": (
            "Image visuals and background images are embedded in the report "
            "definition and transferred on every page load. Large images "
            "increase PBIX file size and slow initial render."
        ),
        "recommendation": (
            "Use URL-referenced images hosted on a CDN instead of embedded "
            "images. Compress background images to <200 KB."
        ),
    },
]

RULES_BY_ID = {r["ruleId"]: r for r in RULES}

MAX_EXAMPLES_PER_RULE = 5


# ── JSON parsing helpers ──

def _safe_parse_json(text: str, context: str = "") -> dict | list | None:
    """Parse a JSON string, returning None on failure."""
    if not text or not isinstance(text, str):
        return None
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError) as exc:
        if context:
            print(f"  WARNING: Could not parse JSON in {context}: {exc}", file=sys.stderr)
        return None


def _read_layout_file(path: Path) -> dict | None:
    """Read and parse a Layout file (may lack .json extension)."""
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            text = f.read()
        return json.loads(text)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  WARNING: Could not parse Layout file {path}: {exc}", file=sys.stderr)
        return None


def _get_visual_type(vc: dict) -> str | None:
    """Extract the visual type from a visual container's config string."""
    config = _safe_parse_json(vc.get("config", ""))
    if not config:
        return None
    single_visual = config.get("singleVisual", {})
    return single_visual.get("visualType")


def _get_visual_name(vc: dict) -> str:
    """Extract the visual name/id from a visual container's config string."""
    config = _safe_parse_json(vc.get("config", ""))
    if not config:
        return "unknown"
    return config.get("name", "unknown")


def _get_visual_projections(vc: dict) -> dict:
    """Extract projections from a visual container's parsed config."""
    config = _safe_parse_json(vc.get("config", ""))
    if not config:
        return {}
    single_visual = config.get("singleVisual", {})
    return single_visual.get("projections", {})


def _has_query(vc: dict) -> bool:
    """Check whether a visual container has an associated query."""
    query_str = vc.get("query", "")
    if not query_str or not isinstance(query_str, str):
        return False
    query = _safe_parse_json(query_str)
    if not query:
        return False
    commands = query.get("Commands", [])
    return len(commands) > 0


def _count_projection_columns(vc: dict) -> int:
    """Count the total number of projected columns in a table/matrix visual."""
    projections = _get_visual_projections(vc)
    if not projections:
        return 0

    visual_type = _get_visual_type(vc)
    count = 0

    if visual_type == "pivotTable":
        # For pivot tables, count Rows + Columns + Values
        for key in ("Rows", "Columns", "Values"):
            items = projections.get(key, [])
            if isinstance(items, list):
                count += len(items)
    else:
        # For tableEx and others, count Values
        values = projections.get("Values", [])
        if isinstance(values, list):
            count += len(values)
        # Also check Rows if present (some tables use Rows)
        rows = projections.get("Rows", [])
        if isinstance(rows, list):
            count += len(rows)

    return count


def _is_date_field(query_ref: str) -> bool:
    """Heuristic: check if a query reference looks like a date field."""
    lower = query_ref.lower()
    date_indicators = [
        "date", "calendar", "year", "month", "day", "week",
        "quarter", "period", "time", "fiscal",
    ]
    return any(indicator in lower for indicator in date_indicators)


_SLICER_TYPES = {"slicer", "advancedSlicerVisual", "chicletSlicer", "timeline"}


def _section_has_date_slicer(visual_containers: list[dict]) -> bool:
    """Check whether any slicer on the page references a date-type field."""
    for vc in visual_containers:
        vtype = _get_visual_type(vc)
        if vtype not in _SLICER_TYPES:
            continue

        # Check projections for date fields
        projections = _get_visual_projections(vc)
        for key, items in projections.items():
            if not isinstance(items, list):
                continue
            for item in items:
                query_ref = ""
                if isinstance(item, dict):
                    query_ref = item.get("queryRef", "")
                elif isinstance(item, str):
                    query_ref = item
                if _is_date_field(query_ref):
                    return True

        # Also check the slicer's filter definition
        filters_str = vc.get("filters", "")
        filters = _safe_parse_json(filters_str)
        if isinstance(filters, list):
            for f in filters:
                condition = f.get("filter", {}) if isinstance(f, dict) else {}
                # Walk through filter looking for date references
                filter_json = json.dumps(condition)
                if any(ind in filter_json.lower() for ind in ["date", "calendar", "year"]):
                    return True

    return False


def _section_is_hidden(section: dict) -> bool:
    """Check if a section is hidden (visibility: 1 in its config)."""
    config = _safe_parse_json(section.get("config", ""))
    if not config:
        return False
    return config.get("visibility", 0) == 1


def _has_background_image(section: dict) -> bool:
    """Check if a section has a background image in its config."""
    config = _safe_parse_json(section.get("config", ""))
    if not config:
        return False
    # Background image can appear in various config paths
    config_json = json.dumps(config).lower()
    return "backgroundimage" in config_json or "background_image" in config_json


# ── Rule implementations ──

def _check_v01(sections: list[dict]) -> list[dict]:
    """V01: Too many visuals per page (threshold: 15)."""
    threshold = 15
    examples = []
    for section in sections:
        display_name = section.get("displayName", section.get("name", "Unknown"))
        vc_count = len(section.get("visualContainers", []))
        if vc_count > threshold:
            examples.append({
                "page": display_name,
                "detail": f"{vc_count} visual containers (threshold: {threshold})",
                "recommendation": (
                    "Split into sub-pages or use bookmarks to show/hide "
                    "groups of visuals"
                ),
            })
    return examples


def _check_v02(sections: list[dict]) -> list[dict]:
    """V02: No date slicer on data-heavy pages."""
    examples = []
    data_heavy_types = {"tableEx", "pivotTable"}

    for section in sections:
        display_name = section.get("displayName", section.get("name", "Unknown"))
        vcs = section.get("visualContainers", [])

        has_data_heavy = False
        for vc in vcs:
            vtype = _get_visual_type(vc)
            if vtype in data_heavy_types:
                has_data_heavy = True
                break

        if not has_data_heavy:
            continue

        if not _section_has_date_slicer(vcs):
            # List the data-heavy visual types found
            found_types = set()
            for vc in vcs:
                vtype = _get_visual_type(vc)
                if vtype in data_heavy_types:
                    found_types.add(vtype)

            type_label = ", ".join(sorted(found_types))
            examples.append({
                "page": display_name,
                "detail": (
                    f"Page contains {type_label} visual(s) but no date slicer"
                ),
                "recommendation": (
                    "Add a date slicer or relative date filter to enable "
                    "partition pruning on DirectQuery tables"
                ),
            })

    return examples


def _check_v03(layout: dict) -> list[dict]:
    """V03: Excessive report-level filters (threshold: 10)."""
    threshold = 10
    examples = []

    filters_str = layout.get("filters", "")
    filters = _safe_parse_json(filters_str, context="report-level filters")
    if not isinstance(filters, list):
        return examples

    count = len(filters)
    if count > threshold:
        examples.append({
            "page": "(report-level)",
            "detail": f"{count} report-level filters (threshold: {threshold})",
            "recommendation": (
                "Consolidate report-level filters. Move page-specific "
                "filters to page level"
            ),
        })

    return examples


def _check_v04(sections: list[dict]) -> list[dict]:
    """V04: Hidden pages with active visuals."""
    examples = []

    for section in sections:
        display_name = section.get("displayName", section.get("name", "Unknown"))

        if not _section_is_hidden(section):
            continue

        vcs = section.get("visualContainers", [])
        active_count = 0
        for vc in vcs:
            if _has_query(vc):
                active_count += 1

        if active_count > 0:
            examples.append({
                "page": display_name,
                "detail": (
                    f"Hidden page with {active_count} visual(s) containing "
                    f"active queries"
                ),
                "recommendation": (
                    "Remove the hidden page or strip its visuals if no "
                    "longer needed"
                ),
            })

    return examples


def _check_v05(layout: dict) -> list[dict]:
    """V05: Auto-refresh or suboptimal query reduction settings."""
    examples = []

    config_str = layout.get("config", "")
    config = _safe_parse_json(config_str, context="report config")
    if not config:
        return examples

    slow_ds = config.get("slowDataSourceSettings")
    if not slow_ds or not isinstance(slow_ds, dict):
        return examples

    # Auto-refresh (genuine automatic page refresh interval)
    if "refreshInterval" in slow_ds:
        interval = slow_ds.get("refreshInterval", "unknown")
        examples.append({
            "page": "(report-level)",
            "detail": f"Auto-refresh enabled with interval: {interval}",
            "recommendation": (
                "Disable automatic page refresh unless near-real-time "
                "data is genuinely required. For daily snapshot data, "
                "auto-refresh adds query load with no benefit."
            ),
        })

    # Query reduction settings (separate from auto-refresh)
    qr_findings = []
    if not slow_ds.get("isCrossHighlightingDisabled", True):
        qr_findings.append(
            "Cross-highlighting enabled — hovering on visuals generates "
            "additional DirectQuery queries"
        )
    if not slow_ds.get("isSlicerSelectionsButtonEnabled", False):
        qr_findings.append(
            "Slicer 'Apply' button disabled — each slicer selection "
            "immediately fires queries instead of batching"
        )
    if not slow_ds.get("isFilterSelectionsButtonEnabled", False):
        qr_findings.append(
            "Filter 'Apply' button disabled — each filter change "
            "immediately fires queries"
        )

    if qr_findings:
        examples.append({
            "page": "(report-level)",
            "detail": (
                f"Query reduction settings not fully optimised: "
                f"{'; '.join(qr_findings)}"
            ),
            "recommendation": (
                "In PBI Desktop → File → Options → Report settings → "
                "Query reduction: consider disabling cross-highlighting "
                "and enabling 'Apply' buttons on slicers and filters to "
                "reduce DirectQuery query volume during user interaction."
            ),
        })

    return examples


def _check_v06(sections: list[dict]) -> list[dict]:
    """V06: Wide tables/matrices (>10 columns in projections)."""
    threshold = 10
    examples = []
    wide_types = {"tableEx", "pivotTable"}

    for section in sections:
        display_name = section.get("displayName", section.get("name", "Unknown"))
        vcs = section.get("visualContainers", [])

        for vc in vcs:
            vtype = _get_visual_type(vc)
            if vtype not in wide_types:
                continue

            col_count = _count_projection_columns(vc)
            if col_count > threshold:
                visual_name = _get_visual_name(vc)
                type_label = "matrix" if vtype == "pivotTable" else "table"
                examples.append({
                    "page": display_name,
                    "detail": (
                        f"{type_label} visual '{visual_name}' has "
                        f"{col_count} projected columns (threshold: {threshold})"
                    ),
                    "recommendation": (
                        "Reduce column count to essential fields. Use "
                        "drillthrough pages for detail views"
                    ),
                })

    return examples


def _check_v07(sections: list[dict]) -> list[dict]:
    """V07: Measure-heavy card pages (>8 card/multiRowCard visuals)."""
    threshold = 8
    examples = []
    card_types = {"card", "multiRowCard"}

    for section in sections:
        display_name = section.get("displayName", section.get("name", "Unknown"))
        vcs = section.get("visualContainers", [])

        card_count = 0
        for vc in vcs:
            vtype = _get_visual_type(vc)
            if vtype in card_types:
                card_count += 1

        if card_count > threshold:
            examples.append({
                "page": display_name,
                "detail": (
                    f"{card_count} card/multiRowCard visuals "
                    f"(threshold: {threshold})"
                ),
                "recommendation": (
                    "Consolidate cards into a multi-row card or a single "
                    "matrix visual to reduce parallel query count"
                ),
            })

    return examples


def _check_v08(sections: list[dict]) -> list[dict]:
    """V08: Large embedded images."""
    examples = []

    for section in sections:
        display_name = section.get("displayName", section.get("name", "Unknown"))
        vcs = section.get("visualContainers", [])

        image_visuals = []
        for vc in vcs:
            vtype = _get_visual_type(vc)
            if vtype == "image":
                visual_name = _get_visual_name(vc)
                image_visuals.append(visual_name)

        has_bg = _has_background_image(section)

        findings = []
        if image_visuals:
            findings.append(
                f"{len(image_visuals)} image visual(s)"
            )
        if has_bg:
            findings.append("background image configured")

        if findings:
            examples.append({
                "page": display_name,
                "detail": "; ".join(findings),
                "recommendation": (
                    "Use URL-referenced images hosted on a CDN instead of "
                    "embedded images. Compress backgrounds to <200 KB"
                ),
            })

    return examples


# ── Report analysis ──

def _derive_report_name(layout: dict, source_path: Path) -> str:
    """Derive a human-readable report name from the layout or file path."""
    # Try the report config for a title
    config_str = layout.get("config", "")
    config = _safe_parse_json(config_str)
    if config and isinstance(config, dict):
        # Some layouts store the report name in config
        name = config.get("reportName") or config.get("name")
        if name:
            return name

    # Fall back to parent directory name
    parent = source_path.parent
    if parent.name and parent.name not in (".", ""):
        return parent.name

    return source_path.name


def analyse_layout(layout: dict, source_path: Path) -> dict:
    """Analyse a single Layout file and return a report-level result."""
    report_name = _derive_report_name(layout, source_path)
    sections = layout.get("sections", [])

    total_pages = len(sections)
    total_visuals = sum(
        len(s.get("visualContainers", [])) for s in sections
    )
    hidden_pages = sum(1 for s in sections if _section_is_hidden(s))

    print(f"  Analysing '{report_name}': {total_pages} pages, "
          f"{total_visuals} visuals, {hidden_pages} hidden pages",
          file=sys.stderr)

    # Run all rules
    rule_checks = {
        "V01": _check_v01(sections),
        "V02": _check_v02(sections),
        "V03": _check_v03(layout),
        "V04": _check_v04(sections),
        "V05": _check_v05(layout),
        "V06": _check_v06(sections),
        "V07": _check_v07(sections),
        "V08": _check_v08(sections),
    }

    rule_results = []
    passing_rules = []
    severity_counts = {"high": 0, "medium": 0, "low": 0}

    for rule_id in sorted(rule_checks.keys()):
        examples = rule_checks[rule_id]
        rule_def = RULES_BY_ID[rule_id]

        if examples:
            # Cap examples
            capped = examples[:MAX_EXAMPLES_PER_RULE]
            rule_results.append({
                "ruleId": rule_id,
                "title": rule_def["title"],
                "severity": rule_def["severity"],
                "count": len(examples),
                "examples": capped,
            })
            severity_counts[rule_def["severity"]] += len(examples)
        else:
            passing_rules.append({
                "ruleId": rule_id,
                "title": rule_def["title"],
            })

    total_findings = sum(severity_counts.values())

    return {
        "reportName": report_name,
        "sourcePath": str(source_path),
        "totalPages": total_pages,
        "totalVisuals": total_visuals,
        "hiddenPages": hidden_pages,
        "ruleResults": rule_results,
        "passingRules": passing_rules,
        "summary": {
            "high": severity_counts["high"],
            "medium": severity_counts["medium"],
            "low": severity_counts["low"],
            "totalFindings": total_findings,
        },
    }


# ── Layout file discovery ──

def _find_layout_files(layout_dir: Path) -> list[Path]:
    """Find all files named 'Layout' (no extension) in a directory tree."""
    results = []
    if not layout_dir.is_dir():
        return results

    # Check for Layout file directly in the directory
    direct = layout_dir / "Layout"
    if direct.is_file():
        results.append(direct)

    # Search subdirectories
    for child in sorted(layout_dir.iterdir()):
        if child.is_dir():
            candidate = child / "Layout"
            if candidate.is_file():
                results.append(candidate)
            # Also search one more level deep
            for grandchild in sorted(child.iterdir()):
                if grandchild.is_dir():
                    candidate2 = grandchild / "Layout"
                    if candidate2.is_file():
                        results.append(candidate2)

    # Deduplicate (in case direct and subdir match)
    seen = set()
    deduped = []
    for p in results:
        resolved = p.resolve()
        if resolved not in seen:
            seen.add(resolved)
            deduped.append(p)

    return deduped


# ── Main orchestration ──

def analyse_report_visuals(layout_paths: list[Path]) -> dict:
    """Analyse one or more Layout files and produce the full output."""
    today = datetime.now(timezone.utc).date().isoformat()

    reports = []
    agg_high = 0
    agg_medium = 0
    agg_low = 0
    agg_pages = 0
    agg_visuals = 0

    for lpath in layout_paths:
        print(f"\nProcessing: {lpath}", file=sys.stderr)
        layout = _read_layout_file(lpath)
        if layout is None:
            print(f"  SKIPPED: Could not parse {lpath}", file=sys.stderr)
            continue

        report = analyse_layout(layout, lpath)
        reports.append(report)

        agg_pages += report["totalPages"]
        agg_visuals += report["totalVisuals"]
        agg_high += report["summary"]["high"]
        agg_medium += report["summary"]["medium"]
        agg_low += report["summary"]["low"]

    total_findings = agg_high + agg_medium + agg_low

    return {
        "analysisDate": today,
        "reports": reports,
        "rules": RULES,
        "summary": {
            "reportsAnalysed": len(reports),
            "totalPages": agg_pages,
            "totalVisuals": agg_visuals,
            "high": agg_high,
            "medium": agg_medium,
            "low": agg_low,
            "totalFindings": total_findings,
        },
    }


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Analyse PBIX Layout JSON files for visual-layer performance issues"
        )
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--layout-path", type=Path,
        help="Path to a single Layout file (extracted from .pbix)",
    )
    group.add_argument(
        "--layout-dir", type=Path,
        help=(
            "Path to a directory containing Layout files in subdirectories "
            "(scans for files named 'Layout')"
        ),
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output directory for visual-analysis.json",
    )
    args = parser.parse_args()

    output_dir = args.output.resolve()

    # Resolve layout file(s)
    layout_paths: list[Path] = []
    if args.layout_path:
        resolved = args.layout_path.resolve()
        if not resolved.is_file():
            print(
                f"ERROR: Layout file not found: {resolved}",
                file=sys.stderr,
            )
            sys.exit(1)
        layout_paths.append(resolved)
    elif args.layout_dir:
        resolved_dir = args.layout_dir.resolve()
        if not resolved_dir.is_dir():
            print(
                f"ERROR: Layout directory not found: {resolved_dir}",
                file=sys.stderr,
            )
            sys.exit(1)
        layout_paths = _find_layout_files(resolved_dir)
        if not layout_paths:
            print(
                f"ERROR: No Layout files found in {resolved_dir}",
                file=sys.stderr,
            )
            sys.exit(1)
        print(
            f"Found {len(layout_paths)} Layout file(s) in {resolved_dir}",
            file=sys.stderr,
        )

    # Run analysis
    print("\n=== Report Visual Analysis ===", file=sys.stderr)
    result = analyse_report_visuals(layout_paths)

    # Write output
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "visual-analysis.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    # Print summary to stderr
    summary = result["summary"]
    print(f"\n=== Summary ===", file=sys.stderr)
    print(f"  Reports analysed: {summary['reportsAnalysed']}", file=sys.stderr)
    print(f"  Total pages: {summary['totalPages']}", file=sys.stderr)
    print(f"  Total visuals: {summary['totalVisuals']}", file=sys.stderr)
    print(
        f"  Findings: {summary['high']} high, {summary['medium']} medium, "
        f"{summary['low']} low ({summary['totalFindings']} total)",
        file=sys.stderr,
    )

    for report in result["reports"]:
        print(f"\n  {report['reportName']}:", file=sys.stderr)
        print(
            f"    {report['totalPages']} pages, "
            f"{report['totalVisuals']} visuals, "
            f"{report['hiddenPages']} hidden",
            file=sys.stderr,
        )
        for rr in report["ruleResults"]:
            print(
                f"    [{rr['severity'].upper()}] {rr['ruleId']}: "
                f"{rr['title']} ({rr['count']} finding(s))",
                file=sys.stderr,
            )
        if report["passingRules"]:
            passing_ids = ", ".join(r["ruleId"] for r in report["passingRules"])
            print(f"    Passing: {passing_ids}", file=sys.stderr)

    print(f"\nWritten: {output_file}", file=sys.stderr)


if __name__ == "__main__":
    main()
