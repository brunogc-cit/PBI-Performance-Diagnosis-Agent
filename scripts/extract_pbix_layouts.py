#!/usr/bin/env python3
"""
Extract and normalise Layout JSON from PBIX report files.

Handles two PBIX internal formats:
  - Legacy: Report/Layout as a single UTF-16-LE encoded JSON blob
  - PBIR (Power BI Enhanced Report): Report/definition/pages/<id>/page.json
    + Report/definition/pages/<id>/visuals/<vid>/visual.json

Produces Layout files compatible with analyse_report_visuals.py.

Usage:
    # Extract specific reports (fuzzy name matching)
    python3 scripts/extract_pbix_layouts.py \
      --reports-dir /path/to/powerbi/reports \
      --report-names "ADE - Trade,ADE - Sales" \
      --output output/

    # Extract ALL .pbix files found
    python3 scripts/extract_pbix_layouts.py \
      --reports-dir /path/to/powerbi/reports \
      --output output/
"""

import argparse
import json
import re
import sys
import zipfile
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path


# ── Name matching ──

COMMON_PREFIXES = [
    r"^ADE\s*[-–—]\s*",
    r"^ASOS\s*[-–—]\s*",
]


def _normalise_name(name: str) -> str:
    """Strip common prefixes and normalise whitespace."""
    result = name.strip()
    for prefix_pattern in COMMON_PREFIXES:
        result = re.sub(prefix_pattern, "", result, flags=re.IGNORECASE)
    return result.strip()


def _match_report_name(
    input_name: str, available_dirs: dict[str, Path]
) -> tuple[str | None, str]:
    """Match an input report name to an available directory.

    Returns (matched_dir_name, confidence) or (None, reason).
    """
    normalised = _normalise_name(input_name)

    # 1. Exact match on normalised name (case-insensitive)
    for dir_name in available_dirs:
        if dir_name.lower() == normalised.lower():
            return dir_name, "exact"

    # 2. Substring containment
    for dir_name in available_dirs:
        if normalised.lower() in dir_name.lower() or dir_name.lower() in normalised.lower():
            return dir_name, "substring"

    # 3. Token overlap (>= 50% of tokens match)
    input_tokens = set(normalised.lower().split())
    best_score = 0.0
    best_dir: str | None = None
    for dir_name in available_dirs:
        dir_tokens = set(dir_name.lower().split())
        if not dir_tokens:
            continue
        overlap = len(input_tokens & dir_tokens) / max(len(input_tokens), len(dir_tokens))
        if overlap > best_score:
            best_score = overlap
            best_dir = dir_name
    if best_score >= 0.5 and best_dir:
        return best_dir, "token-overlap"

    # 4. Fuzzy match via SequenceMatcher (threshold 0.6)
    best_ratio = 0.0
    best_dir = None
    for dir_name in available_dirs:
        ratio = SequenceMatcher(None, normalised.lower(), dir_name.lower()).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_dir = dir_name
    if best_ratio >= 0.6 and best_dir:
        return best_dir, "fuzzy"

    return None, f"No matching directory found (best fuzzy score: {best_ratio:.2f})"


# ── PBIX format detection ──

def _detect_pbix_format(zf: zipfile.ZipFile) -> str:
    """Detect whether a .pbix uses legacy or PBIR format."""
    names = set(zf.namelist())

    if "Report/Layout" in names:
        return "legacy"

    if any(n.startswith("Report/definition/pages/") for n in names):
        return "pbir"

    if "Layout" in names:
        return "bare-legacy"

    return "unknown"


# ── Legacy extraction ──

def _extract_legacy_layout(zf: zipfile.ZipFile, entry: str = "Report/Layout") -> dict:
    """Extract and decode a legacy Layout file from a PBIX ZIP."""
    raw = zf.read(entry)

    # Try UTF-16-LE first (standard PBIX encoding), then UTF-8
    for encoding in ("utf-16-le", "utf-8-sig", "utf-8"):
        try:
            text = raw.decode(encoding)
            return json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue

    raise ValueError(f"Could not decode Layout entry '{entry}' with any known encoding")


# ── PBIR reconstruction ──

def _read_zip_json(zf: zipfile.ZipFile, path: str) -> dict:
    """Read a JSON file from inside a ZIP archive."""
    raw = zf.read(path)
    for encoding in ("utf-8-sig", "utf-8", "utf-16-le"):
        try:
            return json.loads(raw.decode(encoding))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
    raise ValueError(f"Could not decode ZIP entry '{path}'")


def _convert_pbir_visual_to_legacy(v_json: dict) -> dict:
    """Convert a PBIR visual.json to a legacy visualContainer dict.

    The output must have:
      - config: stringified JSON with {name, singleVisual: {visualType, projections}}
      - query: stringified JSON with {Commands: [{SemanticQueryDataShapeCommand: ...}]}
      - filters: stringified JSON array
      - x, y, width, height: position values
    """
    visual_block = v_json.get("visual", {})
    query_state = visual_block.get("query", {}).get("queryState", {})

    # Build projections from queryState
    projections: dict = {}
    for role_name, role_data in query_state.items():
        if isinstance(role_data, dict) and "projections" in role_data:
            proj_list = []
            for proj in role_data["projections"]:
                proj_list.append({"queryRef": proj.get("queryRef", "")})
            projections[role_name] = proj_list

    config_dict = {
        "name": v_json.get("name", ""),
        "singleVisual": {
            "visualType": visual_block.get("visualType", "unknown"),
            "projections": projections,
        },
    }

    # Build query string
    has_projections = any(
        isinstance(v, dict) and v.get("projections")
        for v in query_state.values()
    )
    query_str = ""
    if has_projections:
        query_str = json.dumps(
            {"Commands": [{"SemanticQueryDataShapeCommand": {"Query": query_state}}]}
        )

    # Filters from visual-level filterConfig
    filters = v_json.get("filterConfig", {}).get("filters", [])

    pos = v_json.get("position", {})

    return {
        "config": json.dumps(config_dict),
        "query": query_str,
        "filters": json.dumps(filters) if filters else "",
        "x": pos.get("x", 0),
        "y": pos.get("y", 0),
        "width": pos.get("width", 0),
        "height": pos.get("height", 0),
    }


def _convert_pbir_page_to_legacy_section(
    page_json: dict, visual_containers: list[dict]
) -> dict:
    """Convert a PBIR page.json + visuals to a legacy section dict."""
    page_filters = page_json.get("filterConfig", {}).get("filters", [])

    # Visibility: 0 = visible, 1 = hidden
    visibility = page_json.get("visibility", 0)
    config_dict: dict = {"visibility": visibility}

    return {
        "name": page_json.get("name", ""),
        "displayName": page_json.get("displayName", page_json.get("name", "Unknown")),
        "filters": json.dumps(page_filters),
        "visualContainers": visual_containers,
        "config": json.dumps(config_dict),
        "width": page_json.get("width", 1280),
        "height": page_json.get("height", 720),
    }


def _reconstruct_pbir_layout(zf: zipfile.ZipFile) -> dict:
    """Reconstruct a legacy-compatible Layout from PBIR format."""
    # Read report-level config
    report_json: dict = {}
    try:
        report_json = _read_zip_json(zf, "Report/definition/report.json")
    except (KeyError, ValueError):
        pass

    # Read pages metadata
    pages_meta = _read_zip_json(zf, "Report/definition/pages/pages.json")
    page_order = pages_meta.get("pageOrder", [])

    # Build report-level filters
    report_filters = report_json.get("filterConfig", {}).get("filters", [])

    # Report-level config (for auto-refresh detection etc.)
    report_config: dict = {}
    slow_ds = report_json.get("slowDataSourceSettings")
    if slow_ds:
        report_config["slowDataSourceSettings"] = slow_ds

    sections = []
    for page_id in page_order:
        try:
            page_json = _read_zip_json(
                zf, f"Report/definition/pages/{page_id}/page.json"
            )
        except (KeyError, ValueError) as exc:
            print(
                f"  WARNING: Could not read page {page_id}: {exc}",
                file=sys.stderr,
            )
            continue

        # Find all visuals for this page
        visual_prefix = f"Report/definition/pages/{page_id}/visuals/"
        visual_files = sorted(
            n
            for n in zf.namelist()
            if n.startswith(visual_prefix) and n.endswith("/visual.json")
        )

        visual_containers = []
        for vf in visual_files:
            try:
                v_json = _read_zip_json(zf, vf)
                vc = _convert_pbir_visual_to_legacy(v_json)
                visual_containers.append(vc)
            except (KeyError, ValueError) as exc:
                print(
                    f"  WARNING: Could not read visual {vf}: {exc}",
                    file=sys.stderr,
                )

        section = _convert_pbir_page_to_legacy_section(page_json, visual_containers)
        sections.append(section)

    return {
        "sections": sections,
        "filters": json.dumps(report_filters),
        "config": json.dumps(report_config),
    }


# ── PBIX discovery ──

def _find_pbix_files(reports_dir: Path) -> dict[str, Path]:
    """Find all .pbix files under a reports directory.

    Returns a dict mapping the parent directory name (or stem) to the .pbix path.
    """
    result: dict[str, Path] = {}

    for pbix_path in sorted(reports_dir.rglob("*.pbix")):
        # Use parent directory name if the .pbix is in a subdirectory
        if pbix_path.parent != reports_dir:
            key = pbix_path.parent.name
        else:
            # Top-level .pbix files (e.g., "Supplier Booking Performance.pbix")
            key = pbix_path.stem

        # Avoid duplicates — prefer the first found
        if key not in result:
            result[key] = pbix_path

    return result


# ── Main extraction ──

def extract_pbix_layouts(
    reports_dir: Path,
    report_names: list[str] | None,
    output_dir: Path,
) -> dict:
    """Extract Layout JSON from PBIX files.

    Returns a manifest dict with extraction results.
    """
    extracted_dir = output_dir / "pbix_extracted"
    extracted_dir.mkdir(parents=True, exist_ok=True)

    available = _find_pbix_files(reports_dir)
    print(
        f"Found {len(available)} .pbix file(s) in {reports_dir}",
        file=sys.stderr,
    )

    # Determine which reports to extract
    if report_names:
        targets: list[tuple[str, str | None, str]] = []
        for name in report_names:
            matched_dir, confidence = _match_report_name(name, available)
            targets.append((name, matched_dir, confidence))
    else:
        # Extract all
        targets = [(k, k, "all") for k in available]

    results = []
    extracted_count = 0
    failed_count = 0
    format_counts: dict[str, int] = {}

    for input_name, matched_dir, confidence in targets:
        entry: dict = {
            "inputName": input_name,
            "resolvedDir": matched_dir,
            "pbixPath": None,
            "format": None,
            "layoutPath": None,
            "matched": False,
            "confidence": confidence,
        }

        if not matched_dir or matched_dir not in available:
            entry["reason"] = confidence if "No matching" in confidence else f"No matching .pbix found"
            results.append(entry)
            failed_count += 1
            print(
                f"  SKIP: '{input_name}' — {entry['reason']}",
                file=sys.stderr,
            )
            continue

        pbix_path = available[matched_dir]
        entry["pbixPath"] = str(pbix_path)

        try:
            with zipfile.ZipFile(pbix_path, "r") as zf:
                fmt = _detect_pbix_format(zf)
                entry["format"] = fmt

                if fmt == "legacy":
                    layout = _extract_legacy_layout(zf, "Report/Layout")
                elif fmt == "bare-legacy":
                    layout = _extract_legacy_layout(zf, "Layout")
                elif fmt == "pbir":
                    layout = _reconstruct_pbir_layout(zf)
                else:
                    entry["reason"] = f"Unknown PBIX format (no Layout or PBIR structure found)"
                    results.append(entry)
                    failed_count += 1
                    print(
                        f"  SKIP: '{input_name}' ({pbix_path.name}) — unknown format",
                        file=sys.stderr,
                    )
                    continue

            # Write Layout file
            dest = extracted_dir / matched_dir / "Layout"
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(
                json.dumps(layout, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            entry["layoutPath"] = str(dest)
            entry["matched"] = True
            format_counts[fmt] = format_counts.get(fmt, 0) + 1
            extracted_count += 1

            sections = layout.get("sections", [])
            total_visuals = sum(
                len(s.get("visualContainers", [])) for s in sections
            )
            print(
                f"  OK: '{input_name}' → {matched_dir}/{pbix_path.name} "
                f"[{fmt}] — {len(sections)} pages, {total_visuals} visuals",
                file=sys.stderr,
            )

        except Exception as exc:
            entry["reason"] = str(exc)
            results.append(entry)
            failed_count += 1
            print(
                f"  ERROR: '{input_name}' ({pbix_path.name}) — {exc}",
                file=sys.stderr,
            )
            continue

        results.append(entry)

    manifest = {
        "extractionDate": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportsDir": str(reports_dir),
        "requestedReports": report_names or list(available.keys()),
        "results": results,
        "summary": {
            "requested": len(targets),
            "extracted": extracted_count,
            "failed": failed_count,
            "formats": format_counts,
        },
    }

    return manifest


def main():
    parser = argparse.ArgumentParser(
        description="Extract and normalise Layout JSON from PBIX report files"
    )
    parser.add_argument(
        "--reports-dir",
        required=True,
        type=Path,
        help="Path to powerbi/reports/ directory containing .pbix files",
    )
    parser.add_argument(
        "--report-names",
        type=str,
        default=None,
        help=(
            "Comma-separated report names from input.md "
            '(e.g., "ADE - Trade,ADE - Sales"). '
            "If omitted, extracts ALL .pbix files found."
        ),
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Output directory (e.g., output/)",
    )
    args = parser.parse_args()

    reports_dir = args.reports_dir.resolve()
    if not reports_dir.is_dir():
        print(f"ERROR: Reports directory not found: {reports_dir}", file=sys.stderr)
        sys.exit(1)

    output_dir = args.output.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    report_names: list[str] | None = None
    if args.report_names:
        report_names = [n.strip() for n in args.report_names.split(",") if n.strip()]

    print("\n=== PBIX Layout Extraction ===", file=sys.stderr)
    manifest = extract_pbix_layouts(reports_dir, report_names, output_dir)

    # Write manifest
    manifest_file = output_dir / "pbix-extraction-manifest.json"
    with open(manifest_file, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    summary = manifest["summary"]
    print(f"\n=== Summary ===", file=sys.stderr)
    print(f"  Requested: {summary['requested']}", file=sys.stderr)
    print(f"  Extracted: {summary['extracted']}", file=sys.stderr)
    print(f"  Failed:    {summary['failed']}", file=sys.stderr)
    print(f"  Formats:   {summary['formats']}", file=sys.stderr)
    print(f"\nManifest written: {manifest_file}", file=sys.stderr)

    if summary["extracted"] == 0:
        print("\nWARNING: No layouts were extracted!", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
