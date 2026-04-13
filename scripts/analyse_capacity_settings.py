#!/usr/bin/env python3
"""
Capacity Settings Analyser for Fabric / Power BI.

Analyses query history to simulate the impact of different Fabric capacity
management setting thresholds. Helps capacity admins decide optimal values
for query timeout, memory limit, and row set counts.

Usage:
    python3 analyse_capacity_settings.py \
      --query-data output/query-history-export.json \
      --taxonomy output/model-taxonomy.json \
      --output output/
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from statistics import median


# ── Threshold constants ──

TIMEOUT_THRESHOLDS_SECONDS = [300, 225, 180, 120, 60, 30]

DURATION_BUCKETS = [
    ("0-10s", 0, 10_000),
    ("10-30s", 10_000, 30_000),
    ("30-60s", 30_000, 60_000),
    ("1-2min", 60_000, 120_000),
    ("2-3.75min", 120_000, 225_000),
    (">3.75min", 225_000, float("inf")),
]

ROW_SET_THRESHOLDS = [1_000_000, 500_000, 100_000]

ROWS_PRODUCED_BUCKETS = [
    ("0-1K", 0, 1_000),
    ("1K-10K", 1_000, 10_000),
    ("10K-100K", 10_000, 100_000),
    ("100K-1M", 100_000, 1_000_000),
    (">1M", 1_000_000, float("inf")),
]


def _read_json(path: Path) -> dict | list | None:
    """Read a JSON file, returning None on failure."""
    if not path.is_file():
        return None
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  WARNING: Could not parse {path}: {exc}", file=sys.stderr)
        return None


def _safe_int(val, default: int | None = None) -> int | None:
    """Safely convert a value to int."""
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _safe_float(val, default: float | None = None) -> float | None:
    """Safely convert a value to float."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _parse_date(iso_str: str | None) -> datetime | None:
    """Parse an ISO date string, returning None on failure."""
    if not iso_str:
        return None
    try:
        # Handle various ISO formats
        cleaned = iso_str.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned)
    except (ValueError, TypeError):
        return None


def _round_pct(count: int, total: int, decimals: int = 3) -> float:
    """Calculate percentage rounded to given decimal places."""
    if total == 0:
        return 0.0
    return round(count / total * 100, decimals)


def _compute_period(queries: list[dict]) -> dict:
    """Determine the date range covered by the query data."""
    dates: list[datetime] = []
    for q in queries:
        dt = _parse_date(q.get("start_time"))
        if dt:
            dates.append(dt)

    if not dates:
        today = datetime.now(timezone.utc).date().isoformat()
        return {"startDate": today, "endDate": today}

    dates.sort()
    return {
        "startDate": dates[0].date().isoformat(),
        "endDate": dates[-1].date().isoformat(),
    }


# ── Query Timeout Analysis ──

def _analyse_query_timeout(queries: list[dict]) -> dict:
    """Simulate query timeout settings against actual query durations."""
    durations_ms: list[int] = []
    for q in queries:
        d = _safe_int(q.get("total_duration_ms"))
        if d is not None and d >= 0:
            durations_ms.append(d)

    total = len(durations_ms)
    print(f"  Query timeout analysis: {total} queries with valid duration data", file=sys.stderr)

    # Simulations for each threshold
    simulations: list[dict] = []
    for threshold_s in TIMEOUT_THRESHOLDS_SECONDS:
        threshold_ms = threshold_s * 1000
        impacted = [d for d in durations_ms if d > threshold_ms]
        count = len(impacted)
        total_impact_ms = sum(impacted)
        simulations.append({
            "timeoutSeconds": threshold_s,
            "queriesImpacted": count,
            "pctImpacted": _round_pct(count, total),
            "totalDurationImpactedMs": total_impact_ms,
        })

    # Duration distribution
    distribution: list[dict] = []
    for label, lo, hi in DURATION_BUCKETS:
        count = sum(1 for d in durations_ms if lo <= d < hi)
        distribution.append({
            "bucket": label,
            "count": count,
            "pct": round(count / total * 100, 1) if total > 0 else 0.0,
        })

    # Recommendation logic: find lowest threshold that impacts < 0.1% of queries
    recommended = 225  # safe default
    for sim in simulations:
        if sim["pctImpacted"] < 0.1:
            recommended = sim["timeoutSeconds"]

    # Build rationale
    rec_sim = next((s for s in simulations if s["timeoutSeconds"] == recommended), None)
    if rec_sim:
        rationale = (
            f"Only {rec_sim['pctImpacted']}% of queries ({rec_sim['queriesImpacted']} total) "
            f"exceed {recommended}s. Follow Luke's methodology: start at 225s, monitor 2-5 days, "
            f"then tighten."
        )
    else:
        rationale = "Start at 225s as the PBI report default, monitor, then adjust."

    # Summary statistics for notes
    if durations_ms:
        median_ms = median(durations_ms)
        p95_idx = int(len(durations_ms) * 0.95)
        sorted_d = sorted(durations_ms)
        p95_ms = sorted_d[min(p95_idx, len(sorted_d) - 1)]
        max_ms = max(durations_ms)
        print(
            f"  Duration stats: median={median_ms:.0f}ms, P95={p95_ms:.0f}ms, max={max_ms:.0f}ms",
            file=sys.stderr,
        )

    return {
        "settingName": "Query Timeout (seconds)",
        "currentDefault": 3600,
        "pbiReportDefault": 225,
        "description": (
            "An integer that defines the timeout, in seconds, for queries. "
            "Default is 3600 seconds (60 minutes). PBI reports override this "
            "with ~180 seconds."
        ),
        "simulations": simulations,
        "durationDistribution": distribution,
        "recommendation": {
            "value": recommended,
            "rationale": rationale,
            "lukeMethodology": (
                "Lower Query Timeout first. If still allowing massive queries through, "
                "lower to 60s. Adjust accordingly — it will take monitoring."
            ),
        },
        "notes": [
            "PBI report default timeout is 225 seconds. Capacity timeout only affects PBI if set below 225s.",
            "SSAS/AAS default is 600 seconds. Setting capacity timeout below 600s affects SSAS/AAS queries.",
            "External tools (XMLA, Analyse in Excel) have no default limit — capacity timeout applies directly.",
            "Workspace-level timeout must be configured via SSMS. Tenant admins cannot enforce it across workspaces.",
        ],
    }


# ── Row Set Count Analysis ──

def _analyse_row_set_counts(queries: list[dict]) -> tuple[dict, dict]:
    """Simulate intermediate and result row set count limits."""
    rows_produced: list[int] = []
    for q in queries:
        r = _safe_int(q.get("rows_produced"))
        if r is not None and r >= 0:
            rows_produced.append(r)

    total = len(rows_produced)
    print(f"  Row set count analysis: {total} queries with valid rows_produced data", file=sys.stderr)

    # Intermediate row set simulations
    intermediate_sims: list[dict] = []
    for limit in ROW_SET_THRESHOLDS:
        impacted = [r for r in rows_produced if r > limit]
        count = len(impacted)
        intermediate_sims.append({
            "limit": limit,
            "queriesImpacted": count,
            "pctImpacted": _round_pct(count, total),
        })

    # Rows produced distribution
    distribution: list[dict] = []
    for label, lo, hi in ROWS_PRODUCED_BUCKETS:
        count = sum(1 for r in rows_produced if lo <= r < hi)
        distribution.append({
            "bucket": label,
            "count": count,
            "pct": round(count / total * 100, 1) if total > 0 else 0.0,
        })

    # Recommendation for intermediate row set count
    over_1m = sum(1 for r in rows_produced if r > 1_000_000)
    pct_over_1m = _round_pct(over_1m, total)
    intermediate = {
        "settingName": "Max Intermediate Row Set Count",
        "currentDefault": 1_000_000,
        "allowedRange": [100_000, 2_147_483_647],
        "description": (
            "Max number of intermediate rows returned by DirectQuery. "
            "Prevents massive cross-joins from consuming resources."
        ),
        "simulations": intermediate_sims,
        "rowsProducedDistribution": distribution,
        "recommendation": {
            "value": 1_000_000,
            "rationale": (
                f"Default of 1M is appropriate. Only {pct_over_1m}% of queries "
                f"({over_1m} total) return >1M rows."
            ),
        },
    }

    # Result row set count (same data, different framing)
    result_sims: list[dict] = []
    for limit in ROW_SET_THRESHOLDS:
        impacted = [r for r in rows_produced if r > limit]
        count = len(impacted)
        result_sims.append({
            "limit": limit,
            "queriesImpacted": count,
            "pctImpacted": _round_pct(count, total),
        })

    result_rowset = {
        "settingName": "Max Result Row Set Count",
        "currentDefault": 1_000_000,
        "allowedRange": [100_000, 2_147_483_647],
        "description": (
            "Max number of rows returned by any individual DAX query. "
            "Prevents expensive queries from consuming resources."
        ),
        "simulations": result_sims,
        "recommendation": {
            "value": 1_000_000,
            "rationale": "Default is appropriate for current workload.",
        },
    }

    return intermediate, result_rowset


# ── Query Memory Limit ──

def _analyse_query_memory_limit() -> dict:
    """Produce memory limit recommendation (not data-driven — expert guidance)."""
    return {
        "settingName": "Query Memory Limit (%)",
        "currentDefault": None,
        "description": (
            "Limits how much memory can be used by temporary results during "
            "a DAX query. Specified as percentage."
        ),
        "lukeRecommendation": 10,
        "warning": (
            "Too low stops large optimised reports from working. "
            "Start at 10%, monitor, adjust to 5% if needed."
        ),
        "recommendation": {
            "value": 10,
            "rationale": (
                "Start at 10% as recommended by Microsoft specialist. "
                "Monitor for 2-5 business days. If massive queries still "
                "pass through, lower to 5%."
            ),
        },
    }


# ── Max Offline Dataset Size ──

def _analyse_offline_dataset_size(taxonomy: dict | None) -> dict:
    """Estimate model sizes from taxonomy table data."""
    model_size_estimates: list[dict] = []

    if taxonomy and isinstance(taxonomy, dict):
        tables = taxonomy.get("tables", [])
        total_size_gb = 0.0
        model_name = taxonomy.get("modelName", "Unknown Model")

        for t in tables:
            # Look for volumetry data (sizeGB from enrichment)
            size_gb = _safe_float(t.get("sizeGB"))
            if size_gb is not None:
                total_size_gb += size_gb

        if total_size_gb > 0:
            model_size_estimates.append({
                "model": model_name,
                "estimatedSizeGB": round(total_size_gb, 3),
                "note": f"Sum of table sizes from taxonomy ({len(tables)} tables)",
            })
            print(
                f"  Offline dataset size: {model_name} estimated at {total_size_gb:.3f} GB",
                file=sys.stderr,
            )
        else:
            print(
                "  Offline dataset size: no volumetry data in taxonomy, using defaults",
                file=sys.stderr,
            )

    # Determine recommended value
    if model_size_estimates:
        max_size = max(e["estimatedSizeGB"] for e in model_size_estimates)
        # Provide headroom: round up to next sensible value
        if max_size <= 1:
            recommended = 3
        elif max_size <= 3:
            recommended = 5
        elif max_size <= 5:
            recommended = 8
        else:
            recommended = 10
        rationale = (
            f"Largest model is {max_size:.3f} GB. Setting to {recommended} GB "
            f"provides headroom for growth while preventing very large datasets."
        )
    else:
        recommended = 5
        rationale = (
            "No model size data available. Default of 5 GB is a conservative "
            "starting point — adjust based on actual model sizes from Fabric admin."
        )

    return {
        "settingName": "Max Offline Dataset Size (GB)",
        "currentDefault": "Set by SKU",
        "allowedRange": [0.1, 10],
        "description": (
            "Maximum size of the offline dataset in memory. "
            "Prevents large datasets from consuming capacity memory."
        ),
        "modelSizeEstimates": model_size_estimates,
        "recommendation": {
            "value": recommended,
            "rationale": rationale,
        },
    }


# ── Main orchestration ──

def analyse_capacity_settings(
    queries: list[dict],
    taxonomy: dict | None,
) -> dict:
    """Run all capacity setting simulations and return the full analysis."""
    total = len(queries)
    period = _compute_period(queries)
    today = datetime.now(timezone.utc).date().isoformat()

    print(f"\n=== Capacity Settings Analysis ===", file=sys.stderr)
    print(f"  Total queries: {total}", file=sys.stderr)
    print(f"  Period: {period['startDate']} to {period['endDate']}", file=sys.stderr)

    query_timeout = _analyse_query_timeout(queries)
    intermediate_rowset, result_rowset = _analyse_row_set_counts(queries)
    memory_limit = _analyse_query_memory_limit()
    offline_size = _analyse_offline_dataset_size(taxonomy)

    return {
        "analysisDate": today,
        "totalQueriesAnalysed": total,
        "period": period,
        "queryTimeout": query_timeout,
        "queryMemoryLimit": memory_limit,
        "maxIntermediateRowSetCount": intermediate_rowset,
        "maxResultRowSetCount": result_rowset,
        "maxOfflineDatasetSizeGB": offline_size,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Analyse query history to simulate Fabric capacity setting thresholds"
    )
    parser.add_argument(
        "--query-data", required=True, type=Path,
        help="Path to query-history-export.json",
    )
    parser.add_argument(
        "--taxonomy", type=Path, default=None,
        help="Path to model-taxonomy.json (optional, for dataset size estimation)",
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output directory for capacity-settings-analysis.json",
    )
    args = parser.parse_args()

    query_data_path = args.query_data.resolve()
    output_dir = args.output.resolve()

    if not query_data_path.is_file():
        print(f"ERROR: Query data file not found: {query_data_path}", file=sys.stderr)
        sys.exit(1)

    # Read query history
    print(f"Reading query data from {query_data_path}...", file=sys.stderr)
    queries_raw = _read_json(query_data_path)
    if queries_raw is None:
        print(f"ERROR: Cannot parse query data: {query_data_path}", file=sys.stderr)
        sys.exit(1)

    if isinstance(queries_raw, dict):
        # Handle wrapped format: {"queries": [...]} or similar
        queries = (
            queries_raw.get("queries")
            or queries_raw.get("data")
            or queries_raw.get("results")
            or []
        )
        if not isinstance(queries, list):
            queries = []
    elif isinstance(queries_raw, list):
        queries = queries_raw
    else:
        print(f"ERROR: Unexpected format in {query_data_path}", file=sys.stderr)
        sys.exit(1)

    if not queries:
        print("ERROR: No query records found in the input file", file=sys.stderr)
        sys.exit(1)

    # Read optional taxonomy
    taxonomy: dict | None = None
    if args.taxonomy:
        taxonomy_path = args.taxonomy.resolve()
        if taxonomy_path.is_file():
            taxonomy = _read_json(taxonomy_path)
            if taxonomy:
                print(f"Loaded taxonomy from {taxonomy_path}", file=sys.stderr)
            else:
                print(f"  WARNING: Could not parse taxonomy: {taxonomy_path}", file=sys.stderr)
        else:
            print(f"  WARNING: Taxonomy file not found: {taxonomy_path} (skipping)", file=sys.stderr)

    # Run analysis
    result = analyse_capacity_settings(queries, taxonomy)

    # Write output
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "capacity-settings-analysis.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    # Print summary to stderr
    print(f"\n=== Summary ===", file=sys.stderr)
    print(f"  Queries analysed: {result['totalQueriesAnalysed']}", file=sys.stderr)
    print(f"  Period: {result['period']['startDate']} to {result['period']['endDate']}", file=sys.stderr)

    qt = result["queryTimeout"]
    print(f"\n  Query Timeout:", file=sys.stderr)
    print(f"    Recommended: {qt['recommendation']['value']}s", file=sys.stderr)
    for sim in qt["simulations"]:
        print(
            f"    {sim['timeoutSeconds']:>4}s → {sim['queriesImpacted']} queries impacted "
            f"({sim['pctImpacted']}%)",
            file=sys.stderr,
        )

    qm = result["queryMemoryLimit"]
    print(f"\n  Query Memory Limit:", file=sys.stderr)
    print(f"    Recommended: {qm['recommendation']['value']}%", file=sys.stderr)

    ir = result["maxIntermediateRowSetCount"]
    print(f"\n  Max Intermediate Row Set Count:", file=sys.stderr)
    print(f"    Recommended: {ir['recommendation']['value']:,}", file=sys.stderr)
    for sim in ir["simulations"]:
        print(
            f"    {sim['limit']:>10,} → {sim['queriesImpacted']} queries impacted "
            f"({sim['pctImpacted']}%)",
            file=sys.stderr,
        )

    od = result["maxOfflineDatasetSizeGB"]
    print(f"\n  Max Offline Dataset Size:", file=sys.stderr)
    print(f"    Recommended: {od['recommendation']['value']} GB", file=sys.stderr)
    for est in od.get("modelSizeEstimates", []):
        print(f"    {est['model']}: {est['estimatedSizeGB']} GB", file=sys.stderr)

    print(f"\nWritten: {output_file}", file=sys.stderr)


if __name__ == "__main__":
    main()
