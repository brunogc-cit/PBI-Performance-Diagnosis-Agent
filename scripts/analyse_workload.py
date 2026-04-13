#!/usr/bin/env python3
"""
Workload & Capacity Scaling Analyser for Fabric / Power BI.

Analyses CU consumption patterns by workspace, recommends surge protection
thresholds, workload isolation strategies, and evaluates capacity scaling
decisions. Combines Phase 3 (workload/surge protection) and Phase 10
(capacity scaling) from the improvement plan.

Usage:
    python3 scripts/analyse_workload.py \
      --query-data output/query-history-export.json \
      --capacity-config output/capacity-config.json \
      --output output/
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def _safe_int(val, default: int = 0) -> int:
    """Safely convert a value to int."""
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return int(val)
    try:
        cleaned = str(val).replace(",", "").strip()
        if not cleaned:
            return default
        return int(float(cleaned))
    except (ValueError, TypeError):
        return default


def _parse_start_time(raw: str) -> datetime | None:
    """Parse a start_time string in various ISO-8601 formats."""
    if not raw:
        return None
    try:
        cleaned = raw.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned)
    except (ValueError, TypeError):
        return None


def _round_pct(count: int | float, total: int | float, decimals: int = 1) -> float:
    """Calculate percentage rounded to given decimal places."""
    if total == 0:
        return 0.0
    return round(count / total * 100, decimals)


def _load_query_records(path: Path) -> list[dict]:
    """Load query records from a JSON file, handling wrapped formats."""
    data = _read_json(path)
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("queries", "data", "records", "results"):
            if key in data and isinstance(data[key], list):
                return data[key]
    return []


# ---------------------------------------------------------------------------
# Hourly distribution analysis
# ---------------------------------------------------------------------------

def _compute_hourly_distribution(records: list[dict]) -> tuple[list[dict], dict]:
    """
    Compute hourly query distribution and peak/off-peak statistics.

    Returns (hourly_distribution, peak_stats).
    """
    hourly: dict[int, list[int]] = defaultdict(list)

    for rec in records:
        start_raw = str(rec.get("start_time", ""))
        ts = _parse_start_time(start_raw)
        if ts is None:
            continue
        duration_ms = _safe_int(rec.get("total_duration_ms", 0))
        hourly[ts.hour].append(duration_ms)

    distribution: list[dict] = []
    for hour in range(24):
        bucket = hourly.get(hour, [])
        total_dur = sum(bucket)
        count = len(bucket)
        distribution.append({
            "hour": hour,
            "queryCount": count,
            "totalDurationMs": total_dur,
            "avgDurationMs": int(total_dur / count) if count > 0 else 0,
        })

    # Peak hour
    peak_entry = max(distribution, key=lambda h: h["queryCount"])
    peak_hour = peak_entry["hour"]
    peak_queries = peak_entry["queryCount"]

    # Off-peak average (exclude peak hour, only count hours with > 0 queries)
    off_peak_entries = [h for h in distribution if h["hour"] != peak_hour and h["queryCount"] > 0]
    if off_peak_entries:
        off_peak_avg = int(sum(h["queryCount"] for h in off_peak_entries) / len(off_peak_entries))
    else:
        off_peak_avg = 0

    ratio = round(peak_queries / off_peak_avg, 1) if off_peak_avg > 0 else 0.0

    peak_stats = {
        "peakHour": peak_hour,
        "peakHourQueries": peak_queries,
        "offPeakAvgQueries": off_peak_avg,
        "peakToOffPeakRatio": ratio,
    }

    return distribution, peak_stats


# ---------------------------------------------------------------------------
# User distribution analysis
# ---------------------------------------------------------------------------

def _compute_user_distribution(records: list[dict]) -> list[dict]:
    """Group queries by user, sorted by total duration descending."""
    user_data: dict[str, dict] = defaultdict(lambda: {
        "totalQueries": 0,
        "totalDurationMs": 0,
    })

    for rec in records:
        username = rec.get("executed_as_user_name", "") or ""
        if not username:
            continue
        duration_ms = _safe_int(rec.get("total_duration_ms", 0))
        user_data[username]["totalQueries"] += 1
        user_data[username]["totalDurationMs"] += duration_ms

    total_queries = sum(u["totalQueries"] for u in user_data.values())
    total_duration = sum(u["totalDurationMs"] for u in user_data.values())

    users: list[dict] = []
    for username, stats in user_data.items():
        users.append({
            "username": username,
            "totalQueries": stats["totalQueries"],
            "pctOfTotal": _round_pct(stats["totalQueries"], total_queries),
            "totalDurationMs": stats["totalDurationMs"],
            "pctOfDuration": _round_pct(stats["totalDurationMs"], total_duration),
        })

    users.sort(key=lambda u: u["totalDurationMs"], reverse=True)
    return users


# ---------------------------------------------------------------------------
# Period calculation
# ---------------------------------------------------------------------------

def _compute_period(records: list[dict]) -> dict:
    """Determine the date range covered by the query data."""
    earliest: datetime | None = None
    latest: datetime | None = None

    for rec in records:
        ts = _parse_start_time(str(rec.get("start_time", "")))
        if ts is None:
            continue
        if earliest is None or ts < earliest:
            earliest = ts
        if latest is None or ts > latest:
            latest = ts

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if earliest and latest:
        start_date = earliest.strftime("%Y-%m-%d")
        end_date = latest.strftime("%Y-%m-%d")
        days_analysed = max((latest.date() - earliest.date()).days, 1)
    else:
        start_date = today
        end_date = today
        days_analysed = 0

    return {
        "startDate": start_date,
        "endDate": end_date,
        "daysAnalysed": days_analysed,
    }


# ---------------------------------------------------------------------------
# Surge protection recommendations
# ---------------------------------------------------------------------------

def _recommend_surge_protection(
    peak_stats: dict,
    capacity_config: dict | None,
) -> dict:
    """
    Recommend surge protection thresholds based on peak-to-off-peak ratio.

    Rule-based:
      - ratio > 5x  -> rejection 80%, recovery 60%
      - ratio > 3x  -> rejection 70%, recovery 50%
      - otherwise    -> rejection 90%, recovery 70%
    """
    ratio = peak_stats.get("peakToOffPeakRatio", 0.0)

    if ratio > 5:
        rejection = 80
        recovery = 60
    elif ratio > 3:
        rejection = 70
        recovery = 50
    else:
        rejection = 90
        recovery = 70

    # Capacity name for implementation steps
    cap_name = "your capacity"
    if capacity_config:
        cap_name = capacity_config.get("capacityName", cap_name)

    ratio_str = f"{ratio}x" if ratio > 0 else "unknown"
    if ratio > 5:
        rationale = (
            f"Peak-to-off-peak ratio of {ratio_str} indicates significant load spikes. "
            f"Setting rejection at {rejection}% and recovery at {recovery}% provides "
            f"a buffer before deep throttling."
        )
    elif ratio > 3:
        rationale = (
            f"Peak-to-off-peak ratio of {ratio_str} shows moderate load variation. "
            f"Setting rejection at {rejection}% and recovery at {recovery}% balances "
            f"protection with availability."
        )
    else:
        rationale = (
            f"Peak-to-off-peak ratio of {ratio_str} shows relatively even load distribution. "
            f"Conservative thresholds ({rejection}% rejection, {recovery}% recovery) "
            f"avoid unnecessary blocking."
        )

    capacity_level = {
        "recommendedRejectionThreshold": rejection,
        "recommendedRecoveryThreshold": recovery,
        "rationale": rationale,
        "implementationSteps": [
            "Open Fabric Admin Portal \u2192 Capacity settings",
            f"Select {cap_name}",
            "Expand Surge protection",
            "Set Background Operations to On",
            f"Set Rejection threshold to {rejection}%",
            f"Set Recovery threshold to {recovery}%",
            "Select Apply",
        ],
    }

    # Workspace-level surge protection
    mission_critical: list[str] = []
    blocked: list[str] = []
    ws_rationale = "No capacity configuration provided. Review workspace criticality manually."

    if capacity_config:
        mission_critical = capacity_config.get("missionCriticalWorkspaces", [])
        if mission_critical:
            ws_names = ", ".join(mission_critical)
            ws_rationale = (
                f"{ws_names} identified as mission critical. "
                f"Mission Critical status ensures these workspaces are immune "
                f"from surge protection blocking."
            )
        else:
            ws_rationale = (
                "No mission-critical workspaces specified. Consider designating "
                "workspaces that serve production dashboards as Mission Critical."
            )

    # CU limit percentage — reserve capacity for mission-critical workspaces
    cu_limit = 25
    if len(mission_critical) > 2:
        cu_limit = 15  # More granular limit when multiple critical workspaces

    ws_steps: list[str] = []
    for ws in mission_critical:
        ws_steps.append(f"Navigate to Workspace settings for {ws}")
        ws_steps.append("Set workspace state to Mission Critical")

    workspace_level = {
        "missionCritical": mission_critical,
        "blocked": blocked,
        "cuLimitPct": cu_limit,
        "rationale": ws_rationale,
        "implementationSteps": ws_steps if ws_steps else [
            "Identify production workspaces",
            "Navigate to Workspace settings",
            "Set workspace state to Mission Critical",
        ],
    }

    return {
        "capacityLevel": capacity_level,
        "workspaceLevel": workspace_level,
    }


# ---------------------------------------------------------------------------
# Workload isolation
# ---------------------------------------------------------------------------

def _recommend_workload_isolation(capacity_config: dict | None) -> dict:
    """Identify non-production workspaces on production capacity."""
    non_prod: list[str] = []
    recommendation = "No capacity configuration provided. Review workspace placement manually."
    rationale = ""
    target_capacity = ""

    if capacity_config:
        workspaces = capacity_config.get("workspaces", [])
        all_caps = capacity_config.get("allCapacities", [])
        cap_name = capacity_config.get("capacityName", "")

        # Find non-prod workspaces
        for ws in workspaces:
            env = ws.get("environment", "").lower()
            if env in ("dev", "e2e", "test", "staging", "sandbox", "uat"):
                non_prod.append(ws.get("name", ""))

        # Find a smaller capacity to recommend
        smaller_caps = [
            c for c in all_caps
            if c.get("cu", 0) <= 8
            and c.get("status", "").lower() == "active"
        ]

        if non_prod:
            if smaller_caps:
                target = smaller_caps[0]
                target_capacity = f"{target['name']} ({target['sku']})"
                recommendation = (
                    f"Move dev/E2E workspaces from {cap_name} to a smaller capacity "
                    f"({target['sku']}) or use existing {target['name']} ({target['sku']})"
                )
            else:
                recommendation = (
                    f"Move dev/E2E workspaces from {cap_name} to a separate smaller "
                    f"capacity (F4 or F8)"
                )
            rationale = (
                "Non-production workspaces compete for CU with production workloads "
                "during testing/development sprints."
            )
        else:
            recommendation = "All workspaces appear to be production. No isolation changes needed."
            rationale = "No non-production workspaces detected on production capacity."

    return {
        "nonProdOnProdCapacity": non_prod,
        "recommendation": recommendation,
        "rationale": rationale,
    }


# ---------------------------------------------------------------------------
# Query Scale-Out
# ---------------------------------------------------------------------------

def _recommend_query_scale_out(capacity_config: dict | None) -> dict:
    """Recommend Query Scale-Out settings based on capacity configuration."""
    if not capacity_config:
        return {
            "recommendation": "Enable Query Scale-Out for high-concurrency semantic models",
            "candidateModels": [],
            "prerequisite": "Large semantic model storage format must be enabled first",
            "rationale": "No capacity configuration provided. Review semantic model settings manually.",
            "implementationSteps": [
                "Enable Large semantic model storage format",
                "Trigger a full refresh of the semantic model",
                "Enable Query scale-out toggle",
                "Monitor via Capacity Metrics app",
            ],
        }

    sm_settings = capacity_config.get("semanticModelSettings", {})
    large_format = sm_settings.get("largeStorageFormat", False)
    scale_out = sm_settings.get("queryScaleOut", False)
    model_size = sm_settings.get("modelSizeMB", 0)

    # Candidate models from production workspaces
    workspaces = capacity_config.get("workspaces", [])
    candidates = [
        ws["name"] for ws in workspaces
        if ws.get("environment", "").lower() == "prod"
    ]

    prereq = "Large semantic model storage format must be enabled first"
    if large_format:
        prereq = "Large semantic model storage format is already enabled"

    steps: list[str] = []
    if not large_format:
        size_note = f" (model size: {model_size} MB)" if model_size else ""
        steps.append(f"Enable Large semantic model storage format{size_note}")
        steps.append("Trigger a full refresh of the semantic model")
    if not scale_out:
        steps.append("Enable Query scale-out toggle")
    steps.append("Monitor via Capacity Metrics app")

    rationale = (
        "Multiple concurrent users querying the same models. "
        "Read-only replicas distribute the load."
    )
    if scale_out:
        rationale = "Query Scale-Out is already enabled. Monitor replica sync and CU usage."

    return {
        "recommendation": "Enable Query Scale-Out for high-concurrency semantic models" if not scale_out else "Query Scale-Out already enabled — monitor performance",
        "candidateModels": candidates,
        "prerequisite": prereq,
        "rationale": rationale,
        "implementationSteps": steps,
    }


# ---------------------------------------------------------------------------
# Self-serve isolation
# ---------------------------------------------------------------------------

def _recommend_self_serve_isolation() -> dict:
    """Recommend self-serve workspace isolation strategy."""
    return {
        "recommendation": "Consider a separate smaller capacity for self-serve reporting",
        "rationale": (
            "Self-serve users create unpredictable query patterns that can spike CU usage. "
            "Isolating to a dedicated capacity protects enterprise reports."
        ),
        "prosAndCons": {
            "pros": [
                "Isolates performance impact",
                "Protects enterprise report stability",
                "Easier to identify inefficient self-serve reports",
            ],
            "cons": [
                "Additional cost for separate capacity",
                "Management overhead",
                "Users may need to switch workspaces",
            ],
        },
    }


# ---------------------------------------------------------------------------
# Capacity overage
# ---------------------------------------------------------------------------

def _recommend_capacity_overage(peak_stats: dict) -> dict:
    """Recommend capacity overage settings."""
    ratio = peak_stats.get("peakToOffPeakRatio", 0.0)
    ratio_str = f"{ratio}x" if ratio > 0 else "significantly"

    return {
        "recommendation": "Enable Capacity Overage (preview feature) for burst capacity during peak hours",
        "rationale": (
            f"Peak hours show query volume {ratio_str} above off-peak. "
            f"Overage provides temporary additional CU without permanent scaling."
        ),
    }


# ---------------------------------------------------------------------------
# Capacity scaling analysis
# ---------------------------------------------------------------------------

def _analyse_capacity_scaling(capacity_config: dict | None) -> dict | None:
    """Analyse current capacity scaling decisions and produce pros/cons."""
    if not capacity_config:
        return None

    cap_name = capacity_config.get("capacityName", "")
    sku = capacity_config.get("capacitySKU", "")
    cu = capacity_config.get("capacityUnits", 0)
    region = capacity_config.get("region", "")
    quota = capacity_config.get("subscriptionQuota", 0)
    prev_sku = capacity_config.get("previousSku", "")
    scaled_date = capacity_config.get("scaledDate", "")

    all_caps = capacity_config.get("allCapacities", [])
    total_active_cu = sum(
        c.get("cu", 0) for c in all_caps
        if c.get("status", "").lower() == "active"
    )

    quota_util = ""
    quota_pct = 0.0
    if quota > 0:
        quota_pct = round(total_active_cu / quota * 100, 1)
        quota_util = f"{total_active_cu} / {quota} ({quota_pct}%)"

    # Determine scaling analysis
    if prev_sku:
        prev_cu = _sku_to_cu(prev_sku)
        action = f"Monitor {sku} utilisation post-scaling before further changes"
        next_sku = _next_sku(sku)
        rationale = (
            f"{sku} doubles CU headroom from {prev_cu} to {cu}. "
            f"Evaluate if this resolves throttling before considering {next_sku}."
        )

        # Build pros/cons for the scaling decision
        pros = [
            f"Doubles available CU ({prev_cu}\u2192{cu}) \u2014 directly reduces throttling during peak hours",
            "Enables larger concurrent query workloads without deep throttling",
        ]
        if quota > 0:
            pros.append(
                f"Subscription quota ({quota}) provides headroom for future growth"
            )
        pros.append(
            f"{sku} unlocks higher memory limits and more aggressive parallel query execution"
        )

        cons = [
            f"Doubles Fabric capacity cost ({sku} is 2x price of {prev_sku})",
            "Does NOT fix inefficient queries \u2014 expensive queries still consume proportionally more CU",
            "Risk of masking underlying problems: performance issues become cost issues instead",
        ]
        if quota > 0:
            cons.append(
                f"Quota increase ({quota}) means total active CU across all capacities "
                f"could reach {quota} \u2014 budget oversight needed"
            )
        cons.append("Should be combined with query optimisation for sustainable improvement")
    else:
        action = f"Review current {sku} utilisation via Capacity Metrics app"
        rationale = (
            f"Current capacity is {sku} ({cu} CU). "
            f"Monitor utilisation patterns before making scaling decisions."
        )
        pros = [
            f"Current {sku} provides {cu} CU for workloads",
        ]
        cons = [
            "No previous SKU data available for comparison",
            "Monitor utilisation to determine if scaling is needed",
        ]

    return {
        "currentCapacity": {
            "name": cap_name,
            "sku": sku,
            "capacityUnits": cu,
            "region": region,
            "subscriptionQuota": quota,
            "previousSku": prev_sku,
            "scaledDate": scaled_date,
        },
        "allCapacities": all_caps,
        "totalActiveCU": total_active_cu,
        "quotaUtilisation": quota_util,
        "scalingAnalysis": {
            "action": action,
            "rationale": rationale,
        },
        "prosAndCons": {
            "pros": pros,
            "cons": cons,
        },
    }


def _sku_to_cu(sku: str) -> int:
    """Convert an F-series SKU name to CU count."""
    try:
        return int(sku.upper().replace("F", ""))
    except (ValueError, AttributeError):
        return 0


def _next_sku(sku: str) -> str:
    """Return the next F-series SKU (double the CU)."""
    cu = _sku_to_cu(sku)
    if cu > 0:
        return f"F{cu * 2}"
    return "next SKU"


# ---------------------------------------------------------------------------
# Semantic model settings analysis
# ---------------------------------------------------------------------------

def _analyse_semantic_model_settings(capacity_config: dict | None) -> dict | None:
    """Analyse semantic model settings and produce recommendations."""
    if not capacity_config:
        return None

    sm_settings = capacity_config.get("semanticModelSettings", {})
    large_format = sm_settings.get("largeStorageFormat", False)
    scale_out = sm_settings.get("queryScaleOut", False)
    model_size = sm_settings.get("modelSizeMB", 0)

    # Production workspaces for candidate list
    workspaces = capacity_config.get("workspaces", [])
    prod_workspaces = [
        ws["name"] for ws in workspaces
        if ws.get("environment", "").lower() == "prod"
    ]

    # Large Storage Format
    large_format_status = "On" if large_format else "Off"
    if large_format:
        lsf_recommendation = "Already enabled"
        lsf_pros = [
            "Already active \u2014 no action needed",
            "Prerequisite for Query Scale-Out is satisfied",
        ]
        lsf_cons = [
            "One-way migration \u2014 cannot easily revert (already committed)",
        ]
    else:
        lsf_recommendation = "Enable"
        size_note = f" (current: {model_size} MB)" if model_size else ""
        lsf_pros = [
            f"Optimised storage format for models approaching 1 GB{size_note}",
            "Prerequisite for Query Scale-Out",
            "Improved memory management for large DirectQuery models",
            "No impact on end users \u2014 transparent change",
        ]
        lsf_cons = [
            "Increases storage consumption on capacity",
            "One-way migration \u2014 cannot easily revert",
            "Requires a full dataset refresh after enabling",
            "May take significant time for the initial conversion",
        ]

    large_format_section = {
        "currentStatus": large_format_status,
        "modelSizeMB": model_size,
        "recommendation": lsf_recommendation,
        "pros": lsf_pros,
        "cons": lsf_cons,
    }

    # Query Scale-Out
    scale_out_status = "On" if scale_out else "Off"
    prereq_status = f"Large semantic model storage format (currently {large_format_status})"

    if scale_out:
        qso_recommendation = "Already enabled \u2014 monitor replica sync"
        qso_pros = [
            "Already active \u2014 queries are distributed across replicas",
            "Refresh operations are isolated from interactive queries",
        ]
        qso_cons = [
            "Uses additional capacity CU for replicas",
            "Read-only replicas may lag behind primary by a few seconds after refresh",
        ]
    else:
        qso_recommendation = "Enable after Large Storage Format" if not large_format else "Enable"
        candidates_str = ", ".join(prod_workspaces) if prod_workspaces else "production workspaces"
        qso_pros = [
            "Isolates refresh from interactive queries",
            "Distributes queries across read-only replicas during peak load",
            f"Critical for high-concurrency models ({candidates_str})",
            "Directly addresses the concurrency problem",
        ]
        qso_cons = [
            "Uses additional capacity CU for replicas",
            "Read-only replicas may lag behind primary by a few seconds after refresh",
            "Users may see slightly stale data during refresh windows",
            "Requires monitoring to ensure replicas stay in sync",
        ]

    query_scale_out_section = {
        "currentStatus": scale_out_status,
        "prerequisite": prereq_status,
        "recommendation": qso_recommendation,
        "pros": qso_pros,
        "cons": qso_cons,
    }

    return {
        "largeStorageFormat": large_format_section,
        "queryScaleOut": query_scale_out_section,
    }


# ---------------------------------------------------------------------------
# Main analysis orchestration
# ---------------------------------------------------------------------------

def analyse_workload(
    records: list[dict],
    capacity_config: dict | None,
) -> dict:
    """Run all workload analyses and return the full result."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if not records:
        return _empty_result(today, capacity_config)

    # Period
    period = _compute_period(records)

    # Hourly distribution
    hourly_distribution, peak_stats = _compute_hourly_distribution(records)

    # User distribution
    user_distribution = _compute_user_distribution(records)

    # Surge protection
    surge_protection = _recommend_surge_protection(peak_stats, capacity_config)

    # Workload isolation
    workload_isolation = _recommend_workload_isolation(capacity_config)

    # Query Scale-Out
    query_scale_out = _recommend_query_scale_out(capacity_config)

    # Self-serve isolation
    self_serve_isolation = _recommend_self_serve_isolation()

    # Capacity overage
    capacity_overage = _recommend_capacity_overage(peak_stats)

    # Capacity scaling (requires capacity config)
    capacity_scaling = _analyse_capacity_scaling(capacity_config)

    # Semantic model settings (requires capacity config)
    semantic_model_settings = _analyse_semantic_model_settings(capacity_config)

    return {
        "analysisDate": today,
        "period": period,
        "hourlyDistribution": hourly_distribution,
        "peakHour": peak_stats["peakHour"],
        "peakHourQueries": peak_stats["peakHourQueries"],
        "offPeakAvgQueries": peak_stats["offPeakAvgQueries"],
        "peakToOffPeakRatio": peak_stats["peakToOffPeakRatio"],
        "userDistribution": user_distribution,
        "surgeProtection": surge_protection,
        "workloadIsolation": workload_isolation,
        "queryScaleOut": query_scale_out,
        "selfServeIsolation": self_serve_isolation,
        "capacityOverage": capacity_overage,
        "capacityScaling": capacity_scaling,
        "semanticModelSettings": semantic_model_settings,
    }


def _empty_result(today: str, capacity_config: dict | None) -> dict:
    """Return a valid empty result structure when no query data is available."""
    return {
        "analysisDate": today,
        "period": {"startDate": today, "endDate": today, "daysAnalysed": 0},
        "hourlyDistribution": [
            {"hour": h, "queryCount": 0, "totalDurationMs": 0, "avgDurationMs": 0}
            for h in range(24)
        ],
        "peakHour": 0,
        "peakHourQueries": 0,
        "offPeakAvgQueries": 0,
        "peakToOffPeakRatio": 0.0,
        "userDistribution": [],
        "surgeProtection": _recommend_surge_protection(
            {"peakToOffPeakRatio": 0.0}, capacity_config,
        ),
        "workloadIsolation": _recommend_workload_isolation(capacity_config),
        "queryScaleOut": _recommend_query_scale_out(capacity_config),
        "selfServeIsolation": _recommend_self_serve_isolation(),
        "capacityOverage": _recommend_capacity_overage({"peakToOffPeakRatio": 0.0}),
        "capacityScaling": _analyse_capacity_scaling(capacity_config),
        "semanticModelSettings": _analyse_semantic_model_settings(capacity_config),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Analyse CU consumption patterns, recommend surge protection thresholds, "
            "workload isolation, and evaluate capacity scaling decisions"
        )
    )
    parser.add_argument(
        "--query-data", required=True, type=Path,
        help="Path to query-history-export.json",
    )
    parser.add_argument(
        "--capacity-config", type=Path, default=None,
        help="Path to capacity-config.json (optional)",
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output directory for workload-analysis.json",
    )
    args = parser.parse_args()

    query_data_path = args.query_data.resolve()
    output_dir = args.output.resolve()

    # ── Load query data ──
    if not query_data_path.is_file():
        print(f"ERROR: Query data file not found: {query_data_path}", file=sys.stderr)
        sys.exit(1)

    print(f"  Loading query data from {query_data_path}...", file=sys.stderr)
    records = _load_query_records(query_data_path)
    if not records:
        print("  WARNING: No query records found in the input file.", file=sys.stderr)
    else:
        print(f"  Loaded {len(records):,} query records.", file=sys.stderr)

    # ── Load capacity config (optional) ──
    capacity_config: dict | None = None
    if args.capacity_config:
        cap_path = args.capacity_config.resolve()
        if cap_path.is_file():
            cap_data = _read_json(cap_path)
            if isinstance(cap_data, dict):
                capacity_config = cap_data
                print(f"  Loaded capacity config from {cap_path}", file=sys.stderr)
            else:
                print(f"  WARNING: Could not parse capacity config: {cap_path}", file=sys.stderr)
        else:
            print(f"  WARNING: Capacity config file not found: {cap_path} (skipping)", file=sys.stderr)

    # ── Run analysis ──
    print("  Analysing workload patterns...", file=sys.stderr)
    result = analyse_workload(records, capacity_config)

    # ── Write output ──
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "workload-analysis.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    # ── Print summary to stderr ──
    period = result["period"]
    print(f"\n=== Workload Analysis ===", file=sys.stderr)
    print(
        f"  Period: {period['startDate']} to {period['endDate']} "
        f"({period['daysAnalysed']} days)",
        file=sys.stderr,
    )
    print(f"  Total query records: {len(records):,}", file=sys.stderr)

    # Peak hour
    peak_hour = result["peakHour"]
    peak_queries = result["peakHourQueries"]
    off_peak = result["offPeakAvgQueries"]
    ratio = result["peakToOffPeakRatio"]
    print(
        f"\n  Peak hour: {peak_hour:02d}:00 UTC ({peak_queries:,} queries)",
        file=sys.stderr,
    )
    print(f"  Off-peak average: {off_peak:,} queries/hour", file=sys.stderr)
    print(f"  Peak-to-off-peak ratio: {ratio}x", file=sys.stderr)

    # User distribution
    users = result["userDistribution"]
    if users:
        print(f"\n  Top users by total duration:", file=sys.stderr)
        for u in users[:5]:
            print(
                f"    {u['username']}: {u['totalQueries']:,} queries "
                f"({u['pctOfTotal']}% of total), "
                f"{u['pctOfDuration']}% of duration",
                file=sys.stderr,
            )

    # Surge protection
    sp = result["surgeProtection"]["capacityLevel"]
    print(
        f"\n  Surge protection recommendation: "
        f"rejection={sp['recommendedRejectionThreshold']}%, "
        f"recovery={sp['recommendedRecoveryThreshold']}%",
        file=sys.stderr,
    )

    # Workload isolation
    wi = result["workloadIsolation"]
    if wi["nonProdOnProdCapacity"]:
        print(
            f"  Non-prod workspaces on prod capacity: "
            f"{', '.join(wi['nonProdOnProdCapacity'])}",
            file=sys.stderr,
        )

    # Capacity scaling
    cs = result.get("capacityScaling")
    if cs:
        cur = cs["currentCapacity"]
        print(
            f"\n  Capacity: {cur['name']} ({cur['sku']}, {cur['capacityUnits']} CU)",
            file=sys.stderr,
        )
        if cs["quotaUtilisation"]:
            print(f"  Quota utilisation: {cs['quotaUtilisation']}", file=sys.stderr)
        print(f"  Scaling action: {cs['scalingAnalysis']['action']}", file=sys.stderr)
    else:
        print(
            "\n  Capacity scaling: skipped (no --capacity-config provided)",
            file=sys.stderr,
        )

    # Semantic model settings
    sms = result.get("semanticModelSettings")
    if sms:
        lsf = sms["largeStorageFormat"]
        qso = sms["queryScaleOut"]
        print(
            f"\n  Large Storage Format: {lsf['currentStatus']} "
            f"\u2192 {lsf['recommendation']}",
            file=sys.stderr,
        )
        print(
            f"  Query Scale-Out: {qso['currentStatus']} "
            f"\u2192 {qso['recommendation']}",
            file=sys.stderr,
        )

    print(f"\nWritten: {output_file}", file=sys.stderr)


if __name__ == "__main__":
    main()
