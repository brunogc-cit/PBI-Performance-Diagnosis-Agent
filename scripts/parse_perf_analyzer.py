#!/usr/bin/env python3
"""Parse Power BI Desktop Performance Analyzer JSON exports.

Reads a Performance Analyzer JSON file, analyses visual load times,
query durations, and render costs, then outputs a structured summary
JSON and prints a human-readable table to stdout.

Usage:
    python3 parse_perf_analyzer.py --input <path-to-json> --output <output-dir>
"""

import argparse
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_iso(ts: str) -> datetime | None:
    """Parse an ISO-8601 timestamp string, returning None on failure."""
    if not ts:
        return None
    try:
        # Handle Z suffix and +00:00 variants
        ts = ts.replace("Z", "+00:00")
        return datetime.fromisoformat(ts)
    except (ValueError, TypeError):
        return None


def duration_ms(start_str: str | None, end_str: str | None) -> float | None:
    """Return milliseconds between two ISO timestamps, or None."""
    if not start_str or not end_str:
        return None
    start = parse_iso(start_str)
    end = parse_iso(end_str)
    if start is None or end is None:
        return None
    delta = (end - start).total_seconds() * 1000
    return max(delta, 0)


def fmt_seconds(ms: float) -> str:
    """Format milliseconds as a human-readable seconds string."""
    return f"{ms / 1000:.2f}s"


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def build_children_index(events: list[dict]) -> dict[str, list[dict]]:
    """Map parentId -> list of child events."""
    children: dict[str, list[dict]] = {}
    for ev in events:
        pid = ev.get("parentId")
        if pid:
            children.setdefault(pid, []).append(ev)
    return children


def collect_descendants(event_id: str, children_index: dict[str, list[dict]]) -> list[dict]:
    """Recursively collect all descendant events of a given event id."""
    result: list[dict] = []
    stack = list(children_index.get(event_id, []))
    while stack:
        ev = stack.pop()
        result.append(ev)
        eid = ev.get("id")
        if eid and eid in children_index:
            stack.extend(children_index[eid])
    return result


def sum_durations(events: list[dict], name_filter: str) -> float:
    """Sum durations (ms) of events matching a given name."""
    total = 0.0
    for ev in events:
        if ev.get("name") == name_filter:
            d = duration_ms(ev.get("start"), ev.get("end"))
            if d is not None:
                total += d
    return total


def analyse_visual(lifecycle_event: dict, children_index: dict[str, list[dict]]) -> dict | None:
    """Analyse a single Visual Container Lifecycle event."""
    metrics = lifecycle_event.get("metrics") or {}
    eid = lifecycle_event.get("id")
    if not eid:
        return None

    total = duration_ms(lifecycle_event.get("start"), lifecycle_event.get("end"))
    if total is None:
        return None

    # Collect ALL descendants (not just direct children)
    descendants = collect_descendants(eid, children_index)

    query_ms = sum_durations(descendants, "Query")
    render_ms = sum_durations(descendants, "Render")
    data_transform_ms = sum_durations(descendants, "Data View Transform")
    resource_load_ms = sum_durations(descendants, "Visual Container Resource Load")
    # otherMs: don't double-subtract dataTransform — it's part of render
    other_ms = max(total - query_ms - render_ms - resource_load_ms, 0)

    query_pct = round((query_ms / total) * 100, 1) if total > 0 else 0.0
    render_pct = round((render_ms / total) * 100, 1) if total > 0 else 0.0

    flags: list[str] = []
    if query_ms > 1000:
        flags.append("SLOW_QUERY")
    if render_ms > 1000:
        flags.append("SLOW_RENDER")
    if total > 3000:
        flags.append("EXCEEDS_3S")
    if total > 10000:
        flags.append("EXCEEDS_10S")

    return {
        "visualId": metrics.get("visualId", ""),
        "visualTitle": metrics.get("visualTitle", "(untitled)"),
        "visualType": metrics.get("visualType", "unknown"),
        "totalMs": round(total, 1),
        "queryMs": round(query_ms, 1),
        "renderMs": round(render_ms, 1),
        "dataTransformMs": round(data_transform_ms, 1),
        "resourceLoadMs": round(resource_load_ms, 1),
        "otherMs": round(other_ms, 1),
        "queryPct": query_pct,
        "renderPct": render_pct,
        "flags": flags,
    }


def analyse_user_actions(
    events: list[dict],
    visual_lifecycles: list[dict],
) -> list[dict]:
    """Group Visual Container Lifecycle events under their preceding User Action."""
    # Collect user actions in order
    user_actions: list[dict] = []
    for ev in events:
        if ev.get("name") == "User Action":
            ua_start = parse_iso(ev.get("start"))
            if ua_start is None:
                continue
            user_actions.append({
                "event": ev,
                "start": ua_start,
            })

    # Sort by start time
    user_actions.sort(key=lambda x: x["start"])

    results: list[dict] = []
    for idx, ua in enumerate(user_actions):
        label = (ua["event"].get("metrics") or {}).get("sourceLabel", "Unknown")
        ua_start = ua["start"]
        # Window: from this UA start to next UA start (or infinity)
        if idx + 1 < len(user_actions):
            ua_end = user_actions[idx + 1]["start"]
        else:
            ua_end = None

        associated: list[dict] = []
        for vlc in visual_lifecycles:
            vlc_start = parse_iso(vlc.get("start"))
            if vlc_start is None:
                continue
            if vlc_start >= ua_start and (ua_end is None or vlc_start < ua_end):
                associated.append(vlc)

        if not associated:
            results.append({
                "label": label,
                "timestamp": ua["event"].get("start", ""),
                "visualCount": 0,
                "totalDurationMs": 0,
            })
            continue

        starts: list[datetime] = []
        ends: list[datetime] = []
        for v in associated:
            s = parse_iso(v.get("start"))
            e = parse_iso(v.get("end"))
            if s:
                starts.append(s)
            if e:
                ends.append(e)

        total_duration = 0.0
        if starts and ends:
            total_duration = (max(ends) - min(starts)).total_seconds() * 1000

        results.append({
            "label": label,
            "timestamp": ua["event"].get("start", ""),
            "visualCount": len(associated),
            "totalDurationMs": round(total_duration, 1),
        })

    return results


def compute_summary(visuals: list[dict]) -> dict:
    """Compute aggregate statistics from analysed visuals."""
    total_visuals = len(visuals)
    if total_visuals == 0:
        return {
            "totalVisuals": 0,
            "avgLoadMs": 0,
            "medianLoadMs": 0,
            "p95LoadMs": 0,
            "slowVisualCount": 0,
            "slowQueryCount": 0,
            "slowRenderCount": 0,
            "pageLoadMs": 0,
            "dominantBottleneck": "balanced",
        }

    load_times = [v["totalMs"] for v in visuals]
    query_pcts = [v["queryPct"] for v in visuals]
    render_pcts = [v["renderPct"] for v in visuals]

    avg_load = statistics.mean(load_times)
    median_load = statistics.median(load_times)

    # P95: use nearest-rank method
    sorted_times = sorted(load_times)
    p95_idx = max(0, int(len(sorted_times) * 0.95) - 1)
    p95_load = sorted_times[min(p95_idx, len(sorted_times) - 1)]

    slow_visual_count = sum(1 for v in visuals if v["totalMs"] > 3000)
    slow_query_count = sum(1 for v in visuals if v["queryMs"] > 1000)
    slow_render_count = sum(1 for v in visuals if v["renderMs"] > 1000)

    page_load = max(load_times)  # best approximation from visual data

    avg_query_pct = statistics.mean(query_pcts) if query_pcts else 0
    avg_render_pct = statistics.mean(render_pcts) if render_pcts else 0

    if avg_query_pct > 60:
        bottleneck = "query"
    elif avg_render_pct > 60:
        bottleneck = "render"
    else:
        bottleneck = "balanced"

    return {
        "totalVisuals": total_visuals,
        "avgLoadMs": round(avg_load, 1),
        "medianLoadMs": round(median_load, 1),
        "p95LoadMs": round(p95_load, 1),
        "slowVisualCount": slow_visual_count,
        "slowQueryCount": slow_query_count,
        "slowRenderCount": slow_render_count,
        "pageLoadMs": round(page_load, 1),
        "dominantBottleneck": bottleneck,
    }


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_summary_table(
    source_file: str,
    total_events: int,
    visuals: list[dict],
    summary: dict,
    user_actions: list[dict],
) -> None:
    """Print a human-readable summary table to stdout."""
    # Find page load from the largest user action duration, falling back to summary
    page_load_ms = summary["pageLoadMs"]
    for ua in user_actions:
        if "Refresh" in ua.get("label", ""):
            if ua["totalDurationMs"] > page_load_ms:
                page_load_ms = ua["totalDurationMs"]

    print()
    print("=== Power BI Performance Analyzer Summary ===")
    print(f"Source: {source_file}")
    print(f"Total Events: {total_events}")
    print(f"Total Visuals: {summary['totalVisuals']}")
    print(f"Page Load Time: {fmt_seconds(page_load_ms)}")
    print()

    if visuals:
        print("--- Slowest Visuals ---")
        header = f"{'#':<4}{'Visual Title':<30}{'Type':<16}{'Total':>8}{'Query':>8}{'Render':>8}  Flags"
        print(header)

        display_count = min(len(visuals), 20)
        for i, v in enumerate(visuals[:display_count], start=1):
            title = v["visualTitle"][:28]
            vtype = v["visualType"][:14]
            flags_str = ", ".join(v["flags"]) if v["flags"] else ""
            print(
                f"{i:<4}{title:<30}{vtype:<16}"
                f"{fmt_seconds(v['totalMs']):>8}"
                f"{fmt_seconds(v['queryMs']):>8}"
                f"{fmt_seconds(v['renderMs']):>8}"
                f"  {flags_str}"
            )

        if len(visuals) > display_count:
            print(f"  ... and {len(visuals) - display_count} more visuals")
        print()

    print("--- Summary ---")
    print(
        f"Avg Visual Load: {fmt_seconds(summary['avgLoadMs'])} | "
        f"Median: {fmt_seconds(summary['medianLoadMs'])} | "
        f"P95: {fmt_seconds(summary['p95LoadMs'])}"
    )
    print(
        f"Slow Visuals (>3s): {summary['slowVisualCount']} | "
        f"Slow Queries (>1s): {summary['slowQueryCount']} | "
        f"Slow Renders (>1s): {summary['slowRenderCount']}"
    )
    print(f"Dominant Bottleneck: {summary['dominantBottleneck'].capitalize()}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse Power BI Performance Analyzer JSON exports."
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Path to the Performance Analyzer JSON file.",
    )
    parser.add_argument(
        "--output", "-o",
        required=True,
        help="Directory to write perf-summary.json into.",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output)

    if not input_path.is_file():
        print(f"Error: input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    # -- Parse --
    try:
        with open(input_path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"Error reading input: {exc}", file=sys.stderr)
        sys.exit(1)

    events: list[dict] = data.get("events", [])
    total_events = len(events)

    # -- Build indices --
    children_index = build_children_index(events)

    # -- Analyse visuals --
    visual_lifecycles = [
        ev for ev in events if ev.get("name") == "Visual Container Lifecycle"
    ]

    visuals: list[dict] = []
    for vlc in visual_lifecycles:
        result = analyse_visual(vlc, children_index)
        if result is not None:
            visuals.append(result)

    # Sort by totalMs descending
    visuals.sort(key=lambda v: v["totalMs"], reverse=True)

    # Assign ranks
    for idx, v in enumerate(visuals, start=1):
        v["rank"] = idx

    # -- Analyse user actions --
    user_actions = analyse_user_actions(events, visual_lifecycles)

    # -- Summary --
    summary = compute_summary(visuals)

    # Update pageLoadMs from user actions if applicable
    for ua in user_actions:
        if "Refresh" in ua.get("label", "") and ua["totalDurationMs"] > summary["pageLoadMs"]:
            summary["pageLoadMs"] = round(ua["totalDurationMs"], 1)

    # -- Build output --
    output = {
        "version": "1.0.0",
        "analysedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sourceFile": input_path.name,
        "totalEvents": total_events,
        "userActions": user_actions,
        "visuals": visuals,
        "summary": summary,
    }

    output_file = output_dir / "perf-summary.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    # -- Print human-readable summary --
    print_summary_table(input_path.name, total_events, visuals, summary, user_actions)
    print(f"Written: {output_file}")


if __name__ == "__main__":
    main()
