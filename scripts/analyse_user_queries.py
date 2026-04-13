#!/usr/bin/env python3
"""
User Query Profile Analyser for Databricks query history exports.

Processes query history data (from Databricks system.query.history exports)
to attribute queries to individual users, compute per-user statistics, and
identify training candidates who would benefit from query optimisation guidance.

Usage:
    # From JSON export of query history
    python3 scripts/analyse_user_queries.py --query-data output/query-history-export.json --output output/

    # From CSV export
    python3 scripts/analyse_user_queries.py --csv-file data.csv --output output/
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_json(path: Path) -> list | dict | None:
    """Read a JSON file, returning None on failure."""
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  WARNING: Could not parse {path}: {exc}", file=sys.stderr)
        return None


def _read_csv(path: Path) -> list[dict]:
    """Read a CSV file and return a list of row dicts with normalised column names."""
    rows: list[dict] = []
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                return rows
            for row in reader:
                rows.append(row)
    except OSError as exc:
        print(f"  WARNING: Could not read CSV {path}: {exc}", file=sys.stderr)
    return rows


def _resolve_csv_column(row: dict, candidates: list[str]) -> str | None:
    """Find the first matching column name from a list of candidates."""
    for name in candidates:
        if name in row:
            return name
    # Try case-insensitive match
    lower_map = {k.lower(): k for k in row}
    for name in candidates:
        if name.lower() in lower_map:
            return lower_map[name.lower()]
    return None


def _normalise_csv_rows(rows: list[dict]) -> list[dict]:
    """Convert CSV rows into the canonical JSON record format."""
    if not rows:
        return []

    sample = rows[0]

    # Resolve column names flexibly
    user_col = _resolve_csv_column(sample, [
        "executed_as_user_name", "user_name", "username", "user",
    ])
    duration_col = _resolve_csv_column(sample, [
        "total_duration_ms", "duration_ms", "total_duration", "duration",
    ])
    bytes_col = _resolve_csv_column(sample, [
        "read_bytes", "bytes_read", "total_bytes_read",
    ])
    rows_col = _resolve_csv_column(sample, [
        "rows_produced", "result_rows", "output_rows",
    ])
    time_col = _resolve_csv_column(sample, [
        "start_time", "start_timestamp", "query_start_time", "timestamp",
    ])
    id_col = _resolve_csv_column(sample, [
        "query_id", "id",
    ])
    text_col = _resolve_csv_column(sample, [
        "query_text", "query", "sql_text", "statement",
    ])

    if not user_col:
        print("  ERROR: Cannot find a user name column in CSV.", file=sys.stderr)
        return []
    if not duration_col:
        print("  ERROR: Cannot find a duration column in CSV.", file=sys.stderr)
        return []

    records: list[dict] = []
    for row in rows:
        record: dict = {
            "executed_as_user_name": row.get(user_col, ""),
            "total_duration_ms": _safe_int(row.get(duration_col, "0")),
            "read_bytes": _safe_int(row.get(bytes_col, "0")) if bytes_col else 0,
            "rows_produced": _safe_int(row.get(rows_col, "0")) if rows_col else 0,
            "start_time": row.get(time_col, "") if time_col else "",
            "query_id": row.get(id_col, "") if id_col else "",
            "query_text": row.get(text_col, "") if text_col else "",
        }
        records.append(record)

    return records


def _safe_int(value: str | int | float | None) -> int:
    """Parse a value to int, returning 0 on failure."""
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    try:
        # Handle strings like "1,234,567" or "1234567.0"
        cleaned = str(value).replace(",", "").strip()
        if not cleaned:
            return 0
        return int(float(cleaned))
    except (ValueError, TypeError):
        return 0


def _safe_float(value) -> float:
    """Parse a value to float, returning 0.0 on failure."""
    if value is None:
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def _bytes_to_gb(byte_count: int) -> float:
    """Convert bytes to gigabytes, rounded to 2 decimal places."""
    return round(byte_count / (1024 ** 3), 2)


def _percentile(sorted_values: list[int | float], pct: float) -> int:
    """Compute the given percentile from a sorted list of values."""
    if not sorted_values:
        return 0
    n = len(sorted_values)
    idx = (pct / 100) * (n - 1)
    lower = int(idx)
    upper = min(lower + 1, n - 1)
    fraction = idx - lower
    return int(sorted_values[lower] + fraction * (sorted_values[upper] - sorted_values[lower]))


def _truncate_query(text: str, max_len: int = 200) -> str:
    """Truncate query text to max_len characters, adding ellipsis if needed."""
    if not text:
        return ""
    text = " ".join(text.split())  # Collapse whitespace
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


def _parse_start_time(raw: str) -> datetime | None:
    """Parse a start_time string in various ISO-8601 formats."""
    if not raw:
        return None
    try:
        cleaned = raw.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def analyse_user_queries(records: list[dict]) -> dict:
    """Analyse query records and produce the user-query-profile structure."""

    if not records:
        return _empty_result()

    # ── Collect per-user data ──
    user_data: dict[str, list[dict]] = defaultdict(list)
    all_durations: list[int] = []
    hourly_buckets: dict[int, list[int]] = defaultdict(list)
    earliest: datetime | None = None
    latest: datetime | None = None

    for rec in records:
        username = rec.get("executed_as_user_name", "") or ""
        if not username:
            continue

        duration_ms = _safe_int(rec.get("total_duration_ms", 0))
        read_bytes = _safe_int(rec.get("read_bytes", 0))
        rows_produced = _safe_int(rec.get("rows_produced", 0))
        start_time_raw = str(rec.get("start_time", ""))
        query_id = str(rec.get("query_id", ""))
        query_text = str(rec.get("query_text", ""))

        user_data[username].append({
            "query_id": query_id,
            "duration_ms": duration_ms,
            "read_bytes": read_bytes,
            "rows_produced": rows_produced,
            "start_time": start_time_raw,
            "query_text": query_text,
        })

        all_durations.append(duration_ms)

        # Hourly distribution
        ts = _parse_start_time(start_time_raw)
        if ts:
            hourly_buckets[ts.hour].append(duration_ms)
            if earliest is None or ts < earliest:
                earliest = ts
            if latest is None or ts > latest:
                latest = ts

    if not all_durations:
        return _empty_result()

    # ── Global statistics ──
    all_durations_sorted = sorted(all_durations)
    total_queries = len(all_durations)
    total_duration_ms = sum(all_durations)
    total_bytes = sum(
        _safe_int(rec.get("read_bytes", 0)) for rec in records if rec.get("executed_as_user_name")
    )
    queries_over_10s = sum(1 for d in all_durations if d > 10_000)
    queries_over_30s = sum(1 for d in all_durations if d > 30_000)
    global_avg = int(total_duration_ms / total_queries) if total_queries else 0
    global_p50 = _percentile(all_durations_sorted, 50)
    global_p95 = _percentile(all_durations_sorted, 95)

    # Top-decile threshold (slowest 10%)
    top_decile_threshold = _percentile(all_durations_sorted, 90)

    # ── Period calculation ──
    analysis_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if earliest and latest:
        start_date = earliest.strftime("%Y-%m-%d")
        end_date = latest.strftime("%Y-%m-%d")
        days_analysed = max((latest.date() - earliest.date()).days, 1)
    else:
        start_date = analysis_date
        end_date = analysis_date
        days_analysed = 1

    # ── Per-user profiles ──
    user_profiles: list[dict] = []
    # Track per-user top-decile query counts for training candidate heuristic 2
    user_top_decile_counts: dict[str, int] = {}
    total_top_decile = sum(1 for d in all_durations if d >= top_decile_threshold)

    for username, queries in user_data.items():
        durations = [q["duration_ms"] for q in queries]
        durations_sorted = sorted(durations)
        user_total_queries = len(durations)
        user_total_duration = sum(durations)
        user_total_bytes = sum(q["read_bytes"] for q in queries)
        user_avg = int(user_total_duration / user_total_queries) if user_total_queries else 0
        user_p50 = _percentile(durations_sorted, 50)
        user_p95 = _percentile(durations_sorted, 95)
        user_max = max(durations) if durations else 0
        user_over_10s = sum(1 for d in durations if d > 10_000)
        user_over_30s = sum(1 for d in durations if d > 30_000)

        # Top-decile count for this user
        user_decile_count = sum(1 for d in durations if d >= top_decile_threshold)
        user_top_decile_counts[username] = user_decile_count

        # Top 5 slowest queries
        top_slow = sorted(queries, key=lambda q: q["duration_ms"], reverse=True)[:5]
        top_slow_out = [
            {
                "queryId": q["query_id"],
                "durationMs": q["duration_ms"],
                "readGB": _bytes_to_gb(q["read_bytes"]),
                "rowsProduced": q["rows_produced"],
                "queryPrefix": _truncate_query(q["query_text"], 200),
                "startTime": q["start_time"],
            }
            for q in top_slow
        ]

        user_profiles.append({
            "username": username,
            "totalQueries": user_total_queries,
            "totalDurationMs": user_total_duration,
            "totalGBRead": _bytes_to_gb(user_total_bytes),
            "avgDurationMs": user_avg,
            "p50DurationMs": user_p50,
            "p95DurationMs": user_p95,
            "maxDurationMs": user_max,
            "queriesOver10s": user_over_10s,
            "queriesOver30s": user_over_30s,
            "pctOfTotalQueries": round(user_total_queries / total_queries * 100, 1),
            "pctOfTotalDuration": round(user_total_duration / total_duration_ms * 100, 1) if total_duration_ms else 0,
            "topSlowQueries": top_slow_out,
        })

    # Sort by totalDurationMs descending (biggest consumers first)
    user_profiles.sort(key=lambda u: u["totalDurationMs"], reverse=True)

    # ── Training candidates ──
    training_candidates: list[dict] = []
    seen_candidates: set[str] = set()

    for profile in user_profiles:
        username = profile["username"]
        reasons: list[str] = []
        evidence_ids: list[str] = []

        # Heuristic 1: avg query duration >2x global average
        if global_avg > 0 and profile["avgDurationMs"] > 2 * global_avg:
            ratio = round(profile["avgDurationMs"] / global_avg, 1)
            reasons.append(
                f"Average query duration {ratio}x above global average "
                f"({profile['avgDurationMs']:,}ms vs {global_avg:,}ms global)"
            )

        # Heuristic 2: user runs >30% of the slowest 10% of queries
        if total_top_decile > 0:
            user_decile = user_top_decile_counts.get(username, 0)
            user_decile_pct = user_decile / total_top_decile * 100
            if user_decile_pct > 30:
                reasons.append(
                    f"Runs {user_decile_pct:.0f}% of the slowest 10% of all queries "
                    f"({user_decile} of {total_top_decile} queries in the top duration decile)"
                )

        # Heuristic 3: >20 queries over 30s
        if profile["queriesOver30s"] > 20:
            reasons.append(
                f"{profile['queriesOver30s']} queries exceeding 30 seconds in the analysis period"
            )

        if reasons and username not in seen_candidates:
            seen_candidates.add(username)
            # Collect evidence query IDs from top slow queries
            evidence_ids = [
                q["queryId"] for q in profile["topSlowQueries"]
                if q["queryId"]
            ]
            training_candidates.append({
                "username": username,
                "reason": "; ".join(reasons),
                "avgDurationMs": profile["avgDurationMs"],
                "queriesOver30s": profile["queriesOver30s"],
                "evidenceQueryIds": evidence_ids,
            })

    # ── Hourly distribution ──
    hourly_distribution: list[dict] = []
    for hour in range(24):
        bucket = hourly_buckets.get(hour, [])
        hourly_distribution.append({
            "hour": hour,
            "queryCount": len(bucket),
            "avgDurationMs": int(sum(bucket) / len(bucket)) if bucket else 0,
        })

    return {
        "analysisDate": analysis_date,
        "period": {
            "startDate": start_date,
            "endDate": end_date,
            "daysAnalysed": days_analysed,
        },
        "totals": {
            "totalUsers": len(user_data),
            "totalQueries": total_queries,
            "totalDurationMs": total_duration_ms,
            "totalGBRead": _bytes_to_gb(total_bytes),
            "queriesOver10s": queries_over_10s,
            "queriesOver30s": queries_over_30s,
            "avgDurationMs": global_avg,
            "p50DurationMs": global_p50,
            "p95DurationMs": global_p95,
        },
        "users": user_profiles,
        "trainingCandidates": training_candidates,
        "hourlyDistribution": hourly_distribution,
    }


def _empty_result() -> dict:
    """Return a valid empty result structure."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {
        "analysisDate": today,
        "period": {
            "startDate": today,
            "endDate": today,
            "daysAnalysed": 0,
        },
        "totals": {
            "totalUsers": 0,
            "totalQueries": 0,
            "totalDurationMs": 0,
            "totalGBRead": 0.0,
            "queriesOver10s": 0,
            "queriesOver30s": 0,
            "avgDurationMs": 0,
            "p50DurationMs": 0,
            "p95DurationMs": 0,
        },
        "users": [],
        "trainingCandidates": [],
        "hourlyDistribution": [
            {"hour": h, "queryCount": 0, "avgDurationMs": 0} for h in range(24)
        ],
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Analyse Databricks query history to build per-user query profiles and identify training candidates"
    )
    parser.add_argument(
        "--query-data", type=Path, default=None,
        help="Path to JSON file containing query history records",
    )
    parser.add_argument(
        "--csv-file", type=Path, default=None,
        help="Path to CSV file containing query history records",
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output directory for user-query-profile.json",
    )
    args = parser.parse_args()

    if not args.query_data and not args.csv_file:
        print("ERROR: Provide either --query-data (JSON) or --csv-file (CSV).", file=sys.stderr)
        sys.exit(1)

    if args.query_data and args.csv_file:
        print("ERROR: Provide only one of --query-data or --csv-file, not both.", file=sys.stderr)
        sys.exit(1)

    # ── Load records ──
    records: list[dict] = []

    if args.query_data:
        input_path = args.query_data.resolve()
        if not input_path.is_file():
            print(f"ERROR: Input file does not exist: {input_path}", file=sys.stderr)
            sys.exit(1)
        print(f"  Loading JSON query data from {input_path}...", file=sys.stderr)
        data = _read_json(input_path)
        if data is None:
            print("ERROR: Failed to parse JSON input.", file=sys.stderr)
            sys.exit(1)
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            # Support wrapped formats: {"queries": [...]} or {"data": [...]}
            for key in ("queries", "data", "records", "results"):
                if key in data and isinstance(data[key], list):
                    records = data[key]
                    break
            if not records:
                print("ERROR: JSON must be an array or contain a 'queries'/'data' key with an array.", file=sys.stderr)
                sys.exit(1)
        print(f"  Loaded {len(records):,} query records.", file=sys.stderr)

    elif args.csv_file:
        input_path = args.csv_file.resolve()
        if not input_path.is_file():
            print(f"ERROR: CSV file does not exist: {input_path}", file=sys.stderr)
            sys.exit(1)
        print(f"  Loading CSV query data from {input_path}...", file=sys.stderr)
        raw_rows = _read_csv(input_path)
        if not raw_rows:
            print("  WARNING: CSV file is empty or could not be parsed.", file=sys.stderr)
        else:
            records = _normalise_csv_rows(raw_rows)
            print(f"  Loaded {len(records):,} query records from CSV.", file=sys.stderr)

    # ── Analyse ──
    print("  Analysing user query profiles...", file=sys.stderr)
    result = analyse_user_queries(records)

    # ── Write output ──
    output_dir = args.output.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "user-query-profile.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    # ── Print summary to stderr ──
    totals = result["totals"]
    period = result["period"]
    users = result["users"]
    candidates = result["trainingCandidates"]

    print(f"\n=== User Query Profile Analysis ===", file=sys.stderr)
    print(f"Period: {period['startDate']} to {period['endDate']} ({period['daysAnalysed']} days)", file=sys.stderr)
    print(f"Users: {totals['totalUsers']} | Queries: {totals['totalQueries']:,}", file=sys.stderr)
    print(f"Total duration: {totals['totalDurationMs'] / 1000 / 60:.1f} minutes", file=sys.stderr)
    print(f"Total data read: {totals['totalGBRead']:.1f} GB", file=sys.stderr)
    print(f"Avg duration: {totals['avgDurationMs']:,}ms | P50: {totals['p50DurationMs']:,}ms | P95: {totals['p95DurationMs']:,}ms", file=sys.stderr)
    print(f"Queries >10s: {totals['queriesOver10s']:,} | >30s: {totals['queriesOver30s']:,}", file=sys.stderr)

    if users:
        print(f"\nTop 5 users by total duration:", file=sys.stderr)
        for u in users[:5]:
            print(
                f"  {u['username']}: {u['totalQueries']:,} queries, "
                f"avg {u['avgDurationMs']:,}ms, "
                f"{u['pctOfTotalDuration']:.1f}% of total duration, "
                f"{u['totalGBRead']:.1f} GB read",
                file=sys.stderr,
            )

    if candidates:
        print(f"\nTraining candidates identified: {len(candidates)}", file=sys.stderr)
        for c in candidates:
            print(f"  {c['username']}: {c['reason']}", file=sys.stderr)
    else:
        print(f"\nNo training candidates identified.", file=sys.stderr)

    # Hourly peak
    peak_hour = max(result["hourlyDistribution"], key=lambda h: h["queryCount"])
    if peak_hour["queryCount"] > 0:
        print(
            f"\nPeak hour: {peak_hour['hour']:02d}:00 UTC "
            f"({peak_hour['queryCount']:,} queries, avg {peak_hour['avgDurationMs']:,}ms)",
            file=sys.stderr,
        )

    print(f"\nWritten: {output_file}", file=sys.stderr)


if __name__ == "__main__":
    main()
