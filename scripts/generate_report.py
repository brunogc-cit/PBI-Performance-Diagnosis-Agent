#!/usr/bin/env python3
"""
Generate a comprehensive HTML performance diagnosis report.

Reads intermediate JSON outputs from the analysis steps and produces
a single self-contained HTML report with inline CSS.

Usage:
    python3 generate_report.py --input <output-dir> --output <output-dir> --model-name <name>
"""

import argparse
import json
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from html import escape


def _read_json_file(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _badge(severity: str) -> str:
    cls = {
        "high": "badge-high", "High": "badge-high",
        "medium": "badge-medium", "Medium": "badge-medium",
        "low": "badge-low", "Low": "badge-low",
        "critical": "badge-high",
        "directQuery": "badge-high", "dual": "badge-medium", "import": "badge-low",
    }.get(severity, "badge-info")
    return f'<span class="badge {cls}">{escape(str(severity))}</span>'


def _fmt_number(n) -> str:
    if isinstance(n, (int, float)):
        if n >= 1_000_000_000:
            return f"{n / 1_000_000_000:.1f}B"
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n / 1_000:.1f}K"
        return str(int(n)) if isinstance(n, int) else f"{n:.1f}"
    return str(n)


def _where_badges(layers: list[str]) -> str:
    """Build Where badges (Engineering / Semantic Model / Power BI) from layer list."""
    badges = []
    for layer in layers:
        ll = layer.lower()
        if any(kw in ll for kw in ("dbt", "databricks", "engineering")):
            badges.append('<span class="badge badge-high">Engineering</span>')
        if any(kw in ll for kw in ("semantic", "model", "dax", "directquery")):
            badges.append('<span class="badge badge-medium">Semantic Model</span>')
        if any(kw in ll for kw in ("power bi", "pbi report", "report")):
            badges.append('<span class="badge badge-info">Power BI</span>')
    html = " ".join(dict.fromkeys(badges))
    return html or '<span class="badge badge-info">TBD</span>'


def _scope_badge(scope: str) -> str:
    """Render a scope badge (model-wide or report-specific)."""
    if scope == "report-specific":
        return '<span class="badge" style="background:rgba(108,117,125,0.12);color:#6c757d">Report-specific</span>'
    return '<span class="badge badge-info">Model-wide</span>'


def _impact_type_badge(impact_type: str) -> str:
    """Render a performance impact type badge for BPA rules."""
    colours = {
        "latency": ("badge-high", "Latency"),
        "cost": ("badge-medium", "Cost"),
        "quality": ("badge-info", "Quality"),
        "memory": ("badge-low", "Memory"),
    }
    cls, label = colours.get(impact_type, ("badge-info", impact_type.title()))
    return f'<span class="badge {cls}">{label}</span>'


def _priority_badge(priority: str) -> str:
    """Render an optimization priority badge for hot tables."""
    colours = {
        "critical": "badge-high",
        "high": "badge-medium",
        "medium": "badge-info",
        "low": "badge-low",
    }
    cls = colours.get(priority, "badge-info")
    return f'<span class="badge {cls}">{escape(priority)}</span>'


def _fmt_rows(n) -> str:
    """Format row count with appropriate unit."""
    if n is None:
        return "-"
    if isinstance(n, (int, float)):
        if n >= 1_000_000_000:
            return f"{n / 1_000_000_000:.1f}B"
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n / 1_000:.0f}K"
        return str(int(n))
    return str(n)


def _fmt_gb(n) -> str:
    """Format GB size."""
    if n is None:
        return "-"
    if isinstance(n, (int, float)):
        if n >= 1000:
            return f"{n / 1000:.1f} TB"
        return f"{n:.1f} GB"
    return str(n)


def _classification_badge(cls: str) -> str:
    """Render a table classification badge (fact/dimension/bridge/metadata)."""
    colours = {
        "fact": ("badge-high", "fact"),
        "dimension": ("badge-info", "dim"),
        "bridge": ("badge-medium", "bridge"),
        "metadata": ("badge-low", "meta"),
    }
    badge_cls, label = colours.get(cls, ("badge-low", cls))
    return f'<span class="badge {badge_cls}">{label}</span>'


def _perf_bar(name: str, time_label: str, pct: float) -> str:
    colour = "red" if pct > 30 else ("amber" if pct > 10 else "green")
    return f"""<div style="margin-bottom:10px">
      <div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:3px">
        <span style="font-weight:600">{escape(name)}</span><span style="color:var(--muted)">{escape(time_label)}</span>
      </div>
      <div style="background:#e9ecef;border-radius:4px;height:20px;overflow:hidden">
        <div style="height:100%;width:{pct:.1f}%;background:var(--{colour});border-radius:4px;min-width:30px;
             display:flex;align-items:center;padding-left:8px;font-size:11px;font-weight:600;color:#fff">{pct:.1f}%</div>
      </div>
    </div>"""


# ═══════════════════════════════════════════════════════════════════════════════
# CSS
# ═══════════════════════════════════════════════════════════════════════════════
CSS = """
    :root { --dark:#2d2d2d; --accent:#0770cf; --green:#1a8754; --amber:#d4a017; --red:#c0392b; --light-bg:#f8f9fa; --border:#dee2e6; --text:#333; --muted:#6c757d; }
    * { margin:0; padding:0; box-sizing:border-box; }
    body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; color:var(--text); background:var(--light-bg); line-height:1.6; font-size:14px; }
    .report-header { background:var(--dark); color:#fff; padding:32px 40px 24px; }
    .report-header h1 { font-size:24px; font-weight:600; margin-bottom:8px; }
    .report-header .metadata { font-size:13px; color:#adb5bd; display:flex; gap:24px; flex-wrap:wrap; }
    .container { max-width:1200px; margin:0 auto; padding:32px 40px 60px; }
    .section-title { font-size:18px; font-weight:600; color:var(--dark); margin:40px 0 20px; padding-bottom:10px; border-bottom:3px solid var(--accent); }
    .section-title:first-child { margin-top:0; }
    .metric-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:16px; margin-bottom:24px; }
    .metric-card { background:#fff; border:1px solid var(--border); border-radius:8px; padding:20px; text-align:center; }
    .metric-card .value { font-size:28px; font-weight:700; color:var(--dark); line-height:1.2; }
    .metric-card .label { font-size:12px; font-weight:600; text-transform:uppercase; letter-spacing:0.5px; color:var(--muted); margin-top:6px; }
    .metric-card .sub { font-size:12px; color:var(--muted); margin-top:4px; }
    .metric-card.green .value { color:var(--green); }
    .metric-card.amber .value { color:var(--amber); }
    .metric-card.red .value { color:var(--red); }
    .card { background:#fff; border:1px solid var(--border); border-radius:8px; padding:24px; margin-bottom:20px; box-shadow:0 1px 3px rgba(0,0,0,0.06); }
    .card h3 { font-size:15px; font-weight:600; margin-bottom:12px; color:var(--dark); }
    table { width:100%; border-collapse:collapse; font-size:13px; }
    thead th { background:var(--light-bg); text-transform:uppercase; font-size:11px; font-weight:700; letter-spacing:0.5px; color:var(--muted); padding:10px 14px; text-align:left; border-bottom:2px solid var(--border); }
    tbody td { padding:10px 14px; border-bottom:1px solid var(--border); }
    tbody tr:nth-child(even) { background:var(--light-bg); }
    tbody tr:hover { background:#e9ecef; }
    .badge { display:inline-block; padding:3px 10px; border-radius:12px; font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:0.3px; }
    .badge-high { background:rgba(192,57,43,0.12); color:var(--red); }
    .badge-medium { background:rgba(212,160,23,0.15); color:var(--amber); }
    .badge-low { background:rgba(26,135,84,0.12); color:var(--green); }
    .badge-info { background:rgba(7,112,207,0.12); color:var(--accent); }
    .note-box { background:#fff8e1; border-left:4px solid var(--amber); border-radius:0 6px 6px 0; padding:14px 18px; margin:16px 0; font-size:13px; }
    .note-box strong { color:var(--amber); }
    .recommendation-box { background:linear-gradient(135deg,rgba(7,112,207,0.08),rgba(7,112,207,0.03)); border:1px solid rgba(7,112,207,0.2); border-radius:8px; padding:20px 24px; margin:16px 0; }
    .recommendation-box h4 { color:var(--accent); margin-bottom:8px; font-size:14px; }
    .recommendation-box p { font-size:13px; margin-bottom:8px; }
    .quadrant-grid { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
    .quadrant-box { border-radius:8px; padding:20px; }
    .quadrant-box h3 { margin-bottom:12px; font-size:14px; }
    .quadrant-box ul { list-style:none; padding:0; }
    .quadrant-box li { margin-bottom:8px; font-size:13px; }
    details { margin-top:16px; }
    details summary { cursor:pointer; font-weight:600; color:var(--accent); padding:8px 0; font-size:13px; }
    details summary:hover { text-decoration:underline; }
    .report-footer { margin-top:48px; padding-top:20px; border-top:1px solid var(--border); font-size:12px; color:var(--muted); text-align:center; }
    @media (max-width:768px) { .metric-grid,.quadrant-grid { grid-template-columns:1fr; } .container { padding:16px 20px 40px; } }
    @media print { body { background:#fff; font-size:12px; } .card { box-shadow:none; break-inside:avoid; } .section-title { break-after:avoid; } }
"""


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════
def generate_html(
    model_name: str,
    taxonomy: dict | None,
    dax_complexity: dict | None,
    dax_audit: dict | None,
    bpa_results: dict | None,
    dbt_lineage: dict | None,
    source_dir: Path | None = None,
) -> str:
    """Generate the full HTML report from intermediate data."""
    now = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")
    synthesis = _read_json_file(source_dir / "synthesis.json") if source_dir else None
    query_profile = _read_json_file(source_dir / "query-profile.json") if source_dir else None

    # ── Collect statistics ──
    stats = taxonomy.get("statistics", {}) if taxonomy else {}
    total_tables = stats.get("totalTables", 0)
    dq_tables = stats.get("directQueryTables", 0)
    dual_tables = stats.get("dualTables", 0)
    total_measures = stats.get("totalMeasures", 0)
    total_rels = stats.get("totalRelationships", 0)
    bidi_rels = stats.get("bidirectionalRelationships", 0)

    bpa_summary = bpa_results.get("summary", {}) if bpa_results else {}
    bpa_high = bpa_summary.get("high", 0)
    bpa_medium = bpa_summary.get("medium", 0)
    bpa_low = bpa_summary.get("low", 0)
    bpa_total = bpa_summary.get("totalFindings", 0)

    dax_stats = dax_complexity.get("statistics", {}) if dax_complexity else {}
    cross_dq = dax_stats.get("crossMultipleDQ", 0)
    avg_complexity = dax_stats.get("avgComplexityScore", 0)
    complexity_dist = dax_stats.get("complexityDistribution", {})

    dbt_stats_raw = dbt_lineage.get("statistics", {}) if dbt_lineage else {}

    # Health score: always computed from BPA findings (ignore synthesis healthScore
    # to ensure consistency). Deductions capped per severity tier.
    #   High: -10 each (no cap — high findings are serious)
    #   Medium: -3 each (capped at -30)
    #   Low: -1 each (capped at -20 — low findings are code quality noise)
    health = max(0, min(100,
        100 - bpa_high * 10 - min(bpa_medium * 3, 30) - min(bpa_low, 20)))
    health_grade = "A" if health >= 80 else ("B" if health >= 60 else ("C" if health >= 40 else ("D" if health >= 20 else "F")))

    health_class = "green" if health >= 70 else ("amber" if health >= 40 else "red")
    health_label = f"Grade {health_grade}"

    # ── Build sections ──
    sections: list[str] = []
    sec_num = 0

    # ═══════════════════════════════════════════════════════
    # SECTION: EXECUTIVE SUMMARY
    # ═══════════════════════════════════════════════════════
    sec_num += 1
    exec_summary = synthesis.get("executiveSummary", "") if synthesis else ""

    # Git context from synthesis
    git_context_html = ""
    if synthesis and synthesis.get("gitContext"):
        gc = synthesis["gitContext"]
        git_context_html = f"""
    <div class="note-box">
      <strong>Repository Context:</strong> {escape(gc.get('description', ''))}
    </div>"""

    # Databricks daily stats from synthesis
    dbx_daily_html = ""
    if synthesis and synthesis.get("databricksDailyStats"):
        ds = synthesis["databricksDailyStats"]
        dbx_daily_html = f"""
    <div class="card">
      <h3>Databricks PBI Query Load (today)</h3>
      <div class="metric-grid">
        <div class="metric-card red">
          <div class="value">{_fmt_number(ds.get('totalQueries', 0))}</div>
          <div class="label">Total PBI Queries</div>
          <div class="sub">from spn-ade-pbi</div>
        </div>
        <div class="metric-card {'red' if ds.get('slow10s', 0) > 100 else 'amber'}">
          <div class="value">{_fmt_number(ds.get('slow10s', 0))}</div>
          <div class="label">Slow &gt;10s</div>
          <div class="sub">{_fmt_number(ds.get('slow30s', 0))} &gt;30s</div>
        </div>
        <div class="metric-card red">
          <div class="value">{ds.get('totalReadTb', 0):.1f} TB</div>
          <div class="label">Data Read Today</div>
          <div class="sub">P95: {ds.get('p95s', 0):.1f}s, max: {ds.get('maxs', 0):.0f}s</div>
        </div>
        <div class="metric-card">
          <div class="value">{ds.get('avgDurationS', 0):.1f}s</div>
          <div class="label">Avg Query Duration</div>
          <div class="sub">P50: {ds.get('p50s', 0):.1f}s</div>
        </div>
      </div>
    </div>"""

    # Top findings summary for executive section
    top_findings_html = ""
    if synthesis and synthesis.get("topFindings"):
        sf = synthesis["topFindings"]
        # Show critical + high findings (max 5)
        priority_findings = [f for f in sf if f.get("severity") in ("critical", "high")][:5]
        if not priority_findings:
            priority_findings = sf[:3]
        finding_items = ""
        for f in priority_findings:
            fid = escape(str(f.get("id", "")))
            title = escape(str(f.get("title", "")))
            sev = f.get("severity", "medium")
            est = escape(str(f.get("estimatedImprovement", "")))
            where_html = _where_badges(f.get("layers", []))
            rec_text = str(f.get("recommendation", ""))
            # Extract first actionable sentence from recommendation
            first_action = rec_text.split("\n")[0].strip()
            if first_action.startswith("**"):
                # Strip markdown bold markers
                first_action = first_action.replace("**", "")
            if len(first_action) > 200:
                first_action = first_action[:197] + "..."
            finding_items += f"""
          <div style="display:flex;gap:12px;padding:12px 0;border-bottom:1px solid var(--border)">
            <div style="min-width:36px;text-align:center">
              <span style="font-weight:700;font-size:15px;color:var(--dark)">{fid}</span><br>
              {_badge(sev)}
            </div>
            <div style="flex:1">
              <div style="font-weight:600;font-size:13px;margin-bottom:4px">{title}</div>
              <div style="font-size:12px;color:var(--muted);margin-bottom:4px">{escape(first_action)}</div>
              <div style="display:flex;gap:8px;align-items:center">
                {where_html}
                {"<span style='font-size:11px;color:var(--green);font-weight:600'>" + est + "</span>" if est else ""}
              </div>
            </div>
          </div>"""
        total_findings = len(sf)
        critical_count = sum(1 for f in sf if f.get("severity") == "critical")
        high_count = sum(1 for f in sf if f.get("severity") == "high")
        top_findings_html = f"""
    <div class="card">
      <h3>Key Findings &amp; Recommended Actions ({total_findings} total — {critical_count} critical, {high_count} high)</h3>
      {finding_items}
      {"<p style='font-size:12px;color:var(--muted);margin-top:12px;text-align:center'>See section Detailed Recommendations for full implementation guides.</p>" if total_findings > len(priority_findings) else ""}
    </div>"""

    sections.append(f"""
    <h2 class="section-title">{sec_num}. Executive Summary</h2>
    <div class="metric-grid">
      <div class="metric-card {'red' if dq_tables > 5 else 'amber' if dq_tables > 0 else 'green'}">
        <div class="value">{total_tables}</div>
        <div class="label">Total Tables</div>
        <div class="sub">{dq_tables} DirectQuery, {dual_tables} Dual</div>
      </div>
      <div class="metric-card">
        <div class="value">{total_measures}</div>
        <div class="label">Total Measures</div>
        <div class="sub">{complexity_dist.get('critical', 0)} critical, {complexity_dist.get('high', 0)} high</div>
      </div>
      <div class="metric-card {'red' if bpa_high > 0 else 'amber' if bpa_medium > 0 else 'green'}">
        <div class="value">{bpa_total}</div>
        <div class="label">BPA Findings</div>
        <div class="sub">{bpa_high} high, {bpa_medium} medium</div>
      </div>
      <div class="metric-card {health_class}">
        <div class="value">{health}/100</div>
        <div class="label">Health Score</div>
        <div class="sub">{health_label}</div>
      </div>
    </div>
    {"<div class='card'><p>" + escape(exec_summary) + "</p></div>" if exec_summary else ""}
    {top_findings_html}
    {git_context_html}
    {dbx_daily_html}
    """)

    # ═══════════════════════════════════════════════════════
    # SECTION: DATABRICKS QUERY PROFILE
    # ═══════════════════════════════════════════════════════
    if query_profile and query_profile.get("queries"):
        sec_num += 1
        qp_sections = []
        for q in query_profile["queries"]:
            dur = q.get("duration_ms", 0)
            dur_s = f"{dur / 1000:.1f}s" if dur else "?"
            rows_read = q.get("rows_read", 0)
            rows_ret = q.get("rows_returned", 0)
            data_gb = q.get("data_read_gb", 0)
            amp = q.get("scan_amplification_ratio", 0)

            # Metadata table
            meta_rows = ""
            for k, v in [
                ("Report", q.get("report_name", "")),
                ("Visual ID", q.get("visual_id", "")),
                ("Duration", dur_s),
                ("Rows Read", _fmt_number(rows_read)),
                ("Rows Returned", _fmt_number(rows_ret)),
                ("Data Read", f"{data_gb:.2f} GB"),
                ("Scan Amplification", f"{_fmt_number(amp)}:1"),
                ("Photon", f"{q.get('photon_pct', 0)}%"),
                ("Date Filter", q.get("date_filter", {}).get("effective_filter", "?")),
            ]:
                if v:
                    meta_rows += f"<tr><td>{escape(k)}</td><td><strong>{escape(str(v))}</strong></td></tr>"

            # Tables scanned
            table_rows = ""
            for t in q.get("tables_joined", []):
                cols_sel = t.get("columns_selected", "?")
                cols_need = t.get("columns_needed", "?")
                note = t.get("note", "")
                table_rows += f"""<tr>
                  <td>{escape(str(t.get('name', '')))}</td>
                  <td>{_badge('high') if isinstance(cols_sel, int) and cols_sel > 50 else escape(str(cols_sel))}</td>
                  <td>{cols_need}</td>
                  <td>{escape(note)}</td></tr>"""

            # Operators
            ops_html = ""
            ops = q.get("operators", [])
            if ops:
                total_time = sum(o.get("time_min", o.get("time_sec", 0) / 60) for o in ops)
                for o in ops[:6]:
                    t_min = o.get("time_min", o.get("time_sec", 0) / 60)
                    pct = (t_min / total_time * 100) if total_time > 0 else 0
                    label = o.get("note", o.get("type", ""))
                    time_str = f"{t_min:.2f} min" if t_min >= 1 else f"{t_min * 60:.1f}s"
                    ops_html += _perf_bar(f"#{o.get('id', '')} {o.get('type', '')} — {label}", time_str, pct)

            qp_sections.append(f"""
    <div class="card">
      <h3>Query: {escape(q.get('report_name', 'Unknown'))}</h3>
      <div class="metric-grid">
        <div class="metric-card red"><div class="value">{dur_s}</div><div class="label">Duration</div></div>
        <div class="metric-card red"><div class="value">{_fmt_number(rows_read)}</div><div class="label">Rows Read</div><div class="sub">returned: {_fmt_number(rows_ret)}</div></div>
        <div class="metric-card red"><div class="value">{data_gb:.1f} GB</div><div class="label">Data Read</div></div>
        <div class="metric-card red"><div class="value">{_fmt_number(amp)}:1</div><div class="label">Scan Amplification</div></div>
      </div>
      <table><thead><tr><th>Field</th><th>Value</th></tr></thead><tbody>{meta_rows}</tbody></table>
    </div>
    <div class="card">
      <h3>Tables Scanned</h3>
      <table><thead><tr><th>Table</th><th>Cols Selected</th><th>Cols Needed</th><th>Notes</th></tr></thead><tbody>{table_rows}</tbody></table>
    </div>
    {"<div class='card'><h3>Operator Bottlenecks</h3>" + ops_html + "</div>" if ops_html else ""}
    """)

        sections.append(f"""
    <h2 class="section-title">{sec_num}. Databricks Query Profile</h2>
    {"".join(qp_sections)}
    """)

    # ═══════════════════════════════════════════════════════
    # SECTION: MODEL TAXONOMY
    # ═══════════════════════════════════════════════════════
    if taxonomy:
        sec_num += 1
        has_volumetry = any(t.get("volumetry") for t in taxonomy.get("tables", []))
        table_rows = ""
        total_data_gb = 0.0
        for t in taxonomy.get("tables", []):
            src = ""
            if t.get("sourceTable"):
                src = f"{t['sourceCatalog']}.{t['sourceDatabase']}.{t['sourceTable']}"
            vol = t.get("volumetry", {})
            row_count = vol.get("rowCount")
            size_gb = vol.get("sizeGB")
            if size_gb:
                total_data_gb += size_gb
            cls = t.get("classification", "")
            size_class = ""
            if size_gb and size_gb > 10:
                size_class = ' style="background:rgba(192,57,43,0.06)"'
            elif size_gb and size_gb > 1:
                size_class = ' style="background:rgba(212,160,23,0.06)"'
            table_rows += f"""<tr{size_class}>
              <td>{escape(t['name'])}</td>
              <td>{_classification_badge(cls) if cls else '-'}</td>
              <td>{_badge(t['storageMode'])}</td>
              <td>{t['columnCount']}</td>
              <td>{t['measureCount']}</td>
              <td>{_fmt_rows(row_count)}</td>
              <td>{_fmt_gb(size_gb)}</td>
              <td style="font-size:11px">{escape(src)}</td></tr>"""

        # Volumetry summary card
        vol_summary = ""
        if has_volumetry:
            vol_summary = f"""
        <div class="metric-grid" style="margin-bottom:16px">
          <div class="metric-card"><div class="value">{_fmt_gb(total_data_gb)}</div><div class="label">Total Model Data</div></div>
        </div>"""

        rel_rows = ""
        for r in taxonomy.get("relationships", []):
            active = "Yes" if r.get("isActive", True) else "No"
            cross = r.get("crossFilteringBehavior", "oneDirection")
            card = f"{r.get('fromCardinality', '*')}:{r.get('toCardinality', '1')}"
            rel_rows += f"""<tr>
              <td>{escape(r.get('fromTable', ''))}[{escape(r.get('fromColumn', ''))}]</td>
              <td>{escape(r.get('toTable', ''))}[{escape(r.get('toColumn', ''))}]</td>
              <td>{card}</td><td>{cross}</td><td>{active}</td></tr>"""

        # Graph analysis / relationship topology
        graph_html = ""
        ga = taxonomy.get("graphAnalysis", {})
        if ga and ga.get("hubTables"):
            hub_rows = ""
            for h in ga["hubTables"]:
                hub_rows += f"""<tr>
                  <td><strong>{escape(h['name'])}</strong></td>
                  <td>{h['degree']}</td>
                  <td>{_classification_badge(h.get('classification', ''))}</td>
                  <td>{_badge(h.get('storageMode', ''))}</td>
                  <td>{_fmt_rows(h.get('rowCount'))}</td>
                  <td>{_fmt_gb(h.get('sizeGB'))}</td></tr>"""
            graph_html = f"""
        <div class="card"><h3>Relationship Topology</h3>
          <div class="metric-grid" style="margin-bottom:16px">
            <div class="metric-card"><div class="value">{ga.get('maxSnowflakeDepth', 0)}</div><div class="label">Max Snowflake Depth</div></div>
            <div class="metric-card"><div class="value">{ga.get('avgDegree', 0)}</div><div class="label">Avg Degree</div></div>
            <div class="metric-card red"><div class="value">{len(ga.get('hubTables', []))}</div><div class="label">Hub Tables (degree &ge; 5)</div></div>
          </div>
          <table><thead><tr><th>Hub Table</th><th>Degree</th><th>Type</th><th>Storage</th><th>Rows</th><th>Size</th></tr></thead>
            <tbody>{hub_rows}</tbody></table>
        </div>"""

        sections.append(f"""
        <h2 class="section-title">{sec_num}. Model Taxonomy</h2>
        {vol_summary}
        <div class="card"><h3>Tables &amp; Storage Modes</h3>
          <table><thead><tr><th>Table</th><th>Type</th><th>Storage Mode</th><th>Columns</th><th>Measures</th><th>Rows</th><th>Size</th><th>Databricks Source</th></tr></thead>
            <tbody>{table_rows}</tbody></table></div>
        <div class="card"><h3>Relationships ({total_rels} total — {bidi_rels} bidirectional)</h3>
          <table><thead><tr><th>From</th><th>To</th><th>Cardinality</th><th>Cross-Filter</th><th>Active</th></tr></thead>
            <tbody>{rel_rows}</tbody></table></div>
        {graph_html}
        """)

    # ═══════════════════════════════════════════════════════
    # SECTION: DAX COMPLEXITY
    # ═══════════════════════════════════════════════════════
    if dax_complexity:
        sec_num += 1
        dax_s = dax_complexity.get("statistics", {})
        measures = dax_complexity.get("measures", [])
        top_measures = measures[:20]
        measure_rows = ""
        for m in top_measures:
            ctx = m.get("contextTransitions", 0)
            hops = m.get("relationshipHops", 0)
            fa = m.get("filterAllCount", 0)
            subq = m.get("estimatedSQLSubqueries", 0)
            cross = "Yes" if m.get("crossesMultipleDQ") else ""
            measure_rows += f"""<tr>
              <td>{escape(m['name'])}</td><td>{escape(m['hostTable'])}</td>
              <td>{m['complexityScore']}</td>
              <td>{ctx}</td><td>{hops}</td><td>{fa}</td><td>{subq}</td>
              <td>{_badge(m['complexityLevel'])}</td>
              <td>{escape(cross)}</td></tr>"""

        # Summary metrics
        measures_high_subq = dax_s.get("measuresWithHighSubqueries", 0)
        measures_filter_all = dax_s.get("measuresWithFilterAll", 0)

        # Hot tables — enriched
        hot_table_rows = ""
        for ht in dax_complexity.get("hotTables", [])[:10]:
            priority = ht.get("optimizationPriority", "low")
            hot_table_rows += f"""<tr>
              <td>{escape(ht['table'])}</td><td>{_badge(ht['storageMode'])}</td>
              <td>{ht['referenceCount']}</td>
              <td>{_fmt_rows(ht.get('rowCount'))}</td>
              <td>{_fmt_gb(ht.get('sizeGB'))}</td>
              <td>{ht.get('degree', '-')}</td>
              <td>{_priority_badge(priority)}</td></tr>"""

        sections.append(f"""
        <h2 class="section-title">{sec_num}. DAX Complexity Report</h2>
        <div class="metric-grid" style="margin-bottom:16px">
          <div class="metric-card {'red' if measures_high_subq > 10 else 'amber' if measures_high_subq > 0 else 'green'}">
            <div class="value">{measures_high_subq}</div>
            <div class="label">Measures with 5+ SQL Subqueries</div>
          </div>
          <div class="metric-card {'red' if measures_filter_all > 20 else 'amber' if measures_filter_all > 0 else 'green'}">
            <div class="value">{measures_filter_all}</div>
            <div class="label">Measures with FILTER(ALL)</div>
          </div>
          <div class="metric-card">
            <div class="value">{dax_s.get('avgContextTransitions', 0)}</div>
            <div class="label">Avg Context Transitions</div>
          </div>
          <div class="metric-card">
            <div class="value">{dax_s.get('avgComplexityScore', 0)}</div>
            <div class="label">Avg Complexity Score</div>
          </div>
        </div>
        <div class="card"><h3>Top {len(top_measures)} Most Complex Measures</h3>
          <table><thead><tr><th>Measure</th><th>Table</th><th>Score</th><th>Ctx Trans.</th><th>Hops</th><th>FILTER(ALL)</th><th>Est. SQL Subq.</th><th>Level</th><th>Cross-DQ</th></tr></thead>
            <tbody>{measure_rows}</tbody></table></div>
        <div class="card"><h3>Hot Tables — Optimisation Candidates</h3>
          <table><thead><tr><th>Table</th><th>Storage</th><th>Refs</th><th>Rows</th><th>Size</th><th>Degree</th><th>Priority</th></tr></thead>
            <tbody>{hot_table_rows}</tbody></table></div>
        """)

    # ═══════════════════════════════════════════════════════
    # SECTION: BPA FINDINGS (collapsible)
    # ═══════════════════════════════════════════════════════
    if bpa_results:
        sec_num += 1
        findings = bpa_results.get("findings", [])
        passing_rules = bpa_results.get("passingRules", [])

        # Rule violations table — only show FAIL rules with impact type
        rule_rows = ""
        for r in bpa_results.get("ruleResults", []):
            impact = r.get("performanceImpact", "")
            impact_badge = _impact_type_badge(impact) if impact else ""
            impact_desc = escape(r.get("impactDescription", ""))
            rule_rows += f"""<tr>
              <td>{escape(r.get('rule', ''))}</td>
              <td>{r.get('count', 0)}</td>
              <td>{impact_badge}</td>
              <td style="font-size:12px;color:var(--muted)">{impact_desc}</td></tr>"""

        # Passing rules summary
        passing_html = ""
        if passing_rules:
            passing_html = f'<p style="font-size:13px;color:var(--green);margin-top:12px">&#10003; {len(passing_rules)} of {len(passing_rules) + len(bpa_results.get("ruleResults", []))} rules passed (no violations): {", ".join(passing_rules)}</p>'

        # Split findings: High always shown, 10 Medium shown, rest collapsed
        high_findings = [f for f in findings if f.get("severity", "").lower() == "high"]
        medium_findings = [f for f in findings if f.get("severity", "").lower() == "medium"]
        low_findings = [f for f in findings if f.get("severity", "").lower() == "low"]

        def _finding_row(f):
            return f"""<tr>
              <td>{escape(f.get('rule', ''))}</td><td>{_badge(f.get('severity', ''))}</td>
              <td>{escape(f.get('table', ''))}</td><td>{escape(f.get('object', ''))}</td>
              <td>{escape(f.get('message', ''))}</td></tr>"""

        visible_rows = "".join(_finding_row(f) for f in high_findings)
        visible_rows += "".join(_finding_row(f) for f in medium_findings[:10])
        visible_count = len(high_findings) + min(10, len(medium_findings))

        collapsed_rows = "".join(_finding_row(f) for f in medium_findings[10:])
        collapsed_rows += "".join(_finding_row(f) for f in low_findings)
        collapsed_count = len(medium_findings[10:]) + len(low_findings)

        collapsed_html = ""
        if collapsed_count > 0:
            collapsed_html = f"""
        <details>
          <summary>Show {collapsed_count} more findings ({len(medium_findings) - min(10, len(medium_findings))} medium, {len(low_findings)} low)...</summary>
          <table><thead><tr><th>Rule</th><th>Severity</th><th>Table</th><th>Object</th><th>Detail</th></tr></thead>
            <tbody>{collapsed_rows}</tbody></table>
        </details>"""

        sections.append(f"""
        <h2 class="section-title">{sec_num}. Best Practice Findings</h2>
        <div class="card"><h3>Rule Violations</h3>
          <table><thead><tr><th>Rule</th><th>Violations</th><th>Impact Type</th><th>Performance Impact</th></tr></thead>
            <tbody>{rule_rows}</tbody></table>
          {passing_html}
        </div>
        <div class="card">
          <h3>Detailed Findings ({len(findings)} total — showing {visible_count} critical/high/medium)</h3>
          <table><thead><tr><th>Rule</th><th>Severity</th><th>Table</th><th>Object</th><th>Detail</th></tr></thead>
            <tbody>{visible_rows}</tbody></table>
          {collapsed_html}
        </div>
        """)

    # ═══════════════════════════════════════════════════════
    # SECTION: dbt LINEAGE (conditional — value gate)
    # ═══════════════════════════════════════════════════════
    if dbt_lineage:
        sec_num += 1
        has_actionable = dbt_lineage.get("hasActionableFindings", False)
        actionable = dbt_lineage.get("actionableFindings", [])

        if has_actionable:
            # Show actionable findings table
            af_rows = ""
            type_badges = {
                "wide-serve-view": ("badge-high", "Wide View"),
                "missing-filter": ("badge-medium", "Missing Filter"),
                "should-materialise": ("badge-info", "Materialise"),
                "missing-clustering": ("badge-medium", "No Clustering"),
            }
            for af in actionable:
                tb_cls, tb_label = type_badges.get(af.get("type", ""), ("badge-info", af.get("type", "")))
                af_rows += f"""<tr>
                  <td><span class="badge {tb_cls}">{tb_label}</span></td>
                  <td>{escape(af.get('domain', ''))}</td>
                  <td>{escape(af.get('model', ''))}</td>
                  <td>{escape(af.get('detail', ''))}</td></tr>"""

            sections.append(f"""
        <h2 class="section-title">{sec_num}. dbt Lineage Analysis</h2>
        <div class="metric-grid">
          <div class="metric-card"><div class="value">{dbt_stats_raw.get('totalServeModels', 0)}</div><div class="label">Serve Models</div><div class="sub">{dbt_stats_raw.get('domainCount', 0)} domains</div></div>
          <div class="metric-card amber"><div class="value">{dbt_stats_raw.get('materializations', {}).get('view', 0)}</div><div class="label">Views</div><div class="sub">not materialised</div></div>
          <div class="metric-card {'red' if dbt_stats_raw.get('wideModels', 0) > 0 else 'green'}"><div class="value">{dbt_stats_raw.get('wideModels', 0)}</div><div class="label">Wide Models</div><div class="sub">&gt;50 columns</div></div>
          <div class="metric-card red"><div class="value">{len(actionable)}</div><div class="label">Actionable Findings</div></div>
        </div>
        <div class="card"><h3>Actionable Findings ({len(actionable)})</h3>
          <table><thead><tr><th>Type</th><th>Domain</th><th>Model</th><th>Detail</th></tr></thead>
            <tbody>{af_rows}</tbody></table></div>
        """)
        else:
            # Collapsed — no actionable findings
            sections.append(f"""
        <h2 class="section-title">{sec_num}. dbt Lineage Analysis</h2>
        <div class="note-box">
          <strong>No actionable performance findings.</strong> dbt lineage analysed — {dbt_stats_raw.get('totalServeModels', 0)} serve models across {dbt_stats_raw.get('domainCount', 0)} domains.
          All serve views are pass-through views over curated tables with no performance-impacting patterns detected.
        </div>
        """)

    # ═══════════════════════════════════════════════════════
    # SECTION: ROOT CAUSE ANALYSIS (from synthesis)
    # ═══════════════════════════════════════════════════════
    if synthesis and "topFindings" in synthesis:
        sec_num += 1
        syn_findings = synthesis["topFindings"]
        finding_rows = ""
        for f in syn_findings:
            scope = f.get("scope", "model-wide")
            finding_rows += (
                f"<tr><td>{escape(str(f.get('id', '')))}</td>"
                f"<td><strong>{escape(str(f.get('title', '')))}</strong></td>"
                f"<td>{_badge(f.get('severity', 'medium'))}</td>"
                f"<td>{_scope_badge(scope)}</td>"
                f"<td>{_where_badges(f.get('layers', []))}</td>"
                f"<td>{escape(str(f.get('impact', '')))}</td>"
                f"<td>{escape(str(f.get('effort', '')))}</td></tr>\n"
            )

        sections.append(f"""
    <h2 class="section-title">{sec_num}. Root Cause Analysis</h2>
    <div class="card"><h3>Findings by Severity</h3>
      <table><thead><tr><th>#</th><th>Finding</th><th>Severity</th><th>Scope</th><th>Where</th><th>Impact</th><th>Effort</th></tr></thead>
        <tbody>{finding_rows}</tbody></table></div>
    """)

    # ═══════════════════════════════════════════════════════
    # SECTION: RECOMMENDATION QUADRANT
    # ═══════════════════════════════════════════════════════
    if synthesis and "topFindings" in synthesis:
        sec_num += 1
        syn_findings = synthesis["topFindings"]
        quadrants = {"quick_win": [], "strategic": [], "minor": [], "deprioritise": []}
        for f in syn_findings:
            q = f.get("quadrant", "strategic")
            quadrants.setdefault(q, []).append(f)

        def _quad_items(items):
            return "".join(
                f"<li><strong>{escape(str(f.get('id', '')))}</strong>: {escape(str(f.get('title', '')))} "
                f"{_where_badges(f.get('layers', []))}</li>"
                for f in items
            )

        sections.append(f"""
    <h2 class="section-title">{sec_num}. Recommendation Quadrant</h2>
    <div class="quadrant-grid">
      <div class="quadrant-box" style="background:rgba(26,135,84,0.06);border:1px solid rgba(26,135,84,0.2)">
        <h3 style="color:var(--green)">Quick Wins (Low Effort, High Impact)</h3>
        <ul>{_quad_items(quadrants.get('quick_win', []))}</ul>
      </div>
      <div class="quadrant-box" style="background:rgba(7,112,207,0.06);border:1px solid rgba(7,112,207,0.2)">
        <h3 style="color:var(--accent)">Strategic Investments (Higher Effort, High Impact)</h3>
        <ul>{_quad_items(quadrants.get('strategic', []))}</ul>
      </div>
    </div>
    {('<div class="card"><h3>Deprioritise (High Effort, Low Impact)</h3><ul>' + _quad_items(quadrants.get("deprioritise", [])) + '</ul></div>') if quadrants.get("deprioritise") else ""}
    """)

    # ═══════════════════════════════════════════════════════
    # SECTION: DETAILED RECOMMENDATIONS (per finding)
    # ═══════════════════════════════════════════════════════
    if synthesis and "topFindings" in synthesis:
        sec_num += 1
        detail_cards = ""
        # Build evidence lookup from query-profile.json
        evidence_lookup: dict[str, dict] = {}
        if query_profile and query_profile.get("evidence"):
            for ev in query_profile["evidence"]:
                evidence_lookup[ev.get("claimId", "")] = ev

        for f in synthesis["topFindings"]:
            fid = escape(str(f.get("id", "")))
            title = escape(str(f.get("title", "")))
            sev = f.get("severity", "medium")
            scope = f.get("scope", "model-wide")
            desc = escape(str(f.get("description", "")))
            rec = escape(str(f.get("recommendation", "")))
            est = escape(str(f.get("estimatedImprovement", "")))
            where_html = _where_badges(f.get("layers", []))
            impact = escape(str(f.get("impact", "")))
            effort = escape(str(f.get("effort", "")))

            # Impact breakdown (planning / execution / delivery)
            breakdown_html = ""
            ib = f.get("impactBreakdown")
            if ib:
                breakdown_html = f"""<div style="margin-top:12px;padding:12px;background:var(--light-bg);border-radius:6px">
                  <strong>Query Time Breakdown:</strong>
                  <div style="display:flex;gap:16px;margin-top:8px;font-size:13px">
                    <span>Planning: <strong>{escape(str(ib.get('planning', '-')))}</strong></span>
                    <span>Execution: <strong>{escape(str(ib.get('execution', '-')))}</strong></span>
                    <span>Delivery: <strong>{escape(str(ib.get('delivery', '-')))}</strong></span>
                  </div></div>"""

            # Connection mode comparison
            cmc_html = ""
            cmc = f.get("connectionModeComparison")
            if cmc:
                cmc_rows = ""
                for mode_key, mode_label in [("directQuery", "DirectQuery"), ("hybrid", "Hybrid"), ("import", "Import"), ("parameterFilter", "Parameter Filter")]:
                    val = cmc.get(mode_key, "")
                    if val:
                        cmc_rows += f"<tr><td><strong>{mode_label}</strong></td><td>{escape(str(val))}</td></tr>"
                if cmc_rows:
                    cmc_html = f"""<div style="margin-top:12px"><strong>Connection Mode Comparison:</strong>
                      <table style="margin-top:8px"><thead><tr><th>Mode</th><th>Implications</th></tr></thead>
                        <tbody>{cmc_rows}</tbody></table></div>"""

            # Dependencies
            deps_html = ""
            deps = f.get("dependencies", [])
            if deps:
                dep_badges = " ".join(f'<span class="badge badge-info">{escape(d)}</span>' for d in deps)
                dep_note = escape(str(f.get("dependencyNote", "")))
                deps_html = f'<div style="margin-top:12px"><strong>Depends on:</strong> {dep_badges}'
                if dep_note:
                    deps_html += f'<br><span style="font-size:12px;color:var(--muted)">{dep_note}</span>'
                deps_html += '</div>'

            # Sub-findings
            sub_html = ""
            subs = f.get("subFindings", [])
            if subs:
                sub_items = ""
                for sf in subs:
                    sub_items += f"""<div style="border-left:3px solid var(--border);padding:8px 12px;margin:6px 0">
                      <strong>{escape(str(sf.get('relationship', sf.get('item', ''))))}</strong>
                      <br><span style="font-size:12px">{escape(str(sf.get('reason', '')))}</span>
                      <br><span style="font-size:12px;color:var(--green)">Fix: {escape(str(sf.get('recommendation', '')))}</span>
                      <span class="badge badge-info" style="margin-left:8px">{escape(str(sf.get('effort', '')))}</span>
                    </div>"""
                sub_html = f'<div style="margin-top:12px"><strong>Individual Analysis:</strong>{sub_items}</div>'

            # Deep-dive flag
            deep_html = ""
            if f.get("requiresDeepDive"):
                steps = f.get("suggestedAnalysisSteps", [])
                team = f.get("assignedTeam", "")
                step_items = "".join(f"<li>{escape(str(s))}</li>" for s in steps)
                deep_html = f"""<div class="note-box" style="margin-top:12px">
                  <strong>Requires Deep Dive</strong>{"  — assigned to: " + escape(team) if team else ""}
                  {"<ol style='padding-left:18px;margin-top:8px'>" + step_items + "</ol>" if step_items else ""}
                </div>"""

            # Evidence block (from query-profile.json)
            evidence_html = ""
            evidence_ids = f.get("evidenceIds", [])
            if evidence_ids:
                ev_blocks = ""
                for eid in evidence_ids:
                    ev = evidence_lookup.get(eid, {})
                    if ev:
                        sql_snip = escape(str(ev.get("sqlSnippet", "")))[:500]
                        ev_blocks += f"""<div style="border-left:3px solid var(--accent);padding:8px 12px;margin:6px 0">
                          <strong>{escape(str(ev.get('claim', '')))}</strong>
                          <br><span style="font-size:12px">Impact type: {_impact_type_badge(ev.get('impactType', ''))}</span>
                          <br><span style="font-size:12px;color:var(--muted)">{escape(str(ev.get('impactExplanation', '')))}</span>
                          {"<pre style='font-size:11px;background:var(--light-bg);padding:8px;border-radius:4px;margin-top:6px;overflow-x:auto;white-space:pre-wrap'>" + sql_snip + "</pre>" if sql_snip else ""}
                        </div>"""
                if ev_blocks:
                    evidence_html = f"""<details style="margin-top:12px">
                      <summary>Evidence ({len(evidence_ids)} sources)</summary>{ev_blocks}</details>"""

            # Trade-offs
            tradeoffs = f.get("tradeoffs", [])
            tradeoffs_html = ""
            if tradeoffs:
                items = "".join(f"<li>{escape(str(t))}</li>" for t in tradeoffs)
                tradeoffs_html = f'<div style="margin-top:12px"><strong>Trade-offs:</strong><ul style="padding-left:18px;margin-top:4px">{items}</ul></div>'

            # Options (alternative approaches)
            options = f.get("options", [])
            options_html = ""
            if options:
                opt_rows = ""
                for o in options:
                    opt_name = escape(str(o.get("name", "")))
                    opt_desc = escape(str(o.get("description", "")))
                    opt_pros = escape(str(o.get("pros", "")))
                    opt_cons = escape(str(o.get("cons", "")))
                    opt_rows += f"""<tr><td><strong>{opt_name}</strong></td>
                      <td>{opt_desc}</td><td style="color:var(--green)">{opt_pros}</td>
                      <td style="color:var(--red)">{opt_cons}</td></tr>"""
                options_html = f"""<div style="margin-top:12px"><strong>Options:</strong>
                  <table style="margin-top:8px"><thead><tr><th>Option</th><th>Description</th><th>Pros</th><th>Cons</th></tr></thead>
                    <tbody>{opt_rows}</tbody></table></div>"""

            sev_colour = "var(--red)" if sev in ("critical", "high") else ("var(--amber)" if sev == "medium" else "var(--green)")

            detail_cards += f"""
    <div class="card" style="border-left:4px solid {sev_colour}">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
        <h3 style="margin:0">{fid}: {title}</h3>
        <div>{_badge(sev)} {_scope_badge(scope)} {where_html}</div>
      </div>
      <p style="font-size:13px;margin-bottom:12px"><strong>Problem:</strong> {desc}</p>
      <div class="recommendation-box" style="margin:0">
        <h4>How to Fix</h4>
        <p>{rec}</p>
        {"<p style='margin-top:8px'><strong>Estimated improvement:</strong> " + est + "</p>" if est else ""}
      </div>
      <div style="display:flex;gap:24px;margin-top:12px;font-size:13px">
        <span><strong>Impact:</strong> {impact}</span>
        <span><strong>Effort:</strong> {effort}</span>
      </div>
      {breakdown_html}
      {cmc_html}
      {deps_html}
      {sub_html}
      {deep_html}
      {evidence_html}
      {tradeoffs_html}
      {options_html}
    </div>
"""
        sections.append(f"""
    <h2 class="section-title">{sec_num}. Detailed Recommendations</h2>
    <p style="font-size:13px;color:var(--muted);margin-bottom:20px">
      Implementation details, trade-offs, and alternative options for each finding.
    </p>
    {detail_cards}
    """)

    # ═══════════════════════════════════════════════════════
    # SECTION: IMPLEMENTATION ROADMAP
    # ═══════════════════════════════════════════════════════
    if synthesis and synthesis.get("implementationRoadmap"):
        sec_num += 1
        syn_findings = synthesis.get("topFindings", [])
        roadmap_html = ""
        for phase in synthesis["implementationRoadmap"]:
            phase_name = escape(str(phase.get("phase", "")))
            items_rows = ""
            # Support both formats:
            #   Old: {"items": ["F1: description", ...]}
            #   New: {"actions": [{"action": "...", "where": "...", "finding": "F1"}]}
            raw_items = phase.get("actions", phase.get("items", []))
            for item in raw_items:
                if isinstance(item, dict):
                    desc = item.get("action", "")
                    where_val = item.get("where", "")
                    finding_ref = item.get("finding", "")
                    if where_val:
                        where_r = _where_badges([w.strip() for w in where_val.split(",")] if "," in where_val else [where_val])
                    else:
                        fid_list = [f.strip() for f in finding_ref.split(",")]
                        matched = [f for f in syn_findings if f.get("id") in fid_list]
                        layers = []
                        for m in matched:
                            layers.extend(m.get("layers", []))
                        where_r = _where_badges(list(dict.fromkeys(layers))) if layers else '<span class="badge badge-info">TBD</span>'
                    if finding_ref:
                        desc = f"[{finding_ref}] {desc}"
                    items_rows += f"<tr><td><strong>{escape(desc)}</strong></td><td>{where_r}</td></tr>\n"
                else:
                    # Legacy string format: "F1: description"
                    parts = str(item).split(":", 1)
                    fid_ref = parts[0].strip() if len(parts) > 1 else ""
                    desc = parts[1].strip() if len(parts) > 1 else str(item)
                    matched = [f for f in syn_findings if f.get("id") == fid_ref]
                    where_r = _where_badges(matched[0].get("layers", [])) if matched else '<span class="badge badge-info">TBD</span>'
                    items_rows += f"<tr><td><strong>{escape(desc)}</strong></td><td>{where_r}</td></tr>\n"

            roadmap_html += f"""
    <div class="recommendation-box"><h4>{phase_name}</h4>
      <table style="margin-top:12px"><thead><tr><th>Action</th><th>Where</th></tr></thead>
        <tbody>{items_rows}</tbody></table></div>"""

        sections.append(f"""
    <h2 class="section-title">{sec_num}. Implementation Roadmap</h2>
    <div class="note-box">
      <strong>Where:</strong>
      <span class="badge badge-high">Engineering</span> = Databricks / dbt / Delta tables
      &nbsp;&middot;&nbsp;
      <span class="badge badge-medium">Semantic Model</span> = PBI model / DAX / M expressions
      &nbsp;&middot;&nbsp;
      <span class="badge badge-info">Power BI</span> = Report layout / slicers / configuration
    </div>
    {roadmap_html}
    """)

    # ═══════════════════════════════════════════════════════
    # SECTION: HEALTH SCORE SUMMARY
    # ═══════════════════════════════════════════════════════
    sec_num += 1
    sections.append(f"""
    <h2 class="section-title">{sec_num}. Health Score Summary</h2>
    <div class="card" style="text-align:center;padding:40px">
      <div style="font-size:64px;font-weight:700;color:var(--{health_class})">{health}/100</div>
      <div style="font-size:16px;font-weight:600;color:var(--{health_class});margin-top:8px">Grade {health_grade}</div>
    </div>
    """)

    # ═══════════════════════════════════════════════════════
    # ASSEMBLE
    # ═══════════════════════════════════════════════════════
    sections_html = "\n".join(sections)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Performance Diagnosis: {escape(model_name)}</title>
  <style>{CSS}</style>
</head>
<body>
  <div class="report-header">
    <h1>Performance Diagnosis: {escape(model_name)}</h1>
    <div class="metadata">
      <span>{now}</span>
      <span>PBI Performance Diagnosis Agent</span>
      <span>Health: {health}/100 ({health_grade})</span>
      <span>Mode: {escape(synthesis.get('analysisMode', 'model-wide').replace('-', ' ').title()) if synthesis else 'Standard'}</span>
    </div>
  </div>
  <div class="container">
    {sections_html}
    <div class="report-footer">
      Generated by PBI Performance Diagnosis Agent &middot; {now} &middot; Health Score: {health}/100 (Grade {health_grade})
    </div>
  </div>
</body>
</html>"""

    return html


def main():
    parser = argparse.ArgumentParser(
        description="Generate HTML performance diagnosis report"
    )
    parser.add_argument("--input", required=True, type=Path, help="Directory containing intermediate JSON files")
    parser.add_argument("--output", required=True, type=Path, help="Output directory for HTML report")
    parser.add_argument("--model-name", required=True, help="Name of the semantic model")
    parser.add_argument(
        "--run-label", required=False, default="",
        help="Short description for the run subdirectory (e.g., 'ade-sales-query-diagnosis'). "
             "If omitted, a label is derived from the model name.",
    )
    args = parser.parse_args()

    input_dir = args.input.resolve()
    output_dir = args.output.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create timestamped subdirectory for this execution
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")
    run_label = re.sub(r"[^a-zA-Z0-9_-]", "-", (args.run_label or args.model_name).strip().lower())
    run_dir = output_dir / f"{run_ts}_{run_label}"
    run_dir.mkdir(parents=True, exist_ok=True)

    taxonomy = _read_json_file(input_dir / "model-taxonomy.json")
    dax_complexity = _read_json_file(input_dir / "dax-complexity.json")
    dax_audit = _read_json_file(input_dir / "dax-audit.json")
    bpa_results = _read_json_file(input_dir / "bpa-results.json")
    dbt_lineage = _read_json_file(input_dir / "dbt-lineage.json")

    html = generate_html(
        model_name=args.model_name,
        taxonomy=taxonomy,
        dax_complexity=dax_complexity,
        dax_audit=dax_audit,
        bpa_results=bpa_results,
        dbt_lineage=dbt_lineage,
        source_dir=input_dir,
    )

    safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", args.model_name)
    output_file = run_dir / f"{safe_name}_Performance_Diagnosis.html"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    # Move intermediate JSON files into the run subdirectory
    json_files = ["model-taxonomy.json", "dax-audit.json", "dax-complexity.json",
                  "dbt-lineage.json", "bpa-results.json", "synthesis.json",
                  "query-profile.json", "perf-summary.json", "databricks-profile.json"]
    moved = []
    for jf in json_files:
        src = input_dir / jf
        if src.is_file():
            shutil.move(str(src), str(run_dir / jf))
            moved.append(jf)

    print(f"Report generated: {output_file}")
    if moved:
        print(f"Moved {len(moved)} intermediate files to: {run_dir}/")


if __name__ == "__main__":
    main()
