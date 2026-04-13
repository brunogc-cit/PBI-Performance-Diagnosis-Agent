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
    """Build Where category badges from layer list.

    Categories:
      PBI Report   — report layout, pages, slicers, filters
      PBI Visual   — specific visual configurations, card types
      Semantic Model — DAX measures, relationships, table/column config
      dbt Models   — dbt SQL, materialisation, clustering, serve views
    """
    badges = []
    for layer in layers:
        ll = layer.lower()
        if any(kw in ll for kw in ("dbt", "databricks", "engineering", "serve view", "curated", "delta")):
            badges.append('<span class="badge badge-high">dbt Models</span>')
        if any(kw in ll for kw in ("semantic", "model", "dax", "directquery", "measure", "relationship", "table config")):
            badges.append('<span class="badge badge-medium">Semantic Model</span>')
        if any(kw in ll for kw in ("visual", "card", "matrix visual", "chart")):
            badges.append('<span class="badge" style="background:rgba(108,117,125,0.12);color:#6c757d">PBI Visual</span>')
        if any(kw in ll for kw in ("power bi", "pbi report", "report", "page", "slicer", "filter", "layout")):
            badges.append('<span class="badge badge-info">PBI Report</span>')
    html = " ".join(dict.fromkeys(badges))
    return html or '<span class="badge badge-info">TBD</span>'


def _where_with_location(layers: list[str], location: str = "") -> str:
    """Build Where badges with an actual location string.

    Returns category badge(s) followed by the specific location
    (e.g., page name, table name, dbt model name).
    """
    badges_html = _where_badges(layers)
    if location:
        return f'{badges_html} <span style="font-size:11px;color:var(--muted)">{escape(location)}</span>'
    return badges_html


def _extract_location_from_action(action_text: str, where_val: str) -> str:
    """Best-effort extraction of a specific location from an action description.

    Looks for common patterns like table names, page names, model names, etc.
    """
    import re as _re
    # dbt model names (serve_*, curated_*)
    m = _re.search(r'\b(serve_\w+|curated_\w+)\b', action_text)
    if m:
        return m.group(1)
    # PBI table names in quotes or after "on"
    m = _re.search(r'(?:on|from|in)\s+["\']?([A-Z][a-z][\w\s]+(?:Table|View))["\']?', action_text)
    if m:
        return m.group(1).strip()
    # Report/page names (e.g., "Trade report", "Sales page")
    m = _re.search(r'\b(\w+(?:\s+\w+)?)\s+(?:report|page|dashboard)\b', action_text, _re.IGNORECASE)
    if m:
        return m.group(0).strip()
    # Specific table names (e.g., "fact_order_line_v1", "dim_date_v2")
    m = _re.search(r'\b((?:fact|dim|bridge)_\w+)\b', action_text, _re.IGNORECASE)
    if m:
        return m.group(1)
    return ""


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


def _hot_table_optimisation_tips(ht: dict) -> list[str]:
    """Generate specific optimisation tips for a hot table based on its characteristics."""
    tips: list[str] = []
    mode = ht.get("storageMode", "unknown")
    rows = ht.get("rowCount") or 0
    refs = ht.get("referenceCount", 0)
    degree = ht.get("degree", 0)
    size_gb = ht.get("sizeGB") or 0

    if mode == "directQuery":
        if rows > 1_000_000_000:
            tips.append(
                "<b>Create aggregation tables</b> &mdash; pre-aggregate at grain "
                "(e.g. daily/weekly) to avoid scanning billions of rows per query. "
                "Configure as Import with automatic fallback to DirectQuery for detail-level visuals."
            )
        elif rows > 100_000_000:
            tips.append(
                "<b>Consider Dual mode</b> &mdash; switching from DirectQuery to Dual "
                "allows the engine to cache results for dimension-side joins, reducing "
                "redundant Databricks queries on repeated page loads."
            )
        tips.append(
            "<b>Column pruning</b> &mdash; remove hidden or unreferenced columns "
            "from the model. Each column in a DirectQuery table adds to the generated "
            "SQL SELECT clause, increasing scan width and query duration."
        )
        if refs > 50:
            tips.append(
                f"<b>Simplify DAX references</b> &mdash; this table is referenced by "
                f"{refs} measures. Consolidate similar measures or use calculation groups "
                f"to reduce the number of independent queries hitting Databricks."
            )

    elif mode == "dual":
        tips.append(
            "<b>Evaluate Import mode</b> &mdash; Dual tables fall back to DirectQuery "
            "when joined with DirectQuery fact tables. If this table changes infrequently, "
            "switching to Import with scheduled refresh avoids repeated Databricks scans."
        )
        if refs > 100:
            tips.append(
                f"<b>High reference count ({refs})</b> &mdash; ensure the In-Memory cache "
                f"is effective by enabling Large Storage Format and reviewing refresh schedules. "
                f"A stale cache forces fallback to DirectQuery on every query."
            )

    elif mode == "import":
        if size_gb > 1:
            tips.append(
                f"<b>Enable incremental refresh</b> &mdash; at {_fmt_gb(size_gb)}, full "
                f"refresh is expensive. Partition by date and only refresh the most recent "
                f"window to reduce refresh time and memory pressure."
            )
        if refs > 100:
            tips.append(
                f"<b>Review column cardinality</b> &mdash; with {refs} measure references, "
                f"high-cardinality columns consume disproportionate memory. Consider removing "
                f"or hashing columns not used in slicers/filters."
            )

    if degree >= 10:
        tips.append(
            f"<b>Hub table (degree {degree})</b> &mdash; this table is joined by many "
            f"others, amplifying the cost of every scan. Consider denormalising frequently-"
            f"used attributes into fact tables to reduce relationship hops and simplify "
            f"generated SQL."
        )
    elif degree >= 5:
        tips.append(
            f"<b>High fan-out (degree {degree})</b> &mdash; multiple relationships increase "
            f"the number of JOIN operations in generated SQL. Review whether all relationships "
            f"are actively used by visuals; remove inactive ones to simplify query plans."
        )

    if not tips:
        tips.append("No specific optimisation required &mdash; monitor query performance over time.")

    return tips


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


def _render_tree_node(node: dict, is_last: bool = False) -> str:
    """Recursively render a snowflake tree node as nested HTML <ul>/<li>."""
    mode = node.get("storageMode", "import")
    mode_cls = {"directQuery": "sf-dq", "dual": "sf-dual", "import": "sf-import"}.get(mode, "")
    cls_label = node.get("classification", "")[:3]
    cross = node.get("crossFilter", "oneDirection")
    card = node.get("cardinality", "")
    bidi_cls = " sf-edge-bidi" if cross == "bothDirections" else ""
    bidi_icon = " &#x21C6;" if cross == "bothDirections" else ""
    vol_info = ""
    if node.get("rowCount"):
        vol_info = f" &middot; {_fmt_rows(node['rowCount'])} rows"
    if node.get("sizeGB"):
        vol_info += f" &middot; {_fmt_gb(node['sizeGB'])}"

    edge_html = f'<span class="sf-edge-info{bidi_cls}">[{card}{bidi_icon}]{vol_info}</span>'

    children_html = ""
    if node.get("children"):
        items = ""
        for i, child in enumerate(node["children"]):
            items += _render_tree_node(child, i == len(node["children"]) - 1)
        children_html = f'<ul class="sf-tree">{items}</ul>'

    return f'''<li class="sf-node">
      <span class="sf-node-label {mode_cls}">{escape(node["table"])}
        <span class="badge {_classification_badge_cls(cls_label)}" style="font-size:9px;padding:1px 6px">{cls_label}</span>
        {_badge(mode)}
      </span>{edge_html}
      {children_html}
    </li>'''


def _classification_badge_cls(cls: str) -> str:
    """Return just the badge CSS class for a classification abbreviation."""
    return {
        "fac": "badge-high", "dim": "badge-info",
        "bri": "badge-medium", "met": "badge-low",
    }.get(cls, "badge-low")


def _render_depth_bar(depth_dist: list[dict], total_tables: int) -> str:
    """Render a stacked horizontal bar showing tables per depth level."""
    if not depth_dist or total_tables == 0:
        return ""
    colours = ["var(--accent)", "var(--green)", "var(--amber)", "var(--red)", "var(--muted)", "#8e44ad"]
    bars = ""
    for i, dd in enumerate(depth_dist):
        pct = (dd["tableCount"] / total_tables) * 100
        colour = colours[i % len(colours)]
        dq = dd.get("storageModes", {}).get("directQuery", 0)
        tooltip = f"Depth {dd['depth']}: {dd['tableCount']} tables"
        if dq:
            tooltip += f" ({dq} DQ)"
        bars += f'<div class="sf-depth-segment" style="width:{max(pct, 6):.1f}%;background:{colour}" title="{tooltip}">L{dd["depth"]}</div>'
    return f'''<div class="sf-depth-bar">
      <span class="sf-depth-label">Depth</span>{bars}
      <span style="font-size:10px;color:var(--muted);margin-left:4px">{total_tables} tables</span>
    </div>'''


def _render_hub_tree_card(hub: dict) -> str:
    """Render a full snowflake branching card for one hub table."""
    tree = hub.get("branchTree", {})
    branches = tree.get("branches", [])
    if not branches:
        return ""

    total = tree.get("totalReachableTables", 0)
    max_d = tree.get("maxDepth", 0)
    bf = tree.get("branchingFactor", 0)
    depth_dist = tree.get("depthDistribution", [])

    # Root node
    mode = hub.get("storageMode", "unknown")
    vol_info = ""
    if hub.get("rowCount"):
        vol_info += f" &middot; {_fmt_rows(hub['rowCount'])} rows"
    if hub.get("sizeGB"):
        vol_info += f" &middot; {_fmt_gb(hub['sizeGB'])}"

    # Build tree HTML
    tree_items = ""
    for i, branch in enumerate(branches):
        tree_items += _render_tree_node(branch, i == len(branches) - 1)

    # Depth distribution bar
    depth_bar = _render_depth_bar(depth_dist, total)

    # Count DirectQuery tables in cascade
    dq_in_cascade = sum(
        dd.get("storageModes", {}).get("directQuery", 0)
        for dd in depth_dist
    )

    # Cascade warning
    cascade_warning = ""
    if total > 5 or dq_in_cascade > 3:
        cascade_warning = f'''<div class="cascade-card">
          <h4>&#9888; Join Cascade Impact</h4>
          <p>Any query touching <strong>{escape(hub["name"])}</strong> can transitively involve
             <strong>{total} additional tables</strong> across <strong>{max_d} depth levels</strong>.</p>
          {"<p><strong>" + str(dq_in_cascade) + " DirectQuery tables</strong> in the cascade generate separate SQL queries to Databricks, compounding latency.</p>" if dq_in_cascade > 0 else ""}
          <p style="font-size:11px;color:var(--muted)">Each relationship hop adds JOINs to the generated SQL. Deeper chains = more subqueries = slower response.</p>
        </div>'''

    return f'''<div class="card" style="margin-top:12px">
      <h3 style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
        <span class="sf-node-label sf-root">{escape(hub["name"])}</span>
        <span style="font-size:12px;color:var(--muted);font-weight:400">
          degree {hub["degree"]} &middot; {total} reachable tables &middot; {max_d} max depth &middot; branching factor {bf}{vol_info}
        </span>
      </h3>
      {depth_bar}
      <div style="overflow-x:auto;padding:8px 0">
        <ul class="sf-tree">{tree_items}</ul>
      </div>
      {cascade_warning}
    </div>'''


def _render_cascade_summary_table(join_cascades: list[dict], top_n: int = 15) -> str:
    """Render a summary table of top tables by join cascade size."""
    if not join_cascades:
        return ""
    rows = ""
    for c in join_cascades[:top_n]:
        cascade_extra = "color:var(--red);font-weight:600;" if c["cascadeTables"] > 10 else (
            "color:var(--amber);font-weight:600;" if c["cascadeTables"] > 5 else "")
        dq_extra = "color:var(--red);font-weight:600;" if c["cascadeDirectQuery"] > 3 else ""
        rows += f'''<tr>
          <td><strong>{escape(c["table"])}</strong></td>
          <td>{_classification_badge(c.get("classification", ""))}</td>
          <td>{_badge(c.get("storageMode", ""))}</td>
          <td style="text-align:right;{cascade_extra}">{c["cascadeTables"]}</td>
          <td style="text-align:right;{dq_extra}">{c["cascadeDirectQuery"]}</td>
          <td style="text-align:right">{c["cascadeMaxDepth"]}</td>
        </tr>'''
    return f'''<div class="card" style="margin-top:16px">
      <h3>Join Cascade &mdash; Tables Transitively Involved per Query</h3>
      <p style="font-size:12px;color:var(--muted);margin-bottom:12px">
        When Power BI queries a table, all related tables in the snowflake model are transitively joined.
        Higher cascade = more JOINs = slower DirectQuery SQL.
      </p>
      <div style="overflow-x:auto">
      <table>
        <thead><tr>
          <th>Table</th><th>Type</th><th>Storage</th>
          <th style="text-align:right">Cascade Tables</th>
          <th style="text-align:right">DQ in Cascade</th>
          <th style="text-align:right">Max Depth</th>
        </tr></thead>
        <tbody>{rows}</tbody>
      </table>
      </div>
    </div>'''


def _render_branching_suggestions(hub_tables: list[dict], join_cascades: list[dict]) -> str:
    """Generate actionable improvement suggestions based on branching analysis."""
    suggestions: list[str] = []

    # Check for deep cascades
    deep_hubs = [h for h in hub_tables if h.get("branchTree", {}).get("maxDepth", 0) >= 4]
    if deep_hubs:
        names = ", ".join(h["name"] for h in deep_hubs[:3])
        suggestions.append(
            f"<strong>Reduce snowflake depth</strong> for {names} &mdash; chains of 4+ relationship hops "
            f"generate deeply nested SQL subqueries. Consider denormalising intermediate dimension "
            f"attributes into the fact table or creating pre-joined aggregation tables."
        )

    # Check for high branching factor
    wide_hubs = [h for h in hub_tables if h.get("branchTree", {}).get("branchingFactor", 0) > 3]
    if wide_hubs:
        names = ", ".join(h["name"] for h in wide_hubs[:3])
        suggestions.append(
            f"<strong>Reduce branching factor</strong> for {names} &mdash; high fan-out from hub tables "
            f"means each query pulls in many dimension tables. Review whether all relationships are "
            f"actively used by report visuals; unused relationships can be removed or deactivated."
        )

    # Check for DQ tables deep in the tree
    dq_deep = []
    for h in hub_tables:
        tree = h.get("branchTree", {})
        for dd in tree.get("depthDistribution", []):
            if dd["depth"] >= 2 and dd.get("storageModes", {}).get("directQuery", 0) > 0:
                dq_deep.append(h["name"])
                break
    if dq_deep:
        suggestions.append(
            "<strong>Convert deep DirectQuery dimensions to Dual/Import</strong> &mdash; tables beyond "
            "depth 1 that are DirectQuery force additional roundtrips to Databricks. Small lookup "
            "tables (&lt; 1M rows) at depth 2+ should use Import or Dual storage mode."
        )

    # Check for bidirectional relationships in branches
    bidi_in_branches: set[str] = set()
    for h in hub_tables:
        def _find_bidi(nodes: list[dict], hub_name: str = h["name"]) -> None:
            for n in nodes:
                if n.get("crossFilter") == "bothDirections":
                    bidi_in_branches.add(f"{hub_name} &rarr; {n['table']}")
                _find_bidi(n.get("children", []), hub_name)
        _find_bidi(h.get("branchTree", {}).get("branches", []))
    if bidi_in_branches:
        examples = ", ".join(list(bidi_in_branches)[:3])
        suggestions.append(
            f"<strong>Remove bidirectional cross-filtering</strong> in branches ({examples}) &mdash; "
            f"bidirectional filters propagate across the entire snowflake chain, multiplying the "
            f"number of SQL JOINs. Use CROSSFILTER() in specific DAX measures instead."
        )

    # Check for very large cascades
    big_cascades = [c for c in join_cascades if c["cascadeTables"] > 15]
    if big_cascades:
        names = ", ".join(c["table"] for c in big_cascades[:3])
        suggestions.append(
            f"<strong>Consider aggregation tables</strong> for {names} &mdash; queries involving 15+ "
            f"transitively joined tables are expensive. Pre-computed aggregation tables for common "
            f"grain levels (daily, weekly) dramatically reduce the join footprint."
        )

    if not suggestions:
        return ""

    items = "".join(f"<li>{s}</li>" for s in suggestions)
    return f'''<div class="sf-suggestion">
      <h4>Snowflake Optimisation Suggestions</h4>
      <ul>{items}</ul>
    </div>'''


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
    .section-title { font-size:18px; font-weight:600; color:var(--dark); margin:0; padding:0; }
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
    .matrix-wrapper { position:relative; margin:24px 0 24px 48px; }
    .matrix-y-label { position:absolute; left:-48px; top:50%; transform:rotate(-90deg) translateX(-50%); transform-origin:0 0; font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:1px; color:var(--muted); white-space:nowrap; }
    .matrix-x-label { text-align:center; font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:1px; color:var(--muted); margin-top:8px; }
    .matrix-grid { display:grid; grid-template-columns:1fr 1fr; grid-template-rows:1fr 1fr; gap:12px; }
    .matrix-box { border-radius:8px; padding:20px; min-height:120px; position:relative; }
    .matrix-box h3 { margin-bottom:4px; font-size:14px; font-weight:700; }
    .matrix-box .matrix-subtitle { font-size:11px; color:var(--muted); margin-bottom:12px; }
    .matrix-box .matrix-count { display:inline-block; background:rgba(255,255,255,0.7); border-radius:12px; padding:2px 10px; font-size:11px; font-weight:700; margin-bottom:8px; }
    .matrix-box ul { list-style:none; padding:0; margin:0; }
    .matrix-box li { margin-bottom:6px; font-size:13px; line-height:1.4; }
    .matrix-box details summary { font-size:13px; color:inherit; padding:4px 0; }
    .matrix-box details summary:hover { text-decoration:underline; }
    .matrix-actions-table { margin-top:8px; font-size:12px; }
    .matrix-actions-table td { padding:6px 10px; }
    .matrix-actions-table tr:nth-child(even) { background:rgba(255,255,255,0.4); }
    .matrix-arrow-right { position:absolute; bottom:-22px; left:50%; font-size:14px; color:var(--muted); }
    .matrix-arrow-up { position:absolute; left:-28px; top:50%; font-size:14px; color:var(--muted); }
    details { margin-top:16px; }
    details summary { cursor:pointer; font-weight:600; color:var(--accent); padding:8px 0; font-size:13px; }
    details summary:hover { text-decoration:underline; }
    .filter-bar { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:14px; }
    .filter-btn { display:inline-block; padding:4px 14px; border-radius:14px; font-size:11px; font-weight:700; letter-spacing:0.3px; cursor:pointer; border:1.5px solid var(--border); background:#fff; color:var(--muted); transition:all 0.15s; user-select:none; }
    .filter-btn:hover { border-color:var(--accent); color:var(--accent); }
    .filter-btn.active { background:var(--accent); border-color:var(--accent); color:#fff; }
    .filter-btn.active-latency { background:rgba(192,57,43,0.12); border-color:var(--red); color:var(--red); }
    .filter-btn.active-cost { background:rgba(212,160,23,0.15); border-color:var(--amber); color:var(--amber); }
    .filter-btn.active-memory { background:rgba(26,135,84,0.12); border-color:var(--green); color:var(--green); }
    .filter-btn.active-quality { background:rgba(7,112,207,0.12); border-color:var(--accent); color:var(--accent); }
    .filter-btn.active-high { background:rgba(192,57,43,0.12); border-color:var(--red); color:var(--red); }
    .filter-btn.active-medium { background:rgba(212,160,23,0.15); border-color:var(--amber); color:var(--amber); }
    .filter-btn.active-low { background:rgba(26,135,84,0.12); border-color:var(--green); color:var(--green); }
    .filter-count { font-size:10px; font-weight:400; margin-left:2px; opacity:0.8; }
    .report-footer { margin-top:48px; padding-top:20px; border-top:1px solid var(--border); font-size:12px; color:var(--muted); text-align:center; }
    .tooltip-wrap { position:relative; cursor:help; border-bottom:1px dashed currentColor; }
    .tooltip-wrap::after { content:attr(data-tooltip); position:absolute; bottom:120%; left:50%; transform:translateX(-50%); background:var(--dark); color:#fff; padding:10px 14px; border-radius:6px; font-size:11px; line-height:1.5; white-space:pre-line; min-width:320px; max-width:420px; opacity:0; pointer-events:none; transition:opacity 0.2s; z-index:100; box-shadow:0 4px 12px rgba(0,0,0,0.2); }
    .tooltip-wrap:hover::after { opacity:1; }
    .heat-green { background: rgba(26,135,84,0.15); }
    .heat-yellow { background: rgba(212,160,23,0.15); }
    .heat-red { background: rgba(192,57,43,0.15); }
    .sortable-th { cursor: pointer; user-select: none; }
    .sortable-th:hover { background: #dee2e6; }
    .sortable-th::after { content: ' \\25B4\\25BE'; font-size: 10px; color: var(--muted); }
    .bar-container { background:#e9ecef; border-radius:4px; height:18px; overflow:hidden; }
    .bar-fill { height:100%; border-radius:4px; min-width:2px; }
    .pros-cons-grid { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
    .pros-cons-grid ul { list-style:none; padding:0; }
    .pros-cons-grid li { margin-bottom:8px; font-size:13px; padding-left:20px; position:relative; }
    .pros-cons-grid li::before { position:absolute; left:0; }
    .pros-list li::before { content:'\\2713'; color:var(--green); }
    .cons-list li::before { content:'\\2717'; color:var(--red); }
    .report-section { margin-bottom:8px; border:1px solid var(--border); border-radius:8px; background:#fff; }
    .report-section > summary { list-style:none; cursor:pointer; padding:16px 24px; font-size:18px; font-weight:600; color:var(--dark); border-bottom:3px solid transparent; display:flex; align-items:center; gap:12px; user-select:none; }
    .report-section > summary:hover { background:var(--light-bg); }
    .report-section > summary::before { content:'\\25B6'; font-size:12px; color:var(--accent); transition:transform 0.2s; flex-shrink:0; }
    .report-section[open] > summary { border-bottom-color:var(--accent); }
    .report-section[open] > summary::before { transform:rotate(90deg); }
    .report-section > summary::-webkit-details-marker { display:none; }
    .report-section > .section-body { padding:24px; }
    .toc-card { background:#fff; border:1px solid var(--border); border-radius:8px; padding:20px 24px; margin-bottom:24px; }
    .toc-card h3 { font-size:15px; font-weight:600; margin-bottom:12px; color:var(--dark); }
    .toc-list { list-style:none; padding:0; columns:2; column-gap:24px; }
    .toc-list li { margin-bottom:6px; font-size:13px; break-inside:avoid; }
    .toc-list a { color:var(--accent); text-decoration:none; }
    .toc-list a:hover { text-decoration:underline; }
    .approach-card { background:#fff; border:1px solid var(--border); border-radius:8px; padding:24px; margin-bottom:24px; }
    .approach-card h3 { font-size:15px; font-weight:600; margin-bottom:12px; color:var(--dark); }
    .approach-card ul { padding-left:20px; font-size:13px; }
    .approach-card li { margin-bottom:6px; }
    .input-badge { display:inline-block; padding:2px 8px; border-radius:4px; font-size:11px; font-weight:600; margin-right:6px; }
    .input-active { background:rgba(26,135,84,0.12); color:var(--green); }
    .input-inactive { background:rgba(0,0,0,0.06); color:var(--muted); }
    /* Snowflake tree visualisation */
    .sf-tree { list-style:none; padding-left:0; margin:0; }
    .sf-tree .sf-tree { padding-left:24px; border-left:2px solid var(--border); margin-left:8px; }
    .sf-node { padding:6px 0; position:relative; }
    .sf-node::before { content:''; position:absolute; left:-24px; top:16px; width:20px; height:0; border-top:2px solid var(--border); }
    .sf-tree > .sf-node:first-child::before { display:none; }
    .sf-tree > .sf-tree > .sf-node::before { display:block; }
    .sf-node-label { display:inline-flex; align-items:center; gap:6px; padding:4px 10px; border-radius:6px; font-size:12px; font-weight:600; border:1px solid var(--border); background:#fff; }
    .sf-node-label.sf-root { background:var(--dark); color:#fff; border-color:var(--dark); font-size:13px; padding:6px 14px; }
    .sf-node-label.sf-dq { border-color:var(--red); background:rgba(192,57,43,0.06); }
    .sf-node-label.sf-dual { border-color:var(--amber); background:rgba(212,160,23,0.06); }
    .sf-node-label.sf-import { border-color:var(--green); background:rgba(26,135,84,0.06); }
    .sf-edge-info { font-size:10px; color:var(--muted); font-weight:400; margin-left:4px; }
    .sf-edge-bidi { color:var(--red); font-weight:600; }
    .sf-depth-bar { display:flex; align-items:center; gap:4px; margin-bottom:6px; }
    .sf-depth-segment { height:22px; border-radius:4px; display:flex; align-items:center; justify-content:center; font-size:10px; font-weight:700; color:#fff; min-width:24px; }
    .sf-depth-label { font-size:11px; color:var(--muted); min-width:54px; font-weight:600; }
    .cascade-card { background:linear-gradient(135deg,rgba(192,57,43,0.06),rgba(212,160,23,0.03)); border:1px solid rgba(192,57,43,0.15); border-radius:8px; padding:16px 20px; margin:12px 0; }
    .cascade-card h4 { font-size:13px; font-weight:700; color:var(--red); margin-bottom:8px; }
    .cascade-card p { font-size:12px; margin-bottom:6px; }
    .cascade-table td { font-size:12px; }
    .sf-suggestion { background:linear-gradient(135deg,rgba(26,135,84,0.08),rgba(26,135,84,0.02)); border:1px solid rgba(26,135,84,0.2); border-radius:8px; padding:16px 20px; margin:12px 0; }
    .sf-suggestion h4 { font-size:13px; font-weight:700; color:var(--green); margin-bottom:8px; }
    .sf-suggestion ul { padding-left:18px; font-size:12px; }
    .sf-suggestion li { margin-bottom:6px; }
    @media (max-width:768px) { .metric-grid,.quadrant-grid { grid-template-columns:1fr; } .container { padding:16px 20px 40px; } .sf-tree .sf-tree { padding-left:16px; } }
    @media print { body { background:#fff; font-size:12px; } .card { box-shadow:none; break-inside:avoid; } .report-section { break-inside:avoid; } .report-section[open] > summary { break-after:avoid; } .tooltip-wrap::after { display:none; } .report-section > summary::before { content:''; } }
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
    now = datetime.now(timezone.utc).strftime("%d %b %Y")
    synthesis = _read_json_file(source_dir / "synthesis.json") if source_dir else None
    query_profile = _read_json_file(source_dir / "query-profile.json") if source_dir else None
    dbx_profile = _read_json_file(source_dir / "databricks-profile.json") if source_dir else None

    # New analysis JSONs (conditional — sections render only when present)
    user_query_profile = _read_json_file(source_dir / "user-query-profile.json") if source_dir else None
    capacity_settings = _read_json_file(source_dir / "capacity-settings-analysis.json") if source_dir else None
    workload_analysis = _read_json_file(source_dir / "workload-analysis.json") if source_dir else None
    column_memory = _read_json_file(source_dir / "column-memory-analysis.json") if source_dir else None
    engineering_bpa = _read_json_file(source_dir / "engineering-bpa-results.json") if source_dir else None
    visual_analysis = _read_json_file(source_dir / "visual-analysis.json") if source_dir else None

    # Build per-table query stats lookup from databricks-profile.json
    table_query_stats: dict[str, dict] = {}
    if dbx_profile and dbx_profile.get("tableQueryStats"):
        for tqs in dbx_profile["tableQueryStats"]:
            tname = tqs.get("tableName", "").lower()
            if tname:
                table_query_stats[tname] = tqs

    # Enrich taxonomy tables with volumetry from databricks-profile.json
    # (Step 1 runs before Step 4, so taxonomy has no volumetry at creation time)
    if taxonomy and dbx_profile and dbx_profile.get("tables"):
        dbx_vol: dict[str, dict] = {}
        for dt in dbx_profile["tables"]:
            dtname = dt.get("tableName", "").lower()
            if dtname:
                dbx_vol[dtname] = {"rowCount": dt.get("rowCount"), "sizeGB": dt.get("sizeGB")}
        for t in taxonomy.get("tables", []):
            if not t.get("volumetry") and t.get("sourceTable"):
                src = t["sourceTable"].lower()
                if src in dbx_vol:
                    t["volumetry"] = dbx_vol[src]

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
    complexity_dist = dax_stats.get("byComplexity", dax_stats.get("complexityDistribution", {}))

    dbt_stats_raw = dbt_lineage.get("statistics", {}) if dbt_lineage else {}

    # Engineering BPA summary — used in approach card
    eng_bpa_summary = engineering_bpa.get("summary", {}) if engineering_bpa else {}

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
        total_q = ds.get("totalQueries", 0) or 0
        slow10 = ds.get("slow10s", 0) or 0
        slow30 = ds.get("slow30s", 0) or 0
        # Compute percentages: use pre-computed if available, else derive
        pct_slow10 = ds.get("pctSlow10s") or (round(slow10 * 100 / total_q, 1) if total_q else 0)
        pct_slow30 = ds.get("pctSlow30s") or (round(slow30 * 100 / total_q, 1) if total_q else 0)
        pct_cached = ds.get("pctCached", 0) or 0
        sessions = ds.get("distinctSessions", 0) or 0
        avg_qps = ds.get("avgQueriesPerSession") or (round(total_q / sessions, 1) if sessions else 0)
        period_label = f"last {ds['periodDays']}d" if ds.get("periodDays") and ds["periodDays"] > 1 else "today"

        # Sessions card (conditionally shown)
        sessions_card = ""
        if sessions:
            sessions_card = f"""
        <div class="metric-card">
          <div class="value">{_fmt_number(sessions)}</div>
          <div class="label">Report Sessions</div>
          <div class="sub">~{avg_qps} queries/session</div>
        </div>"""

        # Cache card (conditionally shown)
        cache_card = ""
        if pct_cached:
            cache_cls = "green" if pct_cached > 50 else ("amber" if pct_cached > 20 else "red")
            cache_card = f"""
        <div class="metric-card {cache_cls}">
          <div class="value">{pct_cached:.1f}%</div>
          <div class="label">Cache Hit Rate</div>
          <div class="sub">queries served from cache</div>
        </div>"""

        slow10_cls = "red" if pct_slow10 > 10 else ("amber" if pct_slow10 > 3 else "")
        dbx_daily_html = f"""
    <div class="card">
      <h3>Databricks PBI Query Load
        <span style="font-weight:400;font-size:12px;color:var(--muted);margin-left:12px">{period_label}</span></h3>
      <div class="metric-grid">
        <div class="metric-card red">
          <div class="value">{_fmt_number(total_q)}</div>
          <div class="label">Total PBI Queries</div>
          <div class="sub">from spn-ade-pbi</div>
        </div>{sessions_card}
        <div class="metric-card {slow10_cls}">
          <div class="value">{_fmt_number(slow10)} <span style="font-size:14px;font-weight:400">({pct_slow10:.1f}%)</span></div>
          <div class="label">Slow &gt;10s</div>
          <div class="sub">{_fmt_number(slow30)} &gt;30s ({pct_slow30:.1f}%)</div>
        </div>
        <div class="metric-card red">
          <div class="value">{ds.get('totalReadTb', 0):.1f} TB</div>
          <div class="label">Data Read</div>
          <div class="sub">P95: {ds.get('p95s', 0):.1f}s, max: {ds.get('maxs', 0):.0f}s</div>
        </div>
        <div class="metric-card">
          <div class="value">{ds.get('avgDurationS', 0):.1f}s</div>
          <div class="label">Avg Query Duration</div>
          <div class="sub">P50: {ds.get('p50s', 0):.1f}s</div>
        </div>{cache_card}
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
    <div class="metric-grid" style="grid-template-columns:repeat(3,1fr); max-width:900px; margin-left:auto; margin-right:auto">
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
    </div>
    {"<div class='card'><p>" + escape(exec_summary) + "</p></div>" if exec_summary else ""}
    {top_findings_html}
    {git_context_html}
    {dbx_daily_html}
    """)

    # ═══════════════════════════════════════════════════════
    # SECTION: QUERY ATTRIBUTION DASHBOARD
    # ═══════════════════════════════════════════════════════
    if user_query_profile:
        sec_num += 1
        uqp_totals = user_query_profile.get("totals", {})
        uqp_users = user_query_profile.get("users", [])
        uqp_training = user_query_profile.get("trainingCandidates", [])
        training_usernames = {tc.get("username", "").lower() for tc in uqp_training}

        # Compute percentile thresholds for heat-map colouring
        all_avg_durations = sorted([u.get("avgDurationMs", 0) for u in uqp_users])
        p50_dur = all_avg_durations[len(all_avg_durations) // 2] if all_avg_durations else 0
        p90_dur = all_avg_durations[int(len(all_avg_durations) * 0.9)] if all_avg_durations else 0

        def _heat_class(val, p50, p90):
            if val > p90: return "heat-red"
            if val > p50: return "heat-yellow"
            return "heat-green"

        user_rows = ""
        for u in uqp_users[:30]:  # Cap at 30 users
            uname = u.get("username", "Unknown")
            is_training = uname.lower() in training_usernames
            badge = ' <span class="badge badge-high">Training</span>' if is_training else ""
            heat = _heat_class(u.get("avgDurationMs", 0), p50_dur, p90_dur)
            user_rows += f"""<tr class="{heat}">
              <td>{escape(uname)}{badge}</td>
              <td data-val="{u.get('totalQueries', 0)}">{_fmt_number(u.get('totalQueries', 0))}</td>
              <td data-val="{u.get('avgDurationMs', 0)}">{_fmt_number(u.get('avgDurationMs', 0))} ms</td>
              <td data-val="{u.get('p95DurationMs', 0)}">{_fmt_number(u.get('p95DurationMs', 0))} ms</td>
              <td data-val="{u.get('maxDurationMs', 0)}">{_fmt_number(u.get('maxDurationMs', 0))} ms</td>
              <td data-val="{u.get('totalGBRead', 0)}">{u.get('totalGBRead', 0):.1f} GB</td>
              <td data-val="{u.get('queriesOver10s', 0)}">{u.get('queriesOver10s', 0)}</td>
              <td data-val="{u.get('queriesOver30s', 0)}">{u.get('queriesOver30s', 0)}</td>
            </tr>"""

        sections.append(f"""
        <h2 class="section-title">{sec_num}. Query Attribution Dashboard</h2>
        <div class="metric-grid">
          <div class="metric-card"><div class="value">{uqp_totals.get('totalUsers', 0)}</div><div class="label">Total Users</div></div>
          <div class="metric-card"><div class="value">{_fmt_number(uqp_totals.get('totalQueries', 0))}</div><div class="label">Total Queries</div></div>
          <div class="metric-card amber"><div class="value">{_fmt_number(uqp_totals.get('queriesOver10s', 0))}</div><div class="label">Queries &gt;10s</div></div>
          <div class="metric-card red"><div class="value">{_fmt_number(uqp_totals.get('queriesOver30s', 0))}</div><div class="label">Queries &gt;30s</div></div>
        </div>
        <div class="card">
          <h3>Per-User Query Profile</h3>
          <p style="font-size:12px;color:var(--muted);margin-bottom:12px">Colour coding: <span class="heat-green" style="padding:2px 8px;border-radius:3px">below median</span> <span class="heat-yellow" style="padding:2px 8px;border-radius:3px">above median</span> <span class="heat-red" style="padding:2px 8px;border-radius:3px">above P90</span></p>
          <table id="user-query-table">
            <thead><tr>
              <th class="sortable-th">Username</th><th class="sortable-th">Queries</th><th class="sortable-th">Avg Duration</th>
              <th class="sortable-th">P95</th><th class="sortable-th">Max</th><th class="sortable-th">GB Read</th>
              <th class="sortable-th">&gt;10s</th><th class="sortable-th">&gt;30s</th>
            </tr></thead>
            <tbody>{user_rows}</tbody>
          </table>
        </div>
        {"<div class='card'><h3>Training Candidates</h3><ul>" + "".join(f"<li><strong>{escape(tc.get('username', ''))}</strong>: {escape(tc.get('reason', ''))}</li>" for tc in uqp_training) + "</ul></div>" if uqp_training else ""}
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
        has_query_stats = bool(table_query_stats)

        # Pre-build PBI table name → Databricks source table name mapping
        pbi_to_dbx: dict[str, str] = {}
        for t in taxonomy.get("tables", []):
            if t.get("sourceTable"):
                pbi_to_dbx[t["name"]] = t["sourceTable"].lower()

        # Group tables by classification for better visual hierarchy
        tables_by_class: dict[str, list] = {"fact": [], "dimension": [], "bridge": [], "metadata": [], "": []}
        total_data_gb = 0.0
        for t in taxonomy.get("tables", []):
            vol = t.get("volumetry", {})
            size_gb = vol.get("sizeGB")
            if size_gb:
                total_data_gb += size_gb
            cls = t.get("classification", "")
            if cls in tables_by_class:
                tables_by_class[cls].append(t)
            else:
                tables_by_class[""].append(t)

        # Build rows grouped by classification
        group_labels = {
            "fact": ("Fact Tables", "var(--red)"),
            "dimension": ("Dimension Tables", "var(--accent)"),
            "bridge": ("Bridge Tables", "var(--amber)"),
            "metadata": ("Metadata Tables", "var(--muted)"),
            "": ("Other Tables", "var(--muted)"),
        }

        table_rows = ""
        for cls_key in ("fact", "dimension", "bridge", "metadata", ""):
            group = tables_by_class.get(cls_key, [])
            if not group:
                continue
            label, color = group_labels[cls_key]
            table_rows += f"""<tr class="group-header"><td colspan="{'9' if has_query_stats else '6'}" style="background:rgba(0,0,0,0.03);font-weight:700;font-size:12px;text-transform:uppercase;letter-spacing:0.5px;color:{color};padding:8px 14px;border-bottom:2px solid {color}">{label} ({len(group)})</td></tr>"""

            for t in group:
                src = ""
                if t.get("sourceTable"):
                    full_src = f"{t['sourceCatalog']}.{t['sourceDatabase']}.{t['sourceTable']}"
                    # Exclude personal dev schemas from display (e.g., dbt_dev.rafael_diassantos.*)
                    _EXCLUDED_SCHEMAS = {"rafael_diassantos"}
                    if t.get("sourceDatabase", "") not in _EXCLUDED_SCHEMAS:
                        src = full_src
                vol = t.get("volumetry", {})
                row_count = vol.get("rowCount")
                size_gb = vol.get("sizeGB")
                is_large = size_gb and size_gb > 10
                is_medium = size_gb and size_gb > 1

                row_style = ""
                if is_large:
                    row_style = ' style="background:rgba(192,57,43,0.04)"'
                elif is_medium:
                    row_style = ' style="background:rgba(212,160,23,0.04)"'

                # Query stats columns (conditional)
                qs_cells = ""
                if has_query_stats:
                    tqs = table_query_stats.get(pbi_to_dbx.get(t["name"], ""), {})
                    daily_q = tqs.get("dailyQueries")
                    avg_dur = tqs.get("avgDurationMs")
                    p95_dur = tqs.get("p95DurationMs")
                    dur_style = ""
                    if avg_dur and avg_dur > 10000:
                        dur_style = ' style="color:var(--red);font-weight:600"'
                    elif avg_dur and avg_dur > 3000:
                        dur_style = ' style="color:var(--amber);font-weight:600"'
                    muted_dash = '<span style="color:#6c757d">-</span>'
                    daily_q_html = str(int(daily_q)) if daily_q is not None else muted_dash
                    avg_dur_html = f"{avg_dur:,.0f} ms" if avg_dur is not None else muted_dash
                    p95_dur_html = f"{p95_dur:,.0f} ms" if p95_dur is not None else muted_dash
                    qs_cells = (
                        f"<td style='text-align:right'>{daily_q_html}</td>"
                        f"<td{dur_style} style='text-align:right'>{avg_dur_html}</td>"
                        f"<td style='text-align:right'>{p95_dur_html}</td>"
                    )

                # Bold fact table names, highlight large row counts
                name_style = "font-weight:600" if cls_key == "fact" else ""
                rows_html = _fmt_rows(row_count)
                if row_count and row_count > 1_000_000_000:
                    rows_html = f'<span style="color:var(--red);font-weight:600">{rows_html}</span>'
                elif row_count and row_count > 100_000_000:
                    rows_html = f'<span style="color:var(--amber);font-weight:600">{rows_html}</span>'

                size_html = _fmt_gb(size_gb)
                if is_large:
                    size_html = f'<span style="color:var(--red);font-weight:600">{size_html}</span>'
                elif is_medium:
                    size_html = f'<span style="color:var(--amber);font-weight:600">{size_html}</span>'

                table_rows += f"""<tr{row_style}>
              <td style="{name_style}">{escape(t['name'])}</td>
              <td>{_badge(t['storageMode'])}</td>
              <td style="text-align:right">{t['columnCount']}</td>
              <td style="text-align:right">{t['measureCount']}</td>
              <td style="text-align:right">{rows_html}</td>
              <td style="text-align:right">{size_html}</td>
              <td style="font-size:11px;color:var(--muted);max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{escape(src)}">{escape(src)}</td>{qs_cells}</tr>"""

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
                tree = h.get("branchTree", {})
                reachable = tree.get("totalReachableTables", 0)
                max_tree_d = tree.get("maxDepth", 0)
                bf = tree.get("branchingFactor", 0)
                hub_rows += f"""<tr>
                  <td><strong>{escape(h['name'])}</strong></td>
                  <td>{h['degree']}</td>
                  <td>{_classification_badge(h.get('classification', ''))}</td>
                  <td>{_badge(h.get('storageMode', ''))}</td>
                  <td>{_fmt_rows(h.get('rowCount'))}</td>
                  <td>{_fmt_gb(h.get('sizeGB'))}</td>
                  <td style="text-align:right">{reachable}</td>
                  <td style="text-align:right">{max_tree_d}</td>
                  <td style="text-align:right">{bf}</td></tr>"""

            # Build snowflake branching tree cards for each hub
            tree_cards = ""
            for h in ga["hubTables"]:
                tree_cards += _render_hub_tree_card(h)

            # Join cascade summary table
            cascade_html = _render_cascade_summary_table(ga.get("joinCascades", []))

            # Suggestions
            suggestions_html = _render_branching_suggestions(
                ga["hubTables"], ga.get("joinCascades", [])
            )

            graph_html = f"""
        <div class="card"><h3>Relationship Topology</h3>
          <div class="metric-grid" style="margin-bottom:16px">
            <div class="metric-card"><div class="value">{ga.get('maxSnowflakeDepth', 0)}</div><div class="label">Max Snowflake Depth</div></div>
            <div class="metric-card"><div class="value">{ga.get('avgDegree', 0)}</div><div class="label">Avg Degree</div></div>
            <div class="metric-card red"><div class="value">{len(ga.get('hubTables', []))}</div><div class="label">Hub Tables (degree &ge; 5)</div></div>
          </div>
          <div style="overflow-x:auto">
          <table><thead><tr><th>Hub Table</th><th>Degree</th><th>Type</th><th>Storage</th><th>Rows</th><th>Size</th><th style="text-align:right">Reachable</th><th style="text-align:right">Max Depth</th><th style="text-align:right">Branch Factor</th></tr></thead>
            <tbody>{hub_rows}</tbody></table>
          </div>
        </div>
        <div class="card"><h3>Snowflake Branching &mdash; Hub Relationship Trees</h3>
          <p style="font-size:12px;color:var(--muted);margin-bottom:12px">
            Each tree below shows the full relationship chain radiating from a hub table.
            Every branch represents a JOIN that Power BI generates in DirectQuery SQL.
            Deeper and wider trees = more complex queries = slower performance.
            <span style="display:inline-flex;gap:8px;margin-left:8px">
              <span class="sf-node-label sf-dq" style="font-size:10px;padding:2px 6px">DirectQuery</span>
              <span class="sf-node-label sf-dual" style="font-size:10px;padding:2px 6px">Dual</span>
              <span class="sf-node-label sf-import" style="font-size:10px;padding:2px 6px">Import</span>
              <span class="sf-edge-info sf-edge-bidi" style="font-size:10px">&#x21C6; = bidirectional</span>
            </span>
          </p>
          {tree_cards}
        </div>
        {cascade_html}
        {suggestions_html}"""

        qs_headers = "<th style='text-align:right'>Daily Queries</th><th style='text-align:right'>Avg Duration</th><th style='text-align:right'>P95 Duration</th>" if has_query_stats else ""
        table_count = len(taxonomy.get("tables", []))
        fact_count = len(tables_by_class.get("fact", []))
        dim_count = len(tables_by_class.get("dimension", []))
        sections.append(f"""
        <h2 class="section-title">{sec_num}. Model Taxonomy</h2>
        {vol_summary}
        <div class="card"><h3>Tables &amp; Storage Modes
          <span style="font-weight:400;font-size:12px;color:var(--muted);margin-left:12px">{table_count} tables &middot; {total_rels} relationships ({bidi_rels} bidirectional)</span></h3>
          <div style="display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap">
            <span class="badge badge-high">{fact_count} Fact</span>
            <span class="badge badge-info">{dim_count} Dimension</span>
            <span class="badge badge-medium">{len(tables_by_class.get('bridge', []))} Bridge</span>
            <span class="badge badge-low">{len(tables_by_class.get('metadata', [])) + len(tables_by_class.get('', []))} Meta/Other</span>
          </div>
          <div style="overflow-x:auto">
          <table><thead><tr><th>Table</th><th>Storage Mode</th><th style="text-align:right">Columns</th><th style="text-align:right">Measures</th><th style="text-align:right">Rows</th><th style="text-align:right">Size</th><th>Databricks Source</th>{qs_headers}</tr></thead>
            <tbody>{table_rows}</tbody></table>
          </div></div>
        <details>
          <summary>Relationships ({total_rels} total &mdash; {bidi_rels} bidirectional)</summary>
          <div class="card" style="margin-top:12px">
            <table><thead><tr><th>From</th><th>To</th><th>Cardinality</th><th>Cross-Filter</th><th>Active</th></tr></thead>
              <tbody>{rel_rows}</tbody></table>
          </div>
        </details>
        {graph_html}
        """)

    # ═══════════════════════════════════════════════════════
    # SECTION: MEMORY & COLUMN ANALYSIS
    # ═══════════════════════════════════════════════════════
    if column_memory:
        sec_num += 1
        cm_summary = column_memory.get("summary", {})
        cm_tables = column_memory.get("topTablesByMemory", [])
        cm_candidates = column_memory.get("removalCandidates", [])

        top_table_rows = ""
        for t in cm_tables[:10]:
            top_table_rows += f"""<tr>
              <td><strong>{escape(t.get('name', ''))}</strong></td>
              <td>{t.get('estimatedMemoryMB', 0):.1f} MB</td>
              <td>{t.get('columnCount', 0)}</td>
              <td>{t.get('removalCandidates', 0)}</td>
            </tr>"""

        candidate_rows = ""
        for c in cm_candidates[:15]:
            candidate_rows += f"""<tr>
              <td>{escape(c.get('table', ''))}</td>
              <td>{escape(c.get('column', ''))}</td>
              <td>{escape(c.get('dataType', ''))}</td>
              <td>{'Yes' if c.get('isHidden') else 'No'}</td>
              <td>{escape(c.get('reason', ''))}</td>
              <td style="font-weight:600">{c.get('estimatedSavingsMB', 0):.1f} MB</td>
            </tr>"""

        sections.append(f"""
        <h2 class="section-title">{sec_num}. Memory &amp; Column Analysis</h2>
        <div class="metric-grid">
          <div class="metric-card"><div class="value">{cm_summary.get('estimatedTotalMemoryMB', 0):.0f} MB</div><div class="label">Estimated Total Memory</div></div>
          <div class="metric-card"><div class="value">{cm_summary.get('totalColumns', 0)}</div><div class="label">Total Columns</div></div>
          <div class="metric-card amber"><div class="value">{cm_summary.get('removalCandidateCount', 0)}</div><div class="label">Removal Candidates</div></div>
          <div class="metric-card green"><div class="value">{cm_summary.get('potentialSavingsMB', 0):.0f} MB ({cm_summary.get('potentialSavingsPct', 0):.1f}%)</div><div class="label">Potential Savings</div></div>
        </div>
        <div class="card">
          <h3>Top Tables by Estimated Memory</h3>
          <table>
            <thead><tr><th>Table</th><th>Estimated Memory</th><th>Columns</th><th>Removal Candidates</th></tr></thead>
            <tbody>{top_table_rows}</tbody>
          </table>
        </div>
        {"<div class='card' style='margin-top:16px'><h3>Column Removal Candidates</h3><table><thead><tr><th>Table</th><th>Column</th><th>Type</th><th>Hidden</th><th>Reason</th><th>Savings</th></tr></thead><tbody>" + candidate_rows + "</tbody></table></div>" if candidate_rows else ""}
        """)

    # ═══════════════════════════════════════════════════════
    # SECTION: DAX COMPLEXITY
    # ═══════════════════════════════════════════════════════
    if dax_complexity:
        sec_num += 1
        dax_s = dax_complexity.get("statistics", {})
        measures = dax_complexity.get("measures", [])

        # Time intelligence suffixes — measures ending with these are TI variants
        _TI_SUFFIXES = (
            " LY", " PY", " LW", " LM",
            " YTD", " MTD", " WTD", " QTD", " HTD",
            " SPLY", " FYTD", " FHTD",
            " LY YTD", " LY MTD", " LY WTD", " LY QTD", " LY HTD",
            " vs LY", " vs PY",
            " % LY", " % PY",
            " OB LM", " T&R LM",
        )

        def _is_ti_measure(name: str) -> bool:
            upper = name.strip().upper()
            return any(upper.endswith(s) for s in _TI_SUFFIXES)

        # Take top 30 to have enough non-TI measures when filter is on
        top_measures = measures[:30]
        ti_count_in_top = sum(1 for m in top_measures if _is_ti_measure(m["name"]))
        measure_rows = ""
        for m in top_measures:
            ctx = m.get("contextTransitions", 0)
            hops = m.get("relationshipHops", 0)
            fa = m.get("filterAllCount", 0)
            subq = m.get("estimatedSQLSubqueries", 0)
            cross = "Yes" if m.get("crossesMultipleDQ") else ""
            is_ti = _is_ti_measure(m["name"])
            ti_attr = ' data-ti="1"' if is_ti else ''
            ti_badge = ' <span style="font-size:9px;color:var(--muted);border:1px solid var(--border);border-radius:3px;padding:0 4px;margin-left:4px;vertical-align:middle">TI</span>' if is_ti else ''
            measure_rows += f"""<tr{ti_attr}>
              <td>{escape(m['name'])}{ti_badge}</td><td>{escape(m['hostTable'])}</td>
              <td>{m['complexityScore']}</td>
              <td>{ctx}</td><td>{hops}</td><td>{fa}</td><td>{subq}</td>
              <td>{_badge(m['complexityLevel'])}</td>
              <td>{escape(cross)}</td></tr>"""

        # Summary metrics
        measures_high_subq = dax_s.get("measuresWithHighSubqueries", 0)
        measures_filter_all = dax_s.get("measuresWithFilterAll", 0)

        # Hot tables — enriched with optimisation tips
        ht_has_qs = bool(table_query_stats) and taxonomy is not None
        ht_col_count = 7 + (2 if ht_has_qs else 0)
        hot_table_rows = ""
        for ht in dax_complexity.get("hotTables", [])[:10]:
            priority = ht.get("optimizationPriority", "low")
            ht_qs_cells = ""
            if ht_has_qs:
                ht_dbx = pbi_to_dbx.get(ht["table"], "") if taxonomy else ""
                ht_tqs = table_query_stats.get(ht_dbx, {})
                ht_daily = ht_tqs.get("dailyQueries")
                ht_avg = ht_tqs.get("avgDurationMs")
                ht_dur_style = ""
                if ht_avg and ht_avg > 10000:
                    ht_dur_style = ' style="color:var(--red);font-weight:600"'
                elif ht_avg and ht_avg > 3000:
                    ht_dur_style = ' style="color:var(--amber);font-weight:600"'
                ht_qs_cells = (
                    f"<td>{int(ht_daily) if ht_daily is not None else '-'}</td>"
                    f"<td{ht_dur_style}>{f'{ht_avg:,.0f} ms' if ht_avg is not None else '-'}</td>"
                )
            hot_table_rows += f"""<tr>
              <td>{escape(ht['table'])}</td><td>{_badge(ht['storageMode'])}</td>
              <td>{ht['referenceCount']}</td>
              <td>{_fmt_rows(ht.get('rowCount'))}</td>
              <td>{_fmt_gb(ht.get('sizeGB'))}</td>
              <td>{ht.get('degree', '-')}</td>
              <td>{_priority_badge(priority)}</td>{ht_qs_cells}</tr>"""
            # Optimisation tips row
            tips = _hot_table_optimisation_tips(ht)
            tips_html = "".join(f"<li>{t}</li>" for t in tips)
            hot_table_rows += f"""<tr class="ht-tips-row">
              <td colspan="{ht_col_count}" style="padding:8px 14px 14px 28px;border-bottom:2px solid var(--border);background:var(--light-bg)">
                <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.3px;color:var(--muted);margin-bottom:4px">Optimisation Opportunities</div>
                <ul style="margin:0;padding-left:18px;font-size:12px;color:var(--dark);line-height:1.7">{tips_html}</ul>
              </td></tr>"""

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
        <div class="card">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
            <h3 style="margin:0">Top Most Complex Measures <span id="dax-count" style="font-weight:400;color:var(--muted)">({len(top_measures)})</span></h3>
            <label style="display:flex;align-items:center;gap:6px;font-size:12px;color:var(--muted);cursor:pointer;user-select:none">
              <input type="checkbox" id="dax-ti-toggle" checked onchange="toggleTI(this.checked)">
              Show Time Intelligence ({ti_count_in_top})
            </label>
          </div>
          <table id="dax-top-table"><thead><tr><th>Measure</th><th>Table</th><th>Score</th><th>Ctx Trans.</th><th>Hops</th><th>FILTER(ALL)</th><th>Est. SQL Subq.</th><th>Level</th><th>Cross-DQ</th></tr></thead>
            <tbody>{measure_rows}</tbody></table>
          <script>
            function toggleTI(show) {{
              var rows = document.querySelectorAll('#dax-top-table tbody tr[data-ti]');
              rows.forEach(function(r) {{ r.style.display = show ? '' : 'none'; }});
              var visible = document.querySelectorAll('#dax-top-table tbody tr:not([style*="display: none"])').length;
              document.getElementById('dax-count').textContent = '(' + visible + ')';
            }}
          </script>
        </div>
        <div class="card"><h3>Hot Tables &mdash; Optimisation Candidates</h3>
          <table><thead><tr><th>Table</th><th>Storage</th><th>Refs</th><th>Rows</th><th>Size</th><th>Degree</th><th>Priority</th>{"<th>Daily Queries</th><th>Avg Duration</th>" if ht_has_qs else ""}</tr></thead>
            <tbody>{hot_table_rows}</tbody></table></div>
        """)

    # ═══════════════════════════════════════════════════════
    # SECTION: BPA FINDINGS (full analysis with filters)
    # ═══════════════════════════════════════════════════════
    if bpa_results:
        sec_num += 1
        all_rule_results = bpa_results.get("ruleResults", [])
        passing_rules = bpa_results.get("passingRules", [])
        all_findings = bpa_results.get("findings", [])

        # Build rule → impact mapping for tagging findings
        rule_impact_map: dict[str, str] = {}
        for r in all_rule_results:
            rule_impact_map[r.get("rule", "")] = r.get("performanceImpact", "quality")

        # Count rules by impact type for filter badges
        impact_counts: dict[str, int] = {}
        for r in all_rule_results:
            imp = r.get("performanceImpact", "quality")
            impact_counts[imp] = impact_counts.get(imp, 0) + 1

        # Rule violations table — ALL rules, sorted by count
        sorted_rules = sorted(all_rule_results, key=lambda r: r.get("count", 0), reverse=True)
        rule_rows = ""
        for r in sorted_rules:
            impact = r.get("performanceImpact", "quality")
            impact_badge = _impact_type_badge(impact)
            impact_desc = escape(r.get("impactDescription", ""))
            rule_rows += f"""<tr data-impact="{impact}">
              <td>{escape(r.get('rule', ''))}</td>
              <td>{r.get('count', 0)}</td>
              <td>{impact_badge}</td>
              <td style="font-size:12px;color:var(--muted)">{impact_desc}</td></tr>"""

        # Filter buttons for Rule Violations
        rule_filter_btns = f'<span class="filter-btn active" data-filter-value="all" onclick="bpaFilter(\'bpa-rule-bar\',\'bpa-rule-table\',\'impact\',\'all\')">All<span class="filter-count">({len(sorted_rules)})</span></span>'
        for imp_type in ["latency", "cost", "memory", "quality"]:
            cnt = impact_counts.get(imp_type, 0)
            if cnt > 0:
                rule_filter_btns += f'<span class="filter-btn" data-filter-value="{imp_type}" onclick="bpaFilter(\'bpa-rule-bar\',\'bpa-rule-table\',\'impact\',\'{imp_type}\')">{imp_type.title()}<span class="filter-count">({cnt})</span></span>'

        # Passing rules summary
        passing_html = ""
        if passing_rules:
            passing_html = f'<p style="font-size:13px;color:var(--green);margin-top:12px">&#10003; {len(passing_rules)} rules passed (no violations): {", ".join(passing_rules)}</p>'

        # --- Detailed Findings with filters ---
        # Tag each finding with its impact type
        findings = all_findings  # show ALL findings

        # Count findings by impact and severity for filter badges
        finding_impact_counts: dict[str, int] = {}
        finding_severity_counts: dict[str, int] = {}
        for f in findings:
            f_impact = rule_impact_map.get(f.get("rule", ""), "quality")
            finding_impact_counts[f_impact] = finding_impact_counts.get(f_impact, 0) + 1
            sev = f.get("severity", "").lower()
            finding_severity_counts[sev] = finding_severity_counts.get(sev, 0) + 1

        # Sort findings: High first, then Medium, then Low
        severity_order = {"high": 0, "medium": 1, "low": 2}
        sorted_findings = sorted(findings, key=lambda f: severity_order.get(f.get("severity", "").lower(), 3))

        def _finding_row_tagged(f):
            f_impact = rule_impact_map.get(f.get("rule", ""), "quality")
            f_sev = f.get("severity", "").lower()
            return f"""<tr data-impact="{f_impact}" data-severity="{f_sev}">
              <td>{escape(f.get('rule', ''))}</td><td>{_badge(f.get('severity', ''))}</td>
              <td>{_impact_type_badge(f_impact)}</td>
              <td>{escape(f.get('table', ''))}</td><td>{escape(f.get('object', ''))}</td>
              <td>{escape(f.get('message', ''))}</td></tr>"""

        # All rows in a single table — first 20 visible, rest hidden with data-overflow
        visible_limit = 20
        all_finding_rows = ""
        for idx, f in enumerate(sorted_findings):
            f_impact = rule_impact_map.get(f.get("rule", ""), "quality")
            f_sev = f.get("severity", "").lower()
            overflow = ' data-overflow="1" style="display:none"' if idx >= visible_limit else ''
            all_finding_rows += f"""<tr data-impact="{f_impact}" data-severity="{f_sev}"{overflow}>
              <td>{escape(f.get('rule', ''))}</td><td>{_badge(f.get('severity', ''))}</td>
              <td>{_impact_type_badge(f_impact)}</td>
              <td>{escape(f.get('table', ''))}</td><td>{escape(f.get('object', ''))}</td>
              <td>{escape(f.get('message', ''))}</td></tr>"""

        overflow_count = max(0, len(sorted_findings) - visible_limit)
        overflow_toggle = ""
        if overflow_count > 0:
            overflow_toggle = f'<div style="margin-top:12px"><a href="#" id="bpa-detail-toggle" style="font-size:13px;font-weight:600;color:var(--accent)" onclick="toggleBpaOverflow(event)">Show {overflow_count} more findings...</a></div>'

        # Filter buttons for Detailed Findings — impact type + severity
        detail_filter_btns_impact = f'<span class="filter-btn active" data-filter-value="all" onclick="bpaFilter(\'bpa-detail-bar-impact\',\'bpa-detail-table\',\'impact\',\'all\')">All<span class="filter-count">({len(findings)})</span></span>'
        for imp_type in ["latency", "cost", "memory", "quality"]:
            cnt = finding_impact_counts.get(imp_type, 0)
            if cnt > 0:
                detail_filter_btns_impact += f'<span class="filter-btn" data-filter-value="{imp_type}" onclick="bpaFilter(\'bpa-detail-bar-impact\',\'bpa-detail-table\',\'impact\',\'{imp_type}\')">{imp_type.title()}<span class="filter-count">({cnt})</span></span>'

        detail_filter_btns_severity = f'<span class="filter-btn active" data-filter-value="all" onclick="bpaFilter(\'bpa-detail-bar-severity\',\'bpa-detail-table\',\'severity\',\'all\')">All<span class="filter-count">({len(findings)})</span></span>'
        for sev in ["high", "medium", "low"]:
            cnt = finding_severity_counts.get(sev, 0)
            if cnt > 0:
                detail_filter_btns_severity += f'<span class="filter-btn" data-filter-value="{sev}" onclick="bpaFilter(\'bpa-detail-bar-severity\',\'bpa-detail-table\',\'severity\',\'{sev}\')">{sev.title()}<span class="filter-count">({cnt})</span></span>'

        sections.append(f"""
        <h2 class="section-title">{sec_num}. Best Practice Findings</h2>
        <div class="card">
          <h3>Rule Violations</h3>
          <div class="filter-bar" id="bpa-rule-bar">
            <span style="font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;padding:5px 0">Impact:</span>
            {rule_filter_btns}
          </div>
          <table id="bpa-rule-table"><thead><tr><th>Rule</th><th>Violations</th><th>Impact Type</th><th>Performance Impact</th></tr></thead>
            <tbody>{rule_rows}</tbody></table>
          {passing_html}
        </div>
        <div class="card">
          <h3>Detailed Findings (<span id="bpa-detail-table-count">{len(findings)}</span> total)</h3>
          <div style="display:flex;gap:24px;flex-wrap:wrap;margin-bottom:4px">
            <div class="filter-bar" id="bpa-detail-bar-impact">
              <span style="font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;padding:5px 0">Impact:</span>
              {detail_filter_btns_impact}
            </div>
            <div class="filter-bar" id="bpa-detail-bar-severity">
              <span style="font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;padding:5px 0">Severity:</span>
              {detail_filter_btns_severity}
            </div>
          </div>
          <table id="bpa-detail-table"><thead><tr><th>Rule</th><th>Severity</th><th>Impact</th><th>Table</th><th>Object</th><th>Detail</th></tr></thead>
            <tbody>{all_finding_rows}</tbody></table>
          {overflow_toggle}
        </div>
        """)

    # ═══════════════════════════════════════════════════════
    # SECTION: dbt BEST PRACTICES (full analysis with filters)
    # ═══════════════════════════════════════════════════════
    if engineering_bpa:
        sec_num += 1
        eng_rules = engineering_bpa.get("ruleResults", [])
        eng_passing = engineering_bpa.get("passingRules", [])

        # Summary across ALL rules
        eng_high = sum(r["count"] for r in eng_rules if r.get("severity") == "high")
        eng_medium = sum(r["count"] for r in eng_rules if r.get("severity") == "medium")
        eng_low = sum(r["count"] for r in eng_rules if r.get("severity") == "low")
        eng_total = eng_high + eng_medium + eng_low

        # Count rules by impact type for filter badges
        eng_impact_counts: dict[str, int] = {}
        for r in eng_rules:
            imp = r.get("impact", r.get("performanceImpact", "quality"))
            eng_impact_counts[imp] = eng_impact_counts.get(imp, 0) + 1

        # Sort ALL rules by count descending
        sorted_eng_rules = sorted(eng_rules, key=lambda r: r.get("count", 0), reverse=True)

        eng_rows = ""
        for rule in sorted_eng_rules:
            examples_html = ""
            for ex in rule.get("examples", [])[:3]:
                examples_html += f"<div style='font-size:11px;color:var(--muted);margin-top:4px'>{escape(ex.get('model', ''))} → {escape(ex.get('detail', ''))}</div>"
            impact = rule.get("impact", rule.get("performanceImpact", "quality"))
            impact_badge = _impact_type_badge(impact) if impact else ""
            eng_rows += f"""<tr data-impact="{impact}">
              <td><strong>{escape(rule.get('ruleId', ''))}</strong></td>
              <td>{escape(rule.get('title', ''))}{examples_html}</td>
              <td>{_badge(rule.get('severity', 'medium'))}</td>
              <td>{impact_badge}</td>
              <td style="font-weight:600">{rule.get('count', 0)}</td>
              <td style="font-size:12px">{escape(rule.get('recommendation', ''))}</td>
            </tr>"""

        # Filter buttons for Engineering BPA
        eng_filter_btns = f'<span class="filter-btn active" data-filter-value="all" onclick="bpaFilter(\'eng-bpa-bar\',\'eng-bpa-table\',\'impact\',\'all\')">All<span class="filter-count">({len(sorted_eng_rules)})</span></span>'
        for imp_type in ["latency", "cost", "memory", "quality"]:
            cnt = eng_impact_counts.get(imp_type, 0)
            if cnt > 0:
                eng_filter_btns += f'<span class="filter-btn" data-filter-value="{imp_type}" onclick="bpaFilter(\'eng-bpa-bar\',\'eng-bpa-table\',\'impact\',\'{imp_type}\')">{imp_type.title()}<span class="filter-count">({cnt})</span></span>'

        passing_html = ""
        if eng_passing:
            passing_items = ", ".join(f"{p.get('ruleId', '')}: {p.get('title', '')}" for p in eng_passing)
            passing_html = f"<details><summary>Passing rules ({len(eng_passing)})</summary><p style='font-size:12px;color:var(--muted);margin-top:8px'>{escape(passing_items)}</p></details>"

        sections.append(f"""
        <h2 class="section-title">{sec_num}. dbt Best Practices {'<span class="badge badge-high">dbt Models</span>' if eng_high > 0 else ''}</h2>
        <div class="metric-grid">
          <div class="metric-card red"><div class="value">{eng_high}</div><div class="label">High</div></div>
          <div class="metric-card amber"><div class="value">{eng_medium}</div><div class="label">Medium</div></div>
          <div class="metric-card green"><div class="value">{eng_low}</div><div class="label">Low</div></div>
          <div class="metric-card"><div class="value">{eng_total}</div><div class="label">Total Findings</div></div>
        </div>
        <div class="card">
          <h3>Rule Violations</h3>
          <div class="filter-bar" id="eng-bpa-bar">
            <span style="font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;padding:5px 0">Impact:</span>
            {eng_filter_btns}
          </div>
          <table id="eng-bpa-table">
            <thead><tr><th>Rule</th><th>Description</th><th>Severity</th><th>Impact</th><th>Count</th><th>Recommendation</th></tr></thead>
            <tbody>{eng_rows}</tbody>
          </table>
          {passing_html}
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
    # SECTION: REPORT VISUAL ANALYSIS
    # ═══════════════════════════════════════════════════════
    if visual_analysis:
        sec_num += 1
        va_summary = visual_analysis.get("summary", {})
        va_reports = visual_analysis.get("reports", [])

        # Collect all visual finding rows with severity for sorting
        _SEV_ORDER = {"high": 0, "medium": 1, "low": 2}
        visual_items: list[tuple[int, str]] = []  # (sort_key, html_row)
        for report in va_reports:
            report_name = report.get("reportName", "Unknown Report")
            for rule in report.get("ruleResults", []):
                sev = rule.get("severity", "medium").lower()
                sev_key = _SEV_ORDER.get(sev, 9)
                for ex in rule.get("examples", [])[:3]:
                    page = ex.get("page", ex.get("detail", ""))
                    # Build "Report / Page" location string
                    location = f"{report_name} / {page}" if page else report_name
                    row = f"""<tr>
                      <td>{_badge(sev)}</td>
                      <td><strong>{escape(rule.get('ruleId', ''))}</strong>: {escape(rule.get('title', ''))}</td>
                      <td>{escape(location)}</td>
                      <td style="font-size:12px">{escape(ex.get('recommendation', rule.get('recommendation', '')))}</td>
                    </tr>"""
                    visual_items.append((sev_key, row))

        # Sort by severity: high first, then medium, then low
        visual_items.sort(key=lambda x: x[0])
        visual_rows = "".join(row for _, row in visual_items)

        sections.append(f"""
        <h2 class="section-title">{sec_num}. Report Visual Analysis <span class="badge badge-info">PBI Report</span></h2>
        <div class="metric-grid">
          <div class="metric-card"><div class="value">{va_summary.get('totalPages', 0)}</div><div class="label">Pages Analysed</div></div>
          <div class="metric-card"><div class="value">{va_summary.get('totalVisuals', 0)}</div><div class="label">Total Visuals</div></div>
          <div class="metric-card red"><div class="value">{va_summary.get('high', 0)}</div><div class="label">High</div></div>
          <div class="metric-card amber"><div class="value">{va_summary.get('medium', 0)}</div><div class="label">Medium</div></div>
        </div>
        <div class="card">
          <table>
            <thead><tr><th>Severity</th><th>Rule</th><th>Report / Page</th><th>Recommendation</th></tr></thead>
            <tbody>{visual_rows}</tbody>
          </table>
        </div>
        """)

    # ═══════════════════════════════════════════════════════
    # SECTION: CAPACITY SETTINGS ANALYSIS
    # ═══════════════════════════════════════════════════════
    if capacity_settings:
        sec_num += 1
        qt = capacity_settings.get("queryTimeout", {})
        qt_rec = qt.get("recommendation", {})
        qt_dist = qt.get("durationDistribution", [])

        # Build duration distribution bars
        dist_bars = ""
        max_count = max((b.get("count", 0) for b in qt_dist), default=1) or 1
        for b in qt_dist:
            pct = b.get("count", 0) / max_count * 100
            colour = "var(--green)" if b.get("pct", 0) > 50 else ("var(--amber)" if b.get("pct", 0) > 1 else "var(--red)")
            dist_bars += f"""<div style="display:flex;align-items:center;gap:12px;margin-bottom:6px">
              <span style="width:80px;font-size:12px;text-align:right;font-weight:600">{escape(b.get('bucket', ''))}</span>
              <div class="bar-container" style="flex:1"><div class="bar-fill" style="width:{pct:.1f}%;background:{colour}"></div></div>
              <span style="width:80px;font-size:12px">{_fmt_number(b.get('count', 0))} ({b.get('pct', 0):.1f}%)</span>
            </div>"""

        # Notes from Luke's methodology
        notes_html = ""
        for note in qt.get("notes", []):
            notes_html += f"<li style='font-size:12px;margin-bottom:4px'>{escape(note)}</li>"

        sections.append(f"""
        <h2 class="section-title">{sec_num}. Capacity Settings Analysis <span class="badge badge-high">Capacity Admin</span></h2>
        <div class="card">
          <h3>Query Timeout Simulation</h3>
          <div class="metric-grid" style="grid-template-columns:repeat(3,1fr)">
            <div class="metric-card"><div class="value">{qt.get('currentDefault', 3600)}s</div><div class="label">Current Default</div></div>
            <div class="metric-card"><div class="value">{qt.get('pbiReportDefault', 225)}s</div><div class="label">PBI Report Default</div></div>
            <div class="metric-card green"><div class="value">{qt_rec.get('value', '-')}s</div><div class="label">Recommended</div></div>
          </div>
          <h4 style="margin-top:16px">Query Duration Distribution</h4>
          {dist_bars}
          <div class="note-box" style="margin-top:16px">
            <strong>Luke&rsquo;s Methodology:</strong> {escape(qt_rec.get('lukeMethodology', qt.get('lukeMethodology', '')))}
          </div>
          {"<ul style='margin-top:12px'>" + notes_html + "</ul>" if notes_html else ""}
        </div>
        """)

    # ═══════════════════════════════════════════════════════
    # SECTION: WORKLOAD & INFRASTRUCTURE
    # ═══════════════════════════════════════════════════════
    if workload_analysis:
        sec_num += 1
        wa = workload_analysis
        sp = wa.get("surgeProtection", {})
        cs = wa.get("capacityScaling", {})
        sms = wa.get("semanticModelSettings", {})

        # Hourly distribution heatmap
        hourly = wa.get("hourlyDistribution", [])
        hour_cells = ""
        max_q = max((h.get("queryCount", 0) for h in hourly), default=1) or 1
        for h in hourly:
            intensity = h.get("queryCount", 0) / max_q
            bg = f"rgba(192,57,43,{intensity:.2f})" if intensity > 0.5 else f"rgba(212,160,23,{intensity:.2f})" if intensity > 0.2 else f"rgba(26,135,84,{intensity:.2f})"
            hour_cells += f'<td style="text-align:center;background:{bg};font-size:11px;padding:4px;min-width:36px" title="Hour {h.get("hour",0)}: {h.get("queryCount",0)} queries">{h.get("queryCount",0)}</td>'

        # Surge protection
        sp_cap = sp.get("capacityLevel", {})
        sp_ws = sp.get("workspaceLevel", {})

        # Capacity scaling pros/cons
        scaling_html = ""
        if cs:
            cur = cs.get("currentCapacity", {})
            pros = cs.get("prosAndCons", {}).get("pros", [])
            cons = cs.get("prosAndCons", {}).get("cons", [])
            scaling_html = f"""
            <div class="card" style="margin-top:16px">
              <h3>Capacity Scaling: {escape(cur.get('previousSku', '?'))} → {escape(cur.get('sku', '?'))}</h3>
              <div class="metric-grid" style="grid-template-columns:repeat(3,1fr)">
                <div class="metric-card"><div class="value">{escape(cur.get('sku', '-'))}</div><div class="label">Current SKU</div></div>
                <div class="metric-card"><div class="value">{cur.get('capacityUnits', '-')}</div><div class="label">Capacity Units</div></div>
                <div class="metric-card"><div class="value">{escape(str(cur.get('subscriptionQuota', '-')))}</div><div class="label">Subscription Quota</div></div>
              </div>
              <div class="pros-cons-grid" style="margin-top:16px">
                <div><h4 style="color:var(--green)">Pros</h4><ul class="pros-list">{"".join(f"<li>{escape(p)}</li>" for p in pros)}</ul></div>
                <div><h4 style="color:var(--red)">Cons</h4><ul class="cons-list">{"".join(f"<li>{escape(c)}</li>" for c in cons)}</ul></div>
              </div>
              <div class="note-box" style="margin-top:12px"><strong>Key insight:</strong> Scaling alone does not fix inefficient queries — combine with query optimisation for sustainable performance.</div>
            </div>"""

        # Semantic model settings
        settings_html = ""
        if sms:
            lsf = sms.get("largeStorageFormat", {})
            qso = sms.get("queryScaleOut", {})
            if lsf or qso:
                settings_html = f"""
                <div class="card" style="margin-top:16px">
                  <h3>Semantic Model Settings</h3>
                  <table>
                    <thead><tr><th>Setting</th><th>Current</th><th>Recommendation</th><th>Impact</th></tr></thead>
                    <tbody>
                      <tr><td>Large Semantic Model Storage Format</td><td>{_badge('Off') if not lsf.get('currentStatus', 'Off') == 'On' else _badge('On')}</td><td><strong>{escape(lsf.get('recommendation', 'Enable'))}</strong></td><td>Prerequisite for Query Scale-Out. Model is {lsf.get('modelSizeMB', '?')} MB.</td></tr>
                      <tr><td>Query Scale-Out</td><td>{_badge('Off')}</td><td><strong>{escape(qso.get('recommendation', 'Enable'))}</strong></td><td>Distributes queries across read-only replicas during peak load.</td></tr>
                    </tbody>
                  </table>
                  <details style="margin-top:12px">
                    <summary>Pros &amp; Cons — Large Storage Format</summary>
                    <div class="pros-cons-grid" style="margin-top:8px">
                      <div><ul class="pros-list">{"".join(f"<li>{escape(p)}</li>" for p in lsf.get('pros', []))}</ul></div>
                      <div><ul class="cons-list">{"".join(f"<li>{escape(c)}</li>" for c in lsf.get('cons', []))}</ul></div>
                    </div>
                  </details>
                  <details style="margin-top:8px">
                    <summary>Pros &amp; Cons — Query Scale-Out</summary>
                    <div class="pros-cons-grid" style="margin-top:8px">
                      <div><ul class="pros-list">{"".join(f"<li>{escape(p)}</li>" for p in qso.get('pros', []))}</ul></div>
                      <div><ul class="cons-list">{"".join(f"<li>{escape(c)}</li>" for c in qso.get('cons', []))}</ul></div>
                    </div>
                  </details>
                </div>"""

        sections.append(f"""
        <h2 class="section-title">{sec_num}. Workload &amp; Infrastructure <span class="badge badge-high">Capacity Admin</span></h2>
        <div class="card">
          <h3>Hourly Query Distribution</h3>
          <div style="overflow-x:auto">
            <table style="table-layout:fixed"><thead><tr>{"".join(f'<th style="text-align:center;font-size:10px;padding:4px">{h:02d}</th>' for h in range(24))}</tr></thead>
            <tbody><tr>{hour_cells}</tr></tbody></table>
          </div>
          <p style="font-size:12px;color:var(--muted);margin-top:8px">Peak hour: {wa.get('peakHour', '?')}:00 ({_fmt_number(wa.get('peakHourQueries', 0))} queries). Peak-to-off-peak ratio: {wa.get('peakToOffPeakRatio', '?')}x</p>
        </div>
        <div class="card" style="margin-top:16px">
          <h3>Surge Protection Recommendations</h3>
          <div class="metric-grid" style="grid-template-columns:repeat(2,1fr)">
            <div class="metric-card"><div class="value">{sp_cap.get('recommendedRejectionThreshold', '-')}%</div><div class="label">Rejection Threshold</div><div class="sub">{escape(sp_cap.get('rationale', ''))}</div></div>
            <div class="metric-card"><div class="value">{sp_cap.get('recommendedRecoveryThreshold', '-')}%</div><div class="label">Recovery Threshold</div></div>
          </div>
          {"<div class='note-box' style='margin-top:12px'><strong>Mission Critical:</strong> " + ", ".join(escape(w) for w in sp_ws.get('missionCritical', [])) + "</div>" if sp_ws.get('missionCritical') else ""}
        </div>
        {scaling_html}
        {settings_html}
        """)

    # ═══════════════════════════════════════════════════════
    # SECTION: ACTION-PRIORITY MATRIX
    # ═══════════════════════════════════════════════════════
    if synthesis and "topFindings" in synthesis:
        sec_num += 1
        syn_findings = synthesis["topFindings"]

        # --- Normalise quadrant keys ---
        _q_map = {
            "quick win": "quick_win", "quick_win": "quick_win", "Quick Win": "quick_win",
            "strategic investment": "strategic", "strategic": "strategic", "Strategic Investment": "strategic",
            "minor improvement": "minor", "minor": "minor", "Minor Improvement": "minor",
            "deprioritise": "deprioritise", "Deprioritise": "deprioritise",
            "thankless": "deprioritise", "Thankless Tasks": "deprioritise",
        }
        quadrants: dict[str, list] = {"quick_win": [], "strategic": [], "minor": [], "deprioritise": []}
        for f in syn_findings:
            q = _q_map.get(f.get("quadrant", "strategic"), "strategic")
            quadrants.setdefault(q, []).append(f)

        # --- Map roadmap actions into quadrants by finding reference ---
        finding_quad_lookup = {f.get("id"): _q_map.get(f.get("quadrant", "strategic"), "strategic") for f in syn_findings}
        roadmap_by_quad: dict[str, list] = {"quick_win": [], "strategic": [], "minor": [], "deprioritise": []}
        for phase in synthesis.get("implementationRoadmap", []):
            phase_name = phase.get("phase", "")
            for item in phase.get("actions", phase.get("items", [])):
                if isinstance(item, dict):
                    action_desc = item.get("action", "")
                    where_val = item.get("where", "")
                    location_val = item.get("location", "")
                    finding_ref = item.get("finding", "")
                    impact_val = item.get("impact", "")
                else:
                    action_desc = str(item)
                    where_val, location_val, finding_ref, impact_val = "", "", "", ""
                # Determine quadrant from finding reference
                fids = [fid.strip() for fid in finding_ref.split(",") if fid.strip()] if finding_ref else []
                target_q = "strategic"  # default
                for fid in fids:
                    if fid in finding_quad_lookup:
                        target_q = finding_quad_lookup[fid]
                        break
                # Build WHERE with category + actual location
                # Prefer explicit location field, fall back to regex extraction from action text
                location = location_val or _extract_location_from_action(action_desc, where_val)
                if where_val:
                    layers_list = [w.strip() for w in where_val.split(",")] if "," in where_val else [where_val]
                    where_r = _where_with_location(layers_list, location)
                else:
                    matched = [f for f in syn_findings if f.get("id") in fids]
                    layers = []
                    for m in matched:
                        layers.extend(m.get("layers", []))
                    where_r = _where_with_location(list(dict.fromkeys(layers)), location) if layers else '<span class="badge badge-info">TBD</span>'
                roadmap_by_quad.setdefault(target_q, []).append({
                    "action": action_desc,
                    "where_html": where_r,
                    "finding": finding_ref,
                    "impact": impact_val,
                    "phase": phase_name,
                })

        def _matrix_findings(items):
            return "".join(
                f"<li><strong>{escape(str(f.get('id', '')))}</strong>: {escape(str(f.get('title', '')))} "
                f"{_where_badges(f.get('layers', []))}</li>"
                for f in items
            )

        def _matrix_actions_table(actions):
            if not actions:
                return '<p style="font-size:12px;color:var(--muted);margin-top:8px">No actions mapped to this quadrant.</p>'
            rows = ""
            for a in actions:
                ref = f"[{escape(a['finding'])}] " if a.get("finding") else ""
                rows += f"<tr><td>{ref}<strong>{escape(a['action'])}</strong></td><td>{a['where_html']}</td></tr>\n"
            return f"""<table class="matrix-actions-table" style="width:100%;border-collapse:collapse">
              <thead><tr><th style="text-align:left;font-size:11px;padding:4px 10px">Action</th><th style="text-align:left;font-size:11px;padding:4px 10px">Category / Location</th></tr></thead>
              <tbody>{rows}</tbody></table>"""

        def _matrix_box(key, title, subtitle, colour_bg, colour_border, colour_title, icon, findings, actions):
            count = len(findings)
            action_count = len(actions)
            findings_html = _matrix_findings(findings) if findings else '<li style="color:var(--muted)">No findings in this quadrant</li>'
            actions_html = _matrix_actions_table(actions)
            return f"""<div class="matrix-box" style="background:{colour_bg};border:1px solid {colour_border}">
        <h3 style="color:{colour_title}">{icon} {title}</h3>
        <div class="matrix-subtitle">{subtitle}</div>
        <span class="matrix-count" style="color:{colour_title}">{count} finding{"s" if count != 1 else ""} &middot; {action_count} action{"s" if action_count != 1 else ""}</span>
        <details>
          <summary style="color:{colour_title}">View findings &amp; actions</summary>
          <div style="margin-top:8px">
            <div style="font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;color:var(--muted);margin-bottom:6px">Findings</div>
            <ul>{findings_html}</ul>
            <div style="font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;color:var(--muted);margin:12px 0 6px">Implementation Actions</div>
            {actions_html}
          </div>
        </details>
      </div>"""

        qw_box = _matrix_box("quick_win", "Quick Wins", "High Impact, Low Effort &mdash; top priority",
                             "rgba(26,135,84,0.06)", "rgba(26,135,84,0.25)", "var(--green)", "&#9889;",
                             quadrants.get("quick_win", []), roadmap_by_quad.get("quick_win", []))
        mp_box = _matrix_box("strategic", "Major Projects", "High Impact, High Effort &mdash; plan carefully",
                             "rgba(7,112,207,0.06)", "rgba(7,112,207,0.25)", "var(--accent)", "&#128640;",
                             quadrants.get("strategic", []), roadmap_by_quad.get("strategic", []))
        fi_box = _matrix_box("minor", "Fill-ins", "Low Impact, Low Effort &mdash; if time permits",
                             "rgba(108,117,125,0.06)", "rgba(108,117,125,0.2)", "#6c757d", "&#128221;",
                             quadrants.get("minor", []), roadmap_by_quad.get("minor", []))
        tt_box = _matrix_box("deprioritise", "Thankless Tasks", "Low Impact, High Effort &mdash; avoid or re-evaluate",
                             "rgba(192,57,43,0.06)", "rgba(192,57,43,0.2)", "var(--red)", "&#9888;",
                             quadrants.get("deprioritise", []), roadmap_by_quad.get("deprioritise", []))

        sections.append(f"""
    <h2 class="section-title">{sec_num}. Action-Priority Matrix</h2>
    <p style="font-size:13px;color:var(--muted);margin-bottom:8px">
      All findings and implementation actions plotted by <strong>effort</strong> vs <strong>impact</strong>
      to maximise productivity. Expand each quadrant to see findings and their mapped actions.
    </p>
    <div class="note-box" style="margin-bottom:16px">
      <strong>Category:</strong>
      <span class="badge badge-info">PBI Report</span> = Report layout / pages / slicers / filters
      &nbsp;&middot;&nbsp;
      <span class="badge" style="background:rgba(108,117,125,0.12);color:#6c757d">PBI Visual</span> = Visual configs / cards / matrices
      &nbsp;&middot;&nbsp;
      <span class="badge badge-medium">Semantic Model</span> = DAX measures / relationships / table config
      &nbsp;&middot;&nbsp;
      <span class="badge badge-high">dbt Models</span> = dbt SQL / materialisation / clustering / serve views
    </div>
    <div class="matrix-wrapper">
      <div class="matrix-y-label">&larr; Low Impact &nbsp;&nbsp;&nbsp; High Impact &rarr;</div>
      <div class="matrix-grid">
        {qw_box}
        {mp_box}
        {fi_box}
        {tt_box}
      </div>
      <div class="matrix-x-label">&larr; Low Effort &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; High Effort &rarr;</div>
    </div>
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

            # Affected objects — show exactly which pages, tables, models, etc. are impacted
            affected_html = ""
            ao = f.get("affectedObjects")
            if ao and isinstance(ao, dict):
                ao_parts = []
                _ao_labels = [
                    ("pages", "Pages"), ("tables", "Tables"), ("measures", "Measures"),
                    ("dbtModels", "dbt Models"), ("relationships", "Relationships"),
                    ("columns", "Columns"), ("visuals", "Visuals"),
                ]
                for key, label in _ao_labels:
                    items = ao.get(key, [])
                    if items:
                        item_badges = ", ".join(f"<code>{escape(str(i))}</code>" for i in items[:10])
                        overflow = f" (+{len(items) - 10} more)" if len(items) > 10 else ""
                        ao_parts.append(f"<strong>{label}:</strong> {item_badges}{overflow}")
                if ao_parts:
                    affected_html = f"""<div style="margin-top:12px;padding:12px;background:var(--light-bg);border-radius:6px;font-size:13px">
                      <strong>Affected Objects:</strong>
                      <div style="margin-top:6px">{"<br>".join(ao_parts)}</div>
                    </div>"""

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
      {affected_html}
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
        # Build Findings by Severity reference table (formerly standalone Root Cause Analysis section)
        severity_ref_rows = ""
        for sf in synthesis["topFindings"]:
            sf_scope = sf.get("scope", "model-wide")
            severity_ref_rows += (
                f"<tr><td>{escape(str(sf.get('id', '')))}</td>"
                f"<td><strong>{escape(str(sf.get('title', '')))}</strong></td>"
                f"<td>{_badge(sf.get('severity', 'medium'))}</td>"
                f"<td>{_scope_badge(sf_scope)}</td>"
                f"<td>{_where_badges(sf.get('layers', []))}</td>"
                f"<td>{escape(str(sf.get('impact', '')))}</td>"
                f"<td>{escape(str(sf.get('effort', '')))}</td></tr>\n"
            )

        sections.append(f"""
    <h2 class="section-title">{sec_num}. Detailed Recommendations</h2>
    <p style="font-size:13px;color:var(--muted);margin-bottom:20px">
      Implementation details, trade-offs, and alternative options for each finding.
    </p>
    {detail_cards}
    <details style="margin-top:24px">
      <summary>Findings by Severity &mdash; Quick Reference Table</summary>
      <div class="card" style="margin-top:12px">
        <table><thead><tr><th>#</th><th>Finding</th><th>Severity</th><th>Scope</th><th>Category</th><th>Impact</th><th>Effort</th></tr></thead>
          <tbody>{severity_ref_rows}</tbody></table>
      </div>
    </details>
    """)

    # (Implementation Roadmap is now merged into the Action-Priority Matrix section above)

    # ═══════════════════════════════════════════════════════
    # ASSEMBLE — wrap sections in collapsible <details>, build TOC & approach
    # ═══════════════════════════════════════════════════════

    # Extract section titles from <h2> tags and wrap each in <details>
    wrapped_sections: list[str] = []
    toc_entries: list[tuple[str, str, str]] = []  # (sec_id, number, title_text)

    for sec_html in sections:
        # Extract the <h2 class="section-title">N. Title ...</h2>
        h2_match = re.search(r'<h2\s+class="section-title">(.*?)</h2>', sec_html, re.DOTALL)
        if h2_match:
            raw_title = h2_match.group(1)
            # Strip HTML tags for TOC text
            title_text = re.sub(r'<[^>]+>', '', raw_title).strip()
            # Extract section number for anchor
            num_match = re.match(r'(\d+)', title_text)
            sec_id = f"sec-{num_match.group(1)}" if num_match else f"sec-{len(toc_entries)+1}"
            toc_entries.append((sec_id, num_match.group(1) if num_match else str(len(toc_entries)+1), title_text))

            # Remove the <h2> from content — it becomes the <summary>
            body_content = sec_html[:h2_match.start()] + sec_html[h2_match.end():]
            wrapped_sections.append(
                f'<details id="{sec_id}" class="report-section">'
                f'<summary>{raw_title}</summary>'
                f'<div class="section-body">{body_content}</div>'
                f'</details>'
            )
        else:
            wrapped_sections.append(sec_html)

    sections_html = "\n".join(wrapped_sections)

    # ── Build Approach & TOC ──
    # Determine which inputs were active
    inputs_list = [
        ("Semantic Model (Tabular Editor JSON)", bool(taxonomy), "Model structure, tables, relationships, measures, storage modes"),
        ("DAX Measures Analysis", bool(dax_complexity), "Complexity scoring, anti-pattern detection, context transitions"),
        ("Best Practice Analyser (BPA)", bool(bpa_results), f"{bpa_total} findings across 12 rules"),
        ("dbt Source Code (Serve + Curated)", bool(dbt_lineage), "Lineage, materialisation, clustering, column pruning"),
        ("dbt Best Practices", bool(engineering_bpa), f"{eng_bpa_summary.get('totalFindings', 0)} findings across 15 rules" if engineering_bpa else ""),
        ("Databricks Metadata & Volumetry", bool(dbx_profile), "Row counts, table sizes, query statistics"),
        ("Databricks Query History", bool(query_profile or user_query_profile), "Per-user attribution, duration profiling"),
        ("Capacity Settings Simulation", bool(capacity_settings), "Timeout, memory limit, row set thresholds",
         "Requires query history export (JSON/CSV) with per-query memory and row-set metrics; system.query.history does not expose memory consumption per statement"),
        ("Workload & Infrastructure Analysis", bool(workload_analysis), "Surge protection, capacity scaling, model settings",
         "Requires Fabric Admin API data (capacity CU consumption, surge protection config, workspace-to-capacity mapping) which cannot be queried programmatically; needs manual export from the Fabric Admin Portal"),
        ("Column Memory Estimation", bool(column_memory), f"{column_memory.get('summary', {}).get('removalCandidateCount', 0)} removal candidates" if column_memory else "",
         "Requires Databricks volumetry at column level (per-column cardinality and byte width); DESCRIBE DETAIL only provides table-level stats, not column-level storage estimates"),
        ("PBIX Visual Layer Analysis", bool(visual_analysis), f"{visual_analysis.get('summary', {}).get('totalFindings', 0)} visual rule findings" if visual_analysis else "",
         "No PBIX report file was extracted; requires running unzip on the .pbix to obtain the Layout JSON for visual-level rule checks"),
        ("Synthesis & Cross-Reference", bool(synthesis), "Root cause analysis, implementation roadmap"),
    ]

    input_bullets = ""
    for entry in inputs_list:
        label, active, detail = entry[0], entry[1], entry[2]
        skip_reason = entry[3] if len(entry) > 3 else ""
        badge_cls = "input-active" if active else "input-inactive"
        status = "Active" if active else "Skipped"
        if active and detail:
            detail_str = f" &mdash; {escape(detail)}"
        elif not active and skip_reason:
            detail_str = f" &mdash; {escape(skip_reason)}"
        else:
            detail_str = ""
        input_bullets += f'<li><span class="input-badge {badge_cls}">{status}</span> <strong>{escape(label)}</strong>{detail_str}</li>\n'

    toc_items = ""
    for sec_id, num, title in toc_entries:
        toc_items += f'<li><a href="#{sec_id}">{escape(title)}</a></li>\n'

    active_count = sum(1 for entry in inputs_list if entry[1])
    analysis_mode = synthesis.get("analysisMode", "model-wide").replace("-", " ").title() if synthesis else "Standard"

    approach_html = f"""
    <div class="approach-card">
      <h3>Analysis Approach</h3>
      <p style="font-size:13px;color:var(--muted);margin-bottom:12px">
        This report was produced by the <strong>PBI Performance Diagnosis Agent</strong> using an automated
        {active_count}-source analysis pipeline. The agent examines the full stack &mdash; from Databricks
        infrastructure through dbt data pipelines to the Power BI semantic model and report layer &mdash;
        to identify performance bottlenecks and produce evidence-backed recommendations.
      </p>
      <h4 style="font-size:13px;margin-bottom:8px">Data Sources &amp; Tools</h4>
      <ul>{input_bullets}</ul>
    </div>
    <div class="toc-card">
      <h3>Table of Contents</h3>
      <ol class="toc-list">{toc_items}</ol>
    </div>
    """

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
      <span>Mode: {escape(analysis_mode)}</span>
    </div>
  </div>
  <div class="container">
    {approach_html}
    {sections_html}
    <div class="report-footer">
      Generated by PBI Performance Diagnosis Agent &middot; {now}
    </div>
  </div>
<script>
document.querySelectorAll('.sortable-th').forEach(th=>{{th.addEventListener('click',()=>{{const t=th.closest('table'),i=Array.from(th.parentNode.children).indexOf(th),rows=Array.from(t.querySelectorAll('tbody tr')),a=th.dataset.sort!=='asc';th.dataset.sort=a?'asc':'desc';rows.sort((x,y)=>{{const va=x.children[i]?.dataset.val||x.children[i]?.textContent||'',vb=y.children[i]?.dataset.val||y.children[i]?.textContent||'',na=parseFloat(va),nb=parseFloat(vb);if(!isNaN(na)&&!isNaN(nb))return a?na-nb:nb-na;return a?va.localeCompare(vb):vb.localeCompare(va)}});rows.forEach(r=>t.querySelector('tbody').appendChild(r))}})}});

function bpaFilter(barId, tableId, attr, value) {{
  var bar = document.getElementById(barId);
  bar.querySelectorAll('.filter-btn').forEach(function(b){{ b.classList.remove('active','active-latency','active-cost','active-memory','active-quality','active-high','active-medium','active-low'); }});
  var clicked = bar.querySelector('[data-filter-value="'+value+'"]');
  if(clicked){{ clicked.classList.add('active'); if(value!=='all') clicked.classList.add('active-'+value); }}
  var rows = document.querySelectorAll('#'+tableId+' tbody tr');
  var shown = 0;
  /* Collect active filters from both filter bars for this table */
  var impactBar = document.getElementById(tableId.replace('table','bar-impact'));
  var sevBar = document.getElementById(tableId.replace('table','bar-severity'));
  var impactVal = 'all', sevVal = 'all';
  if(impactBar){{ var a=impactBar.querySelector('.filter-btn.active'); if(a) impactVal=a.getAttribute('data-filter-value')||'all'; }}
  if(sevBar){{ var a=sevBar.querySelector('.filter-btn.active'); if(a) sevVal=a.getAttribute('data-filter-value')||'all'; }}
  var isFiltered = (impactVal!=='all' || sevVal!=='all');
  rows.forEach(function(r){{
    var matchImpact = (impactVal==='all' || r.getAttribute('data-impact')===impactVal);
    var matchSev = (sevVal==='all' || r.getAttribute('data-severity')===sevVal);
    if(matchImpact && matchSev){{
      r.style.display=''; shown++;
    }} else {{
      r.style.display='none';
    }}
  }});
  /* Update overflow toggle visibility */
  var toggle = document.getElementById(tableId.replace('table','toggle'));
  if(toggle){{
    if(isFiltered){{ toggle.style.display='none'; }}
    else {{ toggle.style.display=''; _resetBpaOverflow(tableId); }}
  }}
  var counter = document.getElementById(tableId+'-count');
  if(counter) counter.textContent = shown;
}}

function toggleBpaOverflow(e) {{
  e.preventDefault();
  var rows = document.querySelectorAll('#bpa-detail-table tbody tr[data-overflow]');
  var link = document.getElementById('bpa-detail-toggle');
  var showing = rows[0] && rows[0].style.display !== 'none';
  rows.forEach(function(r){{ r.style.display = showing ? 'none' : ''; }});
  link.textContent = showing ? 'Show ' + rows.length + ' more findings...' : 'Show first 20 only';
}}

function _resetBpaOverflow(tableId) {{
  var rows = document.querySelectorAll('#'+tableId+' tbody tr[data-overflow]');
  rows.forEach(function(r){{ r.style.display = 'none'; }});
  var toggle = document.getElementById('bpa-detail-toggle');
  if(toggle) toggle.textContent = 'Show ' + rows.length + ' more findings...';
}}
</script>
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
                  "query-profile.json", "perf-summary.json", "databricks-profile.json",
                  "user-query-profile.json", "capacity-settings-analysis.json",
                  "workload-analysis.json", "column-memory-analysis.json",
                  "engineering-bpa-results.json", "visual-analysis.json"]
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
