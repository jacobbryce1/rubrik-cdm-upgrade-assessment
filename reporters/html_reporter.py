"""
HTML Reporter - Multi-cluster dashboard + per-cluster drill-down.
Generates:
  - 00_dashboard.html: Cross-cluster overview with cards and issue table
  - cluster_<name>.html: Per-cluster detail with collapsible sections,
    sortable/filterable tables, and color-coded status cells
"""
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List
from collectors import (
    MultiClusterAssessment,
    ClusterAssessment,
    CollectionResult,
)

logger = logging.getLogger(__name__)


def generate_multi(
    assessment: MultiClusterAssessment, output_dir: str
) -> List[str]:
    """Generate HTML files for multi-cluster assessment."""
    html_dir = Path(output_dir) / "html"
    html_dir.mkdir(parents=True, exist_ok=True)
    generated = []

    # Dashboard
    dashboard_html = _build_dashboard(assessment)
    dash_file = html_dir / "00_dashboard.html"
    with open(dash_file, "w", encoding="utf-8") as f:
        f.write(dashboard_html)
    generated.append(str(dash_file))

    # Per-cluster reports
    for cid, ca in assessment.cluster_assessments.items():
        safe_name = _safe_filename(ca.cluster_name)
        cluster_html = _build_cluster_report(ca, assessment)
        cluster_file = html_dir / f"cluster_{safe_name}.html"
        with open(cluster_file, "w", encoding="utf-8") as f:
            f.write(cluster_html)
        generated.append(str(cluster_file))

    logger.info(f"  HTML: {len(generated)} files generated")
    return generated


def _build_dashboard(ma: MultiClusterAssessment) -> str:
    """Build the cross-cluster overview dashboard."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tb = len(ma.global_blockers)
    tw = len(ma.global_warnings)

    # Cluster cards
    cards_html = ""
    for cid, ca in ma.cluster_assessments.items():
        safe_name = _safe_filename(ca.cluster_name)
        if ca.total_blockers > 0:
            border_color = "#dc3545"
            icon = "&#128683;"
        elif ca.total_warnings > 0:
            border_color = "#ffc107"
            icon = "&#9888;&#65039;"
        else:
            border_color = "#28a745"
            icon = "&#9989;"

        blocker_color = (
            "#dc3545" if ca.total_blockers > 0 else "#28a745"
        )
        warning_color = (
            "#ffc107" if ca.total_warnings > 0 else "#28a745"
        )

        error_html = ""
        if ca.assessment_error:
            error_html = (
                '<div style="color:#dc3545;font-size:0.8em;'
                'margin-top:8px;">'
                f"Error: {_esc(ca.assessment_error)}</div>"
            )

        cards_html += (
            '<div style="background:white;border-radius:10px;'
            'padding:20px;box-shadow:0 2px 8px rgba(0,0,0,0.06);'
            f'border-left:5px solid {border_color};">'
            f"<h3>{icon} {_esc(ca.cluster_name)}</h3>"
            '<div style="color:#6c757d;font-size:0.85em;'
            'margin-top:4px;">'
            f"v{_esc(ca.cluster_version)} | "
            f"{ca.node_count} nodes | "
            f"{_esc(ca.cluster_type)}</div>"
            '<div style="display:flex;gap:20px;margin:12px 0;">'
            '<div style="text-align:center;">'
            '<span style="font-size:1.5em;font-weight:700;'
            f'color:{blocker_color};">{ca.total_blockers}</span>'
            "<br><small>Blockers</small></div>"
            '<div style="text-align:center;">'
            '<span style="font-size:1.5em;font-weight:700;'
            f'color:{warning_color};">{ca.total_warnings}</span>'
            "<br><small>Warnings</small></div>"
            '<div style="text-align:center;">'
            f'<span style="font-size:1.5em;font-weight:700;">'
            f"{ca.node_count}</span>"
            "<br><small>Nodes</small></div></div>"
            f"{error_html}"
            f'<a href="cluster_{safe_name}.html" '
            'style="color:#0070c9;text-decoration:none;'
            'font-weight:600;font-size:0.9em;">'
            "View Full Report &rarr;</a></div>"
        )

    # Issues table rows
    issues_rows = ""
    for b in ma.global_blockers:
        issues_rows += (
            "<tr><td>"
            '<span style="background:#dc3545;color:white;'
            'padding:2px 8px;border-radius:10px;'
            'font-size:0.8em;">BLOCKER</span></td>'
            f"<td>{_esc(b['cluster'])}</td>"
            f"<td>{_esc(b['section'])}</td>"
            f"<td>{_esc(b['message'])}</td></tr>"
        )
    for w in ma.global_warnings:
        issues_rows += (
            "<tr><td>"
            '<span style="background:#ffc107;color:#333;'
            'padding:2px 8px;border-radius:10px;'
            'font-size:0.8em;">WARNING</span></td>'
            f"<td>{_esc(w['cluster'])}</td>"
            f"<td>{_esc(w['section'])}</td>"
            f"<td>{_esc(w['message'])}</td></tr>"
        )

    # Skipped clusters
    skipped_html = ""
    if ma.skipped_clusters:
        skipped_rows = ""
        for sc in ma.skipped_clusters:
            skipped_rows += (
                f"<tr><td>{_esc(sc.get('name', ''))}</td>"
                f"<td>{_esc(sc.get('version', ''))}</td>"
                f"<td>{_esc(sc.get('status', ''))}</td>"
                f"<td>{_esc(sc.get('skip_reason', ''))}</td>"
                "</tr>"
            )
        skipped_html = (
            '<div style="background:white;border-radius:10px;'
            "padding:25px;margin-top:20px;"
            'box-shadow:0 2px 8px rgba(0,0,0,0.06);">'
            f"<h2>Skipped Clusters ({len(ma.skipped_clusters)})"
            "</h2><table><thead><tr>"
            "<th>Cluster</th><th>Version</th>"
            "<th>Status</th><th>Reason</th>"
            f"</tr></thead><tbody>{skipped_rows}"
            "</tbody></table></div>"
        )

    # Badge colors
    blocker_bg = "#dc3545" if tb > 0 else "#28a745"
    warning_bg = "#ffc107" if tw > 0 else "#28a745"
    warning_fg = "#333" if tw > 0 else "white"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CDM Upgrade Assessment - Multi-Cluster Dashboard</title>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; background:#f4f6f9; color:#333; line-height:1.6; }}
.hdr {{ background:linear-gradient(135deg,#1a1a2e,#16213e); color:white; padding:30px 40px; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; }}
.hdr h1 {{ font-size:1.8em; font-weight:600; }}
.hdr-meta {{ text-align:right; opacity:0.9; }}
.main {{ max-width:1400px; margin:0 auto; padding:30px; }}
.stats-row {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(160px,1fr)); gap:15px; margin-bottom:30px; }}
.stat-card {{ background:white; border-radius:10px; padding:20px; text-align:center; box-shadow:0 2px 8px rgba(0,0,0,0.06); }}
.stat-card .label {{ font-size:0.75em; color:#6c757d; text-transform:uppercase; letter-spacing:0.5px; }}
.stat-card .value {{ font-size:2em; font-weight:700; margin:5px 0; }}
.cluster-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(320px,1fr)); gap:20px; margin:20px 0; }}
.section-box {{ background:white; border-radius:10px; padding:25px; margin-top:20px; box-shadow:0 2px 8px rgba(0,0,0,0.06); }}
table {{ width:100%; border-collapse:collapse; font-size:0.88em; }}
th {{ background:#f0f2f5; padding:10px 12px; text-align:left; font-weight:600; border-bottom:2px solid #ddd; cursor:pointer; }}
th:hover {{ background:#e3e7ed; }}
td {{ padding:8px 12px; border-bottom:1px solid #eee; max-width:400px; overflow:hidden; text-overflow:ellipsis; }}
tr:hover {{ background:#f8f9ff; }}
.filter-input {{ width:100%; padding:10px 15px; border:1px solid #ddd; border-radius:8px; font-size:0.9em; margin-bottom:10px; outline:none; }}
.filter-input:focus {{ border-color:#0070c9; box-shadow:0 0 0 3px rgba(0,112,201,0.15); }}
@media print {{ .hdr {{ -webkit-print-color-adjust:exact; print-color-adjust:exact; }} }}
</style>
</head>
<body>
<div class="hdr">
<div>
<h1>&#128274; Rubrik CDM Multi-Cluster Upgrade Assessment</h1>
<div style="margin-top:8px;opacity:0.85;">Cross-Cluster Compatibility Dashboard</div>
</div>
<div class="hdr-meta">
<div><strong>RSC:</strong> {_esc(ma.rsc_base_url)}</div>
<div><strong>Target CDM:</strong> {_esc(ma.target_cdm_version)}</div>
<div><strong>Generated:</strong> {ts}</div>
<div style="margin-top:8px;">
<span style="background:{blocker_bg};color:white;padding:4px 12px;border-radius:20px;font-size:0.85em;font-weight:600;">{tb} Blockers</span>
<span style="background:{warning_bg};color:{warning_fg};padding:4px 12px;border-radius:20px;font-size:0.85em;font-weight:600;">{tw} Warnings</span>
</div>
</div>
</div>
<div class="main">
<div class="stats-row">
<div class="stat-card"><div class="label">Discovered</div><div class="value">{ma.total_clusters_discovered}</div></div>
<div class="stat-card"><div class="label">Assessed</div><div class="value">{ma.total_clusters_assessed}</div></div>
<div class="stat-card"><div class="label">Skipped</div><div class="value">{ma.total_clusters_skipped}</div></div>
<div class="stat-card"><div class="label">Blockers</div><div class="value" style="color:{blocker_bg};">{tb}</div></div>
<div class="stat-card"><div class="label">Warnings</div><div class="value" style="color:{'#ffc107' if tw > 0 else '#28a745'};">{tw}</div></div>
</div>
<h2 style="margin-bottom:15px;">Cluster Assessments ({ma.total_clusters_assessed})</h2>
<div class="cluster-grid">{cards_html}</div>
<div class="section-box">
<h2>All Issues Across All Clusters ({tb + tw})</h2>
<input type="text" class="filter-input" placeholder="&#128269; Filter issues..." id="issue-filter">
<table id="issues-table">
<thead><tr><th>Severity</th><th>Cluster</th><th>Section</th><th>Message</th></tr></thead>
<tbody>{issues_rows}</tbody>
</table>
</div>
{skipped_html}
</div>
<script>
document.getElementById('issue-filter').addEventListener('input', function() {{
    var f = this.value.toLowerCase();
    var rows = document.querySelectorAll('#issues-table tbody tr');
    rows.forEach(function(row) {{
        row.style.display = row.textContent.toLowerCase().includes(f) ? '' : 'none';
    }});
}});
</script>
</body>
</html>"""


def _build_cluster_report(
    ca: ClusterAssessment, ma: MultiClusterAssessment
) -> str:
    """Build detailed HTML report for a single cluster."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Navigation
    nav_html = ""
    for key, r in ca.results.items():
        nav_html += (
            f'<li><a href="#{r.section_id}" '
            'style="display:block;padding:6px 20px;'
            'text-decoration:none;color:#555;font-size:0.85em;'
            'border-left:3px solid transparent;"'
            ' onmouseover="this.style.borderLeftColor=\'#0070c9\';'
            'this.style.color=\'#0070c9\'"'
            ' onmouseout="this.style.borderLeftColor=\'transparent\';'
            'this.style.color=\'#555\'">'
            f"{_esc(r.section_name)}</a></li>"
        )

    # Sections
    sections_html = ""
    for key, r in ca.results.items():
        sections_html += _build_section_html(r)

    # Badge colors
    blocker_bg = (
        "#dc3545" if ca.total_blockers > 0 else "#28a745"
    )
    warning_bg = (
        "#ffc107" if ca.total_warnings > 0 else "#28a745"
    )
    warning_fg = (
        "#333" if ca.total_warnings > 0 else "white"
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{_esc(ca.cluster_name)} - CDM Upgrade Assessment</title>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; background:#f4f6f9; color:#333; line-height:1.6; }}
.hdr {{ background:linear-gradient(135deg,#1a1a2e,#16213e); color:white; padding:25px 40px; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; }}
.hdr h1 {{ font-size:1.5em; font-weight:600; }}
.hdr-meta {{ text-align:right; opacity:0.9; }}
.layout {{ display:flex; min-height:calc(100vh - 100px); }}
.sidebar {{ width:250px; background:white; border-right:1px solid #ddd; padding:15px 0; position:sticky; top:0; height:100vh; overflow-y:auto; }}
.sidebar ul {{ list-style:none; }}
.back-link {{ display:block; padding:10px 20px; color:#0070c9; text-decoration:none; font-weight:600; font-size:0.9em; border-bottom:1px solid #eee; margin-bottom:10px; }}
.back-link:hover {{ background:#f4f6f9; }}
.main-content {{ flex:1; padding:25px; max-width:calc(100% - 250px); overflow-x:hidden; }}
.section {{ background:white; border-radius:10px; margin-bottom:20px; box-shadow:0 2px 8px rgba(0,0,0,0.06); overflow:hidden; }}
.section-header {{ padding:15px 25px; background:#1a1a2e; color:white; cursor:pointer; display:flex; justify-content:space-between; align-items:center; }}
.section-header h3 {{ font-size:1em; font-weight:500; }}
.section-header:hover {{ background:#252547; }}
.section-body {{ padding:15px 25px; display:none; }}
.section.open .section-body {{ display:block; }}
.toggle {{ font-size:1.2em; transition:transform 0.3s; }}
.section.open .toggle {{ transform:rotate(90deg); }}
.issue {{ padding:8px 12px; border-radius:6px; margin:4px 0; font-size:0.85em; }}
.issue-blocker {{ background:#ffeaec; border-left:4px solid #dc3545; }}
.issue-warning {{ background:#fff8e1; border-left:4px solid #ffc107; }}
.summary-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(250px,1fr)); gap:8px; margin:10px 0; }}
.summary-item {{ display:flex; justify-content:space-between; padding:5px 10px; background:#f8f9fa; border-radius:5px; font-size:0.85em; }}
.summary-key {{ font-weight:600; color:#555; }}
table {{ width:100%; border-collapse:collapse; font-size:0.85em; }}
th {{ background:#f0f2f5; padding:8px 10px; text-align:left; font-weight:600; border-bottom:2px solid #ddd; cursor:pointer; white-space:nowrap; }}
th:hover {{ background:#e3e7ed; }}
td {{ padding:6px 10px; border-bottom:1px solid #eee; max-width:350px; overflow:hidden; text-overflow:ellipsis; }}
tr:hover {{ background:#f8f9ff; }}
.filter-input {{ width:100%; padding:8px 12px; border:1px solid #ddd; border-radius:8px; font-size:0.85em; margin-bottom:8px; outline:none; }}
.filter-input:focus {{ border-color:#0070c9; box-shadow:0 0 0 3px rgba(0,112,201,0.15); }}
.table-info {{ font-size:0.75em; color:#999; margin-top:5px; }}
@media (max-width:768px) {{ .layout {{ flex-direction:column; }} .sidebar {{ width:100%; height:auto; position:relative; }} .main-content {{ max-width:100%; }} }}
@media print {{ .sidebar {{ display:none; }} .section-body {{ display:block !important; }} }}
</style>
</head>
<body>
<div class="hdr">
<div>
<h1>&#128274; {_esc(ca.cluster_name)}</h1>
<div style="margin-top:5px;opacity:0.85;">CDM Upgrade Assessment Detail</div>
</div>
<div class="hdr-meta">
<div><strong>Version:</strong> {_esc(ca.cluster_version)}</div>
<div><strong>Target:</strong> {_esc(ma.target_cdm_version)}</div>
<div><strong>Nodes:</strong> {ca.node_count}</div>
<div><strong>Generated:</strong> {ts}</div>
<div style="margin-top:8px;">
<span style="background:{blocker_bg};color:white;padding:4px 12px;border-radius:20px;font-size:0.85em;font-weight:600;">{ca.total_blockers} Blockers</span>
<span style="background:{warning_bg};color:{warning_fg};padding:4px 12px;border-radius:20px;font-size:0.85em;font-weight:600;">{ca.total_warnings} Warnings</span>
</div>
</div>
</div>
<div class="layout">
<nav class="sidebar">
<a href="00_dashboard.html" class="back-link">&larr; Back to Dashboard</a>
<ul>{nav_html}</ul>
</nav>
<main class="main-content">
{sections_html}
</main>
</div>
<script>
document.querySelectorAll('.section-header').forEach(function(header) {{
    header.addEventListener('click', function() {{
        this.parentElement.classList.toggle('open');
    }});
}});
document.querySelectorAll('.filter-input').forEach(function(input) {{
    input.addEventListener('input', function() {{
        var f = this.value.toLowerCase();
        var container = this.nextElementSibling;
        if (!container) return;
        var table = container.querySelector('table') || container;
        var rows = table.querySelectorAll('tbody tr');
        rows.forEach(function(row) {{
            row.style.display = row.textContent.toLowerCase().includes(f) ? '' : 'none';
        }});
    }});
}});
document.querySelectorAll('th').forEach(function(th) {{
    th.addEventListener('click', function() {{
        var table = this.closest('table');
        var tbody = table.querySelector('tbody');
        var rows = Array.from(tbody.querySelectorAll('tr'));
        var idx = Array.from(this.parentElement.children).indexOf(this);
        var asc = this.dataset.sort !== 'asc';
        this.parentElement.querySelectorAll('th').forEach(function(h) {{ delete h.dataset.sort; }});
        this.dataset.sort = asc ? 'asc' : 'desc';
        rows.sort(function(a, b) {{
            var av = (a.children[idx] || {{}}).textContent || '';
            var bv = (b.children[idx] || {{}}).textContent || '';
            var an = parseFloat(av.replace(/[^0-9.-]/g, ''));
            var bn = parseFloat(bv.replace(/[^0-9.-]/g, ''));
            if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
            return asc ? av.localeCompare(bv) : bv.localeCompare(av);
        }});
        rows.forEach(function(row) {{ tbody.appendChild(row); }});
    }});
}});
// Open first section by default
var first = document.querySelector('.section');
if (first) first.classList.add('open');
</script>
</body>
</html>"""


def _build_section_html(result: CollectionResult) -> str:
    """Build HTML for one section within a cluster report."""
    # Issues
    issues_html = ""
    for b in result.blockers:
        issues_html += (
            '<div class="issue issue-blocker">'
            f"<strong>BLOCKER:</strong> {_esc(b)}</div>"
        )
    for w in result.warnings:
        issues_html += (
            '<div class="issue issue-warning">'
            f"<strong>WARNING:</strong> {_esc(w)}</div>"
        )

    # Summary grid
    summary_html = '<div class="summary-grid">'
    for k, v in result.summary.items():
        display_val = v
        if isinstance(v, dict):
            display_val = "<br>".join(
                f"{dk}: {dv}" for dk, dv in v.items()
            )
        elif isinstance(v, list):
            display_val = ", ".join(str(x) for x in v)
        summary_html += (
            '<div class="summary-item">'
            f'<span class="summary-key">'
            f"{_esc(k.replace('_', ' ').title())}</span>"
            f"<span>{_esc(str(display_val))}</span></div>"
        )
    summary_html += "</div>"

    # Data table
    table_html = ""
    if result.details:
        # Collect all columns
        columns = list(
            dict.fromkeys(
                k for row in result.details for k in row.keys()
            )
        )

        # Headers
        headers = "".join(
            f"<th>{_esc(c.replace('_', ' ').title())}</th>"
            for c in columns
        )

        # Rows
        rows_html = ""
        for row in result.details:
            cells = ""
            for col in columns:
                val = row.get(col, "")
                if isinstance(val, (list, dict)):
                    val = str(val)
                cell_val = _esc(str(val))

                # Color-code status fields
                style = ""
                if col in (
                    "status", "connection_status",
                    "severity", "state",
                    "compliance_status", "eos_status",
                ):
                    lval = str(val).lower()
                    if lval in (
                        "connected", "ok", "active",
                        "in_compliance", "true",
                        "eos_status_supported",
                    ):
                        style = (
                            ' style="color:#28a745;'
                            'font-weight:600;"'
                        )
                    elif lval in (
                        "disconnected", "failed",
                        "degraded", "blocker", "false",
                        "eos_status_not_supported",
                    ):
                        style = (
                            ' style="color:#dc3545;'
                            'font-weight:600;"'
                        )
                    elif lval in (
                        "warning", "unknown",
                        "eos_status_plan_upgrade",
                    ):
                        style = (
                            ' style="color:#e67e00;'
                            'font-weight:600;"'
                        )

                cells += f"<td{style}>{cell_val}</td>"
            rows_html += f"<tr>{cells}</tr>"

        table_html = (
            '<input type="text" class="filter-input" '
            'placeholder="&#128269; Type to filter rows...">'
            '<div style="overflow-x:auto;">'
            f"<table><thead><tr>{headers}</tr></thead>"
            f"<tbody>{rows_html}</tbody></table></div>"
            f'<div class="table-info">'
            f"{len(result.details)} rows - "
            f"Click column headers to sort</div>"
        )

    # Badges
    blocker_badge = ""
    if result.blockers:
        blocker_badge = (
            '<span style="background:#dc3545;color:white;'
            "padding:2px 8px;border-radius:10px;"
            'font-size:0.8em;margin-left:10px;">'
            f"{len(result.blockers)} blockers</span>"
        )
    warning_badge = ""
    if result.warnings:
        warning_badge = (
            '<span style="background:#ffc107;color:#333;'
            "padding:2px 8px;border-radius:10px;"
            'font-size:0.8em;margin-left:5px;">'
            f"{len(result.warnings)} warnings</span>"
        )

    return (
        f'<div class="section" id="{result.section_id}">'
        '<div class="section-header">'
        f"<h3>{_esc(result.section_name)}"
        f"{blocker_badge}{warning_badge}</h3>"
        '<span class="toggle">&#9654;</span></div>'
        f'<div class="section-body">'
        f"{issues_html}{summary_html}{table_html}"
        "</div></div>"
    )


def _safe_filename(name: str) -> str:
    """Convert cluster name to safe filename."""
    return (
        name.replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
    )


def _esc(s) -> str:
    """HTML escape."""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )