#!/usr/bin/env python3
"""
Rubrik CDM Pre-Upgrade Compatibility Assessment — Orchestrator

Security remediations applied:
  F-07  Output directories created with mode 0o700; output files set to 0o600
        after writing, preventing world-readable infrastructure data.
  F-10  Failed collectors are tracked per-cluster.  ClusterAssessment gains a
        PARTIAL status surfaced in all output formats with a visible banner.
  F-17  All API-sourced values in generate_html_report() now pass through _esc()
        without exception — no raw string concatenation with untrusted data.
  F-19  A SHA-256 manifest (manifest.sha256) is written after every run covering
        all output files, providing tamper-evidence for audit use cases.
"""

import os
import sys
import csv
import json
import time
import html as html_mod
import hashlib
import logging
import threading
import traceback
import concurrent.futures
from datetime import datetime
from pathlib import Path

from config import Config, setup_logging
from rsc_client import RSCClient
from cluster_discovery import (
    DiscoveredCluster,
    discover_all_clusters,
    enrich_cluster,
    filter_clusters,
)
from models import (
    ClusterAssessment,
    MultiClusterAssessment,
    StreamingMultiAssessment,
    create_multi_assessment,
)

logger = logging.getLogger("main")


# ──────────────────────────────────────────────────────────────
# F-07: secure_mkdir / secure_write helpers
# ──────────────────────────────────────────────────────────────
def _secure_mkdir(path: str) -> None:
    """Create directory with owner-only permissions (0o700)."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(p, 0o700)
    except OSError:
        pass  # Windows or read-only FS — best-effort


def _secure_write(path: str, content: str, mode: str = "w") -> None:
    """Write a file then restrict permissions to owner-read/write (0o600)."""
    with open(path, mode, encoding="utf-8") as fh:
        fh.write(content)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def _secure_write_binary(path: str, content: bytes) -> None:
    """Write binary file then restrict permissions to 0o600."""
    with open(path, "wb") as fh:
        fh.write(content)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


# ──────────────────────────────────────────────────────────────
# F-19: SHA-256 manifest
# ──────────────────────────────────────────────────────────────
def _write_sha256_manifest(file_paths: list, output_dir: str) -> str:
    """
    Write a SHA-256 manifest covering all output files.
    Returns the path to the manifest file.
    """
    manifest_path = os.path.join(output_dir, "manifest.sha256")
    lines = []
    for fpath in sorted(file_paths):
        if not os.path.isfile(fpath):
            continue
        h = hashlib.sha256()
        with open(fpath, "rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        rel = os.path.relpath(fpath, output_dir)
        lines.append(f"{h.hexdigest()}  {rel}")
    manifest_content = "\n".join(lines) + "\n"
    _secure_write(manifest_path, manifest_content)
    logger.info(" [MANIFEST] SHA-256 manifest written: %s", manifest_path)
    return manifest_path


# ==============================================================
# Progress Tracker
# ==============================================================

class ProgressTracker:
    """Thread-safe progress tracker with ETA."""

    def __init__(self, total: int, label: str = "items") -> None:
        self.total = total
        self.label = label
        self.completed = 0
        self.failed = 0
        self.lock = threading.Lock()
        self.start_time = time.time()

    @property
    def summary(self) -> dict:
        with self.lock:
            return {
                "total": self.total,
                "completed": self.completed,
                "failed": self.failed,
                "elapsed": time.time() - self.start_time,
            }

    def complete(self, name: str = "") -> None:
        with self.lock:
            self.completed += 1
            self._log_progress(name, "completed")

    def fail(self, name: str = "") -> None:
        with self.lock:
            self.failed += 1
            self.completed += 1
            self._log_progress(name, "FAILED")

    def _log_progress(self, name: str, status: str) -> None:
        elapsed = time.time() - self.start_time
        done = self.completed
        remaining = self.total - done
        eta_str = ""
        if done > 0 and remaining > 0:
            eta_sec = (elapsed / done) * remaining
            eta_str = f" | ETA: {eta_sec:.0f}s"
        logger.info(
            " Progress: %d/%d %s (%s: %s)%s",
            done, self.total, self.label, status, name, eta_str,
        )


# ==============================================================
# F-10: Single Cluster Assessment with PARTIAL tracking
# ==============================================================

def assess_single_cluster(
    client: RSCClient,
    cluster: DiscoveredCluster,
    target_version: str,
) -> ClusterAssessment:
    """
    Assess one cluster for upgrade readiness.

    F-10: Each collector failure is recorded in ca.failed_collectors.
          If any collector fails the assessment status becomes PARTIAL
          and a WARNING finding is added so users cannot mistake a
          partial result for a clean one.
    """
    start = time.time()
    ca = ClusterAssessment(
        cluster_name=cluster.name,
        cluster_id=cluster.cluster_id,
        version=cluster.version,
        target_version=target_version,
        cluster_type=cluster.cluster_type,
        node_count=cluster.node_count,
        location=cluster.location,
        connected_state=cluster.connected_state,
        assessment_start=datetime.utcnow().isoformat(),
    )

    client.set_target_cluster(
        cluster.cluster_id,
        node_ips=cluster.node_ips,
        name=cluster.name,
        version=cluster.version,
    )
    ca.cdm_api_available = client.is_cdm_available(cluster.cluster_id)

    # ── RSC-based collectors (always run) ──
    _run_collector(ca, cluster, "upgrade_prechecks", lambda: (
        __import__("collectors.upgrade_prechecks", fromlist=["collect_upgrade_prechecks"])
        .collect_upgrade_prechecks(client, cluster, target_version)
    ))
    _run_collector(ca, cluster, "workload_inventory", lambda: (
        __import__("collectors.workload_inventory", fromlist=["collect_workload_inventory"])
        .collect_workload_inventory(client, cluster)
    ))
    _run_collector(ca, cluster, "sla_compliance", lambda: (
        __import__("collectors.sla_compliance", fromlist=["collect_sla_compliance"])
        .collect_sla_compliance(client, cluster)
    ))
    _run_collector(ca, cluster, "host_inventory", lambda: (
        __import__("collectors.host_inventory", fromlist=["collect_host_inventory"])
        .collect_host_inventory(client, cluster)
    ))
    _run_collector(ca, cluster, "compatibility_validator", lambda: (
        __import__("collectors.compatibility_validator",
                   fromlist=["collect_compatibility_validation"])
        .collect_compatibility_validation(client, cluster, target_version)
    ))

    # ── CDM direct collectors ──
    if ca.cdm_api_available:
        for collector_name, module_name, func_name in [
            ("cdm_system_status",      "collectors.cdm_system_status",      "collect_system_status"),
            ("cdm_live_mounts",        "collectors.cdm_live_mounts",        "collect_live_mounts"),
            ("cdm_archive_replication","collectors.cdm_archive_replication","collect_archive_replication"),
            ("cdm_network_config",     "collectors.cdm_network_config",     "collect_network_config"),
            ("cdm_workloads",          "collectors.cdm_workloads",          "collect_cdm_workloads"),
        ]:
            _run_collector(ca, cluster, collector_name, lambda mn=module_name, fn=func_name: (
                getattr(__import__(mn, fromlist=[fn]), fn)(client, cluster)
            ))
    else:
        ca.add_issue(
            severity="INFO",
            category="CONNECTIVITY",
            check="cdm_direct_api",
            message=(
                "CDM direct API not available for "
                + cluster.name
                + ". CDM-direct checks skipped."
            ),
            detail=(
                "Node IPs: " + str(cluster.node_ips)
                + ", CDM direct enabled: " + str(Config.CDM_DIRECT_ENABLED)
                + ". Ensure network connectivity to cluster node IPs for full assessment."
            ),
        )

    # F-10: If any collectors failed, mark PARTIAL and add a visible WARNING
    if ca.failed_collectors:
        ca.status = "PARTIAL"
        ca.add_issue(
            severity="WARNING",
            category="ASSESSMENT_INTEGRITY",
            check="collector_failures",
            message=(
                f"Assessment is PARTIAL — {len(ca.failed_collectors)} collector(s) "
                f"failed: {', '.join(ca.failed_collectors)}. "
                "Results may be incomplete. Do not treat this as a clean report."
            ),
            detail=(
                "Review the log file for full error details. "
                "Re-run after resolving connectivity or permission issues."
            ),
        )
    else:
        ca.status = "COMPLETED"

    ca.assessment_duration_sec = time.time() - start
    ca.assessment_end = datetime.utcnow().isoformat()
    return ca


def _run_collector(
    ca: ClusterAssessment,
    cluster: DiscoveredCluster,
    name: str,
    fn,
) -> None:
    """Run a single collector, recording failures in ca.failed_collectors."""
    try:
        result = fn()
        ca.add_collection_result(result)
        ca.checks_performed.append(name)
    except Exception as e:
        logger.error(" [%s] %s failed: %s", cluster.name, name, e)
        # F-10: record which collectors failed
        if not hasattr(ca, "failed_collectors"):
            ca.failed_collectors = []
        ca.failed_collectors.append(name)


# ==============================================================
# Report Generation
# ==============================================================

def generate_reports(ma: MultiClusterAssessment, output_dir: str) -> list:
    """Generate all configured report formats and write a SHA-256 manifest."""
    generated_files: list = []

    # ── JSON ──
    if "json" in Config.REPORT_FORMATS:
        json_file = os.path.join(output_dir, "assessment_report.json")
        try:
            content = json.dumps(ma.to_dict(), indent=2, default=str)
            _secure_write(json_file, content)          # F-07
            generated_files.append(json_file)
            logger.info(" [JSON] report: %s", json_file)
        except Exception as e:
            logger.error(" [JSON] failed: %s", e)

    # ── CSV: Issues ──
    if "csv" in Config.REPORT_FORMATS:
        issues_csv = os.path.join(output_dir, "all_issues.csv")
        try:
            rows = []
            data = ma.to_dict()
            for a in data.get("assessments", []):
                cname = a.get("cluster_name", "")
                for finding in a.get("findings", []):
                    rows.append([
                        cname,
                        finding.get("severity", "INFO"),
                        finding.get("section", ""),
                        finding.get("message", ""),
                        finding.get("recommendation", ""),
                    ])
            import io
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(["cluster_name", "severity", "section", "message", "recommendation"])
            writer.writerows(rows)
            _secure_write(issues_csv, buf.getvalue())  # F-07
            generated_files.append(issues_csv)
            logger.info(" [CSV] issues: %s", issues_csv)
        except Exception as e:
            logger.error(" [CSV] issues failed: %s", e)

    # ── CSV: Summary ──
    summary_csv = os.path.join(output_dir, "cluster_summary.csv")
    try:
        import io
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "cluster_name", "cluster_id", "version", "target_version",
            "node_count", "location", "blockers", "warnings", "info",
            "cdm_api_available", "duration_sec", "status",
        ])
        data = ma.to_dict()
        for a in data.get("assessments", []):
            writer.writerow([
                a.get("cluster_name", ""),
                a.get("cluster_id", ""),
                a.get("version", ""),
                a.get("target_version", ""),
                a.get("node_count", 0),
                a.get("location", ""),
                a.get("blockers", 0),
                a.get("warnings", 0),
                a.get("info", 0),
                a.get("cdm_api_available", False),
                a.get("duration_sec", 0),
                a.get("status", "UNKNOWN"),       # F-10: include status
            ])
        _secure_write(summary_csv, buf.getvalue())     # F-07
        generated_files.append(summary_csv)
        logger.info(" [CSV] summary: %s", summary_csv)
    except Exception as e:
        logger.error(" [CSV] summary failed: %s", e)

    # ── HTML ──
    if "html" in Config.REPORT_FORMATS:
        try:
            html_file = generate_html_report(ma, output_dir)
            generated_files.append(html_file)
            logger.info(" [HTML] report: %s", html_file)
        except Exception as e:
            logger.error(" [HTML] failed: %s", e)
            logger.debug(traceback.format_exc())

    # F-19: Write SHA-256 manifest covering all output files
    if generated_files:
        manifest = _write_sha256_manifest(generated_files, output_dir)
        generated_files.append(manifest)

    return generated_files


# ==============================================================
# HTML Report
# F-17: Every API-sourced value passes through _esc().
#        No raw string concatenation with untrusted data.
# F-10: PARTIAL status banner rendered prominently.
# ==============================================================

def generate_html_report(ma: MultiClusterAssessment, output_dir: str) -> str:
    """Generate a comprehensive HTML dashboard."""

    def _esc(val) -> str:
        """HTML-escape every value — never concatenate raw API data."""
        return html_mod.escape(str(val)) if val is not None else "—"

    def _severity_class(sev: str) -> str:
        s = str(sev).upper()
        if s in ("BLOCKER", "CRITICAL", "ERROR"):
            return "blocker"
        if s in ("WARNING", "WARN"):
            return "warning"
        return "info"

    def _severity_badge(sev: str) -> str:
        s = str(sev).upper()
        if s in ("BLOCKER", "CRITICAL", "ERROR"):
            return '<span class="badge badge-blocker">BLOCKER</span>'
        if s in ("WARNING", "WARN"):
            return '<span class="badge badge-warning">WARNING</span>'
        return '<span class="badge badge-info">INFO</span>'

    html_file = os.path.join(output_dir, "assessment_report.html")
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    data = ma.to_dict()

    total_assessed  = data.get("total_assessed", 0)
    total_failed    = data.get("total_failed", 0)
    total_skipped   = data.get("total_skipped", 0)
    total_blockers  = data.get("total_blockers", 0)
    total_warnings  = data.get("total_warnings", 0)
    total_info      = data.get("total_info", 0)
    target_ver      = data.get("target_version", "?")

    # F-10: count PARTIAL assessments for banner
    assessments = data.get("assessments", [])
    partial_count = sum(
        1 for a in assessments if a.get("status", "") == "PARTIAL"
    )

    # Enrich streaming-mode summaries with per-cluster JSON if needed
    cluster_dir = os.path.join(output_dir, "clusters")
    for a in assessments:
        if not a.get("issues"):
            safe_name = "".join(
                c if c.isalnum() or c in "-_" else "_"
                for c in a.get("cluster_name", "")
            )
            detail_path = os.path.join(cluster_dir, safe_name + ".json")
            if os.path.exists(detail_path):
                try:
                    with open(detail_path, "r", encoding="utf-8") as f:
                        detail = json.load(f)
                    a["issues"] = detail.get("issues", [])
                except Exception:
                    a["issues"] = []
            else:
                a["issues"] = []

    if hasattr(ma, "assessments"):
        for idx, raw_a in enumerate(ma.assessments):
            if hasattr(raw_a, "issues") and raw_a.issues:
                if idx < len(assessments) and not assessments[idx].get("issues"):
                    assessments[idx]["issues"] = [
                        i.to_dict() for i in raw_a.issues
                    ]

    # Build cluster cards
    cluster_cards = ""
    all_issues_rows = ""

    for a in assessments:
        # F-17: every value through _esc()
        cname    = _esc(a.get("cluster_name", "Unknown"))
        cver     = _esc(a.get("version", "?"))
        ctarget  = _esc(a.get("target_version", target_ver))
        nodes    = _esc(a.get("node_count", "?"))
        location = _esc(a.get("location", "—"))
        platform = _esc(a.get("cluster_type", "—"))
        b        = int(a.get("blockers",  a.get("total_blockers",  0)) or 0)
        w        = int(a.get("warnings",  a.get("total_warnings",  0)) or 0)
        i        = int(a.get("info",      a.get("total_info",      0)) or 0)
        d        = round(float(a.get("duration_sec", a.get("assessment_duration_sec", 0)) or 0), 1)
        checks   = a.get("checks_performed", [])
        issues   = a.get("issues", [])
        cdm_api  = bool(a.get("cdm_api_available", False))
        status   = a.get("status", "COMPLETED")

        if b > 0:
            card_border = "#dc3545"
            card_status = "BLOCKERS FOUND"
            card_status_class = "status-blocker"
        elif status == "PARTIAL":
            card_border = "#e67e22"
            card_status = "PARTIAL ASSESSMENT"
            card_status_class = "status-warning"
        elif w > 0:
            card_border = "#ffc107"
            card_status = "WARNINGS"
            card_status_class = "status-warning"
        else:
            card_border = "#28a745"
            card_status = "READY"
            card_status_class = "status-ok"

        open_attr = " open" if (b > 0 or w > 0 or status == "PARTIAL") else ""

        cluster_cards += (
            f'<div class="cluster-card" style="border-left:5px solid {_esc(card_border)};">\n'
            f'  <div class="cluster-card-header">\n'
            f'    <div>\n'
            f'      <h3 class="cluster-name">{cname}</h3>\n'
            f'      <span class="cluster-meta">'
            f'{cver} &#8594; {ctarget} &nbsp;|&nbsp; '
            f'{nodes} nodes &nbsp;|&nbsp; {location} &nbsp;|&nbsp; {platform}'
            f'</span>\n'
            f'    </div>\n'
            f'    <span class="cluster-status {_esc(card_status_class)}">'
            f'{_esc(card_status)}</span>\n'
            f'  </div>\n'
            f'  <div class="cluster-card-metrics">\n'
            f'    <div class="metric"><span class="metric-val blocker-text">{b}</span>'
            f'<span class="metric-label">Blockers</span></div>\n'
            f'    <div class="metric"><span class="metric-val warning-text">{w}</span>'
            f'<span class="metric-label">Warnings</span></div>\n'
            f'    <div class="metric"><span class="metric-val info-text">{i}</span>'
            f'<span class="metric-label">Info</span></div>\n'
            f'    <div class="metric"><span class="metric-val">{d}s</span>'
            f'<span class="metric-label">Duration</span></div>\n'
            f'    <div class="metric"><span class="metric-val">{len(checks)}</span>'
            f'<span class="metric-label">Checks</span></div>\n'
            f'    <div class="metric"><span class="metric-val">'
            f'{"&#9989;" if cdm_api else "&#10060;"}</span>'
            f'<span class="metric-label">CDM API</span></div>\n'
            f'    <div class="metric"><span class="metric-val">'
            f'{_esc(status)}</span>'
            f'<span class="metric-label">Status</span></div>\n'
            f'  </div>\n'
        )

        bw_issues = [
            f for f in issues
            if str(f.get("severity", "")).upper()
            in ("BLOCKER", "CRITICAL", "ERROR", "WARNING", "WARN")
        ]
        info_issues = [
            f for f in issues
            if str(f.get("severity", "")).upper()
            not in ("BLOCKER", "CRITICAL", "ERROR", "WARNING", "WARN")
        ]

        if issues:
            if bw_issues:
                cluster_cards += (
                    f'  <details class="findings-detail"{open_attr}>\n'
                    f'    <summary class="issues-summary">&#9888; '
                    f'{len(bw_issues)} Blocker/Warning finding(s)</summary>\n'
                    f'    <table class="findings-table">\n'
                    f'      <tr><th>Severity</th><th>Category</th>'
                    f'<th>Check</th><th>Message</th><th>Detail</th></tr>\n'
                )
                for f in bw_issues:
                    sev  = f.get("severity", "INFO")
                    cat  = _esc(f.get("category", "—"))
                    chk  = _esc(f.get("check", "—"))
                    msg  = _esc(f.get("message", "—"))
                    det  = _esc(f.get("detail", "—"))
                    rc   = _severity_class(sev)
                    cluster_cards += (
                        f'      <tr class="{rc}">'
                        f'<td>{_severity_badge(sev)}</td>'
                        f'<td>{cat}</td><td>{chk}</td>'
                        f'<td>{msg}</td><td>{det}</td></tr>\n'
                    )
                    all_issues_rows += (
                        f'<tr class="{rc}">'
                        f'<td>{_severity_badge(sev)}</td>'
                        f'<td>{cname}</td><td>{cat}</td>'
                        f'<td>{msg}</td><td>{det}</td></tr>\n'
                    )
                cluster_cards += "    </table>\n  </details>\n"

            if info_issues:
                cluster_cards += (
                    f'  <details class="findings-detail">\n'
                    f'    <summary>{len(info_issues)} informational finding(s)</summary>\n'
                    f'    <table class="findings-table">\n'
                    f'      <tr><th>Severity</th><th>Category</th>'
                    f'<th>Check</th><th>Message</th><th>Detail</th></tr>\n'
                )
                for f in info_issues:
                    sev = f.get("severity", "INFO")
                    cat = _esc(f.get("category", "—"))
                    chk = _esc(f.get("check", "—"))
                    msg = _esc(f.get("message", "—"))
                    det = _esc(f.get("detail", "—"))
                    cluster_cards += (
                        f'      <tr class="info">'
                        f'<td>{_severity_badge(sev)}</td>'
                        f'<td>{cat}</td><td>{chk}</td>'
                        f'<td>{msg}</td><td>{det}</td></tr>\n'
                    )
                cluster_cards += "    </table>\n  </details>\n"
        else:
            cluster_cards += (
                '  <p class="no-findings">&#9989; No findings — '
                'cluster appears upgrade-ready.</p>\n'
            )

        cluster_cards += "</div>\n"

    # F-10: PARTIAL banner
    partial_banner = ""
    if partial_count > 0:
        partial_banner = (
            f'<div class="partial-banner">'
            f'&#9888; <strong>{partial_count} cluster(s) have PARTIAL assessments</strong> '
            f'— one or more data collectors failed. These results may be incomplete. '
            f'Check logs and re-run before making upgrade decisions.'
            f'</div>\n'
        )

    # Cross-cluster issues section
    if all_issues_rows:
        issues_section = (
            f'<h2>&#9888; All Blockers &amp; Warnings Across All Clusters '
            f'({total_blockers + total_warnings})</h2>\n'
            f'<p style="color:#666;font-size:13px;">Must be reviewed before proceeding.</p>\n'
            f'<table class="data-table">\n'
            f'<tr><th>Severity</th><th>Cluster</th><th>Category</th>'
            f'<th>Message</th><th>Detail</th></tr>\n'
            f'{all_issues_rows}</table>\n'
        )
    else:
        issues_section = (
            '<h2>&#9888; Cross-Cluster Issues</h2>\n'
            '<p class="no-findings">&#9989; No blockers or warnings found.</p>\n'
        )

    # Assemble HTML
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Rubrik CDM Pre-Upgrade Assessment</title>
<style>
* {{ box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
       margin: 0; padding: 20px; background: #f5f7fa; color: #333; }}
h1 {{ color: #003366; border-bottom: 3px solid #00b4e0; padding-bottom: 10px; }}
h2 {{ color: #2c3e50; margin-top: 30px; }}
.summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px,1fr));
                  gap: 12px; margin: 20px 0; }}
.summary-card {{ background: white; border-radius: 8px; padding: 16px;
                  text-align: center; box-shadow: 0 1px 4px rgba(0,0,0,.1); }}
.summary-val {{ font-size: 2em; font-weight: bold; }}
.blocker-text {{ color: #dc3545; }}
.warning-text {{ color: #e67e22; }}
.info-text {{ color: #2980b9; }}
.cluster-card {{ background: white; border-radius: 8px; padding: 16px;
                  margin: 12px 0; box-shadow: 0 1px 4px rgba(0,0,0,.1); }}
.cluster-card-header {{ display: flex; justify-content: space-between; align-items: flex-start; }}
.cluster-name {{ margin: 0; font-size: 1.1em; color: #003366; }}
.cluster-meta {{ font-size: .85em; color: #666; }}
.cluster-status {{ font-weight: bold; font-size: .85em; padding: 4px 10px;
                    border-radius: 4px; white-space: nowrap; }}
.status-blocker {{ background: #dc3545; color: white; }}
.status-warning {{ background: #ffc107; color: #333; }}
.status-ok {{ background: #28a745; color: white; }}
.cluster-card-metrics {{ display: flex; gap: 20px; margin: 12px 0; flex-wrap: wrap; }}
.metric {{ text-align: center; }}
.metric-val {{ font-size: 1.4em; font-weight: bold; display: block; }}
.metric-label {{ font-size: .75em; color: #888; }}
.findings-detail {{ margin-top: 10px; }}
.findings-detail summary {{ cursor: pointer; font-weight: bold; padding: 6px;
                             background: #f8f9fa; border-radius: 4px; }}
.issues-summary {{ color: #c0392b; }}
.findings-table {{ width: 100%; border-collapse: collapse; margin-top: 8px; font-size: .9em; }}
.findings-table th {{ background: #2c3e50; color: white; padding: 8px; text-align: left; }}
.findings-table td {{ padding: 6px 8px; border-bottom: 1px solid #eee; vertical-align: top; }}
.findings-table tr.blocker {{ background: #fff5f5; }}
.findings-table tr.warning {{ background: #fffbf0; }}
.findings-table tr.info {{ background: #f0f8ff; }}
.badge {{ padding: 2px 8px; border-radius: 3px; font-size: .8em; font-weight: bold; }}
.badge-blocker {{ background: #dc3545; color: white; }}
.badge-warning {{ background: #ffc107; color: #333; }}
.badge-info {{ background: #17a2b8; color: white; }}
.data-table {{ width: 100%; border-collapse: collapse; margin: 10px 0; font-size: .9em; }}
.data-table th {{ background: #2c3e50; color: white; padding: 8px; }}
.data-table td {{ padding: 6px 8px; border-bottom: 1px solid #eee; vertical-align: top; }}
.no-findings {{ color: #28a745; font-style: italic; }}
.partial-banner {{ background: #fff3cd; border: 2px solid #e67e22; border-radius: 6px;
                    padding: 12px 16px; margin: 16px 0; color: #7d4e00; font-size: 1em; }}
.integrity-notice {{ background: #e8f4f8; border-left: 4px solid #2980b9;
                      padding: 8px 12px; margin: 12px 0; font-size: .85em; color: #555; }}
footer {{ margin-top: 40px; padding-top: 16px; border-top: 1px solid #ddd;
           font-size: .8em; color: #999; text-align: center; }}
</style>
</head>
<body>
<h1>&#128737; Rubrik CDM Pre-Upgrade Assessment</h1>
<p>Generated: {_esc(timestamp)} &nbsp;|&nbsp; Target Version: {_esc(target_ver)}</p>

{partial_banner}

<div class="integrity-notice">
&#128274; Output integrity: a SHA-256 manifest (<code>manifest.sha256</code>) has been
written to the output directory. Verify it before acting on these results.
</div>

<div class="summary-grid">
  <div class="summary-card">
    <div class="summary-val">{total_assessed}</div><div>Assessed</div>
  </div>
  <div class="summary-card">
    <div class="summary-val blocker-text">{total_blockers}</div><div>Blockers</div>
  </div>
  <div class="summary-card">
    <div class="summary-val warning-text">{total_warnings}</div><div>Warnings</div>
  </div>
  <div class="summary-card">
    <div class="summary-val info-text">{total_info}</div><div>Info</div>
  </div>
  <div class="summary-card">
    <div class="summary-val" style="color:#e67e22">{partial_count}</div><div>Partial</div>
  </div>
  <div class="summary-card">
    <div class="summary-val" style="color:#c0392b">{total_failed}</div><div>Failed</div>
  </div>
  <div class="summary-card">
    <div class="summary-val" style="color:#888">{total_skipped}</div><div>Skipped</div>
  </div>
</div>

{issues_section}

<h2>&#128202; Cluster Details</h2>
{cluster_cards}

<footer>
  Rubrik CDM Pre-Upgrade Assessment &mdash; {_esc(timestamp)}<br>
  This report may contain sensitive infrastructure data. Keep it confidential.
</footer>
</body>
</html>"""

    # F-07: write with restricted permissions
    _secure_write(html_file, html)
    return html_file


# ==============================================================
# Final Summary
# ==============================================================

def print_final_summary(
    ma: MultiClusterAssessment,
    files: list,
    output_dir: str,
    progress=None,
    api_stats=None,
) -> None:
    data = ma.to_dict()
    logger.info("")
    logger.info("=" * 70)
    logger.info(" ASSESSMENT COMPLETE")
    logger.info("=" * 70)
    logger.info(" Target CDM Version : %s", data.get("target_version", "?"))
    logger.info(" Clusters Assessed  : %d", data.get("total_assessed", 0))
    logger.info(" Clusters Skipped   : %d", data.get("total_skipped", 0))
    logger.info(" Clusters Failed    : %d", data.get("total_failed", 0))
    logger.info(" Total Blockers     : %d", data.get("total_blockers", 0))
    logger.info(" Total Warnings     : %d", data.get("total_warnings", 0))
    logger.info(" Total Info         : %d", data.get("total_info", 0))
    logger.info("")
    logger.info(" Output directory   : %s", output_dir)
    for f in files:
        logger.info("   - %s", os.path.basename(f))
    logger.info("=" * 70)
