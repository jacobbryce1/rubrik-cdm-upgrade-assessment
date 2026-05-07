"""
CSV Reporter - Multi-cluster aware.
Generates:
  - Cross-cluster summary CSV
  - Master issues CSV across all clusters
  - Skipped clusters CSV
  - Per-cluster detail CSVs in subdirectories
"""
import csv
import logging
from pathlib import Path
from typing import List
from collectors import MultiClusterAssessment

logger = logging.getLogger(__name__)


def generate_multi(
    assessment: MultiClusterAssessment, output_dir: str
) -> List[str]:
    """Generate CSV files for multi-cluster assessment."""
    csv_dir = Path(output_dir) / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    generated = []

    # =================================================================
    # 1. CROSS-CLUSTER SUMMARY
    # =================================================================
    summary_file = csv_dir / "00_cross_cluster_summary.csv"
    with open(
        summary_file, "w", newline="", encoding="utf-8"
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "cluster_name", "cluster_id", "version",
                "cluster_type", "node_count", "status",
                "blockers", "warnings", "time_sec", "error",
            ],
        )
        writer.writeheader()
        for cid, ca in assessment.cluster_assessments.items():
            writer.writerow({
                "cluster_name": ca.cluster_name,
                "cluster_id": ca.cluster_id,
                "version": ca.cluster_version,
                "cluster_type": ca.cluster_type,
                "node_count": ca.node_count,
                "status": ca.cluster_status,
                "blockers": ca.total_blockers,
                "warnings": ca.total_warnings,
                "time_sec": round(
                    ca.assessment_duration_sec, 1
                ),
                "error": ca.assessment_error,
            })
    generated.append(str(summary_file))

    # =================================================================
    # 2. ALL ISSUES ACROSS ALL CLUSTERS
    # =================================================================
    issues_file = csv_dir / "00_all_issues.csv"
    with open(
        issues_file, "w", newline="", encoding="utf-8"
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "cluster_name", "cluster_id", "severity",
                "section", "message",
            ],
        )
        writer.writeheader()
        for b in assessment.global_blockers:
            writer.writerow({
                "cluster_name": b["cluster"],
                "cluster_id": b["cluster_id"],
                "severity": "BLOCKER",
                "section": b["section"],
                "message": b["message"],
            })
        for w in assessment.global_warnings:
            writer.writerow({
                "cluster_name": w["cluster"],
                "cluster_id": w["cluster_id"],
                "severity": "WARNING",
                "section": w["section"],
                "message": w["message"],
            })
    generated.append(str(issues_file))

    # =================================================================
    # 3. SKIPPED CLUSTERS
    # =================================================================
    if assessment.skipped_clusters:
        skipped_file = csv_dir / "00_skipped_clusters.csv"
        with open(
            skipped_file, "w", newline="", encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "name", "version", "status", "skip_reason",
                ],
            )
            writer.writeheader()
            for sc in assessment.skipped_clusters:
                writer.writerow(sc)
        generated.append(str(skipped_file))

    # =================================================================
    # 4. PER-CLUSTER DETAIL CSVs
    # =================================================================
    for cid, ca in assessment.cluster_assessments.items():
        safe_name = _safe_filename(ca.cluster_name)
        cluster_dir = csv_dir / f"cluster_{safe_name}"
        cluster_dir.mkdir(parents=True, exist_ok=True)

        # Per-section CSVs
        for section_key, result in ca.results.items():
            if not result.details:
                continue

            filepath = cluster_dir / f"{result.section_id}.csv"
            fieldnames = _get_all_keys(result.details)

            with open(
                filepath, "w", newline="", encoding="utf-8"
            ) as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=fieldnames,
                    extrasaction="ignore",
                )
                writer.writeheader()
                for row in result.details:
                    flat_row = {
                        k: _flatten_value(v)
                        for k, v in row.items()
                    }
                    writer.writerow(flat_row)

            generated.append(str(filepath))

        # Per-cluster issues CSV
        cluster_issues = cluster_dir / "00_issues.csv"
        with open(
            cluster_issues, "w", newline="", encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["severity", "section", "message"],
            )
            writer.writeheader()
            for result in ca.results.values():
                for b in result.blockers:
                    writer.writerow({
                        "severity": "BLOCKER",
                        "section": result.section_name,
                        "message": b,
                    })
                for w in result.warnings:
                    writer.writerow({
                        "severity": "WARNING",
                        "section": result.section_name,
                        "message": w,
                    })
        generated.append(str(cluster_issues))

        # Per-cluster summary CSV
        cluster_summary = cluster_dir / "00_summary.csv"
        with open(
            cluster_summary, "w", newline="", encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["section", "metric", "value"],
            )
            writer.writeheader()
            for result in ca.results.values():
                for metric, value in result.summary.items():
                    writer.writerow({
                        "section": result.section_name,
                        "metric": metric,
                        "value": _flatten_value(value),
                    })
        generated.append(str(cluster_summary))

    logger.info(f"  CSV: {len(generated)} files generated")
    return generated


def _safe_filename(name: str) -> str:
    """Convert cluster name to safe filename."""
    return (
        name.replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
    )


def _get_all_keys(rows: list) -> list:
    """Collect all unique keys preserving insertion order."""
    seen = {}
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen[key] = True
    return list(seen.keys())


def _flatten_value(value) -> str:
    """Convert lists/dicts to string for CSV."""
    if isinstance(value, (list, tuple)):
        return "; ".join(str(v) for v in value)
    if isinstance(value, dict):
        return "; ".join(
            f"{k}={v}" for k, v in value.items()
        )
    return str(value)