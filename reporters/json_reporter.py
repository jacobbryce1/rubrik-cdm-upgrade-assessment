"""
JSON Reporter - Multi-cluster aware.
Generates:
  - Master summary JSON (no raw data, suitable for dashboards)
  - Per-cluster full JSON files (with raw data for deep analysis)
"""
import json
import logging
from pathlib import Path
from typing import List
from collectors import MultiClusterAssessment

logger = logging.getLogger(__name__)


def generate_multi(
    assessment: MultiClusterAssessment, output_dir: str
) -> List[str]:
    """Generate JSON files for multi-cluster assessment."""
    json_dir = Path(output_dir) / "json"
    json_dir.mkdir(parents=True, exist_ok=True)
    generated = []

    # =================================================================
    # 1. MASTER SUMMARY (cross-cluster, no raw data)
    # =================================================================
    master = {
        "meta": {
            "target_cdm_version": assessment.target_cdm_version,
            "rsc_base_url": assessment.rsc_base_url,
            "clusters_discovered": (
                assessment.total_clusters_discovered
            ),
            "clusters_assessed": (
                assessment.total_clusters_assessed
            ),
            "clusters_skipped": (
                assessment.total_clusters_skipped
            ),
            "total_blockers": len(assessment.global_blockers),
            "total_warnings": len(assessment.global_warnings),
        },
        "global_blockers": assessment.global_blockers,
        "global_warnings": assessment.global_warnings,
        "skipped_clusters": assessment.skipped_clusters,
        "cluster_summaries": {},
    }

    for cid, ca in assessment.cluster_assessments.items():
        master["cluster_summaries"][cid] = {
            "name": ca.cluster_name,
            "version": ca.cluster_version,
            "type": ca.cluster_type,
            "status": ca.cluster_status,
            "nodes": ca.node_count,
            "blockers": ca.total_blockers,
            "warnings": ca.total_warnings,
            "duration_sec": round(
                ca.assessment_duration_sec, 1
            ),
            "error": ca.assessment_error,
            "sections": {
                k: {
                    "section_name": r.section_name,
                    "summary": r.summary,
                    "blockers": r.blockers,
                    "warnings": r.warnings,
                    "info_messages": r.info_messages,
                    "detail_count": len(r.details),
                }
                for k, r in ca.results.items()
            },
        }

    master_file = json_dir / "00_master_assessment.json"
    with open(master_file, "w", encoding="utf-8") as f:
        json.dump(master, f, indent=2, default=str)
    generated.append(str(master_file))

    # =================================================================
    # 2. PER-CLUSTER FULL JSON (with raw data)
    # =================================================================
    for cid, ca in assessment.cluster_assessments.items():
        safe_name = _safe_filename(ca.cluster_name)
        cluster_dir = json_dir / f"cluster_{safe_name}"
        cluster_dir.mkdir(parents=True, exist_ok=True)

        # Cluster-level metadata
        cluster_meta = {
            "cluster_id": ca.cluster_id,
            "cluster_name": ca.cluster_name,
            "cluster_version": ca.cluster_version,
            "cluster_type": ca.cluster_type,
            "cluster_status": ca.cluster_status,
            "node_count": ca.node_count,
            "target_cdm_version": ca.target_cdm_version,
            "total_blockers": ca.total_blockers,
            "total_warnings": ca.total_warnings,
            "assessment_duration_sec": round(
                ca.assessment_duration_sec, 1
            ),
            "assessment_error": ca.assessment_error,
        }

        meta_file = cluster_dir / "00_cluster_meta.json"
        with open(meta_file, "w", encoding="utf-8") as f:
            json.dump(cluster_meta, f, indent=2, default=str)
        generated.append(str(meta_file))

        # Per-section JSON files
        for section_key, result in ca.results.items():
            filepath = (
                cluster_dir / f"{result.section_id}.json"
            )
            output = {
                "cluster_id": result.cluster_id,
                "cluster_name": result.cluster_name,
                "cluster_version": result.cluster_version,
                "section_name": result.section_name,
                "section_id": result.section_id,
                "summary": result.summary,
                "details": result.details,
                "blockers": result.blockers,
                "warnings": result.warnings,
                "info_messages": result.info_messages,
                "raw_data": result.raw_data,
            }
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, default=str)
            generated.append(str(filepath))

        # Combined cluster issues JSON
        issues = {
            "cluster_name": ca.cluster_name,
            "cluster_id": ca.cluster_id,
            "blockers": [],
            "warnings": [],
        }
        for result in ca.results.values():
            for b in result.blockers:
                issues["blockers"].append({
                    "section": result.section_name,
                    "message": b,
                })
            for w in result.warnings:
                issues["warnings"].append({
                    "section": result.section_name,
                    "message": w,
                })

        issues_file = cluster_dir / "00_all_issues.json"
        with open(issues_file, "w", encoding="utf-8") as f:
            json.dump(issues, f, indent=2, default=str)
        generated.append(str(issues_file))

    logger.info(f"  JSON: {len(generated)} files generated")
    return generated


def _safe_filename(name: str) -> str:
    """Convert cluster name to safe filename."""
    return (
        name.replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
    )