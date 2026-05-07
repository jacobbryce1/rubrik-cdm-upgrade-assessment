"""
Section 7: SLA Domains - Full policy detail
Final version with correct field names.
"""
import logging
from collectors import CollectionResult

logger = logging.getLogger(__name__)


def collect(client) -> CollectionResult:
    logger.info("Collecting SLA Domain Policies...")
    result = CollectionResult(
        section_name="SLA Domain Policies",
        section_id="07_sla_domains",
    )

    from config import Config
    cluster_id = Config.get_current_cluster_id()

    logger.info("  [A] Collecting all SLA Domains...")
    try:
        data = client.graphql("""
            {
                slaDomains(first: 200) {
                    edges {
                        node {
                            id
                            name
                            ... on ClusterSlaDomain {
                                protectedObjectCount
                                isRetentionLockedSla
                                baseFrequency { duration unit }
                                localRetentionLimit { duration unit }
                                snapshotSchedule {
                                    hourly { basicSchedule { frequency retention retentionUnit } }
                                    daily { basicSchedule { frequency retention retentionUnit } }
                                    weekly { basicSchedule { frequency retention retentionUnit } dayOfWeek }
                                    monthly { basicSchedule { frequency retention retentionUnit } dayOfMonth }
                                }
                                archivalSpec {
                                    archivalLocationName
                                    threshold
                                    thresholdUnit
                                }
                                replicationSpecsV2 {
                                    cluster { name }
                                    retentionDuration { duration unit }
                                }
                                cluster { id name }
                            }
                            ... on GlobalSlaReply {
                                description
                                objectTypes
                                protectedObjectCount
                                isRetentionLockedSla
                                isDefault
                                baseFrequency { duration unit }
                                localRetentionLimit { duration unit }
                                snapshotSchedule {
                                    hourly { basicSchedule { frequency retention retentionUnit } }
                                    daily { basicSchedule { frequency retention retentionUnit } }
                                    weekly { basicSchedule { frequency retention retentionUnit } dayOfWeek }
                                    monthly { basicSchedule { frequency retention retentionUnit } dayOfMonth }
                                }
                                archivalSpecs {
                                    storageSetting { name }
                                    threshold
                                    thresholdUnit
                                }
                                replicationSpecsV2 {
                                    cluster { name }
                                    retentionDuration { duration unit }
                                }
                            }
                        }
                    }
                }
            }
        """)
        conn = data.get("slaDomains", {}) or {}
        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            sla_cluster = node.get("cluster", {}) or {}
            is_global = "description" in node
            is_our_cluster = sla_cluster.get("id") == cluster_id
            if not is_our_cluster and not is_global:
                continue

            # Schedule
            schedule = node.get("snapshotSchedule", {}) or {}
            sched_parts = []
            for tier in ["hourly", "daily", "weekly", "monthly"]:
                t = schedule.get(tier)
                if t:
                    bs = t.get("basicSchedule", {}) or {}
                    freq = bs.get("frequency", "?")
                    ret = bs.get("retention", "?")
                    unit = bs.get("retentionUnit", "")
                    sched_parts.append(f"{tier}: every {freq}, retain {ret} {unit}")
            sched_str = " | ".join(sched_parts) or "N/A"

            base_freq = node.get("baseFrequency", {}) or {}
            freq_str = f"{base_freq.get('duration', '?')} {base_freq.get('unit', '?')}" if base_freq else "N/A"

            local_ret = node.get("localRetentionLimit", {}) or {}
            ret_str = f"{local_ret.get('duration', '?')} {local_ret.get('unit', '?')}" if local_ret else "N/A"

            # Archival
            has_archival = False
            archival_target = "None"
            if is_global:
                arch_specs = node.get("archivalSpecs", []) or []
                if arch_specs:
                    has_archival = True
                    ss = arch_specs[0].get("storageSetting", {}) or {}
                    archival_target = ss.get("name", "Unknown")
            else:
                arch_spec = node.get("archivalSpec", {}) or {}
                if arch_spec.get("archivalLocationName"):
                    has_archival = True
                    archival_target = arch_spec["archivalLocationName"]

            # Replication
            repl_specs = node.get("replicationSpecsV2", []) or []
            has_replication = len(repl_specs) > 0
            repl_targets = [
                (r.get("cluster", {}) or {}).get("name", "?") for r in repl_specs
            ]

            result.details.append({
                "sla_id": node.get("id", ""),
                "sla_name": node.get("name", ""),
                "sla_type": "Global" if is_global else "Cluster",
                "schedule_summary": sched_str,
                "base_frequency": freq_str,
                "local_retention": ret_str,
                "replication_enabled": has_replication,
                "replication_targets": ", ".join(repl_targets),
                "archival_enabled": has_archival,
                "archival_target": archival_target,
                "protected_objects": node.get("protectedObjectCount", 0),
                "is_retention_locked": node.get("isRetentionLockedSla", False),
                "is_default": node.get("isDefault", False),
            })

        logger.info(f"    Found {len(result.details)} SLA domains")
    except Exception as e:
        logger.warning(f"  SLA collection failed: {e}")

    # Warnings
    empty = [s for s in result.details if s.get("protected_objects", 0) == 0 and s.get("sla_type") == "Cluster"]
    if empty:
        result.info_messages.append(f"{len(empty)} cluster SLA(s) have 0 protected objects.")
    locked = [s for s in result.details if s.get("is_retention_locked")]
    if locked:
        result.info_messages.append(f"{len(locked)} SLA(s) have retention lock.")

    result.summary = {
        "total_sla_domains": len(result.details),
        "cluster_slas": sum(1 for s in result.details if s.get("sla_type") == "Cluster"),
        "global_slas": sum(1 for s in result.details if s.get("sla_type") == "Global"),
        "sla_with_replication": sum(1 for s in result.details if s.get("replication_enabled")),
        "sla_with_archival": sum(1 for s in result.details if s.get("archival_enabled")),
        "retention_locked": len(locked),
        "total_protected_objects": sum(s.get("protected_objects", 0) for s in result.details),
    }

    logger.info(f"  SLAs: {len(result.details)} ({result.summary['sla_with_replication']} repl, {result.summary['sla_with_archival']} archival)")
    return result