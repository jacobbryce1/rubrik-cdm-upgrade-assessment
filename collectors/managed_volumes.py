"""
Section 6: Managed Volumes & SLA-Managed Volumes
RSC-P final version.

Two separate query roots:
  - managedVolumes: Regular/Always-Mounted MVs (count: 5)
  - slaManagedVolumes: SLA-Based MVs (count: 10) - these show in RSC UI
Both share the ManagedVolume type fields.
"""
import logging
from collectors import CollectionResult

logger = logging.getLogger(__name__)


def collect(client) -> CollectionResult:
    logger.info("Collecting Managed Volumes & SLA-Managed Volumes...")
    result = CollectionResult(
        section_name="Managed Volumes & SLA-Managed Volumes",
        section_id="06_managed_volumes",
    )

    from config import Config
    cluster_id = Config.get_current_cluster_id()

    mv_details = []
    sla_mv_details = []

    # =================================================================
    # A) REGULAR MANAGED VOLUMES (Always-Mounted)
    # Query root: managedVolumes
    # =================================================================
    logger.info("  [A] Collecting Regular Managed Volumes...")
    try:
        data = client.graphql("""
            {
                managedVolumes(first: 500) {
                    count
                    edges {
                        node {
                            id
                            name
                            managedVolumeType
                            state
                            mountState
                            protocol
                            provisionedSize
                            numChannels
                            subnet
                            applicationTag
                            protectionDate
                            isRelic
                            effectiveSlaDomain { id name }
                            slaAssignment
                            slaPauseStatus
                            snapshotConnection { count }
                            missedSnapshotConnection { count }
                            cluster { id name }
                        }
                    }
                }
            }
        """)
        conn = data.get("managedVolumes", {}) or {}
        total = conn.get("count", 0)
        logger.info(f"    Regular MVs total: {total}")

        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            nc = node.get("cluster", {}) or {}
            if nc.get("id") != cluster_id:
                continue
            if node.get("isRelic"):
                continue

            sla = node.get("effectiveSlaDomain", {}) or {}
            snaps = node.get(
                "snapshotConnection", {}
            ) or {}
            missed = node.get(
                "missedSnapshotConnection", {}
            ) or {}
            prov = node.get("provisionedSize", 0) or 0

            mv_details.append({
                "category": "Managed Volume",
                "mv_type": str(
                    node.get("managedVolumeType", "N/A")
                ),
                "mv_id": node.get("id", ""),
                "name": node.get("name", ""),
                "state": str(node.get("state", "UNKNOWN")),
                "mount_state": str(
                    node.get("mountState", "N/A")
                ),
                "protocol": str(
                    node.get("protocol", "N/A")
                ),
                "provisioned_size_gb": round(
                    prov / (1024 ** 3), 2
                ) if prov else 0,
                "num_channels": node.get("numChannels", 0),
                "subnet": node.get("subnet", ""),
                "application_tag": str(
                    node.get("applicationTag", "N/A")
                ),
                "protection_date": node.get(
                    "protectionDate", "N/A"
                ),
                "sla_domain_name": sla.get("name", "N/A"),
                "sla_domain_id": sla.get("id", ""),
                "sla_assignment": str(
                    node.get("slaAssignment", "N/A")
                ),
                "sla_paused": node.get(
                    "slaPauseStatus", False
                ),
                "total_snapshots": snaps.get("count", 0),
                "missed_snapshots": missed.get("count", 0),
                "cluster_name": nc.get("name", ""),
            })

        logger.info(
            f"    Regular MVs for this cluster: "
            f"{len(mv_details)}"
        )
    except Exception as e:
        logger.warning(
            f"  Regular MV collection failed: {e}"
        )

    # =================================================================
    # B) SLA MANAGED VOLUMES (SLA-Based)
    # Query root: slaManagedVolumes
    # These are the ones visible in RSC UI
    # =================================================================
    logger.info("  [B] Collecting SLA Managed Volumes...")
    try:
        data = client.graphql("""
            {
                slaManagedVolumes(first: 500) {
                    count
                    edges {
                        node {
                            id
                            name
                            managedVolumeType
                            state
                            mountState
                            protocol
                            provisionedSize
                            numChannels
                            subnet
                            applicationTag
                            protectionDate
                            isRelic
                            effectiveSlaDomain { id name }
                            slaAssignment
                            slaPauseStatus
                            snapshotConnection { count }
                            missedSnapshotConnection { count }
                            cluster { id name }
                        }
                    }
                }
            }
        """)
        conn = data.get("slaManagedVolumes", {}) or {}
        total = conn.get("count", 0)
        logger.info(f"    SLA MVs total: {total}")

        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            nc = node.get("cluster", {}) or {}
            if nc.get("id") != cluster_id:
                continue
            if node.get("isRelic"):
                continue

            sla = node.get("effectiveSlaDomain", {}) or {}
            snaps = node.get(
                "snapshotConnection", {}
            ) or {}
            missed = node.get(
                "missedSnapshotConnection", {}
            ) or {}
            prov = node.get("provisionedSize", 0) or 0

            sla_mv_details.append({
                "category": "SLA Managed Volume",
                "mv_type": str(
                    node.get("managedVolumeType", "N/A")
                ),
                "mv_id": node.get("id", ""),
                "name": node.get("name", ""),
                "state": str(node.get("state", "UNKNOWN")),
                "mount_state": str(
                    node.get("mountState", "N/A")
                ),
                "protocol": str(
                    node.get("protocol", "N/A")
                ),
                "provisioned_size_gb": round(
                    prov / (1024 ** 3), 2
                ) if prov else 0,
                "num_channels": node.get("numChannels", 0),
                "subnet": node.get("subnet", ""),
                "application_tag": str(
                    node.get("applicationTag", "N/A")
                ),
                "protection_date": node.get(
                    "protectionDate", "N/A"
                ),
                "sla_domain_name": sla.get("name", "N/A"),
                "sla_domain_id": sla.get("id", ""),
                "sla_assignment": str(
                    node.get("slaAssignment", "N/A")
                ),
                "sla_paused": node.get(
                    "slaPauseStatus", False
                ),
                "total_snapshots": snaps.get("count", 0),
                "missed_snapshots": missed.get("count", 0),
                "cluster_name": nc.get("name", ""),
            })

        logger.info(
            f"    SLA MVs for this cluster: "
            f"{len(sla_mv_details)}"
        )
    except Exception as e:
        logger.warning(
            f"  SLA MV collection failed: {e}"
        )

    # =================================================================
    # C) MV LIVE MOUNTS (active exports)
    # =================================================================
    logger.info("  [C] Collecting MV Live Mounts...")
    mv_mount_details = []
    try:
        data = client.graphql("""
            {
                managedVolumeLiveMounts(first: 200) {
                    count
                    edges {
                        node {
                            id
                            name
                            numChannels
                            logicalUsedSize
                            managedVolume {
                                id
                                name
                            }
                            cluster { id name }
                        }
                    }
                }
            }
        """)
        conn = data.get(
            "managedVolumeLiveMounts", {}
        ) or {}
        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            nc = node.get("cluster", {}) or {}
            if nc.get("id") != cluster_id:
                continue
            mv_info = node.get("managedVolume", {}) or {}
            used = node.get("logicalUsedSize", 0) or 0
            mv_mount_details.append({
                "category": "MV Live Mount",
                "mount_id": node.get("id", ""),
                "mount_name": node.get("name", ""),
                "managed_volume_name": mv_info.get(
                    "name", ""
                ),
                "managed_volume_id": mv_info.get("id", ""),
                "num_channels": node.get("numChannels", 0),
                "logical_used_gb": round(
                    used / (1024 ** 3), 2
                ) if used else 0,
            })
            result.blockers.append(
                f"MV '{mv_info.get('name', '')}' has an "
                f"active live mount "
                f"(ID: {node.get('id', '')}) - "
                f"must be removed before upgrade."
            )

        if mv_mount_details:
            logger.info(
                f"    Active MV mounts: "
                f"{len(mv_mount_details)}"
            )
        else:
            logger.info("    No active MV mounts")
    except Exception as e:
        logger.warning(f"  MV live mounts failed: {e}")

    # =================================================================
    # D) SLA DOMAINS
    # =================================================================
    logger.info("  [D] Collecting SLA Domains...")
    sla_details = []
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
                                archivalSpec {
                                    archivalLocationName
                                    threshold
                                    thresholdUnit
                                }
                                replicationSpecsV2 {
                                    cluster { name }
                                    retentionDuration {
                                        duration
                                        unit
                                    }
                                }
                                cluster { id name }
                            }
                            ... on GlobalSlaReply {
                                description
                                objectTypes
                                protectedObjectCount
                                isRetentionLockedSla
                                isDefault
                                archivalSpecs {
                                    storageSetting { name }
                                    threshold
                                    thresholdUnit
                                }
                                replicationSpecsV2 {
                                    cluster { name }
                                    retentionDuration {
                                        duration
                                        unit
                                    }
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
            is_our_cluster = (
                sla_cluster.get("id") == cluster_id
            )
            if not is_our_cluster and not is_global:
                continue

            has_archival = False
            archival_target = "None"
            if is_global:
                arch_specs = node.get(
                    "archivalSpecs", []
                ) or []
                if arch_specs:
                    has_archival = True
                    ss = arch_specs[0].get(
                        "storageSetting", {}
                    ) or {}
                    archival_target = ss.get(
                        "name", "Unknown"
                    )
            else:
                arch_spec = node.get(
                    "archivalSpec", {}
                ) or {}
                if arch_spec.get("archivalLocationName"):
                    has_archival = True
                    archival_target = arch_spec[
                        "archivalLocationName"
                    ]

            repl_specs = node.get(
                "replicationSpecsV2", []
            ) or []
            has_replication = len(repl_specs) > 0
            repl_targets = [
                (r.get("cluster", {}) or {}).get(
                    "name", "?"
                )
                for r in repl_specs
            ]

            sla_details.append({
                "category": (
                    "Global SLA" if is_global
                    else "Cluster SLA"
                ),
                "sla_id": node.get("id", ""),
                "name": node.get("name", ""),
                "protected_objects": node.get(
                    "protectedObjectCount", 0
                ),
                "replication_enabled": has_replication,
                "replication_targets": ", ".join(
                    repl_targets
                ),
                "archival_enabled": has_archival,
                "archival_target": archival_target,
                "is_retention_locked": node.get(
                    "isRetentionLockedSla", False
                ),
                "is_default": node.get(
                    "isDefault", False
                ),
                "is_global": is_global,
            })

        logger.info(
            f"    Found {len(sla_details)} SLA domains"
        )
    except Exception as e:
        logger.warning(f"  SLA collection failed: {e}")

    result.raw_data["mv_details"] = mv_details
    result.raw_data["sla_mv_details"] = sla_mv_details
    result.raw_data["mv_mount_details"] = mv_mount_details
    result.raw_data["sla_details"] = sla_details

    # =================================================================
    # Combine details
    # =================================================================
    result.details = (
        mv_details
        + sla_mv_details
        + mv_mount_details
        + sla_details
    )

    # =================================================================
    # Warnings
    # =================================================================
    # Check for MVs in problematic states
    all_mvs = mv_details + sla_mv_details
    for mv in all_mvs:
        state = mv.get("state", "").upper()
        mount = mv.get("mount_state", "").upper()
        name = mv.get("name", "")

        if "WRITABLE" in state or "EXPORTED" in state:
            if "READ_ONLY" not in mount:
                result.warnings.append(
                    f"MV '{name}' state is "
                    f"'{mv.get('state')}' / "
                    f"mount: '{mv.get('mount_state')}'. "
                    f"Verify no active writes during upgrade."
                )

    # Check for paused SLAs on MVs
    paused_mvs = [
        mv for mv in all_mvs
        if mv.get("sla_paused")
    ]
    if paused_mvs:
        result.warnings.append(
            f"{len(paused_mvs)} Managed Volume(s) have "
            f"SLA paused."
        )

    # Check for missed snapshots
    missed_mvs = [
        mv for mv in all_mvs
        if mv.get("missed_snapshots", 0) > 0
    ]
    if missed_mvs:
        result.warnings.append(
            f"{len(missed_mvs)} Managed Volume(s) have "
            f"missed snapshots."
        )

    # Unprotected MVs
    unprotected = [
        mv for mv in all_mvs
        if mv.get("sla_domain_name") in (
            "N/A", "UNPROTECTED", "Unprotected", None
        )
    ]
    if unprotected:
        result.info_messages.append(
            f"{len(unprotected)} Managed Volume(s) are "
            f"not assigned to an SLA."
        )

    # =================================================================
    # Summary
    # =================================================================
    result.summary = {
        "total_regular_mvs": len(mv_details),
        "total_sla_mvs": len(sla_mv_details),
        "total_all_mvs": len(all_mvs),
        "active_mv_mounts": len(mv_mount_details),
        "paused_mvs": len(paused_mvs),
        "mvs_with_missed_snapshots": len(missed_mvs),
        "unprotected_mvs": len(unprotected),
        "total_sla_domains": len(sla_details),
        "cluster_slas": sum(
            1 for s in sla_details
            if s.get("category") == "Cluster SLA"
        ),
        "global_slas": sum(
            1 for s in sla_details
            if s.get("category") == "Global SLA"
        ),
        "slas_with_replication": sum(
            1 for s in sla_details
            if s.get("replication_enabled")
        ),
        "slas_with_archival": sum(
            1 for s in sla_details
            if s.get("archival_enabled")
        ),
        "retention_locked_slas": sum(
            1 for s in sla_details
            if s.get("is_retention_locked")
        ),
    }

    logger.info(
        f"  Regular MVs: {len(mv_details)}, "
        f"SLA MVs: {len(sla_mv_details)}, "
        f"MV Mounts: {len(mv_mount_details)}, "
        f"SLAs: {len(sla_details)}"
    )
    return result