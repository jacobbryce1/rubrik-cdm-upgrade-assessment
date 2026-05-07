"""
Section 1: Cluster Identity, Version & Overall Health
RSC-P compatible with fallback compatibility matrix link.
"""
import logging
from typing import Dict
from collectors import CollectionResult

logger = logging.getLogger(__name__)

COMPAT_MATRIX_FALLBACK = (
    "https://docs.rubrik.com/en-us/saas/cdm/"
    "compatibility-matrix.html"
)


def collect(client) -> CollectionResult:
    logger.info("Collecting Cluster Identity & Health...")
    result = CollectionResult(
        section_name="Cluster Identity & Health",
        section_id="01_cluster_identity",
    )

    from config import Config
    cluster_id = Config.get_current_cluster_id()
    cluster_name = Config.get_current_cluster_name()

    # =================================================================
    # Cluster basic info
    # =================================================================
    cluster = {}
    try:
        data = client.graphql("""
            query ClusterBasic($id: UUID!) {
                cluster(clusterUuid: $id) {
                    id
                    name
                    version
                    status
                    type
                    defaultAddress
                    lastConnectionTime
                    registrationTime
                    passesConnectivityCheck
                    snapshotCount
                    encryptionEnabled
                    estimatedRunway
                    productType
                    timezone
                }
            }
        """, {"id": cluster_id})
        cluster = data.get("cluster", {}) or {}
    except Exception as e:
        logger.warning(f"  Basic cluster query failed: {e}")
    result.raw_data["rsc_cluster"] = cluster

    # =================================================================
    # State
    # =================================================================
    try:
        data = client.graphql("""
            query ClusterState($id: UUID!) {
                cluster(clusterUuid: $id) {
                    state { connectedState }
                }
            }
        """, {"id": cluster_id})
        c = data.get("cluster", {}) or {}
        state = c.get("state", {}) or {}
        if state.get("connectedState"):
            cluster["connectedState"] = state["connectedState"]
    except Exception:
        pass

    # =================================================================
    # Metrics
    # =================================================================
    try:
        data = client.graphql("""
            query ClusterMetrics($id: UUID!) {
                cluster(clusterUuid: $id) {
                    metric {
                        totalCapacity
                        usedCapacity
                        availableCapacity
                        snapshotCapacity
                        liveMountCapacity
                    }
                }
            }
        """, {"id": cluster_id})
        c = data.get("cluster", {}) or {}
        metric = c.get("metric", {}) or {}
        if metric:
            cluster["metric"] = metric
    except Exception:
        pass

    # =================================================================
    # Node details
    # =================================================================
    node_details = []
    node_count = 0
    try:
        data = client.graphql("""
            query ClusterNodes($id: UUID!) {
                cluster(clusterUuid: $id) {
                    cdmClusterNodeDetails {
                        nodeId
                        clusterId
                        dataIpAddress
                        ipmiIpAddress
                    }
                    clusterNodeConnection(first: 50) {
                        count
                        nodes {
                            id
                            status
                            ipAddress
                            brikId
                        }
                    }
                }
            }
        """, {"id": cluster_id})
        c = data.get("cluster", {}) or {}
        cdm_nodes = c.get("cdmClusterNodeDetails", []) or []
        node_conn = c.get("clusterNodeConnection", {}) or {}
        node_conn_nodes = node_conn.get("nodes", []) or []
        node_count = node_conn.get("count", 0)

        node_map: Dict[str, Dict] = {}
        for n in node_conn_nodes:
            nid = n.get("id", "")
            node_map[nid] = {
                "node_id": nid,
                "status": n.get("status", ""),
                "ip_address": n.get("ipAddress", ""),
                "brik_id": n.get("brikId", ""),
            }

        for nd in cdm_nodes:
            nid = nd.get("nodeId", "")
            if nid in node_map:
                node_map[nid]["data_ip"] = nd.get(
                    "dataIpAddress", ""
                )
                node_map[nid]["ipmi_ip"] = nd.get(
                    "ipmiIpAddress", ""
                )
            else:
                node_map[nid] = {
                    "node_id": nid,
                    "status": "",
                    "ip_address": nd.get(
                        "dataIpAddress", ""
                    ),
                    "data_ip": nd.get("dataIpAddress", ""),
                    "ipmi_ip": nd.get("ipmiIpAddress", ""),
                    "brik_id": "",
                }

        node_details = list(node_map.values())
    except Exception as e:
        logger.debug(f"  Node query failed: {e}")

    result.details = node_details
    result.raw_data["node_details"] = node_details

    # =================================================================
    # Upgrade info
    # =================================================================
    upgrade_info = {}
    try:
        data = client.graphql("""
            query ClusterUpgrade($id: UUID!) {
                cluster(clusterUuid: $id) {
                    cdmUpgradeInfo {
                        clusterUuid
                        version
                        downloadedVersion
                        versionStatus
                        previousVersion
                        clusterStatus {
                            status
                            message
                        }
                    }
                }
            }
        """, {"id": cluster_id})
        c = data.get("cluster", {}) or {}
        upgrade_info = c.get("cdmUpgradeInfo", {}) or {}
    except Exception:
        pass
    result.raw_data["upgrade_info"] = upgrade_info

    # =================================================================
    # EOS status
    # =================================================================
    eos_status = "UNKNOWN"
    eos_date = ""
    try:
        data = client.graphql("""
            query ClusterEOS($id: UUID!) {
                cluster(clusterUuid: $id) {
                    eosStatus
                    eosDate
                }
            }
        """, {"id": cluster_id})
        c = data.get("cluster", {}) or {}
        eos_status = c.get("eosStatus", "UNKNOWN")
        eos_date = c.get("eosDate", "")
    except Exception:
        pass

    # =================================================================
    # Release details from portal (needs UPGRADE_CLUSTER)
    # =================================================================
    portal_data = {}
    compat_link = ""
    try:
        data = client.graphql("""
            query ReleaseInfo($uuid: UUID!) {
                getCdmReleaseDetailsForClusterFromSupportPortal(
                    listClusterUuid: [$uuid]
                ) {
                    releaseDetails {
                        name
                        eosStatus
                        eosDate
                        isRecommended
                        isUpgradable
                    }
                    supportSoftwareLink
                    compatibilityMatrixLink
                }
            }
        """, {"uuid": cluster_id})
        if data:
            raw = data.get(
                "getCdmReleaseDetailsForClusterFromSupportPortal"
            )
            if raw and isinstance(raw, dict):
                portal_data = raw
                compat_link = portal_data.get(
                    "compatibilityMatrixLink", ""
                )
    except Exception:
        pass
    result.raw_data["portal_data"] = portal_data

    # Fallback compat link
    if not compat_link:
        compat_link = COMPAT_MATRIX_FALLBACK

    # =================================================================
    # Determine cluster status
    # =================================================================
    cluster_status = (
        cluster.get("connectedState")
        or cluster.get("status")
        or "Connected"
    )

    metric = cluster.get("metric", {}) or {}
    upgrade_status = upgrade_info.get(
        "clusterStatus", {}
    ) or {}

    # =================================================================
    # Build Summary
    # =================================================================
    result.summary = {
        "cluster_name": (
            cluster.get("name") or cluster_name or "N/A"
        ),
        "cluster_id": cluster.get("id") or cluster_id,
        "current_version": cluster.get("version", "N/A"),
        "status": cluster_status,
        "cluster_type": cluster.get("type", "N/A"),
        "product_type": cluster.get("productType", "N/A"),
        "passes_connectivity_check": cluster.get(
            "passesConnectivityCheck", "N/A"
        ),
        "node_count": len(node_details) or node_count,
        "total_capacity_tb": round(
            metric.get("totalCapacity", 0) / (1024 ** 4), 2
        ),
        "used_capacity_tb": round(
            metric.get("usedCapacity", 0) / (1024 ** 4), 2
        ),
        "available_capacity_tb": round(
            metric.get("availableCapacity", 0) / (1024 ** 4), 2
        ),
        "snapshot_capacity_tb": round(
            metric.get("snapshotCapacity", 0) / (1024 ** 4), 2
        ),
        "snapshot_count": cluster.get("snapshotCount", 0),
        "estimated_runway": cluster.get(
            "estimatedRunway", "N/A"
        ),
        "encryption_enabled": cluster.get(
            "encryptionEnabled", "N/A"
        ),
        "eos_status": eos_status,
        "eos_date": eos_date or "N/A",
        "upgrade_version_status": upgrade_info.get(
            "versionStatus", "N/A"
        ),
        "upgrade_cluster_status": upgrade_status.get(
            "status", "N/A"
        ),
        "upgrade_message": upgrade_status.get("message", ""),
        "downloaded_version": upgrade_info.get(
            "downloadedVersion", "None"
        ),
        "previous_version": upgrade_info.get(
            "previousVersion", "N/A"
        ),
        "compatibility_matrix_link": compat_link,
        "registration_time": cluster.get(
            "registrationTime", "N/A"
        ),
        "timezone": cluster.get("timezone", "N/A"),
    }

    # =================================================================
    # Blockers & Warnings
    # =================================================================
    if cluster_status in ("Disconnected", "DISCONNECTED"):
        result.blockers.append(
            f"Cluster status is '{cluster_status}' "
            f"- must be 'Connected' for upgrade."
        )

    if cluster.get("passesConnectivityCheck") is False:
        result.blockers.append(
            "Cluster FAILS RSC connectivity check."
        )

    for nd in node_details:
        nstatus = nd.get("status", "").upper()
        if nstatus and nstatus not in ("OK", ""):
            result.blockers.append(
                f"Node {nd.get('node_id', '?')} "
                f"status: '{nd.get('status')}'"
            )

    if eos_status and "NOT_SUPPORTED" in str(eos_status).upper():
        result.blockers.append(
            "Current CDM version is END OF SUPPORT."
        )
    elif eos_status and "PLAN" in str(eos_status).upper():
        result.warnings.append(
            f"CDM version approaching end of support. "
            f"EOS date: {eos_date or 'unknown'}."
        )

    for rd in portal_data.get("releaseDetails", []):
        if rd.get("isRecommended"):
            result.info_messages.append(
                f"Recommended upgrade: {rd.get('name', '?')}"
            )
            break

    logger.info(
        f"  Cluster: {result.summary['cluster_name']} "
        f"v{result.summary['current_version']} "
        f"(EOS: {eos_status}, "
        f"Nodes: {result.summary['node_count']})"
    )
    return result