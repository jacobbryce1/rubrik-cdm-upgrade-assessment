"""
Section 2: Node Hardware, Disks & Physical Health
RSC-P compatible - final version with correct schema fields.

ClusterDisk fields: diskId, status, nodeId, path, diskType,
    capacityBytes, usableBytes, unallocatedBytes, serial, isEncrypted
(No 'id' or 'usedBytes' on ClusterDisk)
"""
import logging
from collectors import CollectionResult

logger = logging.getLogger(__name__)


def collect(client) -> CollectionResult:
    """Collect node hardware and disk health information."""
    logger.info("Collecting Node Hardware & Disk Health...")
    result = CollectionResult(
        section_name="Node Hardware & Disk Health",
        section_id="02_node_hardware",
    )

    from config import Config
    cluster_id = Config.get_current_cluster_id()

    # =================================================================
    # Nodes via clusterNodeConnection
    # =================================================================
    node_list = []
    try:
        data = client.graphql("""
            query ClusterNodes($id: UUID!) {
                cluster(clusterUuid: $id) {
                    clusterNodeConnection(first: 50) {
                        count
                        nodes {
                            id
                            status
                            ipAddress
                            brikId
                        }
                    }
                    cdmClusterNodeDetails {
                        nodeId
                        clusterId
                        dataIpAddress
                        ipmiIpAddress
                    }
                }
            }
        """, {"id": cluster_id})
        c = data.get("cluster", {}) or {}
        conn = c.get("clusterNodeConnection", {}) or {}
        node_list = conn.get("nodes", []) or []
        cdm_details = c.get("cdmClusterNodeDetails", []) or []

        # Build lookup for CDM details
        cdm_map = {}
        for nd in cdm_details:
            cdm_map[nd.get("nodeId", "")] = nd

        for node in node_list:
            nid = node.get("id", "")
            cdm = cdm_map.get(nid, {})
            result.details.append({
                "node_id": nid,
                "status": node.get("status", ""),
                "ip_address": node.get("ipAddress", ""),
                "brik_id": node.get("brikId", ""),
                "data_ip": cdm.get("dataIpAddress", ""),
                "ipmi_ip": cdm.get("ipmiIpAddress", ""),
            })

            status = node.get("status", "").upper()
            if status and status not in ("OK", ""):
                result.blockers.append(
                    f"Node {nid} status: "
                    f"'{node.get('status')}' "
                    f"- must be healthy for upgrade."
                )
    except Exception as e:
        logger.warning(f"  Node query failed: {e}")

    # =================================================================
    # Disks via clusterDiskConnection
    # Correct fields: diskId, status, nodeId, path, diskType,
    #   capacityBytes, usableBytes, unallocatedBytes, serial
    # NO 'id' or 'usedBytes' on ClusterDisk
    # =================================================================
    disk_list = []
    try:
        data = client.graphql("""
            query ClusterDisks($id: UUID!) {
                cluster(clusterUuid: $id) {
                    clusterDiskConnection(first: 500) {
                        count
                        edges {
                            node {
                                diskId
                                status
                                nodeId
                                path
                                diskType
                                capacityBytes
                                usableBytes
                                unallocatedBytes
                                serial
                                isEncrypted
                            }
                        }
                    }
                }
            }
        """, {"id": cluster_id})
        c = data.get("cluster", {}) or {}
        disk_conn = c.get("clusterDiskConnection", {}) or {}
        disk_edges = disk_conn.get("edges", []) or []

        for edge in disk_edges:
            disk = edge.get("node", {})
            status = disk.get("status", "UNKNOWN")
            capacity = disk.get("capacityBytes", 0) or 0
            usable = disk.get("usableBytes", 0) or 0
            unallocated = disk.get("unallocatedBytes", 0) or 0
            used = usable - unallocated if usable > 0 else 0

            disk_list.append({
                "disk_id": disk.get("diskId", ""),
                "node_id": disk.get("nodeId", ""),
                "path": disk.get("path", ""),
                "disk_type": disk.get("diskType", ""),
                "serial": disk.get("serial", ""),
                "capacity_bytes": capacity,
                "capacity_gb": round(
                    capacity / (1024 ** 3), 2
                ) if capacity else 0,
                "usable_bytes": usable,
                "usable_gb": round(
                    usable / (1024 ** 3), 2
                ) if usable else 0,
                "unallocated_bytes": unallocated,
                "unallocated_gb": round(
                    unallocated / (1024 ** 3), 2
                ) if unallocated else 0,
                "used_gb": round(
                    used / (1024 ** 3), 2
                ) if used else 0,
                "status": status,
                "is_encrypted": disk.get(
                    "isEncrypted", False
                ),
            })

            if status not in (
                "ACTIVE", "OK", "NORMAL", "HEALTHY"
            ):
                result.blockers.append(
                    f"Disk {disk.get('diskId', '')} on node "
                    f"{disk.get('nodeId', '')} status: "
                    f"'{status}' - resolve before upgrade."
                )

        logger.info(
            f"  Found {len(disk_list)} disks"
        )
    except Exception as e:
        logger.warning(f"  Disk query failed: {e}")

    # Add disks to details
    if disk_list:
        result.details.extend(disk_list)

    # =================================================================
    # System status (without systemStatusAffectedNodes sub-selection)
    # =================================================================
    try:
        data = client.graphql("""
            query ClusterHealth($id: UUID!) {
                cluster(clusterUuid: $id) {
                    systemStatus
                    systemStatusMessage
                }
            }
        """, {"id": cluster_id})
        c = data.get("cluster", {}) or {}
        sys_status = c.get("systemStatus", "")
        sys_msg = c.get("systemStatusMessage", "")

        result.raw_data["system_status"] = sys_status
        result.raw_data["system_status_message"] = sys_msg

        if sys_status and sys_status.upper() not in (
            "OK", "NORMAL", ""
        ):
            result.warnings.append(
                f"Cluster system status: {sys_status}"
                f"{' - ' + sys_msg if sys_msg else ''}"
            )
    except Exception as e:
        logger.debug(f"  System status query failed: {e}")

    # =================================================================
    # Summary
    # =================================================================
    healthy_disks = sum(
        1 for d in disk_list
        if d.get("status") in (
            "ACTIVE", "OK", "NORMAL", "HEALTHY"
        )
    )
    degraded_disks = len(disk_list) - healthy_disks

    result.summary = {
        "total_nodes": len(node_list),
        "total_disks": len(disk_list),
        "healthy_disks": healthy_disks,
        "degraded_disks": degraded_disks,
        "total_disk_capacity_tb": round(
            sum(
                d.get("capacityBytes", 0) for d in disk_list
            ) / (1024 ** 4), 2
        ),
        "total_disk_usable_tb": round(
            sum(
                d.get("usableBytes", 0) for d in disk_list
            ) / (1024 ** 4), 2
        ),
        "encrypted_disks": sum(
            1 for d in disk_list
            if d.get("is_encrypted")
        ),
        "system_status": result.raw_data.get(
            "system_status", "N/A"
        ),
    }

    logger.info(
        f"  Nodes: {len(node_list)}, "
        f"Disks: {len(disk_list)} "
        f"({healthy_disks} healthy, "
        f"{degraded_disks} degraded)"
    )
    return result