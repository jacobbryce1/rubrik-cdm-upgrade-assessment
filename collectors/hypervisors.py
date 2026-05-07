"""
Section 3: Hypervisor Versions
Collects only upgrade-relevant data:
  Category, Object Name, Platform Version, Connection Status
"""
import logging
from typing import Dict, List
from collectors import CollectionResult

logger = logging.getLogger(__name__)


def collect(client) -> CollectionResult:
    logger.info("Collecting Hypervisor Versions...")
    result = CollectionResult(
        section_name="Hypervisor Versions",
        section_id="03_hypervisors",
    )

    from config import Config
    cluster_id = Config.get_current_cluster_id()

    hypervisor_details: List[Dict] = []

    # =================================================================
    # A) VMware vCenters
    # =================================================================
    logger.info("  [A] VMware vCenters...")
    try:
        data = client.graphql("""
            {
                vSphereVCenterConnection(first: 200) {
                    count
                    edges {
                        node {
                            id
                            name
                            aboutInfo { version }
                            connectionStatus { status }
                            cluster { id }
                        }
                    }
                }
            }
        """)
        conn = data.get("vSphereVCenterConnection", {}) or {}
        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            if (node.get("cluster", {}) or {}).get("id") != cluster_id:
                continue
            about = node.get("aboutInfo", {}) or {}
            cs = node.get("connectionStatus", {}) or {}
            hypervisor_details.append({
                "category": "VMware vCenter",
                "object_name": node.get("name", ""),
                "platform_version": about.get("version", "Unknown"),
                "connection_status": cs.get("status", "UNKNOWN"),
            })
        vc_count = len([
            h for h in hypervisor_details
            if h["category"] == "VMware vCenter"
        ])
        logger.info(f"    Found {vc_count}")
    except Exception as e:
        logger.warning(f"  vCenter failed: {e}")

    # =================================================================
    # B) VMware ESXi Hosts
    # ESXi version not directly available on VsphereHost type
    # Inherit version from parent vCenter
    # =================================================================
    logger.info("  [B] VMware ESXi Hosts...")
    try:
        data = client.graphql("""
            {
                vSphereHostConnection(first: 500) {
                    count
                    edges {
                        node {
                            id
                            name
                            physicalPath {
                                name
                                objectType
                            }
                            cluster { id }
                        }
                    }
                }
            }
        """)
        conn = data.get("vSphereHostConnection", {}) or {}
        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            if (node.get("cluster", {}) or {}).get("id") != cluster_id:
                continue

            # Find parent vCenter from path
            path = node.get("physicalPath", []) or []
            vcenter_name = ""
            for p in path:
                if "Vcenter" in p.get("objectType", ""):
                    vcenter_name = p.get("name", "")

            # Get version from parent vCenter
            esxi_version = "See vCenter"
            for vc in hypervisor_details:
                if (
                    vc["category"] == "VMware vCenter"
                    and (
                        vc.get("object_name") == vcenter_name
                        or vcenter_name == ""
                    )
                ):
                    esxi_version = (
                        f"vCenter: "
                        f"{vc.get('platform_version', '?')}"
                    )
                    break

            hypervisor_details.append({
                "category": "VMware ESXi Host",
                "object_name": node.get("name", ""),
                "platform_version": esxi_version,
                "connection_status": "Connected",
            })
        esxi_count = len([
            h for h in hypervisor_details
            if h["category"] == "VMware ESXi Host"
        ])
        logger.info(f"    Found {esxi_count}")
    except Exception as e:
        logger.warning(f"  ESXi failed: {e}")

    # =================================================================
    # C) Hyper-V SCVMMs
    # =================================================================
    logger.info("  [C] Hyper-V SCVMMs...")
    try:
        data = client.graphql("""
            {
                hypervScvmms(first: 200) {
                    count
                    edges {
                        node {
                            id
                            name
                            scvmmInfo { version }
                            connectionStatus
                            cluster { id }
                        }
                    }
                }
            }
        """)
        conn = data.get("hypervScvmms", {}) or {}
        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            if (node.get("cluster", {}) or {}).get("id") != cluster_id:
                continue
            info = node.get("scvmmInfo", {}) or {}
            hypervisor_details.append({
                "category": "Hyper-V SCVMM",
                "object_name": node.get("name", ""),
                "platform_version": info.get(
                    "version", "Unknown"
                ),
                "connection_status": str(
                    node.get("connectionStatus", "UNKNOWN")
                ),
            })
        hv_count = len([
            h for h in hypervisor_details
            if h["category"] == "Hyper-V SCVMM"
        ])
        logger.info(f"    Found {hv_count}")
    except Exception as e:
        logger.warning(f"  Hyper-V failed: {e}")

    # =================================================================
    # D) Nutanix AHV Clusters
    # =================================================================
    logger.info("  [D] Nutanix AHV Clusters...")
    try:
        data = client.graphql("""
            {
                nutanixClusters(first: 200) {
                    count
                    edges {
                        node {
                            id
                            name
                            nosVersion
                            connectionStatus { status }
                            cluster { id }
                        }
                    }
                }
            }
        """)
        conn = data.get("nutanixClusters", {}) or {}
        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            if (node.get("cluster", {}) or {}).get("id") != cluster_id:
                continue
            cs = node.get("connectionStatus", {}) or {}
            hypervisor_details.append({
                "category": "Nutanix AHV Cluster",
                "object_name": node.get("name", ""),
                "platform_version": node.get(
                    "nosVersion", "Unknown"
                ),
                "connection_status": cs.get(
                    "status", "UNKNOWN"
                ),
            })
        nx_count = len([
            h for h in hypervisor_details
            if "Nutanix" in h["category"]
        ])
        logger.info(f"    Found {nx_count}")
    except Exception as e:
        logger.warning(f"  Nutanix failed: {e}")

    result.details = hypervisor_details
    result.raw_data["hypervisor_details"] = hypervisor_details

    # =================================================================
    # Warnings — flag disconnected hypervisors
    # =================================================================
    for hv in hypervisor_details:
        cs = hv.get("connection_status", "").upper()
        if hv["category"] == "VMware vCenter":
            if (
                "CONNECTED" not in cs
                and "REFRESHING" not in cs
                and cs not in ("REACHABLE", "UNKNOWN", "")
            ):
                result.blockers.append(
                    f"vCenter '{hv['object_name']}' "
                    f"status: '{hv['connection_status']}'"
                )
        elif hv["category"] == "Nutanix AHV Cluster":
            if (
                "CONNECTED" not in cs
                and cs not in ("UNKNOWN", "")
            ):
                result.warnings.append(
                    f"Nutanix '{hv['object_name']}' "
                    f"status: '{hv['connection_status']}'"
                )

    # =================================================================
    # Summary
    # =================================================================
    result.summary = {
        "total_vcenters": len([
            h for h in hypervisor_details
            if h["category"] == "VMware vCenter"
        ]),
        "total_esxi_hosts": len([
            h for h in hypervisor_details
            if h["category"] == "VMware ESXi Host"
        ]),
        "total_hyperv": len([
            h for h in hypervisor_details
            if "Hyper-V" in h["category"]
        ]),
        "total_nutanix": len([
            h for h in hypervisor_details
            if "Nutanix" in h["category"]
        ]),
        "total_hypervisors": len(hypervisor_details),
    }

    logger.info(
        f"  Total hypervisors: {len(hypervisor_details)}"
    )
    return result