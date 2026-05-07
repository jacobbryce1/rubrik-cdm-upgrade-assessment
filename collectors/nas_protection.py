"""
Section 9: NAS Systems
Collects NAS filer inventory for upgrade compatibility:
  - Vendor type, OS version, protocol support
  - Connection status

Shares, filesets, and templates are not impactful
for CDM upgrade readiness and are excluded.
"""
import logging
from typing import Dict, List
from collectors import CollectionResult

logger = logging.getLogger(__name__)


def collect(client) -> CollectionResult:
    logger.info("Collecting NAS Systems...")
    result = CollectionResult(
        section_name="NAS Systems",
        section_id="09_nas_systems",
    )

    from config import Config
    cluster_id = Config.get_current_cluster_id()

    # =================================================================
    # NAS Systems
    # =================================================================
    logger.info("  [A] Collecting NAS Systems...")
    nas_system_details: List[Dict] = []
    try:
        data = client.graphql("""
            {
                nasSystems(first: 200) {
                    count
                    edges {
                        node {
                            id
                            name
                            vendorType
                            osVersion
                            isNfsSupported
                            isSmbSupported
                            isChangelistEnabled
                            namespaceCount
                            shareCount
                            volumeCount
                            lastRefreshTime
                            lastStatus
                            cluster { id name }
                        }
                    }
                }
            }
        """)
        conn = data.get("nasSystems", {}) or {}
        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            nc = node.get("cluster", {}) or {}
            if nc.get("id") != cluster_id:
                continue

            nas_system_details.append({
                "category": "NAS System",
                "object_name": node.get(
                    "name", ""
                ),
                "vendor_type": str(
                    node.get(
                        "vendorType", "Unknown"
                    )
                ),
                "os_version": node.get(
                    "osVersion", "N/A"
                ),
                "nfs_supported": node.get(
                    "isNfsSupported", False
                ),
                "smb_supported": node.get(
                    "isSmbSupported", False
                ),
                "changelist_enabled": node.get(
                    "isChangelistEnabled", False
                ),
                "share_count": node.get(
                    "shareCount", 0
                ),
                "volume_count": node.get(
                    "volumeCount", 0
                ),
                "last_status": str(
                    node.get("lastStatus", "N/A")
                ),
            })

        logger.info(
            f"    NAS Systems: "
            f"{len(nas_system_details)}"
        )
    except Exception as e:
        logger.warning(
            f"  NAS Systems failed: {e}"
        )

    result.details = nas_system_details
    result.raw_data["nas_system_details"] = (
        nas_system_details
    )

    # =================================================================
    # Warnings
    # =================================================================
    for nas in nas_system_details:
        status = nas.get(
            "last_status", ""
        ).upper()
        if (
            status
            and "CONNECTED" not in status
            and status not in (
                "N/A", "", "NONE",
                "REFRESHING",
            )
            and "REFRESHING" not in status
        ):
            result.warnings.append(
                f"NAS System "
                f"'{nas['object_name']}' "
                f"({nas.get('vendor_type', '?')}) "
                f"status: "
                f"'{nas.get('last_status')}'"
            )

    # =================================================================
    # Vendor breakdown
    # =================================================================
    vendor_breakdown: Dict[str, int] = {}
    for nas in nas_system_details:
        vendor = nas.get(
            "vendor_type", "Unknown"
        )
        vendor_breakdown[vendor] = (
            vendor_breakdown.get(vendor, 0) + 1
        )

    protocol_breakdown: Dict[str, int] = {
        "NFS": sum(
            1 for n in nas_system_details
            if n.get("nfs_supported")
        ),
        "SMB": sum(
            1 for n in nas_system_details
            if n.get("smb_supported")
        ),
    }

    # =================================================================
    # Summary
    # =================================================================
    result.summary = {
        "total_nas_systems": len(
            nas_system_details
        ),
        "nas_vendor_breakdown": vendor_breakdown,
        "nas_protocol_breakdown": (
            protocol_breakdown
        ),
        "changelist_enabled": sum(
            1 for n in nas_system_details
            if n.get("changelist_enabled")
        ),
        "total_shares": sum(
            n.get("share_count", 0)
            for n in nas_system_details
        ),
        "total_volumes": sum(
            n.get("volume_count", 0)
            for n in nas_system_details
        ),
    }

    logger.info(
        f"  NAS Systems: "
        f"{len(nas_system_details)}"
    )
    return result