"""
Section 8: Physical Hosts (Linux & Windows)
Uses inventoryRoot with typeFilter: [PhysicalHost].

Fix: cluster field must be inside ... on PhysicalHost
inline fragment, NOT on the base HierarchyObject type.
"""
import logging
from typing import Dict, List
from collectors import CollectionResult

logger = logging.getLogger(__name__)


def collect(client) -> CollectionResult:
    logger.info("Collecting Physical Hosts...")
    result = CollectionResult(
        section_name="Physical Hosts (Linux & Windows)",
        section_id="08_physical_hosts",
    )

    from config import Config
    cluster_id = Config.get_current_cluster_id()

    host_details: List[Dict] = []

    logger.info(
        "  [A] Collecting all physical hosts "
        "via inventoryRoot..."
    )
    try:
        has_more = True
        cursor = None
        page = 0
        seen_ids: set = set()
        total = 0

        while has_more:
            page += 1
            if cursor:
                data = client.graphql("""
                    query AllHosts($after: String) {
                        inventoryRoot {
                            descendantConnection(
                                first: 100
                                after: $after
                                typeFilter: [PhysicalHost]
                            ) {
                                count
                                pageInfo {
                                    hasNextPage
                                    endCursor
                                }
                                edges {
                                    node {
                                        id
                                        name
                                        objectType
                                        ... on PhysicalHost {
                                            osName
                                            osType
                                            connectionStatus {
                                                connectivity
                                            }
                                            cluster {
                                                id
                                                name
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                """, {"after": cursor})
            else:
                data = client.graphql("""
                    {
                        inventoryRoot {
                            descendantConnection(
                                first: 100
                                typeFilter: [PhysicalHost]
                            ) {
                                count
                                pageInfo {
                                    hasNextPage
                                    endCursor
                                }
                                edges {
                                    node {
                                        id
                                        name
                                        objectType
                                        ... on PhysicalHost {
                                            osName
                                            osType
                                            connectionStatus {
                                                connectivity
                                            }
                                            cluster {
                                                id
                                                name
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                """)

            inv = data.get(
                "inventoryRoot", {}
            ) or {}
            conn = inv.get(
                "descendantConnection", {}
            ) or {}
            pi = conn.get("pageInfo", {}) or {}
            has_more = pi.get(
                "hasNextPage", False
            )
            cursor = pi.get("endCursor")
            edges = conn.get("edges", []) or []

            if not edges:
                has_more = False

            if page == 1:
                total = conn.get("count", 0)
                logger.info(
                    f"    Total hosts in RSC: {total}"
                )

            for edge in edges:
                node = edge.get("node", {}) or {}

                # cluster is inside PhysicalHost
                # inline fragment
                nc = node.get("cluster", {}) or {}
                if nc.get("id") != cluster_id:
                    continue

                host_id = node.get("id", "")
                if host_id in seen_ids:
                    continue
                seen_ids.add(host_id)

                os_type = str(
                    node.get("osType", "UNKNOWN")
                )
                os_name = node.get(
                    "osName", ""
                ) or "Unknown"
                cs = node.get(
                    "connectionStatus", {}
                ) or {}
                connectivity = cs.get(
                    "connectivity", "UNKNOWN"
                )

                if os_type == "WINDOWS":
                    category = "Windows Host"
                elif os_type == "LINUX":
                    category = "Linux Host"
                else:
                    category = "Host"

                host_details.append({
                    "category": category,
                    "object_name": node.get(
                        "name", ""
                    ),
                    "os_name": os_name,
                    "os_type": os_type,
                    "platform_version": os_name,
                    "connectivity": connectivity,
                })

        logger.info(
            f"    Hosts for this cluster: "
            f"{len(host_details)}"
        )
    except Exception as e:
        logger.warning(
            f"  inventoryRoot query failed: {e}"
        )
        logger.debug(
            f"  Error detail:",
            exc_info=True,
        )

    result.details = host_details
    result.raw_data["host_details"] = host_details

    # =================================================================
    # Warnings
    # =================================================================
    disconnected = [
        h for h in host_details
        if h.get("connectivity") not in (
            "CONNECTED", "UNKNOWN",
            "REPLICATED_TARGET", "DELETED",
        )
    ]
    if disconnected:
        result.warnings.append(
            f"{len(disconnected)} host(s) are not "
            f"in CONNECTED status."
        )
        for h in disconnected[:5]:
            result.warnings.append(
                f"  Host '{h['object_name']}' "
                f"({h.get('os_name', '?')}) is "
                f"'{h.get('connectivity')}'"
            )

    replica_hosts = [
        h for h in host_details
        if h.get("connectivity") == "REPLICATED_TARGET"
    ]
    if replica_hosts:
        result.info_messages.append(
            f"{len(replica_hosts)} host(s) are "
            f"replication targets."
        )

    deleted_hosts = [
        h for h in host_details
        if h.get("connectivity") == "DELETED"
    ]
    if deleted_hosts:
        result.info_messages.append(
            f"{len(deleted_hosts)} host(s) are in "
            f"DELETED state."
        )

    # =================================================================
    # OS version breakdown
    # =================================================================
    os_breakdown: Dict[str, int] = {}
    for h in host_details:
        key = (
            h.get("os_name")
            or h.get("os_type", "Unknown")
        )
        os_breakdown[key] = (
            os_breakdown.get(key, 0) + 1
        )

    # =================================================================
    # Summary
    # =================================================================
    result.summary = {
        "total_physical_hosts": len(host_details),
        "windows_hosts": len([
            h for h in host_details
            if h.get("os_type") == "WINDOWS"
        ]),
        "linux_hosts": len([
            h for h in host_details
            if h.get("os_type") == "LINUX"
        ]),
        "other_hosts": len([
            h for h in host_details
            if h.get("os_type") not in (
                "WINDOWS", "LINUX"
            )
        ]),
        "os_version_breakdown": os_breakdown,
        "connected_hosts": len([
            h for h in host_details
            if h.get("connectivity") == "CONNECTED"
        ]),
        "disconnected_hosts": len(disconnected),
        "replica_hosts": len(replica_hosts),
        "deleted_hosts": len(deleted_hosts),
    }

    logger.info(
        f"  Total: {len(host_details)} "
        f"(Win: {result.summary['windows_hosts']}, "
        f"Linux: {result.summary['linux_hosts']}, "
        f"Connected: "
        f"{result.summary['connected_hosts']})"
    )
    return result