#!/usr/bin/env python3
"""
Rubrik Cluster & Node Inventory
Connects to RSC/RSC-P and produces an inventory file.
Fully paginated — no limits on cluster or node count.

Debug logging tracks every step for troubleshooting.
"""
import urllib3
urllib3.disable_warnings()

import os
import sys
import csv
import json
import logging
import requests
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
except ImportError:
    pass


# =============================================================
# LOGGING
# =============================================================

def setup_logging():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"cluster_inventory_debug_{timestamp}.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    fh = logging.FileHandler(
        log_file, mode="w", encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)-7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)-7s] %(message)s",
        datefmt="%H:%M:%S",
    ))
    root.addHandler(ch)

    logging.getLogger("urllib3").setLevel(
        logging.WARNING
    )
    logging.getLogger("requests").setLevel(
        logging.WARNING
    )

    logger = logging.getLogger("inventory")
    logger.info(f"Debug log: {log_file}")
    return logger, log_file


# =============================================================
# CONFIG
# =============================================================

def get_config(logger):
    config = {
        "client_id": os.getenv(
            "RSC_CLIENT_ID", ""
        ),
        "client_secret": os.getenv(
            "RSC_CLIENT_SECRET", ""
        ),
        "token_uri": os.getenv(
            "RSC_ACCESS_TOKEN_URI", ""
        ),
        "base_url": os.getenv(
            "RSC_BASE_URL", ""
        ),
    }
    missing = [
        k for k, v in config.items() if not v
    ]
    if missing:
        env_map = {
            "client_id": "RSC_CLIENT_ID",
            "client_secret": "RSC_CLIENT_SECRET",
            "token_uri": "RSC_ACCESS_TOKEN_URI",
            "base_url": "RSC_BASE_URL",
        }
        logger.error("Missing configuration:")
        for k in missing:
            logger.error(
                f"  {env_map.get(k, k)}"
            )
        sys.exit(1)

    logger.debug(
        f"RSC URL: {config['base_url']}"
    )
    logger.debug(
        f"Client ID: "
        f"{config['client_id'][:12]}..."
    )
    return config


# =============================================================
# RSC CLIENT
# =============================================================

def authenticate(config, logger):
    logger.info("Authenticating with RSC...")
    try:
        resp = requests.post(
            config["token_uri"],
            json={
                "grant_type": "client_credentials",
                "client_id": config["client_id"],
                "client_secret": (
                    config["client_secret"]
                ),
            },
            headers={
                "Content-Type": "application/json"
            },
            timeout=30,
        )
        logger.debug(
            f"Auth status: {resp.status_code}"
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
        logger.info("  ✓ Authenticated")
        return token
    except Exception as e:
        logger.error(f"  ✗ Auth failed: {e}")
        sys.exit(1)


def gql(base_url, token, query, variables, logger):
    """Execute GraphQL with debug logging."""
    url = f"{base_url.rstrip('/')}/api/graphql"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    short = (
        query.replace("\n", " ")
        .replace("  ", " ")
        .strip()[:80]
    )
    logger.debug(f"GQL: {short}...")

    try:
        resp = requests.post(
            url, json=payload,
            headers=headers, timeout=120,
        )
        logger.debug(
            f"GQL status: {resp.status_code}"
        )

        if resp.status_code == 400:
            try:
                errors = resp.json().get(
                    "errors", []
                )
                for err in errors:
                    logger.warning(
                        f"GQL 400: "
                        f"{err.get('message', '')[:200]}"
                    )
            except Exception:
                logger.warning(
                    f"GQL 400: {resp.text[:200]}"
                )
            return {}

        if resp.status_code != 200:
            logger.error(
                f"GQL {resp.status_code}: "
                f"{resp.text[:200]}"
            )
            return {}

        result = resp.json()
        if "errors" in result:
            for err in result["errors"]:
                logger.warning(
                    f"GQL error: "
                    f"{err.get('message', '')[:200]}"
                )
        return result.get("data", {})

    except requests.exceptions.Timeout:
        logger.error("GQL timeout")
        return {}
    except Exception as e:
        logger.error(f"GQL failed: {e}")
        return {}


# =============================================================
# CLUSTER DISCOVERY — FULLY PAGINATED
# =============================================================

def discover_clusters(base_url, token, logger):
    """
    Discover ALL CDM clusters with full pagination.
    No limit on number of clusters returned.
    """
    logger.info("Discovering clusters...")

    all_edges = []
    has_next = True
    cursor = None
    page = 0
    rsc_total = 0

    while has_next:
        page += 1
        logger.debug(
            f"  Cluster page {page} "
            f"(cursor: {cursor})"
        )

        if cursor:
            data = gql(base_url, token, """
                query Clusters($after: String) {
                    clusterConnection(
                        first: 200
                        after: $after
                    ) {
                        count
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                        edges {
                            node {
                                id name version
                                status type
                                defaultAddress
                            }
                        }
                    }
                }
            """, {"after": cursor}, logger)
        else:
            data = gql(base_url, token, """
                {
                    clusterConnection(first: 200) {
                        count
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                        edges {
                            node {
                                id name version
                                status type
                                defaultAddress
                            }
                        }
                    }
                }
            """, None, logger)

        conn = data.get(
            "clusterConnection", {}
        )
        edges = conn.get("edges", [])
        pi = conn.get("pageInfo", {})
        has_next = pi.get("hasNextPage", False)
        cursor = pi.get("endCursor")

        if page == 1:
            rsc_total = conn.get("count", 0)

        logger.debug(
            f"  Page {page}: {len(edges)} edges, "
            f"hasNext={has_next}"
        )

        if not edges:
            has_next = False
        else:
            all_edges.extend(edges)

    logger.info(
        f"  RSC total count: {rsc_total}"
    )
    logger.info(
        f"  Total edges received: "
        f"{len(all_edges)} "
        f"(across {page} page(s))"
    )

    # Process edges
    clusters = []
    skipped = {
        "polaris": 0,
        "no_version": 0,
        "duplicate": 0,
    }
    seen_ids = set()

    logger.debug("")
    logger.debug("Edge processing:")
    logger.debug(
        f"{'#':<4} {'Name':<28} "
        f"{'Version':<18} "
        f"{'Status':<12} "
        f"{'Type':<10} "
        f"{'Result'}"
    )
    logger.debug("-" * 100)

    for idx, edge in enumerate(all_edges, 1):
        node = edge.get("node", {})
        cid = node.get("id", "")
        name = node.get("name", "")
        version = node.get("version", "")
        status = node.get("status", "")
        ctype = node.get("type", "")
        addr = node.get("defaultAddress", "")

        # Skip Polaris
        if cid == (
            "00000000-0000-0000-"
            "0000-000000000000"
        ):
            skipped["polaris"] += 1
            logger.debug(
                f"{idx:<4} {name:<28} "
                f"{'(polaris)':<18} "
                f"{status:<12} "
                f"{ctype:<10} "
                f"SKIP-Polaris"
            )
            continue

        # Skip no version
        if not version:
            skipped["no_version"] += 1
            logger.debug(
                f"{idx:<4} {name:<28} "
                f"{'(none)':<18} "
                f"{status:<12} "
                f"{ctype:<10} "
                f"SKIP-NoVersion"
            )
            continue

        # Skip duplicate
        if cid in seen_ids:
            skipped["duplicate"] += 1
            logger.debug(
                f"{idx:<4} {name:<28} "
                f"{version:<18} "
                f"{status:<12} "
                f"{ctype:<10} "
                f"SKIP-Duplicate"
            )
            continue

        seen_ids.add(cid)
        clusters.append({
            "cluster_name": name,
            "cluster_id": cid,
            "version": version,
            "status": status,
            "type": ctype,
            "address": addr,
        })
        logger.debug(
            f"{idx:<4} {name:<28} "
            f"{version:<18} "
            f"{status:<12} "
            f"{ctype:<10} "
            f"INCLUDED"
        )

    logger.debug("")
    logger.info(
        f"  Included: {len(clusters)}"
    )
    for key, count in skipped.items():
        if count > 0:
            logger.info(
                f"  Skipped ({key}): {count}"
            )

    total_processed = (
        len(clusters)
        + sum(skipped.values())
    )
    logger.info(
        f"  Total processed: {total_processed}"
    )

    if rsc_total != total_processed:
        logger.warning(
            f"  ⚠ RSC count ({rsc_total}) != "
            f"processed ({total_processed}). "
            f"Some edges may be missing."
        )

    return clusters


# =============================================================
# NODE DISCOVERY — FULLY PAGINATED
# =============================================================

def get_cluster_nodes(
    base_url, token, cluster, logger
):
    """
    Get ALL nodes for a cluster.
    Uses both cdmClusterNodeDetails and
    clusterNodeConnection with pagination.
    """
    cid = cluster["cluster_id"]
    cname = cluster["cluster_name"]

    logger.debug(f"  Nodes for {cname} ({cid})")

    # Phase 1: CDM node details (no pagination
    # needed — returns all nodes)
    cdm_nodes = []
    try:
        data = gql(base_url, token, """
            query CDMNodes($id: UUID!) {
                cluster(clusterUuid: $id) {
                    cdmClusterNodeDetails {
                        nodeId
                        clusterId
                        dataIpAddress
                        ipmiIpAddress
                    }
                }
            }
        """, {"id": cid}, logger)
        c = data.get("cluster", {}) or {}
        cdm_nodes = (
            c.get("cdmClusterNodeDetails", [])
            or []
        )
        logger.debug(
            f"    cdmClusterNodeDetails: "
            f"{len(cdm_nodes)}"
        )
    except Exception as e:
        logger.warning(
            f"    CDM node details failed: {e}"
        )

    # Phase 2: clusterNodeConnection (paginated)
    conn_nodes = []
    conn_total = 0
    has_next = True
    cursor = None
    page = 0

    while has_next:
        page += 1
        if cursor:
            data = gql(base_url, token, """
                query ConnNodes(
                    $id: UUID!,
                    $after: String
                ) {
                    cluster(clusterUuid: $id) {
                        clusterNodeConnection(
                            first: 100
                            after: $after
                        ) {
                            count
                            pageInfo {
                                hasNextPage
                                endCursor
                            }
                            nodes {
                                id status
                                ipAddress brikId
                            }
                        }
                    }
                }
            """, {
                "id": cid, "after": cursor
            }, logger)
        else:
            data = gql(base_url, token, """
                query ConnNodes($id: UUID!) {
                    cluster(clusterUuid: $id) {
                        clusterNodeConnection(
                            first: 100
                        ) {
                            count
                            pageInfo {
                                hasNextPage
                                endCursor
                            }
                            nodes {
                                id status
                                ipAddress brikId
                            }
                        }
                    }
                }
            """, {"id": cid}, logger)

        c = data.get("cluster", {}) or {}
        nc = c.get(
            "clusterNodeConnection", {}
        ) or {}
        nodes_page = nc.get("nodes", []) or []
        pi = nc.get("pageInfo", {}) or {}
        has_next = pi.get("hasNextPage", False)
        cursor = pi.get("endCursor")

        if page == 1:
            conn_total = nc.get("count", 0)

        conn_nodes.extend(nodes_page)

        if not nodes_page:
            has_next = False

        logger.debug(
            f"    connPage {page}: "
            f"{len(nodes_page)} nodes, "
            f"hasNext={has_next}"
        )

    logger.debug(
        f"    clusterNodeConnection: "
        f"count={conn_total}, "
        f"received={len(conn_nodes)}"
    )

    if conn_total != len(conn_nodes):
        logger.warning(
            f"    ⚠ {cname}: conn count="
            f"{conn_total} but got "
            f"{len(conn_nodes)} nodes"
        )

    # Phase 3: Merge
    node_map = {}

    for nd in cdm_nodes:
        nid = nd.get("nodeId", "")
        if not nid:
            continue
        node_map[nid] = {
            "cluster_name": cname,
            "cluster_id": cid,
            "cluster_version": cluster["version"],
            "cluster_type": cluster["type"],
            "cluster_status": cluster["status"],
            "node_id": nid,
            "data_ip_address": nd.get(
                "dataIpAddress", ""
            ),
            "ipmi_ip_address": nd.get(
                "ipmiIpAddress", ""
            ),
            "node_status": "",
            "brik_id": "",
        }

    for cn in conn_nodes:
        nid = cn.get("id", "")
        if not nid:
            continue
        if nid in node_map:
            node_map[nid]["node_status"] = (
                cn.get("status", "")
            )
            node_map[nid]["brik_id"] = (
                cn.get("brikId", "")
            )
            if not node_map[nid][
                "data_ip_address"
            ]:
                node_map[nid][
                    "data_ip_address"
                ] = cn.get("ipAddress", "")
        else:
            node_map[nid] = {
                "cluster_name": cname,
                "cluster_id": cid,
                "cluster_version": (
                    cluster["version"]
                ),
                "cluster_type": cluster["type"],
                "cluster_status": (
                    cluster["status"]
                ),
                "node_id": nid,
                "data_ip_address": cn.get(
                    "ipAddress", ""
                ),
                "ipmi_ip_address": "",
                "node_status": cn.get(
                    "status", ""
                ),
                "brik_id": cn.get("brikId", ""),
            }

    nodes = list(node_map.values())

    logger.debug(
        f"    Merged: {len(nodes)} unique nodes "
        f"(cdm={len(cdm_nodes)}, "
        f"conn={len(conn_nodes)})"
    )

    for n in nodes:
        logger.debug(
            f"      {n['node_id'][:35]:<38} "
            f"IP={n['data_ip_address']:<18} "
            f"Status={n['node_status']}"
        )

    return nodes


# =============================================================
# OUTPUT
# =============================================================

def write_csv(inventory, rsc_url, filename, logger):
    if not inventory:
        logger.warning("No inventory data")
        return

    fieldnames = [
        "rsc_instance",
        "cluster_name", "cluster_id",
        "cluster_version", "cluster_type",
        "cluster_status", "node_id",
        "data_ip_address", "ipmi_ip_address",
        "node_status", "brik_id",
    ]

    with open(
        filename, "w", newline="",
        encoding="utf-8",
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=fieldnames
        )
        writer.writeheader()
        for row in inventory:
            row_with_rsc = {
                "rsc_instance": rsc_url,
                **row,
            }
            writer.writerow(row_with_rsc)

    logger.info(
        f"  CSV: {filename} "
        f"({len(inventory)} rows)"
    )


def write_json(
    inventory, rsc_url, clusters,
    filename, logger,
):
    output = {
        "metadata": {
            "rsc_instance": rsc_url,
            "generated": datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "total_clusters": len(clusters),
            "total_nodes": len(inventory),
        },
        "clusters": clusters,
        "inventory": inventory,
    }
    with open(
        filename, "w", encoding="utf-8"
    ) as f:
        json.dump(output, f, indent=2)
    logger.info(f"  JSON: {filename}")


def print_table(inventory, logger):
    if not inventory:
        logger.info("  No inventory data")
        return

    print()
    print(
        f"  {'Cluster':<25} "
        f"{'Cluster ID':<40} "
        f"{'Node ID':<40} "
        f"{'Node IP':<18} "
        f"{'Status'}"
    )
    print("  " + "-" * 135)

    current = ""
    for row in inventory:
        cn = row["cluster_name"]
        if cn != current:
            if current:
                print("  " + "-" * 135)
            current = cn

        nid = row["node_id"]
        if len(nid) > 38:
            nid = nid[:35] + "..."

        print(
            f"  {cn:<25} "
            f"{row['cluster_id']:<40} "
            f"{nid:<40} "
            f"{row['data_ip_address']:<18} "
            f"{row.get('node_status', '')}"
        )
    print()


# =============================================================
# MAIN
# =============================================================

def main():
    logger, log_file = setup_logging()

    print()
    print(
        "╔══════════════════════════════════════╗"
    )
    print(
        "║  Rubrik Cluster & Node Inventory     ║"
    )
    print(
        "╚══════════════════════════════════════╝"
    )
    print()

    config = get_config(logger)
    rsc_url = config["base_url"]
    logger.info(f"RSC: {rsc_url}")
    print()

    token = authenticate(config, logger)
    print()

    clusters = discover_clusters(
        config["base_url"], token, logger
    )
    print()

    logger.info("Collecting node details...")
    inventory = []
    with_nodes = 0
    without_nodes = 0

    for cluster in clusters:
        nodes = get_cluster_nodes(
            config["base_url"], token,
            cluster, logger,
        )
        if nodes:
            inventory.extend(nodes)
            with_nodes += 1
            ips = [
                n["data_ip_address"]
                for n in nodes
                if n["data_ip_address"]
            ]
            logger.info(
                f"  {cluster['cluster_name']:<25} "
                f"v{cluster['version']:<16} "
                f"{len(nodes)} node(s) "
                f"[{', '.join(ips[:3])}"
                f"{'...' if len(ips) > 3 else ''}]"
            )
        else:
            without_nodes += 1
            logger.warning(
                f"  {cluster['cluster_name']:<25} "
                f"v{cluster['version']:<16} "
                f"0 nodes ⚠"
            )

    logger.info("")
    logger.info(
        f"  With nodes: {with_nodes}"
    )
    if without_nodes > 0:
        logger.warning(
            f"  Without nodes: {without_nodes}"
        )
    logger.info(
        f"  Total nodes: {len(inventory)}"
    )
    print()

    logger.info("Inventory:")
    print_table(inventory, logger)

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )
    csv_file = (
        f"cluster_node_inventory_{timestamp}.csv"
    )
    json_file = (
        f"cluster_node_inventory_{timestamp}.json"
    )

    logger.info("Writing output files...")
    write_csv(
        inventory, rsc_url, csv_file, logger
    )
    write_json(
        inventory, rsc_url, clusters,
        json_file, logger,
    )

    # Final summary
    print()
    print(
        "╔══════════════════════════════════════╗"
    )
    print(
        "║  Inventory Complete!                  ║"
    )
    print(
        "╚══════════════════════════════════════╝"
    )
    print()
    print(f"  RSC Instance:     {rsc_url}")
    print(f"  Clusters found:   {len(clusters)}")
    print(f"  - With nodes:     {with_nodes}")
    if without_nodes > 0:
        print(
            f"  - Without nodes:  {without_nodes} ⚠"
        )
    print(f"  Total nodes:      {len(inventory)}")
    print(f"  CSV:              {csv_file}")
    print(f"  JSON:             {json_file}")
    print(f"  Debug log:        {log_file}")
    print()

    # Debug: final accounting
    logger.debug("")
    logger.debug("=" * 60)
    logger.debug("FINAL ACCOUNTING")
    logger.debug("=" * 60)
    logger.debug(
        f"Clusters included: {len(clusters)}"
    )
    logger.debug(
        f"With nodes: {with_nodes}"
    )
    logger.debug(
        f"Without nodes: {without_nodes}"
    )
    logger.debug(
        f"Inventory rows: {len(inventory)}"
    )

    if without_nodes > 0:
        logger.debug("")
        logger.debug("Clusters without nodes:")
        for c in clusters:
            has = any(
                r["cluster_id"] == c["cluster_id"]
                for r in inventory
            )
            if not has:
                logger.debug(
                    f"  {c['cluster_name']} "
                    f"({c['cluster_id']}) "
                    f"v{c['version']} "
                    f"[{c['type']}]"
                )


if __name__ == "__main__":
    main()