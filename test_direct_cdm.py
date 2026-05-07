#!/usr/bin/env python3
"""Test direct CDM API calls using RSC-P token."""
import urllib3
urllib3.disable_warnings()
import json
import requests
from config import Config, setup_logging
setup_logging("INFO")
Config.validate()

# Get RSC-P token (same as rscp_checkDCs.py approach)
resp = requests.post(
    Config.RSC_ACCESS_TOKEN_URI,
    json={
        "grant_type": "client_credentials",
        "client_id": Config.RSC_CLIENT_ID,
        "client_secret": Config.RSC_CLIENT_SECRET,
    },
    headers={"Content-Type": "application/json"},
    timeout=30,
)
resp.raise_for_status()
token = resp.json()["access_token"]
print(f"RSC-P Token obtained: {token[:20]}...\n")

# First get cluster addresses from RSC GraphQL
base = Config.RSC_BASE_URL.rstrip("/")
gql_url = f"{base}/api/graphql"
gql_hdrs = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

# Get cluster details including node IPs
data = requests.post(gql_url, json={"query": """
    {
        clusterConnection(first: 50) {
            edges {
                node {
                    id name version status
                    defaultAddress
                    cdmClusterNodeDetails {
                        nodeId
                        dataIpAddress
                    }
                }
            }
        }
    }
"""}, headers=gql_hdrs, timeout=30).json()

clusters = []
for edge in data.get("data", {}).get("clusterConnection", {}).get("edges", []):
    n = edge.get("node", {})
    cid = n.get("id", "")
    if cid == "00000000-0000-0000-0000-000000000000":
        continue
    nodes = n.get("cdmClusterNodeDetails", []) or []
    node_ips = [
        nd.get("dataIpAddress", "")
        for nd in nodes
        if nd.get("dataIpAddress")
    ]
    clusters.append({
        "id": cid,
        "name": n.get("name", ""),
        "version": n.get("version", ""),
        "address": n.get("defaultAddress", ""),
        "node_ips": node_ips,
    })

print(f"Found {len(clusters)} clusters:\n")
for c in clusters:
    addr = c["address"] or "None"
    ips = ", ".join(c["node_ips"][:3]) or "None"
    print(
        f"  {c['name']:<25} "
        f"v{c['version']:<16} "
        f"Address: {addr:<35} "
        f"Node IPs: {ips}"
    )

print("\n" + "=" * 60)
print("TESTING DIRECT CDM API CALLS")
print("=" * 60)


def test_cdm_direct(name, cluster_addr, endpoint):
    """Test direct CDM API call like rscp_checkDCs.py does."""
    print(f"\n--- {name} ---")
    url = f"https://{cluster_addr}/{endpoint}"
    print(f"  URL: {url}")
    hdrs = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        r = requests.get(
            url, headers=hdrs,
            verify=False, timeout=15,
        )
        print(f"  Status: {r.status_code}")
        ct = r.headers.get("Content-Type", "")
        print(f"  Content-Type: {ct}")
        if "json" in ct:
            d = r.json()
            preview = json.dumps(d, indent=2)[:800]
            print(f"  JSON:\n{preview}")
            return d
        else:
            print(f"  Body: {r.text[:200]}")
            return None
    except requests.exceptions.ConnectionError as e:
        print(f"  CONNECTION FAILED: {e}")
        return None
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


# Test with each cluster that has an address
for c in clusters:
    # Try defaultAddress first, then first node IP
    addrs_to_try = []
    if c["address"] and c["address"] != "None":
        addrs_to_try.append(c["address"])
    for ip in c["node_ips"][:1]:
        if ip:
            addrs_to_try.append(ip)

    if not addrs_to_try:
        print(f"\n--- {c['name']}: No address available, skipping ---")
        continue

    for addr in addrs_to_try:
        print(f"\n\n{'='*40}")
        print(f"CLUSTER: {c['name']} via {addr}")
        print(f"{'='*40}")

        # Test basic cluster info
        result = test_cdm_direct(
            f"{c['name']}: cluster/me",
            addr,
            "api/v1/cluster/me",
        )

        if result and isinstance(result, dict) and "id" in result:
            print(f"\n  *** DIRECT CDM API WORKS! ***")

            # Test more endpoints
            test_cdm_direct(
                f"{c['name']}: hosts",
                addr,
                "api/v1/host?limit=3",
            )
            test_cdm_direct(
                f"{c['name']}: mssql instances",
                addr,
                "api/v1/mssql/instance?limit=3",
            )
            test_cdm_direct(
                f"{c['name']}: oracle hosts",
                addr,
                "api/internal/oracle/host?limit=3",
            )
            # Stop after first working address
            break
        else:
            print(f"  Direct API did not return cluster data")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)