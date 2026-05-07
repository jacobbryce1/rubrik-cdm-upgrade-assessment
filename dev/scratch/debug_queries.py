#!/usr/bin/env python3
"""Debug: Find the correct GraphQL query format for this RSC-P instance."""
import urllib3
urllib3.disable_warnings()

import json
import requests
from config import Config, setup_logging

setup_logging("INFO")

if not Config.validate():
    exit(1)

# Authenticate
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
print(f"Auth OK\n")

base_url = Config.RSC_BASE_URL.rstrip("/")
graphql_url = f"{base_url}/api/graphql"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}


def test_query(name, query, variables=None):
    print(f"--- {name} ---")
    body = {"query": query}
    if variables:
        body["variables"] = variables
    try:
        r = requests.post(
            graphql_url, json=body, headers=headers, timeout=30
        )
        print(f"  Status: {r.status_code}")
        try:
            data = r.json()
            if "errors" in data:
                for err in data["errors"]:
                    msg = err.get("message", str(err))
                    print(f"  ERROR: {msg[:200]}")
            if "data" in data and data["data"]:
                preview = json.dumps(data["data"], indent=2)[:600]
                print(f"  DATA:\n{preview}")
            elif r.status_code != 200:
                print(f"  BODY: {r.text[:300]}")
        except Exception:
            print(f"  BODY: {r.text[:300]}")
    except Exception as e:
        print(f"  EXCEPTION: {e}")
    print("")


# ==============================================
# TEST 1: Schema introspection - find all
# cluster-related query fields
# ==============================================
test_query(
    "Schema: cluster-related fields",
    """
    query {
        __schema {
            queryType {
                fields {
                    name
                }
            }
        }
    }
    """
)

# ==============================================
# TEST 2: Simplest possible query
# ==============================================
test_query(
    "Simple __typename",
    "query { __typename }"
)

# ==============================================
# TEST 3: clusterConnection minimal
# ==============================================
test_query(
    "clusterConnection minimal",
    """
    query {
        clusterConnection(first: 3) {
            count
            edges {
                node {
                    id
                    name
                    version
                    status
                }
            }
        }
    }
    """
)

# ==============================================
# TEST 4: clusterConnection with sortBy
# ==============================================
test_query(
    "clusterConnection with sortBy",
    """
    query {
        clusterConnection(
            first: 3
            sortBy: ClusterName
            sortOrder: ASC
        ) {
            count
            edges {
                node {
                    id
                    name
                }
            }
        }
    }
    """
)

# ==============================================
# TEST 5: clusterConnection WITHOUT sort
# (sortBy might not be valid on RSC-P)
# ==============================================
test_query(
    "clusterConnection NO sort",
    """
    {
        clusterConnection(first: 3) {
            count
            edges {
                node {
                    id
                    name
                }
            }
        }
    }
    """
)

# ==============================================
# TEST 6: allClusterConnection
# ==============================================
test_query(
    "allClusterConnection",
    """
    query {
        allClusterConnection(first: 3) {
            count
            edges {
                node {
                    id
                    name
                    version
                }
            }
        }
    }
    """
)

# ==============================================
# TEST 7: clusterWithUpgradesInfo
# ==============================================
test_query(
    "clusterWithUpgradesInfo",
    """
    query {
        clusterWithUpgradesInfo(first: 3) {
            count
            edges {
                node {
                    id
                    name
                    version
                }
            }
        }
    }
    """
)

# ==============================================
# TEST 8: cluster (single, if you know an ID)
# ==============================================
test_query(
    "cluster single (dummy UUID)",
    """
    query {
        cluster(clusterUuid: "00000000-0000-0000-0000-000000000000") {
            id
            name
            version
        }
    }
    """
)

# ==============================================
# TEST 9: clusterConnection with operation name
# ==============================================
body = {
    "operationName": "ListClusters",
    "query": """
        query ListClusters {
            clusterConnection(first: 3) {
                count
                edges {
                    node {
                        id
                        name
                        version
                    }
                }
            }
        }
    """,
    "variables": {}
}
print("--- clusterConnection with operationName ---")
try:
    r = requests.post(graphql_url, json=body, headers=headers, timeout=30)
    print(f"  Status: {r.status_code}")
    print(f"  BODY: {r.text[:400]}")
except Exception as e:
    print(f"  EXCEPTION: {e}")
print("")

# ==============================================
# TEST 10: POST to /api/v1/cluster instead
# (RSC-P might use REST not GraphQL for some)
# ==============================================
print("--- REST: /api/v1/cluster/me ---")
try:
    r = requests.get(
        f"{base_url}/api/v1/cluster/me",
        headers=headers,
        timeout=30,
    )
    print(f"  Status: {r.status_code}")
    print(f"  BODY: {r.text[:400]}")
except Exception as e:
    print(f"  EXCEPTION: {e}")
print("")

# ==============================================
# PRINT CLUSTER-RELATED SCHEMA FIELDS
# ==============================================
print("=" * 60)
print("AVAILABLE CLUSTER-RELATED QUERY FIELDS:")
print("=" * 60)
body = {
    "query": """
        query {
            __schema {
                queryType {
                    fields {
                        name
                    }
                }
            }
        }
    """
}
try:
    r = requests.post(graphql_url, json=body, headers=headers, timeout=30)
    data = r.json()
    fields = data.get("data", {}).get("__schema", {}).get(
        "queryType", {}
    ).get("fields", [])
    cluster_fields = sorted([
        f["name"] for f in fields
        if "cluster" in f["name"].lower()
    ])
    print(f"\nAll cluster-related queries ({len(cluster_fields)}):")
    for cf in cluster_fields:
        print(f"  - {cf}")

    # Also show all fields for reference
    all_names = sorted([f["name"] for f in fields])
    print(f"\nAll query fields ({len(all_names)}):")
    for name in all_names:
        print(f"  - {name}")
except Exception as e:
    print(f"  Schema query failed: {e}")

print("")
print("=" * 60)
print("DONE. Share this output so we can fix the queries.")
print("=" * 60)