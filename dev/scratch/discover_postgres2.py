#!/usr/bin/env python3
"""Discover PostgreSQL metadata and hostsInfo fields."""
import urllib3
urllib3.disable_warnings()
import json
import requests
from config import Config, setup_logging
setup_logging("INFO")
Config.validate()

resp = requests.post(
    Config.RSC_ACCESS_TOKEN_URI,
    json={"grant_type": "client_credentials",
          "client_id": Config.RSC_CLIENT_ID,
          "client_secret": Config.RSC_CLIENT_SECRET},
    headers={"Content-Type": "application/json"}, timeout=30)
resp.raise_for_status()
token = resp.json()["access_token"]
base = Config.RSC_BASE_URL.rstrip("/")
url = f"{base}/api/graphql"
hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def introspect(type_name):
    print(f"\n--- {type_name} fields ---")
    body = {"query": f'{{ __type(name: "{type_name}") {{ fields {{ name type {{ name kind ofType {{ name }} }} }} }} }}'}
    r = requests.post(url, json=body, headers=hdrs, timeout=30)
    d = r.json()
    t = (d.get("data") or {}).get("__type")
    if t and t.get("fields"):
        for f in sorted(t["fields"], key=lambda x: x["name"]):
            tn = f["type"].get("name") or (f["type"].get("ofType") or {}).get("name", "") or f["type"].get("kind", "")
            print(f"  {f['name']}: {tn}")
    else:
        print("  (not found)")


def test(name, query):
    print(f"\n--- TEST: {name} ---")
    body = {"query": query}
    r = requests.post(url, json=body, headers=hdrs, timeout=30)
    try:
        d = r.json()
        if r.status_code == 200 and "data" in d and d["data"]:
            preview = json.dumps(d["data"], indent=2)[:2000]
            print(f"  OK:\n{preview}")
            return d["data"]
        else:
            errs = d.get("errors", [])
            msg = errs[0].get("message", "")[:300] if errs else r.text[:300]
            print(f"  FAIL: {msg}")
            return None
    except Exception:
        print(f"  FAIL: {r.text[:300]}")
        return None


print("=" * 60)
print("POSTGRESQL METADATA & HOSTSINFO DISCOVERY")
print("=" * 60)

# 1. Introspect metadata types
introspect("PostgreSQLDbClusterMetadata")
introspect("PostgreSQLDatabaseMetadata")
introspect("PostgreSQLDbClusterUserDetails")
introspect("PostgreSQLDbClusterStatus")

# 2. EntityInfo type (on both Database and DbCluster)
introspect("EntityInfo")

# 3. Test DbCluster with metadata
test("DbCluster with metadata", """
    { postgreSQLDbClusters(first: 5) {
        edges { node {
            id name
            metadata {
                ... on PostgreSQLDbClusterMetadata {
                    pgVersion
                    pgVariant
                }
            }
            hostsInfo {
                hostname
                port
                status
            }
            status
            cluster { id name }
        } }
    } }
""")

# 4. If inline fragment fails, try without
test("DbCluster metadata direct", """
    { postgreSQLDbClusters(first: 5) {
        edges { node {
            id name status
            cluster { id name }
        } }
    } }
""")

# 5. Try entityInfo on DbCluster
test("DbCluster with entityInfo", """
    { postgreSQLDbClusters(first: 5) {
        edges { node {
            id name
            entityInfo {
                version
                variant
            }
            cluster { id name }
        } }
    } }
""")

# 6. Try Database with metadata
test("Database with metadata", """
    { postgreSQLDatabases(first: 3) {
        edges { node {
            id name
            metadata {
                ... on PostgreSQLDatabaseMetadata {
                    pgVersion
                }
            }
            cluster { id name }
        } }
    } }
""")

# 7. Try Database entityInfo
test("Database with entityInfo", """
    { postgreSQLDatabases(first: 3) {
        edges { node {
            id name
            entityInfo {
                version
                variant
            }
            cluster { id name }
        } }
    } }
""")

# 8. Single cluster lookup with all fields
PG_CLUSTER_ID = "d5602d29-1000-5704-8e19-4960315d17a3"
test("Single DbCluster full detail", f"""
    {{ postgreSQLDbCluster(fid: "{PG_CLUSTER_ID}") {{
        id name status
        hostsInfo {{
            hostname port status
        }}
        userDetails {{
            username
        }}
        cluster {{ id name }}
    }} }}
""")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)