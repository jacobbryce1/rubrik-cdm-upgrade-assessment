#!/usr/bin/env python3
"""Find all physical hosts with OS info."""
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


def test(name, query):
    print(f"\n--- {name} ---")
    body = {"query": query}
    r = requests.post(url, json=body, headers=hdrs, timeout=30)
    try:
        d = r.json()
        if r.status_code == 200 and "data" in d and d["data"]:
            preview = json.dumps(d["data"], indent=2)[:1500]
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
print("HOST DISCOVERY - FINDING ALL HOSTS WITH OS INFO")
print("=" * 60)

# mssqlTopLevelDescendants works and returns PhysicalHost with osName
# Let's check what other TopLevelDescendants queries exist

test("mssqlTopLevelDescendants (hosts with OS)", """
    { mssqlTopLevelDescendants(first: 5, typeFilter: [PhysicalHost]) {
        count
        edges { node {
            id name objectType
            ... on PhysicalHost {
                osName osType
                connectionStatus { connectivity }
                isMssqlHost isOracleHost
            }
            cluster { id name }
        } }
    } }
""")

# Does typeFilter work?
test("mssqlTopLevelDescendants no filter", """
    { mssqlTopLevelDescendants(first: 200) {
        count
        edges { node {
            id name objectType
            ... on PhysicalHost {
                osName osType
                connectionStatus { connectivity }
                isMssqlHost isOracleHost isExchangeHost
            }
            cluster { id name }
        } }
    } }
""")

# Try physicalHost (single, by ID) - use an ID from mssql results
test("physicalHost single by fid", """
    { physicalHost(fid: "05258734-6875-5e64-a4d6-cbbbc141a20b") {
        id name osName osType
        connectionStatus { connectivity }
        isMssqlHost isOracleHost
        physicalChildConnection(first: 5) {
            edges { node { id name objectType } }
        }
    } }
""")

# Try nasTopLevelDescendants with PhysicalHost inline
test("nasTopLevelDescendants with host detail", """
    { nasTopLevelDescendants(first: 5) {
        count
        edges { node {
            id name objectType
            ... on PhysicalHost {
                osName osType
                connectionStatus { connectivity }
            }
            cluster { id name }
        } }
    } }
""")

# Try hierarchyObjects
test("hierarchySnappables PhysicalHost", """
    { hierarchySnappables(first: 5
        filter: [{ field: OBJECT_TYPE, typeFilters: [PhysicalHost] }]
    ) {
        count
        edges { node {
            id name objectType
            ... on PhysicalHost {
                osName osType
                connectionStatus { connectivity }
            }
            cluster { id name }
        } }
    } }
""")

# Try searchHost
test("searchHost", """
    { searchHost(id: "05258734-6875-5e64-a4d6-cbbbc141a20b") {
        id name osName osType
        connectionStatus { connectivity }
    } }
""")

# Try oracleTopLevelDescendants with PhysicalHost
test("oracleTopLevelDescendants with PhysicalHost", """
    { oracleTopLevelDescendants(first: 5) {
        count
        edges { node {
            id name objectType
            ... on PhysicalHost {
                osName osType
                connectionStatus { connectivity }
            }
            cluster { id name }
        } }
    } }
""")

# Check unique objectTypes in snappableConnection for this env
test("snappableConnection unique types (first 500)", """
    { snappableConnection(first: 500) {
        count
        edges { node { objectType } }
    } }
""")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)