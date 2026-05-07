#!/usr/bin/env python3
"""Final test: Find Linux host FIDs."""
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

CORK = "e711ef1b-83cb-4679-9ef7-44c4de751102"
PALO = "6a271636-9392-4cba-90c5-bdbe227854ab"


def test(name, query, variables=None):
    print(f"\n--- {name} ---")
    body = {"query": query}
    if variables:
        body["variables"] = variables
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
print("LINUX HOST FINAL DISCOVERY")
print("=" * 60)

# 1. mssqlTopLevelDescendants WITHOUT typeFilter
# returns more objects including MSSQL_HOST entries
# Check if any have Linux osType
test("mssqlTopLevel NO filter (all types)", """
    { mssqlTopLevelDescendants(first: 200) {
        count
        edges { node {
            id name objectType
            ... on PhysicalHost {
                osName osType
                connectionStatus { connectivity }
            }
            cluster { id }
        } }
    } }
""")

# 2. Try nasTopLevelDescendants - check if PhysicalHost
# appears with NAS filesets
test("nasTopLevel with PhysicalHost", """
    { nasTopLevelDescendants(first: 200) {
        count
        edges { node {
            id name objectType
            ... on PhysicalHost {
                osName osType
                connectionStatus { connectivity }
            }
            cluster { id }
        } }
    } }
""")

# 3. Known host with children - check what objectTypes
# a host's children have
# Use the mssqlTopLevel to get a host, then check children
data = test("Get a host with children", """
    { mssqlTopLevelDescendants(
        first: 5
        typeFilter: [PhysicalHost]
    ) {
        edges { node {
            id name
            ... on PhysicalHost {
                osName osType
                physicalChildConnection(first: 50) {
                    edges { node {
                        id name objectType
                    } }
                }
            }
            cluster { id }
        } }
    } }
""")

# 4. Try inventorySubHierarchyRoot with different enums
for root in [
    "PHYSICAL_HOST_ROOT",
    "LINUX_HOST_ROOT", 
    "WINDOWS_HOST_ROOT",
    "AllSubHierarchyType",
    "LinuxHost",
]:
    test(f"inventorySubHierarchyRoot({root})", f"""
        {{ inventorySubHierarchyRoot(rootEnum: {root}) {{
            id name objectType
            descendantConnection(first: 5) {{
                count
                edges {{ node {{ id name objectType }} }}
            }}
        }} }}
    """)

# 5. Try polarisInventorySubHierarchyRoot
for root in [
    "PHYSICAL_HOST_ROOT",
    "LINUX_HOST_ROOT",
    "ALL",
]:
    test(f"polarisInventory({root})", f"""
        {{ polarisInventorySubHierarchyRoot(rootEnum: {root}) {{
            id name objectType
            descendantConnection(first: 5) {{
                count
                edges {{ node {{ id name objectType }} }}
            }}
        }} }}
    """)

# 6. Check what inventoryRoot descendant types exist
test("inventoryRoot with typeFilter PhysicalHost", """
    { inventoryRoot {
        descendantConnection(
            first: 10
            typeFilter: [PhysicalHost]
        ) {
            count
            edges { node { 
                id name objectType
                ... on PhysicalHost {
                    osName osType
                    connectionStatus { connectivity }
                }
            } }
        }
    } }
""")

# 7. Try with LINUX_HOST type filter  
test("inventoryRoot typeFilter LINUX_HOST", """
    { inventoryRoot {
        descendantConnection(
            first: 10
            typeFilter: [LinuxHost]
        ) {
            count
            edges { node { id name objectType } }
        }
    } }
""")

# 8. The nuclear option: use inventoryRoot without filter
# but check total count first
test("inventoryRoot PhysicalHost count", """
    { inventoryRoot {
        descendantConnection(
            first: 1
            typeFilter: [PhysicalHost]
        ) {
            count
        }
    } }
""")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)