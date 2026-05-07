#!/usr/bin/env python3
"""Find Linux hosts that aren't in mssqlTopLevelDescendants."""
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

# Use Cork cluster which has the most workloads
CID_CORK = "e711ef1b-83cb-4679-9ef7-44c4de751102"


def test(name, query, variables=None):
    print(f"\n--- {name} ---")
    body = {"query": query}
    if variables:
        body["variables"] = variables
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


print("=" * 60)
print("LINUX HOST DISCOVERY")
print("=" * 60)

# 1. What does mssqlTopLevelDescendants return for Cork?
# We know this works for Windows/MSSQL hosts
test("mssqlTopLevelDescendants PhysicalHost count", """
    { mssqlTopLevelDescendants(
        first: 5
        typeFilter: [PhysicalHost]
    ) {
        count
        edges { node {
            id name objectType
            ... on PhysicalHost { osName osType }
            cluster { id name }
        } }
    } }
""")

# 2. Try nasTopLevelDescendants - might have Linux hosts
test("nasTopLevelDescendants types", """
    { nasTopLevelDescendants(first: 200) {
        count
        edges { node {
            id name objectType
            cluster { id }
        } }
    } }
""")

# 3. Check for a hierarchyObjects or inventorySubHierarchyRoot
test("cdmInventorySubHierarchyRoot", """
    { cdmInventorySubHierarchyRoot(
        rootEnum: PHYSICAL_HOST_ROOT
    ) {
        id name objectType
        descendantConnection(first: 5) {
            count
            edges { node { id name objectType } }
        }
    } }
""")

# 4. Try inventoryRoot
test("inventoryRoot", """
    { inventoryRoot {
        descendantConnection(first: 5) {
            count
            edges { node { id name objectType } }
        }
    } }
""")

# 5. Try cdmHierarchySnappablesNew
test("cdmHierarchySnappablesNew LinuxFileset", """
    { cdmHierarchySnappablesNew(
        first: 5
        objectType: LINUX_HOST_ROOT
    ) {
        count
        edges { node { id name objectType } }
    } }
""")

# 6. Check physicalHost single query for a known Linux host
# First find one via snappableConnection
test("snappable LinuxFileset sample", """
    { snappableConnection(first: 10) {
        edges { node {
            id name objectType
            physicalPath { fid name objectType }
            cluster { id name }
        } }
    } }
""")

# 7. Try hierarchyObjects with filter
test("hierarchyObjects PhysicalHost", """
    { hierarchyObjects(fids: []) {
        id name objectType
    } }
""")

# 8. Try the physicalHost single lookup with a known Cork host
# We'll get a Linux fileset's parent host FID from snappable
print("\n--- Finding Linux fileset host FIDs ---")
data = test("snappable with Linux filesets", f"""
    {{ snappableConnection(first: 500) {{
        edges {{ node {{
            objectType
            physicalPath {{ fid name objectType }}
            cluster {{ id }}
        }} }}
    }} }}
""")

if data:
    conn = data.get("snappableConnection", {}) or {}
    linux_hosts = set()
    for edge in conn.get("edges", []):
        node = edge.get("node", {}) or {}
        if node.get("objectType") != "LinuxFileset":
            continue
        nc = node.get("cluster", {}) or {}
        if nc.get("id") != CID_CORK:
            continue
        path = node.get("physicalPath", []) or []
        for p in path:
            if "Host" in p.get("objectType", ""):
                linux_hosts.add(
                    (p.get("fid", ""), p.get("name", ""))
                )

    print(f"\n  Found {len(linux_hosts)} unique Linux host FIDs:")
    for fid, name in list(linux_hosts)[:10]:
        print(f"    {name}: {fid}")

    # Try physicalHost lookup for the first one
    if linux_hosts:
        first_fid, first_name = list(linux_hosts)[0]
        print(f"\n  Testing physicalHost lookup for: {first_name}")
        test(f"physicalHost({first_name})", f"""
            {{ physicalHost(fid: "{first_fid}") {{
                id name osName osType
                connectionStatus {{ connectivity }}
            }} }}
        """)

# 9. Try oracleTopLevelDescendants for Linux hosts
test("oracleTopLevelDescendants", """
    { oracleTopLevelDescendants(first: 10) {
        count
        edges { node {
            id name objectType
            ... on PhysicalHost { osName osType }
            cluster { id name }
        } }
    } }
""")

# 10. Check HierarchyObjectTypeEnum for host types
introspect("HierarchyObjectTypeEnum")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)