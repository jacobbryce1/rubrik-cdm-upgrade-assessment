#!/usr/bin/env python3
"""Find Linux hosts - try alternative approaches."""
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
print("LINUX HOST DISCOVERY - ROUND 2")
print("=" * 60)

# 1. snappableConnection WITHOUT physicalPath
# Just get IDs and objectTypes to find Linux filesets
test("snappableConnection basic (LinuxFileset check)", """
    { snappableConnection(first: 50) {
        count
        edges { node {
            id name objectType
            cluster { id }
        } }
    } }
""")

# 2. Try linuxFileset single by FID
# First we need a Linux fileset FID - get from snappableConnection
data = test("snappableConnection for LF FIDs", """
    { snappableConnection(first: 500) {
        edges { node { id objectType cluster { id } } }
    } }
""")

linux_fids = []
if data:
    for edge in data.get("snappableConnection", {}).get("edges", []):
        node = edge.get("node", {})
        if node.get("objectType") == "LinuxFileset" and node.get("cluster", {}).get("id") == CID_CORK:
            linux_fids.append(node.get("id"))
    print(f"\n  Found {len(linux_fids)} LinuxFileset FIDs for Cork")
    for fid in linux_fids[:5]:
        print(f"    {fid}")

# 3. Try linuxFileset(fid: "...") to get host info
if linux_fids:
    fid = linux_fids[0]
    test(f"linuxFileset by fid", f"""
        {{ linuxFileset(fid: "{fid}") {{
            id name
            host {{ id name osName osType
                connectionStatus {{ connectivity }}
            }}
        }} }}
    """)

    # Try without host sub-fields
    test(f"linuxFileset host basic", f"""
        {{ linuxFileset(fid: "{fid}") {{
            id name
            host {{ id name }}
        }} }}
    """)

    # Try logicalPath instead
    test(f"linuxFileset logicalPath", f"""
        {{ linuxFileset(fid: "{fid}") {{
            id name
            logicalPath {{ fid name objectType }}
        }} }}
    """)

# 4. Try windowsFileset to compare
data2 = test("snappableConnection WindowsFileset FIDs", """
    { snappableConnection(first: 500) {
        edges { node { id objectType cluster { id } } }
    } }
""")
win_fids = []
if data2:
    for edge in data2.get("snappableConnection", {}).get("edges", []):
        node = edge.get("node", {})
        if node.get("objectType") == "WindowsFileset" and node.get("cluster", {}).get("id") == CID_CORK:
            win_fids.append(node.get("id"))
    print(f"\n  Found {len(win_fids)} WindowsFileset FIDs for Cork")

if win_fids:
    fid = win_fids[0]
    test(f"windowsFileset by fid", f"""
        {{ windowsFileset(fid: "{fid}") {{
            id name
            host {{ id name osName osType
                connectionStatus {{ connectivity }}
            }}
        }} }}
    """)

    test(f"windowsFileset host basic", f"""
        {{ windowsFileset(fid: "{fid}") {{
            id name
            host {{ id name }}
        }} }}
    """)

    test(f"windowsFileset logicalPath", f"""
        {{ windowsFileset(fid: "{fid}") {{
            id name
            logicalPath {{ fid name objectType }}
        }} }}
    """)

# 5. Try physicalHost for a known Cork Linux host
# (if we get a host FID from any of the above)

# 6. Check if inventorySubHierarchyRoot works differently
test("polarisInventorySubHierarchyRoot", """
    { polarisInventorySubHierarchyRoot(rootEnum: PHYSICAL_HOST_ROOT) {
        id name objectType
        descendantConnection(first: 5) {
            count
            edges { node { id name objectType } }
        }
    } }
""")

# 7. Try to get all unique objectTypes
print("\n--- Collecting ALL unique objectTypes ---")
all_types = set()
data = test("all objectTypes", """
    { snappableConnection(first: 500) {
        edges { node { objectType } }
    } }
""")
if data:
    for edge in data.get("snappableConnection", {}).get("edges", []):
        all_types.add(edge.get("node", {}).get("objectType", ""))
    print(f"\n  Unique objectTypes found ({len(all_types)}):")
    for t in sorted(all_types):
        print(f"    {t}")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)