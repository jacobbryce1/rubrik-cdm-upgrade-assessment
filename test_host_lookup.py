#!/usr/bin/env python3
"""Test physicalHost lookup with correct UUID! type."""
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

# These are fileset UUIDs that failed with String! type [1]
# They are fileset UUIDs, not host UUIDs - but let's test both paths
TEST_FIDS = [
    "561831e2-f4c8-40cb-ad9c-a85264619402",
    "b697b2b8-b975-4762-a2bb-efc9620d0017",
    "29aafb39-4293-409a-8591-3ea95d67a89a",
]

# Known Windows host FID from mssqlTopLevelDescendants
KNOWN_HOST_FID = "05258734-6875-5e64-a4d6-cbbbc141a20b"


def test(name, query, variables=None):
    print(f"\n--- {name} ---")
    body = {"query": query}
    if variables:
        body["variables"] = variables
    r = requests.post(url, json=body, headers=hdrs, timeout=30)
    try:
        d = r.json()
        if r.status_code == 200 and "data" in d and d["data"]:
            preview = json.dumps(d["data"], indent=2)[:800]
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
print("PHYSICAL HOST LOOKUP TEST")
print("=" * 60)

# TEST 1: Known Windows host with String! (OLD - should fail)
print("\n\n=== TEST: String! type (OLD - expect FAIL) ===")
test("physicalHost String! type", """
    query LookupHost($fid: String!) {
        physicalHost(fid: $fid) {
            id name osName osType
            connectionStatus { connectivity }
        }
    }
""", {"fid": KNOWN_HOST_FID})

# TEST 2: Known Windows host with UUID! (NEW - should work)
print("\n\n=== TEST: UUID! type (NEW - expect OK) ===")
test("physicalHost UUID! type", """
    query LookupHost($fid: UUID!) {
        physicalHost(fid: $fid) {
            id name osName osType
            connectionStatus { connectivity }
        }
    }
""", {"fid": KNOWN_HOST_FID})

# TEST 3: Fileset FIDs with UUID! - these are fileset IDs
# not host IDs, so they may not resolve as hosts
print("\n\n=== TEST: Fileset FIDs via physicalHost UUID! ===")
for fid in TEST_FIDS:
    test(f"fileset FID as host ({fid[:20]}...)", """
        query LookupHost($fid: UUID!) {
            physicalHost(fid: $fid) {
                id name osName osType
                connectionStatus { connectivity }
            }
        }
    """, {"fid": fid})

# TEST 4: Try HostChildren with UUID! type
print("\n\n=== TEST: HostChildren with UUID! ===")
test("HostChildren UUID!", """
    query HostChildren($fid: UUID!) {
        physicalHost(fid: $fid) {
            id name osName osType
            connectionStatus { connectivity }
            physicalChildConnection(first: 20) {
                edges { node {
                    id name objectType
                } }
            }
        }
    }
""", {"fid": KNOWN_HOST_FID})

# TEST 5: If fileset FIDs don't resolve as hosts,
# we need another way to find the parent host
# Try using the hierarchy - get a LinuxFileset's logical path
print("\n\n=== TEST: Find host from fileset hierarchy ===")

# First get a LinuxFileset FID from snappableConnection
data = test("Get LinuxFileset FID", """
    { snappableConnection(first: 500) {
        edges { node { id objectType cluster { id } } }
    } }
""")

if data:
    linux_fids = []
    cork_id = "e711ef1b-83cb-4679-9ef7-44c4de751102"
    palo_id = "6a271636-9392-4cba-90c5-bdbe227854ab"
    for edge in data.get("snappableConnection", {}).get("edges", []):
        node = edge.get("node", {})
        if node.get("objectType") == "LinuxFileset":
            cid = (node.get("cluster", {}) or {}).get("id")
            if cid in (cork_id, palo_id):
                linux_fids.append(node.get("id"))
    
    print(f"\n  Found {len(linux_fids)} LinuxFileset FIDs for Cork/PaloAlto")
    
    # Try hierarchyObject to get parent
    if linux_fids:
        fid = linux_fids[0]
        print(f"  Testing with: {fid}")
        
        test("hierarchyObject for fileset", f"""
            {{ hierarchyObject(fid: "{fid}") {{
                id name objectType
                effectiveSlaSourceObject {{ fid name objectType }}
            }} }}
        """)

        # Try cdmHierarchySnappableNew
        test("cdmHierarchySnappableNew", f"""
            {{ cdmHierarchySnappableNew(snappableFid: "{fid}") {{
                id name objectType
                physicalPath {{ fid name objectType }}
            }} }}
        """)

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)