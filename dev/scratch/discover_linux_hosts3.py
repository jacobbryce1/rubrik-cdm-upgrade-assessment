#!/usr/bin/env python3
"""Test individual fileset lookups and find all Linux/Windows filesets."""
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

# All cluster IDs from discovery
CLUSTERS = {
    "sh2-Cork": "e711ef1b-83cb-4679-9ef7-44c4de751102",
    "sh1-PaloAlto": "6a271636-9392-4cba-90c5-bdbe227854ab",
    "sh2-aws": "da098a70-f81e-4e6a-923e-ca334819a018",
    "sh1-az": "4f05266a-9edd-42fc-bc83-a4e45ae2fbd0",
}

# Known Linux fileset FIDs from discovery
LINUX_FIDS = [
    "Fileset:::801b283d-36e9-49ff-9fcb-952d88feea88",  # sh2-aws
    "Fileset:::90a60535-ca61-43f5-a42f-39da2af77c03",  # sh1-az
]


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
print("LINUX HOST DISCOVERY - ROUND 3")
print("=" * 60)

# 1. Test linuxFileset individual lookup with host info
for fid in LINUX_FIDS:
    test(f"linuxFileset({fid[:30]}...)", f"""
        {{ linuxFileset(fid: "{fid}") {{
            id name
            host {{
                id name osName osType
                connectionStatus {{ connectivity }}
            }}
            cluster {{ id name }}
        }} }}
    """)

# 2. If host doesn't work, try logicalPath
for fid in LINUX_FIDS:
    test(f"linuxFileset logicalPath({fid[:30]}...)", f"""
        {{ linuxFileset(fid: "{fid}") {{
            id name
            logicalPath {{ fid name objectType }}
            cluster {{ id name }}
        }} }}
    """)

# 3. Get ALL LinuxFilesets across all clusters with pagination
print("\n\n--- Finding ALL LinuxFilesets ---")
all_linux = []
all_windows = []
all_types_count = {}

# snappableConnection returns max 500 - collect all objectTypes
data = test("snappable all (count)", """
    { snappableConnection(first: 1) { count } }
""")

# Now paginate through ALL to find Linux/Windows
has_more = True
cursor = None
page = 0
while has_more and page < 20:
    page += 1
    if cursor:
        query = f"""
            {{ snappableConnection(first: 200, after: "{cursor}") {{
                pageInfo {{ hasNextPage endCursor }}
                edges {{ node {{ id objectType cluster {{ id }} }} }}
            }} }}
        """
    else:
        query = """
            { snappableConnection(first: 200) {
                pageInfo { hasNextPage endCursor }
                edges { node { id objectType cluster { id } } }
            } }
        """
    data = test(f"snappable page {page}", query)
    if not data:
        break
    conn = data.get("snappableConnection", {})
    pi = conn.get("pageInfo", {}) or {}
    has_more = pi.get("hasNextPage", False)
    cursor = pi.get("endCursor")
    edges = conn.get("edges", []) or []
    if not edges:
        break
    for edge in edges:
        node = edge.get("node", {})
        ot = node.get("objectType", "")
        cid = (node.get("cluster", {}) or {}).get("id", "")
        all_types_count[ot] = all_types_count.get(ot, 0) + 1
        if ot == "LinuxFileset":
            all_linux.append({
                "id": node.get("id"),
                "cluster": cid,
            })
        elif ot in ("WindowsFileset", "WindowsVolumeGroup"):
            all_windows.append({
                "id": node.get("id"),
                "cluster": cid,
            })

print(f"\n\n--- FULL RESULTS ---")
print(f"  Total pages scanned: {page}")
print(f"\n  Object type counts:")
for ot, count in sorted(all_types_count.items(), key=lambda x: -x[1]):
    print(f"    {ot}: {count}")

print(f"\n  LinuxFilesets found: {len(all_linux)}")
for lf in all_linux[:10]:
    cname = [k for k, v in CLUSTERS.items() if v == lf["cluster"]]
    print(f"    {lf['id'][:50]} -> {cname[0] if cname else lf['cluster']}")

print(f"\n  WindowsFileset/VolumeGroup found: {len(all_windows)}")
for wf in all_windows[:10]:
    cname = [k for k, v in CLUSTERS.items() if v == wf["cluster"]]
    print(f"    {wf['id'][:50]} -> {cname[0] if cname else wf['cluster']}")

# 4. Test physicalHost lookup for any host from mssqlTopLevel
test("physicalHost Linux test", """
    { mssqlTopLevelDescendants(first: 3, typeFilter: [PhysicalHost]) {
        edges { node {
            ... on PhysicalHost {
                id name osName osType
                physicalChildConnection(first: 10) {
                    edges { node { id name objectType } }
                }
            }
            cluster { id }
        } }
    } }
""")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)