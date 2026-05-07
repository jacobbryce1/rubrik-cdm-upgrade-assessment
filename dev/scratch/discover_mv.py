#!/usr/bin/env python3
"""Discover correct Managed Volume and SLA MV queries."""
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


def test(name, query, variables=None):
    print(f"\n--- TEST: {name} ---")
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
            msg = errs[0].get("message", "")[:200] if errs else r.text[:200]
            print(f"  FAIL: {msg}")
            return None
    except Exception:
        print(f"  FAIL: {r.text[:200]}")
        return None


print("=" * 60)
print("MANAGED VOLUME DISCOVERY")
print("=" * 60)

# 1. Check ManagedVolume type
introspect("ManagedVolume")

# 2. Check SlaManagedVolume type
introspect("SlaManagedVolume")

# 3. Check ManagedVolumeMount
introspect("ManagedVolumeMount")

# 4. Find MV-related query roots
body = {"query": """
    { __schema { queryType { fields { name } } } }
"""}
r = requests.post(url, json=body, headers=hdrs, timeout=30)
d = r.json()
fields = d.get("data", {}).get("__schema", {}).get("queryType", {}).get("fields", [])
mv_fields = sorted([f["name"] for f in fields if "managed" in f["name"].lower() or "volume" in f["name"].lower()])
print(f"\n--- MV-related query roots ({len(mv_fields)}) ---")
for f in mv_fields:
    print(f"  {f}")

# 5. Also check SLA-related
sla_mv = sorted([f["name"] for f in fields if "sla" in f["name"].lower() and "managed" in f["name"].lower()])
print(f"\n--- SLA+MV query roots ({len(sla_mv)}) ---")
for f in sla_mv:
    print(f"  {f}")

# 6. Test different MV queries
test("managedVolumes minimal", """
    { managedVolumes(first: 3) {
        count
        edges { node { id name objectType } }
    } }
""")

test("slaManagedVolumes", """
    { slaManagedVolumes(first: 3) {
        count
        edges { node { id name objectType } }
    } }
""")

test("managedVolumeInventoryStats", """
    { managedVolumeInventoryStats { count } }
""")

test("managedVolumeLiveMounts", """
    { managedVolumeLiveMounts(first: 3) {
        count
        edges { node { id } }
    } }
""")

# 7. Test managed volume with different fields
test("managedVolumes with cluster", """
    { managedVolumes(first: 3) {
        count
        edges {
            node {
                id
                name
                managedVolumeType
                state
                mountState
                cluster { id name }
            }
        }
    } }
""")

# 8. Try slaManagedVolume type
test("slaManagedVolumes with detail", """
    { slaManagedVolumes(first: 3) {
        count
        edges {
            node {
                id
                name
                managedVolumeType
                state
                mountState
                effectiveSlaDomain { id name }
                cluster { id name }
            }
        }
    } }
""")

# 9. Try snappableConnection with MV filter
test("snappableConnection MV types", """
    { snappableConnection(first: 5
        filter: { objectType: [MANAGED_VOLUME] }
    ) {
        count
        edges { node { id name objectType cluster { id name } } }
    } }
""")

test("snappableConnection SLA_MV types", """
    { snappableConnection(first: 5
        filter: { objectType: [SLA_MANAGED_VOLUME] }
    ) {
        count
        edges { node { id name objectType cluster { id name } } }
    } }
""")

# 10. Try without object type filter
test("snappableConnection all - check for MV types", """
    { snappableConnection(first: 200) {
        count
        edges { node { objectType } }
    } }
""")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)