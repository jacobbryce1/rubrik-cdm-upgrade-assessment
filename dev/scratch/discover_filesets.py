#!/usr/bin/env python3
"""Discover Fileset and Volume Group queries."""
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
print("FILESET & VOLUME GROUP DISCOVERY")
print("=" * 60)

# Find fileset/volume-related queries
body = {"query": '{ __schema { queryType { fields { name } } } }'}
r = requests.post(url, json=body, headers=hdrs, timeout=30)
d = r.json()
fields = d.get("data", {}).get("__schema", {}).get("queryType", {}).get("fields", [])

# Fileset queries
fs_fields = sorted([f["name"] for f in fields if "fileset" in f["name"].lower()])
print(f"\n--- Fileset query roots ({len(fs_fields)}) ---")
for f in fs_fields:
    print(f"  {f}")

# Volume group queries
vg_fields = sorted([f["name"] for f in fields if "volume" in f["name"].lower() and "managed" not in f["name"].lower()])
print(f"\n--- Volume Group query roots ({len(vg_fields)}) ---")
for f in vg_fields:
    print(f"  {f}")

# Physical host queries
ph_fields = sorted([f["name"] for f in fields if "physical" in f["name"].lower() or "host" in f["name"].lower()])
print(f"\n--- Physical/Host query roots ({len(ph_fields)}) ---")
for f in ph_fields:
    print(f"  {f}")

# NAS queries
nas_fields = sorted([f["name"] for f in fields if "nas" in f["name"].lower()])
print(f"\n--- NAS query roots ({len(nas_fields)}) ---")
for f in nas_fields:
    print(f"  {f}")

# Types
introspect("LinuxFileset")
introspect("WindowsFileset")
introspect("ShareFileset")
introspect("VolumeGroup")
introspect("PhysicalHost")
introspect("NasSystem")
introspect("HostShare")

# Test queries
test("physicalHosts", """
    { physicalHosts(first: 3) {
        count
        edges { node { id name objectType
            connectionStatus { connectivity }
            cluster { id name }
        } }
    } }
""")

test("physicalHosts no connectivity", """
    { physicalHosts(first: 3) {
        count
        edges { node { id name objectType
            cluster { id name }
        } }
    } }
""")

test("linuxFileset", """
    { linuxFileset(fid: "dummy") { id name } }
""")

test("filesetTemplates", """
    { filesetTemplates(first: 3) {
        count
        edges { node { id name } }
    } }
""")

test("filesetTemplatesByFids", """
    { filesetTemplatesByFids(fids: []) { id name } }
""")

test("nasTopLevelDescendants", """
    { nasTopLevelDescendants(first: 3) {
        count
        edges { node { id name objectType cluster { id name } } }
    } }
""")

test("nasShares", """
    { nasShares(first: 3) {
        count
        edges { node { id name objectType cluster { id name } } }
    } }
""")

test("nasSystems", """
    { nasSystems(first: 3) {
        count
        edges { node { id name objectType cluster { id name } } }
    } }
""")

test("volumeGroupConnection", """
    { volumeGroupConnection(first: 3) {
        count
        edges { node { id name objectType
            effectiveSlaDomain { name }
            cluster { id name }
        } }
    } }
""")

test("hostShares", """
    { hostShares(first: 3) {
        count
        edges { node { id name cluster { id name } } }
    } }
""")

# Check what snappable objectTypes exist
test("snappableConnection objectTypes sample", """
    { snappableConnection(first: 500) {
        edges { node { objectType } }
    } }
""")

# Try hypervisor top level
test("hypervisorTopLevelDescendants", """
    { hypervisorTopLevelDescendants(first: 3) {
        count
        edges { node { id name objectType cluster { id name } } }
    } }
""")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)