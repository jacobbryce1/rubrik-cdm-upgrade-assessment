#!/usr/bin/env python3
"""Discover exact field names for types that failed."""
import urllib3
urllib3.disable_warnings()

import requests
from config import Config, setup_logging

setup_logging("INFO")
Config.validate()

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
base = Config.RSC_BASE_URL.rstrip("/")
url = f"{base}/api/graphql"
hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

CID = "ba522459-b18a-43ff-ba77-3e95aefab7e8"


def introspect(type_name):
    print(f"\n--- {type_name} fields ---")
    body = {"query": f'{{ __type(name: "{type_name}") {{ fields {{ name type {{ name kind ofType {{ name }} }} }} }} }}'}
    r = requests.post(url, json=body, headers=hdrs, timeout=30)
    d = r.json()
    fields = d.get("data", {}).get("__type", {}).get("fields", [])
    if fields:
        for f in sorted(fields, key=lambda x: x["name"]):
            t = f["type"]
            tn = t.get("name") or (t.get("ofType", {}) or {}).get("name", "") or t.get("kind", "")
            print(f"  {f['name']}: {tn}")
    else:
        print(f"  (type not found or no fields)")
    return fields


def test(name, query, variables=None):
    print(f"\n--- TEST: {name} ---")
    body = {"query": query}
    if variables:
        body["variables"] = variables
    r = requests.post(url, json=body, headers=hdrs, timeout=30)
    try:
        d = r.json()
        if r.status_code == 200 and "data" in d and d["data"]:
            import json
            preview = json.dumps(d["data"], indent=2)[:500]
            print(f"  OK: {preview}")
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
print("SCHEMA DISCOVERY ROUND 2")
print("=" * 60)

# 1. ClusterDisk fields
introspect("ClusterDisk")

# 2. VsphereVcenter fields
introspect("VsphereVcenter")

# 3. VsphereHost fields
introspect("VsphereHost")

# 4. HyperVSCVMM fields
introspect("HyperVSCVMM")

# 5. NutanixCluster fields (the Rubrik one, not Nutanix native)
introspect("NutanixCluster")

# 6. MssqlDatabase fields
introspect("MssqlDatabase")

# 7. OracleDatabase fields
introspect("OracleDatabase")

# 8. ManagedVolume fields
introspect("ManagedVolume")

# 9. ClusterSlaDomain fields
introspect("ClusterSlaDomain")

# 10. GlobalSlaReply fields
introspect("GlobalSlaReply")

# 11. ReplicationSpec fields
introspect("ReplicationSpec")

# 12. ArchivalSpec fields (try both names)
introspect("ArchivalSpec")
introspect("ArchivalLocationSpec")

# 13. SnappableConnection filter enum
introspect("ObjectTypeEnum")

# 14. SnappableAggregation
introspect("SnappableAggregation")

# 15. RefreshableObjectConnectionStatus
introspect("RefreshableObjectConnectionStatus")

# 16. PhysicalHost
introspect("PhysicalHost")

# Quick tests with minimal fields
print("\n" + "=" * 60)
print("QUICK FIELD TESTS")
print("=" * 60)

test("vCenter minimal", """
    { vSphereVCenterConnection(first: 1) {
        edges { node { id name connectionStatus } } } }
""")

test("ESXi minimal", """
    { vSphereHostConnection(first: 1) {
        edges { node { id name } } } }
""")

test("MSSQL minimal", """
    { mssqlDatabases(first: 1) {
        edges { node { id name } } } }
""")

test("Oracle minimal", """
    { oracleDatabases(first: 1) {
        edges { node { id name } } } }
""")

test("ManagedVolume minimal", """
    { managedVolumes(first: 1) {
        edges { node { id name } } } }
""")

test("SLA minimal", """
    { slaDomains(first: 1) {
        edges { node { id name } } } }
""")

test("Nutanix minimal", """
    { nutanixClusters(first: 1) {
        edges { node { id name } } } }
""")

test("HyperV minimal", """
    { hypervScvmms(first: 1) {
        edges { node { id name } } } }
""")

test("snappableConnection objectTypes", """
    { snappableConnection(first: 1) {
        count
        edges { node { id name objectType cluster { id } } } } }
""")

test("ClusterDisk no usedBytes", f"""
    {{ cluster(clusterUuid: "{CID}") {{
        clusterDiskConnection(first: 2) {{
            count
            edges {{ node {{ id status nodeId path diskType capacityBytes }} }}
        }}
    }} }}
""")

test("physicalHosts query", """
    { physicalHosts(first: 1) {
        count
        edges { node { id name } } } }
""")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)