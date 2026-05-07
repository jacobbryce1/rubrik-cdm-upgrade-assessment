#!/usr/bin/env python3
"""Discover the exact schema fields available on this RSC-P instance."""
import urllib3
urllib3.disable_warnings()

import json
import requests
from config import Config, setup_logging

setup_logging("INFO")
Config.validate()

# Auth
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


def q(name, query, variables=None):
    body = {"query": query}
    if variables:
        body["variables"] = variables
    r = requests.post(url, json=body, headers=hdrs, timeout=30)
    try:
        d = r.json()
        if r.status_code == 200 and "data" in d:
            print(f"OK   {name}")
            return d["data"]
        else:
            errs = d.get("errors", [])
            msg = errs[0].get("message", "")[:120] if errs else r.text[:120]
            print(f"FAIL {name}: {msg}")
            return None
    except Exception:
        print(f"FAIL {name}: {r.text[:120]}")
        return None


CID = "ba522459-b18a-43ff-ba77-3e95aefab7e8"

print("=" * 60)
print("SCHEMA DISCOVERY FOR RSC-P INSTANCE")
print("=" * 60)
print()

# 1. Cluster type fields
print("--- Cluster type fields ---")
data = q("Cluster.__type", """
    { __type(name: "Cluster") { fields { name type { name kind ofType { name } } } } }
""")
if data:
    fields = data.get("__type", {}).get("fields", [])
    for f in sorted(fields, key=lambda x: x["name"]):
        t = f["type"]
        tname = t.get("name") or (t.get("ofType", {}) or {}).get("name", "") or t.get("kind", "")
        print(f"  {f['name']}: {tname}")
print()

# 2. ClusterMetric type
print("--- ClusterMetric fields ---")
data = q("ClusterMetric.__type", """
    { __type(name: "ClusterMetric") { fields { name } } }
""")
if data:
    for f in data.get("__type", {}).get("fields", []):
        print(f"  {f['name']}")
print()

# 3. CdmNodeDetail type
print("--- CdmNodeDetail fields ---")
data = q("CdmNodeDetail.__type", """
    { __type(name: "CdmNodeDetail") { fields { name } } }
""")
if data:
    for f in data.get("__type", {}).get("fields", []):
        print(f"  {f['name']}")
print()

# 4. CdmUpgradeInfo type
print("--- CdmUpgradeInfo fields ---")
data = q("CdmUpgradeInfo.__type", """
    { __type(name: "CdmUpgradeInfo") { fields { name } } }
""")
if data:
    for f in data.get("__type", {}).get("fields", []):
        print(f"  {f['name']}")
print()

# 5. CdmClusterStatus type
print("--- CdmClusterStatus fields ---")
data = q("CdmClusterStatus.__type", """
    { __type(name: "CdmClusterStatus") { fields { name } } }
""")
if data:
    for f in data.get("__type", {}).get("fields", []):
        print(f"  {f['name']}")
print()

# 6. CdmUpgradeReleaseDetail type
print("--- CdmUpgradeReleaseDetail fields ---")
data = q("CdmUpgradeReleaseDetail.__type", """
    { __type(name: "CdmUpgradeReleaseDetail") { fields { name } } }
""")
if data:
    for f in data.get("__type", {}).get("fields", []):
        print(f"  {f['name']}")
print()

# 7. FeatureListMinimumCdmVersionReply
print("--- FeatureListMinimumCdmVersionReply fields ---")
data = q("FeatureListMinimumCdmVersionReply.__type", """
    { __type(name: "FeatureListMinimumCdmVersionReply") { fields { name } } }
""")
if data:
    for f in data.get("__type", {}).get("fields", []):
        print(f"  {f['name']}")
print()

# 8. Test CDM API tunnel
print("--- CDM API Tunnel Tests ---")
cdm_base = f"{base}/cdm/cluster/{CID}"
for ep in [
    "internal/cluster/me",
    "internal/node",
    "v1/host",
    "v1/vmware/vcenter",
    "v1/mssql/instance",
    "internal/managed_volume",
    "v2/sla_domain",
    "internal/cluster/me/disk",
]:
    try:
        r = requests.get(f"{cdm_base}/{ep}", headers=hdrs, timeout=15, params={"limit": 2})
        if r.status_code == 200:
            try:
                d = r.json()
                if isinstance(d, dict):
                    count = d.get("total", len(d.get("data", [])))
                    print(f"  OK   {ep} (items: {count})")
                else:
                    print(f"  OK   {ep} (response: {str(d)[:80]})")
            except Exception:
                print(f"  OK   {ep} (non-JSON: {r.text[:60]})")
        else:
            print(f"  FAIL {ep}: HTTP {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"  FAIL {ep}: {e}")
print()

# 9. Test which GraphQL query roots work
print("--- GraphQL Query Root Tests ---")
tests = [
    ("physicalHosts", '{ physicalHosts(first: 1) { count } }'),
    ("physicalHostConnection", '{ physicalHostConnection(first: 1) { count } }'),
    ("vSphereVCenterConnection", '{ vSphereVCenterConnection(first: 1) { count } }'),
    ("mssqlDatabases", '{ mssqlDatabases(first: 1) { count } }'),
    ("mssqlTopLevelDescendants", '{ mssqlTopLevelDescendants(first: 1) { count } }'),
    ("oracleDatabases", '{ oracleDatabases(first: 1) { count } }'),
    ("managedVolumes", '{ managedVolumes(first: 1) { count } }'),
    ("slaDomains", '{ slaDomains(first: 1) { edges { node { id name } } } }'),
    ("clusterSlaDomains", f'{{ clusterSlaDomains(cdmClusterUUID: "{CID}", first: 1) {{ edges {{ node {{ id name }} }} }} }}'),
    ("snappableConnection", '{ snappableConnection(first: 1) { count } }'),
    ("nutanixClusters", '{ nutanixClusters(first: 1) { count } }'),
    ("hypervScvmms", '{ hypervScvmms(first: 1) { count } }'),
]
for name, query in tests:
    q(name, query)
print()

# 10. Test cluster query with known-good fields only
print("--- Safe Cluster Detail Query ---")
data = q("cluster basic", """
    query($id: UUID!) {
        cluster(clusterUuid: $id) {
            id name version status type defaultAddress
            lastConnectionTime passesConnectivityCheck
        }
    }
""", {"id": CID})
if data:
    c = data.get("cluster", {})
    print(f"  Cluster: {c.get('name')} v{c.get('version')} [{c.get('status')}]")
print()

print("=" * 60)
print("DONE — Share this output to fix all queries")
print("=" * 60)