#!/usr/bin/env python3
"""Discover fields for missing CDM workload types."""
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
            preview = json.dumps(d["data"], indent=2)[:1200]
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
print("CDM WORKLOAD DISCOVERY")
print("=" * 60)

# Find all relevant query roots
body = {"query": '{ __schema { queryType { fields { name } } } }'}
r = requests.post(url, json=body, headers=hdrs, timeout=30)
d = r.json()
fields = [f["name"] for f in d.get("data", {}).get("__schema", {}).get("queryType", {}).get("fields", [])]

# Exchange
print("\n\n========== EXCHANGE ==========")
ex_fields = [f for f in fields if "exchange" in f.lower()]
print(f"Exchange query roots: {ex_fields}")
introspect("ExchangeDatabase")
introspect("ExchangeDag")
test("exchangeDatabases", """
    { exchangeDatabases(first: 3) {
        count
        edges { node { id name cluster { id name } } }
    } }
""")
test("exchangeDags", """
    { exchangeDags(first: 3) {
        count
        edges { node { id name cluster { id name } } }
    } }
""")

# MongoDB
print("\n\n========== MONGODB ==========")
mongo_fields = [f for f in fields if "mongo" in f.lower()]
print(f"Mongo query roots: {mongo_fields}")
introspect("MongoSource")
introspect("MongoCollection")
test("mongoSources", """
    { mongoSources(first: 3) {
        count
        edges { node { id name cluster { id name } } }
    } }
""")

# Db2
print("\n\n========== DB2 ==========")
db2_fields = [f for f in fields if "db2" in f.lower()]
print(f"Db2 query roots: {db2_fields}")
introspect("Db2Database")
introspect("Db2Instance")
test("db2Databases", """
    { db2Databases(first: 3) {
        count
        edges { node { id name cluster { id name } } }
    } }
""")
test("db2Instances", """
    { db2Instances(first: 3) {
        count
        edges { node { id name cluster { id name } } }
    } }
""")

# MySQL
print("\n\n========== MYSQL ==========")
mysql_fields = [f for f in fields if "mysql" in f.lower()]
print(f"MySQL query roots: {mysql_fields}")
introspect("MysqlDatabase")
introspect("MysqlInstance")
test("mysqlDatabases", """
    { mysqlDatabases(first: 3) {
        count
        edges { node { id name cluster { id name } } }
    } }
""")
test("mysqlInstances", """
    { mysqlInstances(first: 3) {
        count
        edges { node { id name cluster { id name } } }
    } }
""")

# Active Directory
print("\n\n========== ACTIVE DIRECTORY ==========")
ad_fields = [f for f in fields if "activeDirectory" in f or "activedirectory" in f.lower()]
print(f"AD query roots: {ad_fields}")
introspect("ActiveDirectoryDomain")
introspect("ActiveDirectoryDomainController")
test("activeDirectoryDomains", """
    { activeDirectoryDomains(first: 3) {
        count
        edges { node { id name cluster { id name } } }
    } }
""")
test("activeDirectoryDomainControllers", """
    { activeDirectoryDomainControllers(first: 3) {
        count
        edges { node { id name cluster { id name } } }
    } }
""")

# Kubernetes
print("\n\n========== KUBERNETES ==========")
k8s_fields = [f for f in fields if "k8s" in f.lower() or "kubernetes" in f.lower()]
print(f"K8s query roots: {k8s_fields}")
introspect("K8sCluster")
introspect("KubernetesCluster")
test("k8sClusters", """
    { k8sClusters(first: 3) {
        count
        edges { node { id name } }
    } }
""")
test("kubernetesClusters", """
    { kubernetesClusters(first: 3) {
        count
        edges { node { id name } }
    } }
""")

# VMware VMs (count + guest OS if available)
print("\n\n========== VMWARE VMs ==========")
introspect("VsphereVm")
test("vSphereVmNewConnection VM count", """
    { vSphereVmNewConnection(first: 3) {
        count
        edges { node {
            id name
            guestOsName
            guestOsType
            cluster { id name }
        } }
    } }
""")

# Hyper-V VMs
print("\n\n========== HYPER-V VMs ==========")
introspect("HypervVirtualMachine")
test("hypervVirtualMachines", """
    { hypervVirtualMachines(first: 3) {
        count
        edges { node {
            id name
            cluster { id name }
        } }
    } }
""")

# Nutanix VMs
print("\n\n========== NUTANIX VMs ==========")
introspect("NutanixVm")
test("nutanixVms", """
    { nutanixVms(first: 3) {
        count
        edges { node {
            id name
            cluster { id name }
        } }
    } }
""")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)