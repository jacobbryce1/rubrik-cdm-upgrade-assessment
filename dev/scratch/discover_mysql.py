#!/usr/bin/env python3
"""Discover MySQL version and host mapping fields."""
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
print("MYSQL VERSION & HOST DISCOVERY")
print("=" * 60)

# Try different type name variations
for type_name in [
    "MysqlDatabase", "MysqlInstance",
    "MySQLDatabase", "MySQLInstance",
    "MySqlDatabase", "MySqlInstance",
    "MysqlDbInstance", "MysqlDbDatabase",
    "MysqldbInstance", "MysqldbDatabase",
    "KosmosParentHierarchyObjectType",
]:
    introspect(type_name)

# Get a MySQL instance FID to try single lookup
test("mysqlInstances basic", """
    { mysqlInstances(first: 5) {
        count
        edges { node { id name } }
    } }
""")

# Known MySQL instance FID from previous discovery
MYSQL_FID = "30268a50-452b-55f7-861d-6a52f8c14ade"

# Try single instance lookup with various fields
test("mysqlInstance single basic", f"""
    {{ mysqlInstance(fid: "{MYSQL_FID}") {{
        id name
    }} }}
""")

test("mysqlInstance with version", f"""
    {{ mysqlInstance(fid: "{MYSQL_FID}") {{
        id name
        version
    }} }}
""")

test("mysqlInstance with metadata", f"""
    {{ mysqlInstance(fid: "{MYSQL_FID}") {{
        id name
        metadata {{
            version
        }}
    }} }}
""")

test("mysqlInstance with entityInfo", f"""
    {{ mysqlInstance(fid: "{MYSQL_FID}") {{
        id name
        entityInfo {{
            name
        }}
    }} }}
""")

test("mysqlInstance with cluster and path", f"""
    {{ mysqlInstance(fid: "{MYSQL_FID}") {{
        id name
        physicalPath {{
            fid name objectType
        }}
        cluster {{ id name }}
    }} }}
""")

test("mysqlInstance with hostsInfo", f"""
    {{ mysqlInstance(fid: "{MYSQL_FID}") {{
        id name
        hostsInfo {{
            hostname
            port
            status
        }}
    }} }}
""")

# Try MySQL database with version
MYSQL_DB_FID = "0770cc64-a2d7-5d12-8e07-2fb3f6866b40"
test("mysqlDatabase single", f"""
    {{ mysqlDatabase(fid: "{MYSQL_DB_FID}") {{
        id name
    }} }}
""")

test("mysqlDatabase with version", f"""
    {{ mysqlDatabase(fid: "{MYSQL_DB_FID}") {{
        id name
        version
    }} }}
""")

test("mysqlDatabase with metadata", f"""
    {{ mysqlDatabase(fid: "{MYSQL_DB_FID}") {{
        id name
        metadata {{
            version
        }}
    }} }}
""")

# Try mysqlInstances list with more fields
test("mysqlInstances with version field", """
    { mysqlInstances(first: 5) {
        edges { node {
            id name version
            cluster { id name }
        } }
    } }
""")

test("mysqlInstances with status", """
    { mysqlInstances(first: 5) {
        edges { node {
            id name status
            cluster { id name }
        } }
    } }
""")

# Check live mounts
test("mysqlInstanceLiveMounts", """
    { mysqlInstanceLiveMounts(first: 3) {
        count
        edges { node { id } }
    } }
""")

# Also check SAP HANA for version fields while we're at it
test("sapHanaSystem single", """
    { sapHanaSystem(fid: "10857a84-8458-5989-af9d-074be2e606be") {
        id name
    } }
""")

test("sapHanaSystem with version", """
    { sapHanaSystem(fid: "10857a84-8458-5989-af9d-074be2e606be") {
        id name version
    } }
""")

test("sapHanaSystem with metadata", """
    { sapHanaSystem(fid: "10857a84-8458-5989-af9d-074be2e606be") {
        id name
        metadata { version }
    } }
""")

test("sapHanaSystem with hostsInfo", """
    { sapHanaSystem(fid: "10857a84-8458-5989-af9d-074be2e606be") {
        id name
        hostsInfo { hostname port status }
    } }
""")

# Check SapHanaSystem type
introspect("SapHanaSystem")
introspect("SapHanaDatabase")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)