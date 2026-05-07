#!/usr/bin/env python3
"""Discover PostgreSQL variant and version fields."""
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
print("POSTGRESQL VARIANT DISCOVERY")
print("=" * 60)

# 1. PostgreSQLDatabase type
introspect("PostgreSQLDatabase")

# 2. PostgreSQLDbCluster type - likely has variant
introspect("PostgreSQLDbCluster")

# 3. PostgreSQLInstance type?
introspect("PostgreSQLInstance")
introspect("PostgresInstance")

# 4. Query postgreSQLDbClusters with all useful fields
test("postgreSQLDbClusters full", """
    { postgreSQLDbClusters(first: 10) {
        count
        edges { node {
            id name
            physicalPath {
                fid name objectType
            }
            cluster { id name }
        } }
    } }
""")

# 5. Try individual cluster lookup
test("postgreSQLDbCluster single", """
    { postgreSQLDbCluster(fid: "d5602d29-1000-5704-8e19-4960315d17a3") {
        id name
        physicalPath {
            fid name objectType
        }
        cluster { id name }
    } }
""")

# 6. PostgreSQL databases with cluster ref
test("postgreSQLDatabases with path", """
    { postgreSQLDatabases(first: 5) {
        edges { node {
            id name
            physicalPath {
                fid name objectType
            }
            cluster { id name }
        } }
    } }
""")

# 7. Check postgresDbClusterLiveMounts
test("postgresDbClusterLiveMounts", """
    { postgresDbClusterLiveMounts(first: 3) {
        count
        edges { node { id } }
    } }
""")

# 8. Find PostgreSQL-related query roots
body = {"query": '{ __schema { queryType { fields { name } } } }'}
r = requests.post(url, json=body, headers=hdrs, timeout=30)
d = r.json()
fields = [f["name"] for f in d.get("data", {}).get("__schema", {}).get("queryType", {}).get("fields", [])]
pg_fields = sorted([f for f in fields if "postgres" in f.lower() or "postgresql" in f.lower()])
print(f"\n--- PostgreSQL query roots ({len(pg_fields)}) ---")
for f in pg_fields:
    print(f"  {f}")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)