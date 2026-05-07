#!/usr/bin/env python3
"""Discover host/OS info available for filesets."""
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
print("FILESET HOST/OS DISCOVERY")
print("=" * 60)

# Check PhysicalHost fields for OS info
introspect("PhysicalHost")

# Check HostConnectionStatus
introspect("HostConnectionStatus")

# Check GuestOsType enum
introspect("GuestOsType")

# Check RbsUpgradeStatus
introspect("RbsUpgradeStatus")

# Try physicalHosts with correct fields
test("physicalHosts with osName osType", """
    { physicalHosts(first: 5) {
        count
        edges { node {
            id name
            osName osType
            connectionStatus { connectivity }
            cluster { id name }
        } }
    } }
""")

test("physicalHosts osName only", """
    { physicalHosts(first: 5) {
        count
        edges { node {
            id name osName osType
            cluster { id name }
        } }
    } }
""")

test("physicalHosts no os fields", """
    { physicalHosts(first: 5) {
        count
        edges { node {
            id name objectType
            cluster { id name }
        } }
    } }
""")

# Check if connectionStatus needs sub-selection
test("physicalHosts connectionStatus subselect", """
    { physicalHosts(first: 3) {
        count
        edges { node {
            id name
            connectionStatus { connectivity }
        } }
    } }
""")

introspect("HostConnectionStatus")

test("physicalHosts connectionStatus status", """
    { physicalHosts(first: 3) {
        count
        edges { node {
            id name
            connectionStatus { status }
        } }
    } }
""")

# Try getting Linux filesets with host details
test("snappable LinuxFileset with physicalPath", """
    { snappableConnection(first: 10) {
        edges { node {
            id name objectType
            ... on LinuxFileset {
                host { id name osName osType
                    connectionStatus { connectivity }
                }
                pathIncluded
                pathExcluded
            }
            ... on WindowsFileset {
                host { id name osName osType
                    connectionStatus { connectivity }
                }
                pathIncluded
                pathExcluded
            }
            cluster { id name }
        } }
    } }
""")

# Try mssqlTopLevelDescendants for hosts
test("mssqlTopLevelDescendants", """
    { mssqlTopLevelDescendants(first: 5) {
        count
        edges { node {
            id name objectType
            cluster { id name }
            ... on PhysicalHost {
                osName osType
                connectionStatus { connectivity }
            }
        } }
    } }
""")

# Try oracleTopLevelDescendants
test("oracleTopLevelDescendants", """
    { oracleTopLevelDescendants(first: 5) {
        count
        edges { node {
            id name objectType
            cluster { id name }
            ... on OracleHost {
                osName osType
            }
        } }
    } }
""")

# Check FilesetTemplate type
introspect("FilesetTemplate")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)