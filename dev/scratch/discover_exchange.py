#!/usr/bin/env python3
"""Discover Exchange Server fields for version mapping."""
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
print("EXCHANGE & AD HOST MAPPING DISCOVERY")
print("=" * 60)

# Exchange Server type
introspect("ExchangeServer")

# Exchange Server query with host details
test("exchangeServers with host", """
    { exchangeServers(first: 10) {
        count
        edges { node {
            id
            name
            version
            edition
            cluster { id name }
        } }
    } }
""")

# If version doesn't work, try without
test("exchangeServers minimal", """
    { exchangeServers(first: 10) {
        count
        edges { node {
            id name
            cluster { id name }
        } }
    } }
""")

# Exchange Database with server reference
test("exchangeDatabases with server", """
    { exchangeDatabases(first: 3) {
        edges { node {
            id name
            exchangeServer {
                id name
            }
            physicalPath {
                fid name objectType
            }
            cluster { id name }
        } }
    } }
""")

# AD Domain Controllers with host reference
test("AD DCs with host", """
    { activeDirectoryDomainControllers(first: 5) {
        edges { node {
            id name hostname
            host {
                id name osName osType
                connectionStatus { connectivity }
            }
            activeDirectoryDomain { name domainName }
            cluster { id name }
        } }
    } }
""")

# AD DCs without host sub-fields (in case host fails)
test("AD DCs with hostname only", """
    { activeDirectoryDomainControllers(first: 5) {
        edges { node {
            id name hostname
            activeDirectoryDomain { name domainName }
            cluster { id name }
        } }
    } }
""")

# Check if we can get Exchange host from physicalPath
test("Exchange DB physicalPath for host", """
    { exchangeDatabases(first: 3) {
        edges { node {
            id name
            physicalPath {
                fid name objectType
            }
            cluster { id }
        } }
    } }
""")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)