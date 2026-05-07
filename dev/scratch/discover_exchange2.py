#!/usr/bin/env python3
"""Get Exchange Server version via individual lookup."""
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
print("EXCHANGE VERSION DISCOVERY")
print("=" * 60)

# Known Exchange Server FID from previous discovery
EX_FID = "1883e019-14f2-5a9b-a348-db759bea30dd"

# Try individual exchangeServer lookup with version
test("exchangeServer single with version", f"""
    {{ exchangeServer(fid: "{EX_FID}") {{
        id name version totalDbs
        exchangeDag {{ name }}
        host {{
            id name osName osType
            connectionStatus {{ connectivity }}
        }}
        cluster {{ id name }}
    }} }}
""")

# Try without host sub-fields
test("exchangeServer version only", f"""
    {{ exchangeServer(fid: "{EX_FID}") {{
        id name version totalDbs
        cluster {{ id name }}
    }} }}
""")

# Try exchangeServers list with version
test("exchangeServers with version", """
    { exchangeServers(first: 10) {
        count
        edges { node {
            id name version totalDbs
            cluster { id name }
        } }
    } }
""")

# Try exchangeServers with host
test("exchangeServers with host ref", """
    { exchangeServers(first: 3) {
        edges { node {
            id name
            host { id name osName osType }
            cluster { id name }
        } }
    } }
""")

# Get PhysicalHost FID from exchange physicalPath
# and look it up
PH_FID = "208039c8-0d9a-5e3a-8433-96fed242c2c5"
test("physicalHost for Exchange host", f"""
    {{ physicalHost(fid: "{PH_FID}") {{
        id name osName osType
        connectionStatus {{ connectivity }}
    }} }}
""")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)