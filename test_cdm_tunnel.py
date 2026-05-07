#!/usr/bin/env python3
"""Test CDM API tunnel with different URL patterns."""
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
hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Use Cork cluster (OnPrem, 20 nodes, most workloads)
CID = "e711ef1b-83cb-4679-9ef7-44c4de751102"


def test_url(name, url):
    print(f"\n--- {name} ---")
    print(f"  URL: {url}")
    try:
        r = requests.get(url, headers=hdrs, timeout=15)
        print(f"  Status: {r.status_code}")
        ct = r.headers.get("Content-Type", "")
        print(f"  Content-Type: {ct}")
        if "json" in ct or "application/json" in ct:
            try:
                d = r.json()
                preview = json.dumps(d, indent=2)[:500]
                print(f"  JSON: {preview}")
            except Exception:
                print(f"  Body: {r.text[:200]}")
        elif "html" in ct:
            print(f"  HTML response (web UI redirect)")
        else:
            print(f"  Body: {r.text[:200]}")
    except Exception as e:
        print(f"  ERROR: {e}")


print("=" * 60)
print("CDM API TUNNEL TEST")
print(f"Cluster: sh2-Cork ({CID})")
print("=" * 60)

# Pattern 1: /cdm/cluster/{id}/{endpoint}
# (what we originally tried)
test_url(
    "Pattern 1: /cdm/cluster/{id}/internal/cluster/me",
    f"{base}/cdm/cluster/{CID}/internal/cluster/me"
)

# Pattern 2: /api/v1/cluster/{id}/{endpoint}
test_url(
    "Pattern 2: /api/v1/cluster/{id}/internal/cluster/me",
    f"{base}/api/v1/cluster/{CID}/internal/cluster/me"
)

# Pattern 3: /cluster/{id}/api/{endpoint}
test_url(
    "Pattern 3: /cluster/{id}/api/internal/cluster/me",
    f"{base}/cluster/{CID}/api/internal/cluster/me"
)

# Pattern 4: /api/cluster/{id}/{endpoint}
test_url(
    "Pattern 4: /api/cluster/{id}/internal/cluster/me",
    f"{base}/api/cluster/{CID}/internal/cluster/me"
)

# Pattern 5: /cdm/{id}/{endpoint}
test_url(
    "Pattern 5: /cdm/{id}/internal/cluster/me",
    f"{base}/cdm/{CID}/internal/cluster/me"
)

# Pattern 6: Direct to cluster IP/FQDN
# (won't work without cluster address, but test the pattern)
test_url(
    "Pattern 6: /api/v1/cluster/me (no cluster ID)",
    f"{base}/api/v1/cluster/me"
)

# Pattern 7: /cdm/cluster/{id}/v1/{endpoint}
test_url(
    "Pattern 7: v1/host",
    f"{base}/cdm/cluster/{CID}/v1/host"
)

# Pattern 8: /cdm/cluster/{id}/api/v1/{endpoint}
test_url(
    "Pattern 8: api/v1/host",
    f"{base}/cdm/cluster/{CID}/api/v1/host"
)

# Pattern 9: With explicit Accept header
print("\n--- Pattern 9: Accept: application/json ---")
json_hdrs = {
    **hdrs,
    "Accept": "application/json",
}
try:
    r = requests.get(
        f"{base}/cdm/cluster/{CID}/internal/cluster/me",
        headers=json_hdrs, timeout=15
    )
    print(f"  Status: {r.status_code}")
    ct = r.headers.get("Content-Type", "")
    print(f"  Content-Type: {ct}")
    if "json" in ct:
        print(f"  JSON: {json.dumps(r.json(), indent=2)[:500]}")
    else:
        print(f"  Body: {r.text[:200]}")
except Exception as e:
    print(f"  ERROR: {e}")

# Pattern 10: POST to GraphQL with CDM proxy query
print("\n--- Pattern 10: GraphQL cdmProxy query ---")
try:
    body = {
        "query": """
            query CdmProxy($clusterId: UUID!, $path: String!) {
                cdmProxy(clusterUuid: $clusterId, path: $path) {
                    response
                }
            }
        """,
        "variables": {
            "clusterId": CID,
            "path": "/api/internal/cluster/me"
        }
    }
    r = requests.post(
        f"{base}/api/graphql",
        json=body, headers=hdrs, timeout=15
    )
    print(f"  Status: {r.status_code}")
    d = r.json()
    if "data" in d:
        print(f"  Data: {json.dumps(d['data'], indent=2)[:500]}")
    elif "errors" in d:
        print(f"  Error: {d['errors'][0].get('message', '')[:200]}")
    else:
        print(f"  Body: {r.text[:200]}")
except Exception as e:
    print(f"  ERROR: {e}")

# Pattern 11: Check if there's a /relay endpoint
test_url(
    "Pattern 11: /relay/cluster/{id}/internal/cluster/me",
    f"{base}/relay/cluster/{CID}/internal/cluster/me"
)

# Pattern 12: /proxy/ prefix
test_url(
    "Pattern 12: /proxy/cluster/{id}/internal/cluster/me",
    f"{base}/proxy/cluster/{CID}/internal/cluster/me"
)

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)