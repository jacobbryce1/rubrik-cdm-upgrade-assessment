#!/usr/bin/env python3
"""Test CDM authentication methods."""
import urllib3
urllib3.disable_warnings()
import json
import requests
from config import Config, setup_logging
setup_logging("INFO")
Config.validate()

# Get RSC-P token
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
rsc_token = resp.json()["access_token"]
print(f"RSC-P Token: {rsc_token[:30]}...\n")

base = Config.RSC_BASE_URL.rstrip("/")
gql_url = f"{base}/api/graphql"
gql_hdrs = {
    "Authorization": f"Bearer {rsc_token}",
    "Content-Type": "application/json",
}

CID = "e711ef1b-83cb-4679-9ef7-44c4de751102"
CDM_IP = "172.24.87.29"


def test_gql(name, query, variables=None):
    print(f"\n--- GQL: {name} ---")
    body = {"query": query}
    if variables:
        body["variables"] = variables
    r = requests.post(
        gql_url, json=body, headers=gql_hdrs, timeout=30
    )
    try:
        d = r.json()
        if r.status_code == 200 and "data" in d and d["data"]:
            preview = json.dumps(d["data"], indent=2)[:1000]
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


def test_cdm(name, cdm_ip, endpoint, headers):
    print(f"\n--- CDM: {name} ---")
    url = f"https://{cdm_ip}/{endpoint}"
    try:
        r = requests.get(
            url, headers=headers,
            verify=False, timeout=15,
        )
        print(f"  Status: {r.status_code}")
        ct = r.headers.get("Content-Type", "")
        if "json" in ct:
            d = r.json()
            print(f"  JSON: {json.dumps(d, indent=2)[:500]}")
            return d
        else:
            print(f"  Body: {r.text[:200]}")
            return None
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


print("=" * 60)
print("CDM AUTHENTICATION TESTS")
print(f"Cluster: sh2-Cork ({CDM_IP})")
print("=" * 60)

# 1. Check for clusterThriftAuthToken
test_gql("clusterThriftAuthToken", f"""
    {{ cluster(clusterUuid: "{CID}") {{
        clusterThriftAuthToken
    }} }}
""")

# 2. Check for session/token generation mutations
# Search schema for token/session related mutations
body = {"query": """
    { __schema { mutationType { fields { name } } } }
"""}
r = requests.post(gql_url, json=body, headers=gql_hdrs, timeout=30)
d = r.json()
mutations = [
    f["name"] for f in
    d.get("data", {}).get("__schema", {}).get(
        "mutationType", {}
    ).get("fields", [])
]
token_mutations = sorted([
    m for m in mutations
    if any(k in m.lower() for k in [
        "token", "session", "auth", "cdm",
        "cluster"
    ])
])
print(f"\n--- Token/Auth related mutations ({len(token_mutations)}) ---")
for m in token_mutations:
    print(f"  {m}")

# 3. Try generateClusterToken or similar
for mutation_name in [
    "generateClusterApiToken",
    "createCdmSession",
    "generateCdmApiToken",
    "getClusterAuthToken",
]:
    test_gql(f"mutation {mutation_name}", f"""
        mutation {{
            {mutation_name}(
                input: {{ clusterUuid: "{CID}" }}
            ) {{
                token
            }}
        }}
    """)

# 4. Try using the thrift auth token if we got one
# (from test 1)

# 5. Try CDM session creation with service account
print("\n\n--- CDM Session Tests ---")

# 5a. Try POST to /api/v1/session with RSC token
test_cdm(
    "POST /api/v1/session (RSC token in body)",
    CDM_IP,
    "api/v1/session",
    {"Content-Type": "application/json"},
)

# 5b. Try with token as API token
hdrs_api = {
    "Authorization": f"Bearer {rsc_token}",
    "Content-Type": "application/json",
    "User-Agent": "rubrik-cdm-assessment/1.0",
}
test_cdm(
    "GET cluster/me (Bearer RSC token)",
    CDM_IP,
    "api/v1/cluster/me",
    hdrs_api,
)

# 5c. Try Internal auth token format
hdrs_internal = {
    "Authorization": f"Token token={rsc_token}",
    "Content-Type": "application/json",
}
test_cdm(
    "GET cluster/me (Token format)",
    CDM_IP,
    "api/v1/cluster/me",
    hdrs_internal,
)

# 5d. Try as cookie
test_cdm(
    "GET cluster/me (Cookie)",
    CDM_IP,
    "api/v1/cluster/me",
    {
        "Content-Type": "application/json",
        "Cookie": f"token={rsc_token}",
    },
)

# 6. Try POST session creation
print("\n\n--- POST /api/v1/session ---")
try:
    r = requests.post(
        f"https://{CDM_IP}/api/v1/session",
        json={
            "serviceAccountId": Config.RSC_CLIENT_ID,
            "secret": Config.RSC_CLIENT_SECRET,
        },
        headers={"Content-Type": "application/json"},
        verify=False,
        timeout=15,
    )
    print(f"  Status: {r.status_code}")
    ct = r.headers.get("Content-Type", "")
    if "json" in ct:
        print(f"  JSON: {json.dumps(r.json(), indent=2)[:500]}")
    else:
        print(f"  Body: {r.text[:300]}")
except Exception as e:
    print(f"  ERROR: {e}")

# 7. Try /api/v1/service_account/session
print("\n--- POST /api/v1/service_account/session ---")
try:
    r = requests.post(
        f"https://{CDM_IP}/api/v1/service_account/session",
        json={
            "serviceAccountId": Config.RSC_CLIENT_ID,
            "secret": Config.RSC_CLIENT_SECRET,
        },
        headers={"Content-Type": "application/json"},
        verify=False,
        timeout=15,
    )
    print(f"  Status: {r.status_code}")
    ct = r.headers.get("Content-Type", "")
    if "json" in ct:
        d = r.json()
        print(f"  JSON: {json.dumps(d, indent=2)[:500]}")
        if "token" in d:
            print(f"\n  *** CDM TOKEN OBTAINED! ***")
            cdm_token = d["token"]
            # Test it!
            test_cdm(
                "GET cluster/me (CDM token)",
                CDM_IP,
                "api/v1/cluster/me",
                {
                    "Authorization": f"Bearer {cdm_token}",
                    "Content-Type": "application/json",
                },
            )
    else:
        print(f"  Body: {r.text[:300]}")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)