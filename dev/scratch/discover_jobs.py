#!/usr/bin/env python3
"""Discover RSC GraphQL queries for running/queued jobs."""
import urllib3
urllib3.disable_warnings()
import json
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
hdrs = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

# Use Cork cluster ID for filtering
CID_CORK = "e711ef1b-83cb-4679-9ef7-44c4de751102"


def introspect(type_name):
    print(f"\n--- {type_name} fields ---")
    body = {
        "query": f'{{ __type(name: "{type_name}") '
                 f'{{ fields {{ name type {{ name kind '
                 f'ofType {{ name }} }} }} }} }}'
    }
    r = requests.post(
        url, json=body, headers=hdrs, timeout=30
    )
    d = r.json()
    t = (d.get("data") or {}).get("__type")
    if t and t.get("fields"):
        for f in sorted(
            t["fields"], key=lambda x: x["name"]
        ):
            tn = (
                f["type"].get("name")
                or (f["type"].get("ofType") or {}).get(
                    "name", ""
                )
                or f["type"].get("kind", "")
            )
            print(f"  {f['name']}: {tn}")
    else:
        print("  (not found)")


def test(name, query, variables=None):
    print(f"\n--- TEST: {name} ---")
    body = {"query": query}
    if variables:
        body["variables"] = variables
    r = requests.post(
        url, json=body, headers=hdrs, timeout=30
    )
    try:
        d = r.json()
        if (
            r.status_code == 200
            and "data" in d
            and d["data"]
        ):
            preview = json.dumps(
                d["data"], indent=2
            )[:2000]
            print(f"  OK:\n{preview}")
            return d["data"]
        else:
            errs = d.get("errors", [])
            msg = (
                errs[0].get("message", "")[:300]
                if errs
                else r.text[:300]
            )
            print(f"  FAIL: {msg}")
            return None
    except Exception:
        print(f"  FAIL: {r.text[:300]}")
        return None


print("=" * 60)
print("RSC GRAPHQL — JOBS/ACTIVITY DISCOVERY")
print("=" * 60)

# Find activity-related query roots
body = {
    "query": """
        { __schema { queryType {
            fields { name }
        } } }
    """
}
r = requests.post(
    url, json=body, headers=hdrs, timeout=30
)
d = r.json()
fields = [
    f["name"] for f in
    d.get("data", {}).get("__schema", {}).get(
        "queryType", {}
    ).get("fields", [])
]
activity_fields = sorted([
    f for f in fields
    if any(
        k in f.lower()
        for k in [
            "activity", "event", "job",
            "task", "running",
        ]
    )
])
print(f"\nActivity-related query roots "
      f"({len(activity_fields)}):")
for f in activity_fields:
    print(f"  {f}")

# Introspect key types
introspect("ActivitySeries")
introspect("ActivitySeriesConnection")
introspect("ActivityConnection")
introspect("ActivityStatusEnum")

# Test 1: activitySeriesConnection — basic
test("activitySeriesConnection basic", """
    { activitySeriesConnection(first: 3) {
        count
        edges { node {
            id
            activitySeriesId
            lastActivityType
            lastActivityStatus
            objectName
            startTime
            lastUpdated
            cluster { id name }
        } }
    } }
""")

# Test 2: activitySeriesConnection with status filter
test("activitySeriesConnection Running", """
    { activitySeriesConnection(
        first: 5
        filters: {
            lastActivityStatus: [RUNNING]
        }
    ) {
        count
        edges { node {
            id
            activitySeriesId
            lastActivityType
            lastActivityStatus
            objectName
            startTime
            cluster { id name }
        } }
    } }
""")

# Test 3: activitySeriesConnection Queued
test("activitySeriesConnection Queued", """
    { activitySeriesConnection(
        first: 5
        filters: {
            lastActivityStatus: [QUEUED]
        }
    ) {
        count
        edges { node {
            id
            lastActivityType
            lastActivityStatus
            objectName
            cluster { id name }
        } }
    } }
""")

# Test 4: Filter by cluster
test("activitySeriesConnection by cluster", f"""
    {{ activitySeriesConnection(
        first: 5
        filters: {{
            lastActivityStatus: [RUNNING]
            cluster: {{
                id: ["{CID_CORK}"]
            }}
        }}
    ) {{
        count
        edges {{ node {{
            id
            lastActivityType
            lastActivityStatus
            objectName
            cluster {{ id name }}
        }} }}
    }} }}
""")

# Test 5: activities (different query root)
test("activities basic", """
    { activities(first: 3) {
        count
        edges { node {
            id
            activityType
            status
            objectName
            startTime
        } }
    } }
""")

# Test 6: activitySeries (single lookup test)
test("activitySeries query", """
    { activitySeries(
        input: { activitySeriesId: "dummy" }
    ) {
        id
    } }
""")

# Test 7: Try different filter format
test("activitySeriesConnection alt filter", """
    { activitySeriesConnection(
        first: 1
        filters: {
            lastActivityStatus: [RUNNING, QUEUED]
        }
    ) {
        count
    } }
""")

# Test 8: activitySeriesGroupByConnection
test("activitySeriesGroupByConnection", """
    { activitySeriesGroupByConnection(
        first: 10
        groupBy: LastActivityStatus
    ) {
        count
        edges { node {
            groupByInfo {
                ... on ActivityStatusGroupByInfo {
                    status
                }
            }
            activeObjectCount
        } }
    } }
""")

# Test 9: taskDetailConnection
test("taskDetailConnection", """
    { taskDetailConnection(first: 3) {
        count
        edges { node {
            id
            status
            objectName
            objectType
            clusterUuid
        } }
    } }
""")

# Test 10: Check taskchain
test("taskchainInfo", """
    { taskchainInfo(
        clusterUuid: "e711ef1b-83cb-4679-9ef7-44c4de751102"
    ) {
        taskchainId
        state
    } }
""")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)