#!/usr/bin/env python3
"""Discover remaining field names."""
import urllib3
urllib3.disable_warnings()
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
    print(f"\n--- {type_name} ---")
    body = {"query": f'{{ __type(name: "{type_name}") {{ fields {{ name }} }} }}'}
    r = requests.post(url, json=body, headers=hdrs, timeout=30)
    d = r.json()
    t = (d.get("data") or {}).get("__type")
    if t and t.get("fields"):
        for f in sorted(t["fields"], key=lambda x: x["name"]):
            print(f"  {f['name']}")
    else:
        print("  (not found)")

introspect("AboutInformation")
introspect("ScvmmInfo")
introspect("RefreshableObjectConnectionStatus")
introspect("ReplicationSpecV2")
introspect("HourlySnapshotSchedule")
introspect("DailySnapshotSchedule")
introspect("WeeklySnapshotSchedule")
introspect("MonthlySnapshotSchedule")
introspect("SnapshotSchedule")
introspect("ClusterArchivalSpec")

print("\n--- Done ---")