#!/usr/bin/env python3
"""Test correct CDM endpoint paths for upgrade blocker checks."""
import urllib3
urllib3.disable_warnings()
import json
import requests
from config import Config, setup_logging
setup_logging("INFO")
Config.validate()

# Auth to CDM (sh2-Cork)
CDM_IP = "172.24.87.29"
session = requests.Session()
session.verify = False
session.headers.update(
    {"Content-Type": "application/json"}
)
resp = session.post(
    f"https://{CDM_IP}/api/v1/"
    f"service_account/session",
    json={
        "serviceAccountId": Config.RSC_CLIENT_ID,
        "secret": Config.RSC_CLIENT_SECRET,
    },
    timeout=10,
)
token = resp.json()["token"]
session.headers.update(
    {"Authorization": f"Bearer {token}"}
)
print(f"CDM Token obtained for {CDM_IP}\n")


def test(name, endpoint):
    print(f"--- {name} ---")
    url = f"https://{CDM_IP}/{endpoint}"
    r = session.get(url, timeout=15, params={"limit": 3})
    print(f"  {r.status_code}: ", end="")
    if r.status_code == 200:
        try:
            d = r.json()
            if isinstance(d, dict):
                total = d.get("total", len(d.get("data", [])))
                print(f"OK (total: {total})")
                if d.get("data") and len(d["data"]) > 0:
                    preview = json.dumps(d["data"][0], indent=2)[:400]
                    print(f"  Sample:\n{preview}")
                elif not d.get("data"):
                    preview = json.dumps(d, indent=2)[:400]
                    print(f"  Response:\n{preview}")
            elif isinstance(d, list):
                print(f"OK (list, {len(d)} items)")
                if d:
                    preview = json.dumps(d[0], indent=2)[:400]
                    print(f"  Sample:\n{preview}")
            else:
                print(f"OK: {str(d)[:200]}")
        except Exception:
            print(f"OK: {r.text[:200]}")
    elif r.status_code == 404:
        print("NOT FOUND")
    else:
        print(f"{r.text[:150]}")
    print()


print("=" * 60)
print("UPGRADE BLOCKER ENDPOINT DISCOVERY")
print("=" * 60)

# API Token endpoints
test("API tokens internal", "api/internal/api_token")
test("API tokens v1", "api/v1/api_token")
test("API tokens session", "api/v1/session")
test("API tokens auth", "api/internal/authorization/api_token")

# Service Account endpoints
test("Service acct v1", "api/v1/service_account")
test("Service acct internal", "api/internal/service_account")
test("Service acct session v1", "api/v1/service_account/session")

# Event/Job endpoints
test("Events internal", "api/internal/event_series")
test("Events v1", "api/v1/event_series")
test("Events internal status", "api/internal/event_series?status=Active")
test("Events v1 status", "api/v1/event/series")
test("Activity v1", "api/v1/activity")
test("Job internal", "api/internal/job")
test("Notification", "api/internal/notification")

# Live Mount endpoints (verify these still work)
test("VMware mounts", "api/v1/vmware/vm/snapshot/mount")
test("MSSQL mounts", "api/v1/mssql/db/mount")
test("Oracle mounts", "api/internal/oracle/db/mount")
test("MV exports", "api/internal/managed_volume/snapshot/export")

# Archive/Replication endpoints
test("Archive locations", "api/internal/archive/location")
test("Replication targets", "api/internal/replication/target")
test("Replication sources", "api/internal/replication/source")

# Cluster health endpoints
test("System status", "api/internal/cluster/me/system_status")
test("Support tunnel", "api/internal/node/me/support_tunnel")
test("DNS", "api/internal/cluster/me/dns_nameserver")
test("NTP", "api/internal/cluster/me/ntp_server")

print("=" * 60)
print("DONE")
print("=" * 60)