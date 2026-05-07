#!/usr/bin/env python3
"""Test correct CDM REST API endpoint paths."""
import urllib3
urllib3.disable_warnings()
import json
import requests
from config import Config, setup_logging
setup_logging("INFO")
Config.validate()

# Auth to CDM directly (sh2-Cork)
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
    r = session.get(url, timeout=15,
                    params={"limit": 3})
    print(f"  {r.status_code}: ", end="")
    if r.status_code == 200:
        try:
            d = r.json()
            if isinstance(d, dict):
                total = d.get("total", "?")
                items = len(d.get("data", []))
                print(
                    f"OK (total: {total}, "
                    f"items: {items})"
                )
                if d.get("data"):
                    preview = json.dumps(
                        d["data"][0], indent=2
                    )[:300]
                    print(f"  Sample:\n{preview}")
            else:
                print(f"OK: {str(d)[:200]}")
        except Exception:
            print(f"OK: {r.text[:200]}")
    elif r.status_code == 404:
        print("NOT FOUND")
    else:
        print(f"{r.text[:100]}")
    print()


print("=" * 60)
print("CDM REST API ENDPOINT DISCOVERY")
print("=" * 60)
print()

# Exchange endpoints
test("Exchange v1", "api/v1/exchange/server")
test("Exchange internal", "api/internal/exchange/server")
test("Exchange internal 2", "api/internal/exchange_server")
test("Exchange dag v1", "api/v1/exchange/dag")
test("Exchange dag internal", "api/internal/exchange/dag")

# Active Directory endpoints
test("AD domain v1", "api/v1/active_directory/domain")
test("AD domain internal", "api/internal/active_directory/domain")
test("AD domain internal 2", "api/internal/active_directory_domain")
test("AD dc v1", "api/v1/active_directory/domain_controller")
test("AD dc internal", "api/internal/active_directory/domain_controller")

# Database endpoints (verify what works)
test("MSSQL instance v1", "api/v1/mssql/instance")
test("Oracle host", "api/internal/oracle/host")
test("Oracle db", "api/internal/oracle/db")
test("PostgreSQL inst", "api/internal/postgres/instance")
test("SAP HANA system", "api/internal/sap_hana/system")
test("MySQL instance", "api/internal/mysql/instance")
test("MongoDB source", "api/internal/mongo/source")
test("Db2 instance", "api/internal/db2/instance")

# Host endpoint
test("Hosts v1", "api/v1/host")

# VMware endpoints
test("vCenter v1", "api/v1/vmware/vcenter")
test("VMware host v1", "api/v1/vmware/host")

# Kubernetes
test("K8s cluster", "api/internal/kubernetes/cluster")

print("=" * 60)
print("DONE")
print("=" * 60)