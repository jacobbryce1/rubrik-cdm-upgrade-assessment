#!/usr/bin/env python3
"""Test if CDM session reuse causes auth failures."""
import urllib3
urllib3.disable_warnings()
import json
import requests
from config import Config, setup_logging
setup_logging("INFO")
Config.validate()

# Clusters to test
CLUSTERS = [
    ("sh2-Tokyo", "172.24.85.80"),
    ("sh2-CapeTown", "172.24.85.81"),
    ("sh2-Cork", "172.24.87.29"),
    ("sh1-Stuttgart", "172.24.75.81"),
]


def auth_cdm(name, ip, session):
    """Try CDM service account auth."""
    print(f"\n--- {name} ({ip}) ---")
    print(f"  Session headers: {dict(session.headers)}")
    try:
        resp = session.post(
            f"https://{ip}/api/v1/"
            f"service_account/session",
            json={
                "serviceAccountId": (
                    Config.RSC_CLIENT_ID
                ),
                "secret": (
                    Config.RSC_CLIENT_SECRET
                ),
            },
            timeout=10,
        )
        print(f"  Status: {resp.status_code}")
        if resp.status_code == 200:
            d = resp.json()
            token = d.get("token", "")
            print(
                f"  TOKEN: {token[:30]}..."
            )
            return token
        else:
            print(f"  Body: {resp.text[:200]}")
            return None
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


print("=" * 60)
print("TEST 1: Fresh session per cluster")
print("=" * 60)

for name, ip in CLUSTERS:
    # Fresh session each time
    session = requests.Session()
    session.verify = False
    session.headers.update(
        {"Content-Type": "application/json"}
    )
    auth_cdm(name, ip, session)

print("\n\n" + "=" * 60)
print("TEST 2: Reused session (simulates bug)")
print("=" * 60)

# Single shared session
shared_session = requests.Session()
shared_session.verify = False
shared_session.headers.update(
    {"Content-Type": "application/json"}
)

for name, ip in CLUSTERS:
    token = auth_cdm(name, ip, shared_session)
    if token:
        # Simulate what rsc_client does:
        # set the token in session headers
        shared_session.headers.update(
            {"Authorization": f"Bearer {token}"}
        )
        print(f"  -> Set Bearer token in session")

print("\n\n" + "=" * 60)
print("TEST 3: Reused session but CLEAR auth before each")
print("=" * 60)

shared_session2 = requests.Session()
shared_session2.verify = False
shared_session2.headers.update(
    {"Content-Type": "application/json"}
)

for name, ip in CLUSTERS:
    # Clear auth header before each auth attempt
    shared_session2.headers.pop(
        "Authorization", None
    )
    token = auth_cdm(name, ip, shared_session2)
    if token:
        shared_session2.headers.update(
            {"Authorization": f"Bearer {token}"}
        )
        print(f"  -> Set Bearer token in session")

print("\n\n" + "=" * 60)
print("DONE")
print("=" * 60)