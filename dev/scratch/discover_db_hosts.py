#!/usr/bin/env python3
"""Discover host mapping for all database workloads."""
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


def test(name, query):
    print(f"\n--- {name} ---")
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
print("DATABASE HOST MAPPING DISCOVERY")
print("=" * 60)

# MSSQL - physicalPath should have host
test("MSSQL with physicalPath and host details", """
    { mssqlDatabases(first: 3) {
        edges { node {
            id name version
            physicalPath {
                fid name objectType
            }
            cluster { id name }
        } }
    } }
""")

# Oracle - has osType, try physicalPath for host
test("Oracle with host details", """
    { oracleDatabases(first: 3) {
        edges { node {
            id name dbUniqueName osType
            physicalPath {
                fid name objectType
            }
            cluster { id name }
        } }
    } }
""")

# PostgreSQL - fixed query without isRelic
test("PostgreSQL with physicalPath", """
    { postgreSQLDatabases(first: 3) {
        edges { node {
            id name
            physicalPath {
                fid name objectType
            }
            cluster { id name }
        } }
    } }
""")

# PostgreSQL DB Clusters - might have more detail
test("PostgreSQL DB Clusters", """
    { postgreSQLDbClusters(first: 3) {
        count
        edges { node {
            id name
            cluster { id name }
        } }
    } }
""")

# SAP HANA - check for host mapping
test("SAP HANA with physicalPath", """
    { sapHanaSystems(first: 3) {
        edges { node {
            id name
            physicalPath {
                fid name objectType
            }
            cluster { id name }
        } }
    } }
""")

# SAP HANA Database
test("SAP HANA Databases", """
    { sapHanaDatabases(first: 3) {
        count
        edges { node {
            id name
            cluster { id name }
        } }
    } }
""")

# MongoDB - has hostDetails and sourceNodes
test("MongoDB with host details", """
    { mongoSources(first: 3) {
        edges { node {
            id name sourceType status
            hostDetails { hostname port }
            sourceNodes { hostname port }
            physicalPath {
                fid name objectType
            }
            cluster { id name }
        } }
    } }
""")

# MySQL - check for host mapping
test("MySQL instances with physicalPath", """
    { mysqlInstances(first: 3) {
        edges { node {
            id name
            physicalPath {
                fid name objectType
            }
            cluster { id name }
        } }
    } }
""")

# MySQL databases with physicalPath
test("MySQL databases with physicalPath", """
    { mysqlDatabases(first: 3) {
        edges { node {
            id name
            physicalPath {
                fid name objectType
            }
            cluster { id name }
        } }
    } }
""")

# Db2 instances with host
test("Db2 instances with physicalPath", """
    { db2Instances(first: 3) {
        edges { node {
            id name instanceType status
            physicalPath {
                fid name objectType
            }
            cluster { id name }
        } }
    } }
""")

# Db2 databases with host
test("Db2 databases with physicalPath", """
    { db2Databases(first: 3) {
        edges { node {
            id name db2DbType status
            physicalPath {
                fid name objectType
            }
            cluster { id name }
        } }
    } }
""")

# Kubernetes clusters with version and distribution
test("Kubernetes with version", """
    { kubernetesClusters(first: 3) {
        edges { node {
            id name
            k8sVersion distribution status
            cluster { id name }
        } }
    } }
""")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)