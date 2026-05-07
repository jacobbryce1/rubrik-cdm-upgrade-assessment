"""
Section 4: Database Platform Versions
MSSQL, Oracle, PostgreSQL, SAP HANA, MySQL,
MongoDB, Db2

Strategy:
  - MSSQL: CDM api/v1/mssql/instance (works),
            GraphQL fallback
  - Oracle: CDM api/internal/oracle/* (works),
            GraphQL fallback
  - PostgreSQL: GraphQL ONLY (CDM 404) [2] [3]
  - SAP HANA: GraphQL ONLY (CDM 404) [2] [3]
  - MySQL: GraphQL ONLY (CDM 404) [2] [3]
  - MongoDB: GraphQL ONLY (CDM 404) [2] [3]
  - Db2: GraphQL ONLY (CDM 404) [2] [3]
"""
import re
import logging
from typing import Dict, List, Optional
from collectors import CollectionResult

logger = logging.getLogger(__name__)


def collect(client) -> CollectionResult:
    logger.info(
        "Collecting Database Platform Versions..."
    )
    result = CollectionResult(
        section_name="Database Platform Versions",
        section_id="04_databases",
    )

    from config import Config
    cluster_id = Config.get_current_cluster_id()

    db_details: List[Dict] = []
    cdm_available = client.cdm_available

    if cdm_available:
        logger.info(
            "  CDM Direct available "
            "(MSSQL + Oracle only)"
        )
    else:
        logger.info(
            "  Using GraphQL only"
        )

    # =================================================================
    # A) MSSQL — CDM + GraphQL fallback
    # CDM: api/v1/mssql/instance — WORKS
    # =================================================================
    logger.info("  [A] MSSQL...")
    if cdm_available:
        _collect_mssql_cdm(
            client, cluster_id, db_details
        )
    # Always fall back if CDM found nothing
    mssql_count = len([
        d for d in db_details
        if d["category"] == "MSSQL Instance"
    ])
    if mssql_count == 0:
        _collect_mssql_graphql(
            client, cluster_id, db_details
        )

    # =================================================================
    # B) Oracle — CDM + GraphQL fallback
    # CDM: api/internal/oracle/* — WORKS
    # =================================================================
    logger.info("  [B] Oracle...")
    if cdm_available:
        _collect_oracle_cdm(
            client, cluster_id, db_details
        )
    oracle_count = len([
        d for d in db_details
        if d["category"] == "Oracle Database"
    ])
    if oracle_count == 0:
        _collect_oracle_graphql(
            client, cluster_id, db_details
        )

    # =================================================================
    # C) PostgreSQL — GraphQL ONLY
    # CDM endpoint returns 404 [2] [3]
    # =================================================================
    logger.info("  [C] PostgreSQL...")
    _collect_postgres_graphql(
        client, cluster_id, db_details
    )

    # =================================================================
    # D) SAP HANA — GraphQL ONLY
    # CDM endpoint returns 404 [2] [3]
    # =================================================================
    logger.info("  [D] SAP HANA...")
    _collect_saphana_graphql(
        client, cluster_id, db_details
    )

    # =================================================================
    # E) MySQL — GraphQL ONLY
    # CDM endpoint returns 404 [2] [3]
    # =================================================================
    logger.info("  [E] MySQL...")
    _collect_mysql_graphql(
        client, cluster_id, db_details
    )

    # =================================================================
    # F) MongoDB — GraphQL ONLY
    # CDM endpoint returns 404 [2] [3]
    # =================================================================
    logger.info("  [F] MongoDB...")
    _collect_mongodb_graphql(
        client, cluster_id, db_details
    )

    # =================================================================
    # G) Db2 — GraphQL ONLY
    # CDM endpoint returns 404 [2] [3]
    # =================================================================
    logger.info("  [G] Db2...")
    _collect_db2_graphql(
        client, cluster_id, db_details
    )

    result.details = db_details
    result.raw_data["database_details"] = db_details

    # Summary
    mssql_inst = [
        d for d in db_details
        if d["category"] == "MSSQL Instance"
    ]
    mssql_versions: Dict[str, int] = {}
    for inst in mssql_inst:
        prod = inst.get("sql_product", "Unknown")
        mssql_versions[prod] = (
            mssql_versions.get(prod, 0) + 1
        )

    result.summary = {
        "data_source": (
            "CDM+GraphQL"
            if cdm_available
            else "GraphQL"
        ),
        "total_mssql_instances": len(mssql_inst),
        "total_mssql_databases": len([
            d for d in db_details
            if d["category"] == "MSSQL Database"
        ]),
        "mssql_version_breakdown": mssql_versions,
        "total_oracle_dbs": len([
            d for d in db_details
            if d["category"] == "Oracle Database"
        ]),
        "total_oracle_hosts": len([
            d for d in db_details
            if d["category"] == "Oracle Host"
        ]),
        "total_postgresql": len([
            d for d in db_details
            if "PostgreSQL" in d["category"]
        ]),
        "total_sap_hana": len([
            d for d in db_details
            if "SAP HANA" in d["category"]
        ]),
        "total_mysql": len([
            d for d in db_details
            if "MySQL" in d["category"]
        ]),
        "total_mongodb": len([
            d for d in db_details
            if "MongoDB" in d["category"]
        ]),
        "total_db2": len([
            d for d in db_details
            if "Db2" in d["category"]
        ]),
        "total_database_objects": len(db_details),
    }

    logger.info(
        f"  Total database objects: "
        f"{len(db_details)}"
    )
    return result


# =============================================================
# CDM DIRECT API COLLECTORS (MSSQL + Oracle only)
# =============================================================

def _collect_mssql_cdm(
    client, cluster_id, db_details
):
    """MSSQL via CDM REST API."""
    try:
        instances = client.cdm_direct_get_paginated(
            "api/v1/mssql/instance", limit=200
        )
        if instances is None:
            logger.debug(
                "  MSSQL CDM: no data"
            )
            return

        seen = set()
        for inst in instances:
            name = inst.get("name", "")
            version = inst.get(
                "version", "Unknown"
            )
            host_name = inst.get("rootName", "")
            inst_id = inst.get("id", "")

            if inst_id in seen:
                continue
            seen.add(inst_id)

            db_details.append({
                "category": "MSSQL Instance",
                "object_name": name,
                "host_name": host_name,
                "platform_version": version,
                "sql_product": (
                    _mssql_version_to_name(version)
                ),
                "connection_status": inst.get(
                    "status", "N/A"
                ),
            })

        logger.info(
            f"    MSSQL (CDM): "
            f"{len(seen)} instances"
        )
    except Exception as e:
        logger.debug(
            f"  MSSQL CDM failed: {e}"
        )


def _collect_oracle_cdm(
    client, cluster_id, db_details
):
    """Oracle via CDM REST API."""
    try:
        hosts = client.cdm_direct_get_paginated(
            "api/internal/oracle/host", limit=200
        )
        if hosts is None:
            logger.debug(
                "  Oracle CDM: no data"
            )
            return

        seen_hosts = set()
        for host in hosts:
            name = host.get("name", "")
            if name in seen_hosts:
                continue
            seen_hosts.add(name)
            db_details.append({
                "category": "Oracle Host",
                "object_name": name,
                "platform_version": host.get(
                    "oracleVersion", "Unknown"
                ),
                "os_type": host.get(
                    "operatingSystemType", "N/A"
                ),
                "connection_status": host.get(
                    "status", "N/A"
                ),
            })

        dbs = client.cdm_direct_get_paginated(
            "api/internal/oracle/db", limit=200
        )
        oracle_count = 0
        if dbs:
            for db in dbs:
                if db.get("isRelic"):
                    continue
                oracle_count += 1
                db_details.append({
                    "category": "Oracle Database",
                    "object_name": db.get(
                        "name", ""
                    ),
                    "host_name": db.get(
                        "hostName", ""
                    ),
                    "platform_version": db.get(
                        "oracleVersion", "Unknown"
                    ),
                    "db_unique_name": db.get(
                        "dbUniqueName", ""
                    ),
                    "is_rac": db.get(
                        "isRac", False
                    ),
                    "sla_domain": db.get(
                        "effectiveSlaDomainName",
                        "N/A",
                    ),
                })

        logger.info(
            f"    Oracle (CDM): "
            f"{len(seen_hosts)} hosts, "
            f"{oracle_count} databases"
        )
    except Exception as e:
        logger.debug(
            f"  Oracle CDM failed: {e}"
        )


# =============================================================
# GRAPHQL COLLECTORS
# =============================================================

def _collect_mssql_graphql(
    client, cluster_id, db_details
):
    """MSSQL via RSC GraphQL."""
    try:
        data = client.graphql("""
            { mssqlDatabases(first: 500) {
                count
                edges { node {
                    id name version
                    recoveryModel copyOnly
                    isInAvailabilityGroup
                    isRelic isOnline
                    effectiveSlaDomain { name }
                    physicalPath {
                        name objectType
                    }
                    cluster { id }
                } }
            } }
        """)
        conn = data.get(
            "mssqlDatabases", {}
        ) or {}
        seen = set()
        db_count = 0
        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            if (
                (node.get("cluster", {}) or {})
                .get("id") != cluster_id
            ):
                continue
            if node.get("isRelic"):
                continue

            version = node.get(
                "version", "Unknown"
            )
            sla = (
                node.get(
                    "effectiveSlaDomain", {}
                ) or {}
            ).get("name", "N/A")
            path = (
                node.get("physicalPath", [])
                or []
            )
            host_name = ""
            instance_name = ""
            for p in path:
                pt = p.get("objectType", "")
                if "Host" in pt or "HOST" in pt:
                    host_name = p.get("name", "")
                elif (
                    "Instance" in pt
                    or "INSTANCE" in pt
                ):
                    instance_name = p.get(
                        "name", ""
                    )

            key = (
                f"{host_name}|{instance_name}"
                f"|{version}"
            )
            if key not in seen and instance_name:
                seen.add(key)
                db_details.append({
                    "category": "MSSQL Instance",
                    "object_name": instance_name,
                    "host_name": host_name,
                    "platform_version": version,
                    "sql_product": (
                        _mssql_version_to_name(
                            version
                        )
                    ),
                    "connection_status": "Connected",
                })

            db_count += 1
            db_details.append({
                "category": "MSSQL Database",
                "object_name": node.get(
                    "name", ""
                ),
                "host_name": host_name,
                "instance_name": instance_name,
                "platform_version": version,
                "sql_product": (
                    _mssql_version_to_name(version)
                ),
                "recovery_model": node.get(
                    "recoveryModel", "N/A"
                ),
                "sla_domain": sla,
            })

        logger.info(
            f"    MSSQL (GraphQL): "
            f"{len(seen)} instances, "
            f"{db_count} databases"
        )
    except Exception as e:
        logger.warning(
            f"  MSSQL GraphQL failed: {e}"
        )


def _collect_oracle_graphql(
    client, cluster_id, db_details
):
    """Oracle via RSC GraphQL."""
    try:
        data = client.graphql("""
            { oracleDatabases(first: 500) {
                count
                edges { node {
                    id name dbUniqueName
                    numInstances dataGuardType
                    archiveLogMode osType isRelic
                    effectiveSlaDomain { name }
                    physicalPath {
                        name objectType
                    }
                    cluster { id }
                } }
            } }
        """)
        conn = data.get(
            "oracleDatabases", {}
        ) or {}
        oracle_count = 0
        seen_hosts = set()
        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            if (
                (node.get("cluster", {}) or {})
                .get("id") != cluster_id
            ):
                continue
            if node.get("isRelic"):
                continue

            sla = (
                node.get(
                    "effectiveSlaDomain", {}
                ) or {}
            ).get("name", "N/A")
            path = (
                node.get("physicalPath", [])
                or []
            )
            host_name = ""
            for p in path:
                if "Host" in p.get(
                    "objectType", ""
                ):
                    host_name = p.get("name", "")

            os_type = str(
                node.get("osType", "Unknown")
            )
            oracle_version = (
                _extract_oracle_version(
                    node.get("name", ""),
                    host_name,
                )
            )

            if (
                host_name
                and host_name not in seen_hosts
            ):
                seen_hosts.add(host_name)
                db_details.append({
                    "category": "Oracle Host",
                    "object_name": host_name,
                    "platform_version": (
                        oracle_version
                    ),
                    "os_type": os_type,
                })

            oracle_count += 1
            db_details.append({
                "category": "Oracle Database",
                "object_name": node.get(
                    "name", ""
                ),
                "host_name": host_name,
                "platform_version": (
                    oracle_version
                ),
                "db_unique_name": node.get(
                    "dbUniqueName", ""
                ),
                "sla_domain": sla,
            })

        logger.info(
            f"    Oracle (GraphQL): "
            f"{oracle_count} databases, "
            f"{len(seen_hosts)} hosts"
        )
    except Exception as e:
        logger.warning(
            f"  Oracle GraphQL failed: {e}"
        )


def _collect_postgres_graphql(
    client, cluster_id, db_details
):
    """PostgreSQL via GraphQL — cluster name only."""
    try:
        data = client.graphql("""
            { postgreSQLDbClusters(first: 200) {
                count
                edges { node {
                    id name cluster { id }
                } }
            } }
        """)
        conn = data.get(
            "postgreSQLDbClusters", {}
        ) or {}
        pg_count = 0
        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            if (
                (node.get("cluster", {}) or {})
                .get("id") != cluster_id
            ):
                continue
            pg_count += 1
            db_details.append({
                "category": "PostgreSQL Cluster",
                "object_name": node.get(
                    "name", ""
                ),
                "host_name": node.get(
                    "name", ""
                ),
                "platform_version": (
                    "Version unavailable via API"
                ),
            })

        logger.info(
            f"    PostgreSQL (GraphQL): "
            f"{pg_count} clusters"
        )
    except Exception as e:
        logger.warning(
            f"  PostgreSQL GraphQL failed: {e}"
        )


def _collect_saphana_graphql(
    client, cluster_id, db_details
):
    """SAP HANA via GraphQL — name only."""
    try:
        data = client.graphql("""
            { sapHanaSystems(first: 200) {
                count
                edges { node {
                    id name cluster { id }
                } }
            } }
        """)
        conn = data.get(
            "sapHanaSystems", {}
        ) or {}
        sap_count = 0
        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            if (
                (node.get("cluster", {}) or {})
                .get("id") != cluster_id
            ):
                continue
            sap_count += 1
            db_details.append({
                "category": "SAP HANA System",
                "object_name": node.get(
                    "name", ""
                ),
                "platform_version": (
                    "Version unavailable via API"
                ),
            })

        logger.info(
            f"    SAP HANA (GraphQL): "
            f"{sap_count} systems"
        )
    except Exception as e:
        logger.warning(
            f"  SAP HANA GraphQL failed: {e}"
        )


def _collect_mysql_graphql(
    client, cluster_id, db_details
):
    """MySQL via GraphQL — per-instance metadata lookup."""
    try:
        data = client.graphql("""
            { mysqlInstances(first: 200) {
                count
                edges { node {
                    id name cluster { id }
                } }
            } }
        """)
        conn = data.get(
            "mysqlInstances", {}
        ) or {}
        mysql_count = 0
        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            if (
                (node.get("cluster", {}) or {})
                .get("id") != cluster_id
            ):
                continue

            inst_id = node.get("id", "")
            inst_name = node.get("name", "")

            # Per-instance metadata lookup
            version = "Unknown"
            try:
                detail = client.graphql(f"""
                    {{ mysqlInstance(
                        fid: "{inst_id}"
                    ) {{
                        metadata {{ version }}
                    }} }}
                """)
                mi = (
                    detail.get(
                        "mysqlInstance", {}
                    ) or {}
                )
                md = mi.get("metadata", {}) or {}
                if md.get("version"):
                    version = md["version"]
            except Exception:
                pass

            mysql_count += 1
            db_details.append({
                "category": "MySQL Instance",
                "object_name": inst_name,
                "host_name": inst_name,
                "platform_version": version,
            })

        logger.info(
            f"    MySQL (GraphQL): "
            f"{mysql_count} instances"
        )
    except Exception as e:
        logger.warning(
            f"  MySQL GraphQL failed: {e}"
        )


def _collect_mongodb_graphql(
    client, cluster_id, db_details
):
    """MongoDB via GraphQL — sourceType only."""
    try:
        data = client.graphql("""
            { mongoSources(first: 200) {
                count
                edges { node {
                    id name sourceType
                    status isRelic
                    cluster { id }
                } }
            } }
        """)
        conn = data.get(
            "mongoSources", {}
        ) or {}
        mongo_count = 0
        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            if (
                (node.get("cluster", {}) or {})
                .get("id") != cluster_id
            ):
                continue
            if node.get("isRelic"):
                continue
            mongo_count += 1
            db_details.append({
                "category": "MongoDB Source",
                "object_name": node.get(
                    "name", ""
                ),
                "platform_version": str(
                    node.get("sourceType", "N/A")
                ),
                "connection_status": str(
                    node.get("status", "N/A")
                ),
            })

        logger.info(
            f"    MongoDB (GraphQL): "
            f"{mongo_count} sources"
        )
    except Exception as e:
        logger.warning(
            f"  MongoDB GraphQL failed: {e}"
        )


def _collect_db2_graphql(
    client, cluster_id, db_details
):
    """Db2 via GraphQL — instanceType only."""
    try:
        data = client.graphql("""
            { db2Instances(first: 200) {
                count
                edges { node {
                    id name instanceType
                    status
                    cluster { id }
                } }
            } }
        """)
        conn = data.get(
            "db2Instances", {}
        ) or {}
        db2_count = 0
        for edge in conn.get("edges", []):
            node = edge.get("node", {}) or {}
            if (
                (node.get("cluster", {}) or {})
                .get("id") != cluster_id
            ):
                continue
            db2_count += 1
            db_details.append({
                "category": "Db2 Instance",
                "object_name": node.get(
                    "name", ""
                ),
                "platform_version": str(
                    node.get(
                        "instanceType", "N/A"
                    )
                ),
                "connection_status": str(
                    node.get("status", "N/A")
                ),
            })

        logger.info(
            f"    Db2 (GraphQL): "
            f"{db2_count} instances"
        )
    except Exception as e:
        logger.warning(
            f"  Db2 GraphQL failed: {e}"
        )


# =============================================================
# HELPERS
# =============================================================

def _mssql_version_to_name(version: str) -> str:
    if not version:
        return "Unknown"
    ver_map = {
        "16.": "SQL Server 2022",
        "15.": "SQL Server 2019",
        "14.": "SQL Server 2017",
        "13.": "SQL Server 2016",
        "12.": "SQL Server 2014",
        "11.": "SQL Server 2012",
    }
    for prefix, name in ver_map.items():
        if version.startswith(prefix):
            return name
    return f"SQL Server ({version})"


def _extract_oracle_version(
    db_name: str, host_name: str
) -> str:
    for name in [db_name, host_name]:
        match = re.search(
            r'ora(\d+)', name.lower()
        )
        if match:
            return match.group(1)
    return "Unknown"