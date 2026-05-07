"""
Section 11: Upgrade Blockers & Risk Assessment

CDM REST API checks (confirmed working):
  ✅ Active Live Mounts (VMware, MSSQL, Oracle, MV)
  ✅ System Status
  ✅ Support Tunnel
  ✅ DNS/NTP Configuration
  ✅ Archive Locations
  ✅ Replication Targets/Sources

CDM REST API blocked by design:
  ❌ API Tokens — no public endpoint exists
  ❌ Service Accounts — endpoint exists but is
     marked x-rk-block-api-tokens: true, blocking
     ALL Bearer token auth by design (not RBAC)

RSC GraphQL checks:
  ✅ Running/Queued Jobs (activitySeriesConnection)
  ✅ RSC Connectivity/Token Health
  ✅ Version-Specific Risk Warnings
"""
import logging
from typing import Dict, List
from collectors import CollectionResult
from compatibility_matrix import (
    parse_major_version,
    version_to_float,
)

logger = logging.getLogger(__name__)


def collect(client) -> CollectionResult:
    logger.info(
        "Checking Upgrade Blockers & Risks..."
    )
    result = CollectionResult(
        section_name=(
            "Upgrade Blockers & Risk Assessment"
        ),
        section_id="11_upgrade_blockers",
    )

    from config import Config
    target_cdm = (
        Config.TARGET_CDM_VERSION or "9.5"
    )
    cluster_id = Config.get_current_cluster_id()
    cdm_available = client.cdm_available

    target_float = version_to_float(
        parse_major_version(target_cdm)
    )

    findings: List[Dict] = []

    # =================================================================
    # A) CDM SERVICE ACCOUNTS & API TOKENS
    # MANUAL CHECK ONLY
    #
    # CDM service account management API is marked
    # x-rk-block-api-tokens: true — ALL Bearer
    # token auth is blocked by the CDM framework,
    # regardless of role/permissions. This is by
    # design, not a permissions issue.
    #
    # API tokens have no public REST endpoint at
    # all — they live in internal tables only.
    #
    # The only way to check is via CDM UI:
    #   Settings > Users > Service Accounts
    #   Settings > API Tokens
    # =================================================================
    logger.info(
        "  [A] CDM Service Accounts & "
        "API Tokens..."
    )
    if target_float >= 9.4:
        severity = (
            "WARNING"
            if target_float >= 9.5
            else "INFO"
        )

        msg = (
            "CDM service accounts and API "
            "tokens require manual verification "
            "— the CDM REST API blocks "
            "programmatic access to these "
            "endpoints for all token-based "
            "authentication by design "
            "(x-rk-block-api-tokens). "
        )
        if target_float >= 9.5:
            msg += (
                "CRITICAL FOR 9.5.1+: CDM "
                "service accounts "
                "(RBK10900072) and CDM API "
                "tokens (RBK10900073) BLOCK "
                "upgrades to 9.5.1+. Verify "
                "via CDM UI on each cluster: "
                "(1) Settings > Users > "
                "Service Accounts — remove "
                "all CDM-native service "
                "accounts. "
                "(2) Settings > API Tokens — "
                "remove all legacy API tokens. "
                "Migrate all automation to "
                "RSC service accounts before "
                "upgrading."
            )
        else:
            msg += (
                "CDM service accounts and "
                "API tokens are deprecated "
                "in 9.4 and will BLOCK "
                "upgrades to 9.5.1+. Check "
                "CDM UI: Settings > Users > "
                "Service Accounts and "
                "Settings > API Tokens. "
                "Plan migration to RSC "
                "service accounts."
            )

        findings.append({
            "category": (
                "CDM Service Accounts "
                "& API Tokens"
            ),
            "severity": severity,
            "message": msg,
            "detail": (
                "Manual verification required "
                "via CDM UI on each cluster "
                "(REST API blocked by design)"
            ),
            "remediation": (
                "On each CDM cluster UI: "
                "(1) Settings > Users > "
                "Service Accounts — remove "
                "CDM-native accounts. "
                "(2) Settings > API Tokens "
                "— remove legacy tokens. "
                "Migrate to RSC service "
                "accounts."
            ),
        })
        if severity == "WARNING":
            result.warnings.append(msg)
        else:
            result.info_messages.append(msg)
        logger.info(
            f"    Manual check required "
            f"({severity})"
        )

    # =================================================================
    # B) ACTIVE LIVE MOUNTS — BLOCKER
    # All mount endpoints confirmed working
    # =================================================================
    logger.info(
        "  [B] Checking Active Live Mounts..."
    )
    total_mounts = 0
    mount_details = []

    if cdm_available:
        mount_endpoints = [
            (
                "api/v1/vmware/vm/snapshot/mount",
                "VMware",
            ),
            (
                "api/v1/mssql/db/mount",
                "MSSQL",
            ),
            (
                "api/internal/oracle/db/mount",
                "Oracle",
            ),
            (
                "api/internal/"
                "managed_volume/snapshot/export",
                "MV Export",
            ),
        ]

        for endpoint, label in mount_endpoints:
            try:
                data = client.cdm_direct_get(
                    endpoint
                )
                if data and isinstance(
                    data, dict
                ):
                    mounts = data.get("data", [])
                    if (
                        isinstance(mounts, list)
                        and len(mounts) > 0
                    ):
                        count = len(mounts)
                        total_mounts += count
                        mount_details.append(
                            f"{count} {label}"
                        )
            except Exception:
                pass

        if total_mounts > 0:
            detail_str = ", ".join(mount_details)
            msg = (
                f"{total_mounts} active live "
                f"mount(s)/export(s) found "
                f"({detail_str}). All must be "
                f"dismounted before upgrade."
            )
            findings.append({
                "category": (
                    "Active Live Mounts"
                ),
                "severity": "BLOCKER",
                "message": msg,
                "detail": detail_str,
                "remediation": (
                    "Dismount all live mounts "
                    "and unexport all managed "
                    "volumes before upgrade."
                ),
            })
            result.blockers.append(msg)
            logger.info(
                f"    {total_mounts} active "
                f"mounts ({detail_str})"
            )
        else:
            logger.info(
                "    No active live mounts (OK)"
            )
    else:
        logger.info(
            "    Skipped (CDM Direct "
            "not available)"
        )

    # =================================================================
    # C) RUNNING/QUEUED JOBS
    # Uses RSC GraphQL activitySeriesConnection
    # =================================================================
    logger.info(
        "  [C] Checking Running/Queued Jobs..."
    )
    try:
        running_data = client.graphql("""
            {
                activitySeriesConnection(
                    first: 100
                    filters: {
                        lastActivityStatus: [
                            RUNNING
                        ]
                    }
                ) {
                    count
                    edges {
                        node {
                            lastActivityType
                            objectName
                            cluster { id }
                        }
                    }
                }
            }
        """)
        running_conn = (
            running_data.get(
                "activitySeriesConnection", {}
            ) or {}
        )
        running_total = running_conn.get(
            "count", 0
        )

        running_this = 0
        running_types: Dict[str, int] = {}
        for edge in running_conn.get(
            "edges", []
        ):
            node = edge.get("node", {}) or {}
            nc = node.get("cluster", {}) or {}
            if nc.get("id") == cluster_id:
                running_this += 1
                atype = node.get(
                    "lastActivityType", "Other"
                )
                running_types[atype] = (
                    running_types.get(atype, 0)
                    + 1
                )

        queued_data = client.graphql("""
            {
                activitySeriesConnection(
                    first: 100
                    filters: {
                        lastActivityStatus: [
                            QUEUED
                        ]
                    }
                ) {
                    count
                    edges {
                        node {
                            lastActivityType
                            objectName
                            cluster { id }
                        }
                    }
                }
            }
        """)
        queued_conn = (
            queued_data.get(
                "activitySeriesConnection", {}
            ) or {}
        )
        queued_total = queued_conn.get(
            "count", 0
        )

        queued_this = 0
        queued_types: Dict[str, int] = {}
        for edge in queued_conn.get(
            "edges", []
        ):
            node = edge.get("node", {}) or {}
            nc = node.get("cluster", {}) or {}
            if nc.get("id") == cluster_id:
                queued_this += 1
                atype = node.get(
                    "lastActivityType", "Other"
                )
                queued_types[atype] = (
                    queued_types.get(atype, 0)
                    + 1
                )

        if running_this > 0 or queued_this > 0:
            type_parts = []
            for t, c in sorted(
                running_types.items()
            ):
                type_parts.append(
                    f"{c} {t} running"
                )
            for t, c in sorted(
                queued_types.items()
            ):
                type_parts.append(
                    f"{c} {t} queued"
                )
            type_str = (
                ", ".join(type_parts)
                if type_parts else ""
            )

            msg = (
                f"{running_this} running and "
                f"{queued_this} queued job(s) "
                f"on this cluster"
            )
            if type_str:
                msg += f" ({type_str})"
            msg += (
                ". Allow jobs to complete or "
                "cancel before starting "
                "upgrade."
            )

            findings.append({
                "category": "Active Jobs",
                "severity": "WARNING",
                "message": msg,
                "detail": (
                    f"Running: {running_this}, "
                    f"Queued: {queued_this} "
                    f"(from sample of 100)"
                ),
                "remediation": (
                    "Wait for running jobs to "
                    "complete or cancel them "
                    "before upgrade."
                ),
            })
            result.warnings.append(msg)
            logger.info(
                f"    This cluster: "
                f"{running_this} running, "
                f"{queued_this} queued "
                f"(RSC total: "
                f"{running_total} running, "
                f"{queued_total} queued)"
            )
        else:
            logger.info(
                f"    No active jobs on this "
                f"cluster (RSC total: "
                f"{running_total} running, "
                f"{queued_total} queued)"
            )
    except Exception as e:
        logger.debug(
            f"    Job check failed: {e}"
        )

    # =================================================================
    # D) CLUSTER SYSTEM STATUS
    # =================================================================
    logger.info(
        "  [D] Checking System Status..."
    )
    if cdm_available:
        try:
            data = client.cdm_direct_get(
                "api/internal/cluster/me/"
                "system_status"
            )
            if data and isinstance(data, dict):
                status = data.get("status", "")
                if status.lower() != "ok":
                    msg = (
                        f"Cluster system status: "
                        f"'{status}'. Resolve "
                        f"before upgrading."
                    )
                    findings.append({
                        "category": (
                            "System Status"
                        ),
                        "severity": "BLOCKER",
                        "message": msg,
                        "detail": (
                            f"Status: {status}"
                        ),
                        "remediation": (
                            "Investigate and "
                            "resolve cluster "
                            "health issues."
                        ),
                    })
                    result.blockers.append(msg)
                    logger.info(
                        f"    System status: "
                        f"{status}"
                    )
                else:
                    logger.info(
                        "    System status: OK"
                    )
        except Exception as e:
            logger.debug(
                f"    System status: {e}"
            )
    else:
        logger.info(
            "    Skipped (CDM Direct "
            "not available)"
        )

    # =================================================================
    # E) SUPPORT TUNNEL STATUS
    # =================================================================
    logger.info(
        "  [E] Checking Support Tunnel..."
    )
    if cdm_available:
        try:
            data = client.cdm_direct_get(
                "api/internal/node/me/"
                "support_tunnel"
            )
            if data and isinstance(data, dict):
                tunnel = data.get(
                    "isTunnelEnabled", False
                )
                if not tunnel:
                    findings.append({
                        "category": (
                            "Support Tunnel"
                        ),
                        "severity": "INFO",
                        "message": (
                            "Support tunnel is "
                            "disabled. Consider "
                            "enabling before "
                            "upgrade for Rubrik "
                            "support access."
                        ),
                        "detail": (
                            "isTunnelEnabled: "
                            "False"
                        ),
                        "remediation": (
                            "Enable support "
                            "tunnel via CDM UI."
                        ),
                    })
                    result.info_messages.append(
                        "Support tunnel disabled"
                    )
                    logger.info(
                        "    Support tunnel: "
                        "DISABLED"
                    )
                else:
                    logger.info(
                        "    Support tunnel: "
                        "Enabled"
                    )
        except Exception as e:
            logger.debug(
                f"    Support tunnel: {e}"
            )
    else:
        logger.info(
            "    Skipped (CDM Direct "
            "not available)"
        )

    # =================================================================
    # F) DNS & NTP CONFIGURATION
    # =================================================================
    logger.info(
        "  [F] Checking DNS & NTP..."
    )
    if cdm_available:
        try:
            data = client.cdm_direct_get(
                "api/internal/cluster/me/"
                "dns_nameserver"
            )
            if data:
                dns_list = (
                    data if isinstance(data, list)
                    else []
                )
                if len(dns_list) == 0:
                    msg = (
                        "No DNS servers "
                        "configured."
                    )
                    findings.append({
                        "category": "DNS",
                        "severity": "WARNING",
                        "message": msg,
                        "detail": "0 DNS servers",
                        "remediation": (
                            "Configure DNS."
                        ),
                    })
                    result.warnings.append(msg)
                    logger.info(
                        "    DNS: NONE"
                    )
                else:
                    logger.info(
                        f"    DNS: "
                        f"{len(dns_list)} "
                        f"server(s)"
                    )
        except Exception as e:
            logger.debug(f"    DNS: {e}")

        try:
            data = client.cdm_direct_get(
                "api/internal/cluster/me/"
                "ntp_server"
            )
            if data and isinstance(data, dict):
                ntp_list = data.get("data", [])
                if len(ntp_list) == 0:
                    msg = (
                        "No NTP servers "
                        "configured. Time sync "
                        "is critical for "
                        "upgrade."
                    )
                    findings.append({
                        "category": "NTP",
                        "severity": "WARNING",
                        "message": msg,
                        "detail": "0 NTP servers",
                        "remediation": (
                            "Configure NTP."
                        ),
                    })
                    result.warnings.append(msg)
                    logger.info(
                        "    NTP: NONE"
                    )
                else:
                    logger.info(
                        f"    NTP: "
                        f"{len(ntp_list)} "
                        f"server(s)"
                    )
        except Exception as e:
            logger.debug(f"    NTP: {e}")
    else:
        logger.info(
            "    Skipped (CDM Direct "
            "not available)"
        )

    # =================================================================
    # G) ARCHIVE & REPLICATION TOPOLOGY
    # =================================================================
    logger.info(
        "  [G] Checking Archive & Replication..."
    )
    if cdm_available:
        try:
            data = client.cdm_direct_get(
                "api/internal/archive/location"
            )
            if data and isinstance(data, dict):
                locations = data.get("data", [])
                if locations:
                    for loc in locations:
                        loc_type = loc.get(
                            "locationType", ""
                        )
                        loc_name = loc.get(
                            "name", ""
                        )
                        findings.append({
                            "category": (
                                "Archive Location"
                            ),
                            "severity": "INFO",
                            "message": (
                                f"Archive: "
                                f"{loc_name} "
                                f"({loc_type})"
                            ),
                            "detail": loc_type,
                            "remediation": "",
                        })
                        result.info_messages.append(
                            f"Archive: "
                            f"{loc_name}"
                        )

                        if (
                            target_float >= 9.4
                            and (
                                "S3" in str(
                                    loc_type
                                ).upper()
                                or "AWS" in str(
                                    loc_type
                                ).upper()
                            )
                        ):
                            msg = (
                                f"AWS S3 archival "
                                f"target "
                                f"'{loc_name}'. "
                                f"CDM 9.4.1+ has "
                                f"known S3 issues."
                            )
                            findings.append({
                                "category": (
                                    "AWS S3 Risk"
                                ),
                                "severity": "INFO",
                                "message": msg,
                                "detail": (
                                    loc_name
                                ),
                                "remediation": (
                                    "Target "
                                    "9.4.2-p1+"
                                ),
                            })
                    logger.info(
                        f"    Archive: "
                        f"{len(locations)} "
                        f"location(s)"
                    )
                else:
                    logger.info(
                        "    Archive: None"
                    )
        except Exception as e:
            logger.debug(
                f"    Archive: {e}"
            )

        try:
            data = client.cdm_direct_get(
                "api/internal/replication/target"
            )
            if data and isinstance(data, dict):
                targets = data.get("data", [])
                if targets:
                    for t in targets:
                        t_name = t.get(
                            "targetClusterName",
                            "",
                        )
                        t_ver = t.get(
                            "targetClusterVersion",
                            "",
                        )
                        findings.append({
                            "category": (
                                "Replication "
                                "Target"
                            ),
                            "severity": "INFO",
                            "message": (
                                f"Replication: "
                                f"{t_name} "
                                f"v{t_ver}"
                            ),
                            "detail": (
                                f"{t_name} "
                                f"v{t_ver}"
                            ),
                            "remediation": "",
                        })
                        result.info_messages.append(
                            f"Replication: "
                            f"{t_name} v{t_ver}"
                        )
                    logger.info(
                        f"    Replication: "
                        f"{len(targets)} "
                        f"target(s)"
                    )
                else:
                    logger.info(
                        "    Replication: None"
                    )
        except Exception as e:
            logger.debug(
                f"    Replication: {e}"
            )
    else:
        logger.info(
            "    Skipped (CDM Direct "
            "not available)"
        )

    # =================================================================
    # H) RSC CONNECTIVITY / TOKEN HEALTH
    # =================================================================
    logger.info(
        "  [H] Checking RSC Connectivity..."
    )
    try:
        data = client.graphql("""
            query RSCHealth($id: UUID!) {
                cluster(clusterUuid: $id) {
                    passesConnectivityCheck
                    lastConnectionTime
                }
            }
        """, {"id": cluster_id})
        c = data.get("cluster", {}) or {}
        passes = c.get(
            "passesConnectivityCheck"
        )

        if passes is False:
            msg = (
                "Cluster FAILS RSC connectivity "
                "check. An expired RSC token "
                "can cause sync issues "
                "post-upgrade (especially "
                "from 9.2.2+). Resolve before "
                "upgrading."
            )
            findings.append({
                "category": (
                    "RSC Connectivity"
                ),
                "severity": "BLOCKER",
                "message": msg,
                "detail": (
                    "passesConnectivityCheck: "
                    "False"
                ),
                "remediation": (
                    "Re-register cluster with "
                    "RSC or refresh token."
                ),
            })
            result.blockers.append(msg)
            logger.info(
                "    RSC connectivity FAILED"
            )
        else:
            logger.info(
                "    RSC connectivity OK"
            )
    except Exception as e:
        logger.debug(
            f"    RSC health: {e}"
        )

    # =================================================================
    # I) VERSION-SPECIFIC RISK WARNINGS
    # =================================================================
    logger.info(
        "  [I] Checking version-specific "
        "risks..."
    )

    try:
        ver_data = client.graphql("""
            query ClusterVer($id: UUID!) {
                cluster(clusterUuid: $id) {
                    version type
                }
            }
        """, {"id": cluster_id})
        vc = ver_data.get("cluster", {}) or {}
        current_version = vc.get("version", "")
        cluster_type = vc.get("type", "")
    except Exception:
        current_version = ""
        cluster_type = ""

    current_float = version_to_float(
        parse_major_version(current_version)
    ) if current_version else 0.0

    # Cloud cluster disk type
    if (
        cluster_type == "Cloud"
        and target_float >= 9.5
    ):
        msg = (
            "Cloud cluster targeting 9.5.x. "
            "Verify Azure nodes use Premium "
            "SSD v2. Premium SSD v1 will "
            "cause pre-check failure."
        )
        findings.append({
            "category": (
                "Cloud Cluster Disks"
            ),
            "severity": "WARNING",
            "message": msg,
            "detail": (
                f"Type: {cluster_type}"
            ),
            "remediation": (
                "Shut down nodes and upgrade "
                "to Premium SSD v2."
            ),
        })
        result.warnings.append(msg)

    # Nutanix AHV (upgrading to 9.4.x)
    if (
        target_float >= 9.4
        and target_float < 9.5
        and current_float < 9.4
    ):
        try:
            nx = client.graphql("""
                { nutanixClusters(first: 1) {
                    count
                } }
            """)
            nx_count = (
                nx.get(
                    "nutanixClusters", {}
                ) or {}
            ).get("count", 0)
            if nx_count > 0:
                msg = (
                    "Nutanix AHV workloads "
                    "detected. CDM 9.4.x has "
                    "known regressions. "
                    "Ensure target is "
                    "9.4.3-p3+."
                )
                findings.append({
                    "category": (
                        "Nutanix AHV Risk"
                    ),
                    "severity": "WARNING",
                    "message": msg,
                    "detail": (
                        f"{nx_count} "
                        f"cluster(s)"
                    ),
                    "remediation": (
                        "Target 9.4.3-p3+ "
                        "or skip to 9.5.x."
                    ),
                })
                result.warnings.append(msg)
        except Exception:
            pass

    # Oracle RAC (9.4.1/9.4.2)
    if target_cdm.startswith(
        ("9.4.1", "9.4.2")
    ):
        try:
            ora = client.graphql("""
                { oracleDatabases(first: 500) {
                    edges { node {
                        numInstances
                        cluster { id }
                    } }
                } }
            """)
            conn = (
                ora.get(
                    "oracleDatabases", {}
                ) or {}
            )
            rac = sum(
                1 for e in conn.get(
                    "edges", []
                )
                if (
                    (
                        e.get("node", {}).get(
                            "cluster", {}
                        ) or {}
                    ).get("id") == cluster_id
                    and e.get("node", {}).get(
                        "numInstances", 0
                    ) > 1
                )
            )
            if rac > 0:
                msg = (
                    f"{rac} Oracle RAC DB(s). "
                    f"CDM 9.4.1 has "
                    f"ORA-01882. Target "
                    f"9.4.3+."
                )
                findings.append({
                    "category": (
                        "Oracle RAC Risk"
                    ),
                    "severity": "WARNING",
                    "message": msg,
                    "detail": (
                        f"{rac} RAC DB(s)"
                    ),
                    "remediation": (
                        "Target 9.4.3+."
                    ),
                })
                result.warnings.append(msg)
        except Exception:
            pass

    # AD hanging (9.4.3)
    if target_cdm.startswith("9.4.3"):
        try:
            ad = client.graphql("""
                {
                    activeDirectoryDomainControllers(
                        first: 1
                    ) { count }
                }
            """)
            ad_count = (
                ad.get(
                    "activeDirectory"
                    "DomainControllers", {}
                ) or {}
            ).get("count", 0)
            if ad_count > 0:
                msg = (
                    "AD workloads detected. "
                    "CDM 9.4.3 has AD backup "
                    "hanging issues. Target "
                    "9.4.3-p1+."
                )
                findings.append({
                    "category": "AD Risk",
                    "severity": "WARNING",
                    "message": msg,
                    "detail": (
                        f"{ad_count} DC(s)"
                    ),
                    "remediation": (
                        "Target 9.4.3-p1+."
                    ),
                })
                result.warnings.append(msg)
        except Exception:
            pass

    # NAS relic (9.4)
    if target_float >= 9.4:
        try:
            nas = client.graphql("""
                { nasSystems(first: 1) {
                    count
                } }
            """)
            nas_count = (
                nas.get(
                    "nasSystems", {}
                ) or {}
            ).get("count", 0)
            if nas_count > 0:
                msg = (
                    "NAS systems detected. "
                    "After upgrading to 9.4, "
                    "relic NAS filesets may "
                    "not be visible in UI."
                )
                findings.append({
                    "category": (
                        "NAS Relic Risk"
                    ),
                    "severity": "INFO",
                    "message": msg,
                    "detail": (
                        f"{nas_count} NAS "
                        f"system(s)"
                    ),
                    "remediation": (
                        "KB workaround "
                        "available."
                    ),
                })
                result.info_messages.append(msg)
        except Exception:
            pass

    logger.info(
        "  [I] Version risks checked"
    )

    # =================================================================
    # Build results
    # =================================================================
    result.details = findings
    result.raw_data["findings"] = findings

    checks = [
        "CDM Service Accounts & API Tokens "
        "(manual — CDM blocks token-based "
        "API access by design)",
        "Active Live Mounts",
        "Running/Queued Jobs (RSC GraphQL)",
        "System Status",
        "Support Tunnel",
        "DNS/NTP Configuration",
        "Archive Locations",
        "Replication Topology",
        "RSC Connectivity",
        "Version-Specific Risks",
    ]

    result.summary = {
        "total_findings": len(findings),
        "blockers": len(result.blockers),
        "warnings": len(result.warnings),
        "info": len(result.info_messages),
        "cdm_api_available": cdm_available,
        "checks_performed": checks,
    }

    logger.info(
        f"  Upgrade blockers: "
        f"{len(result.blockers)} blockers, "
        f"{len(result.warnings)} warnings, "
        f"{len(result.info_messages)} info"
    )
    return result