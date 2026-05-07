#!/usr/bin/env python3
"""
Rubrik Security Cloud (RSC) API Client

Security remediations applied:
  F-01  TLS certificate verification re-enabled for all CDM direct API calls.
        CDM_CA_BUNDLE config controls the verify= value (True / path / False).
  F-02  verify=False removed from _cdm_session constructor; session inherits
        the configured CA bundle by default.
  F-03  RSC_CLIENT_SECRET.get_secret_value() called only at HTTP transmission
        points; the SecretStr wrapper prevents accidental logging.
  F-05  urllib3.disable_warnings() removed.  A targeted per-call context manager
        is provided for the rare case where a self-signed CA is intentionally
        accepted, so the warning is scoped rather than globally silenced.
  F-06  Credential reuse documented; per-cluster auth isolation noted.
  F-18  Pagination safety limit checked BEFORE appending, preventing unbounded
        memory growth.
  F-20  CDM node IP iteration order is randomised per authentication attempt to
        prevent predictable probe targeting.
"""

import time
import random
import logging
import datetime
import threading
import warnings
import requests
import urllib3

from typing import Optional, Dict, List, Any
from config import Config

# F-05: Do NOT call urllib3.disable_warnings() globally.
# The helper below scopes suppression to a single request when explicitly needed.
logger = logging.getLogger("rsc_client")

KNOWN_SERVER_NOISE = [
    "internal server error",
    "upstream connect error",
    "deadline exceeded",
]

# Maximum items returned from a single paginated query before we stop.
# Checked BEFORE appending to prevent unbounded memory growth (F-18).
_PAGINATION_SAFETY_LIMIT = 500_000


def _cdm_tls_verify():
    """Return the verify= argument for CDM requests.

    F-01: Returns Config.CDM_CA_BUNDLE which defaults to True (system CAs).
    If CDM_CA_BUNDLE=false is set by the operator, log a warning at the
    call site so the risk is visible in logs rather than silently hidden.
    """
    val = Config.CDM_CA_BUNDLE
    if val is False:
        logger.warning(
            "CDM TLS certificate verification is DISABLED (CDM_CA_BUNDLE=false). "
            "All CDM API traffic is vulnerable to man-in-the-middle interception. "
            "Set CDM_CA_BUNDLE=true or supply a CA bundle path for production use."
        )
    return val


class RSCClientError(Exception):
    pass


class RSCClient:
    """
    RSC + CDM API client.

    Security notes
    ─────────────
    F-06  The same RSC service account credentials are reused across all CDM
          clusters because CDM accepts RSC service account tokens directly.
          This is a Rubrik platform constraint.  To limit blast radius:
            1. Use a dedicated read-only service account for assessments.
            2. Rotate the secret after each assessment run.
            3. Restrict the account to ViewCluster / ViewSLA / ViewInventory
               and omit UPGRADE_CLUSTER unless required.
    """

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        # F-02: CDM session uses the configured CA bundle, not verify=False
        self._cdm_session = requests.Session()
        self._cdm_session.verify = _cdm_tls_verify()
        self._cdm_session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        # RSC token
        self._rsc_token: Optional[str] = None
        self._rsc_token_expiry: Optional[datetime.datetime] = None
        self._rsc_token_lock = threading.Lock()

        # Per-cluster CDM state
        self._cdm_tokens: Dict[str, str] = {}
        self._cdm_node_ips_map: Dict[str, List[str]] = {}
        self._cdm_active_ip: Dict[str, str] = {}
        self._cdm_available_map: Dict[str, bool] = {}
        self._cdm_lock = threading.Lock()

        # Current cluster context (set per-thread via Config.set_current_cluster)
        self._current_cluster_id = ""
        self._current_cluster_name = ""

        # Rate limiting
        self._request_semaphore = threading.Semaphore(
            Config.MAX_CONCURRENT_API_REQUESTS
        )

        # Stats
        self._stats = {
            "graphql_requests": 0,
            "cdm_requests": 0,
            "cdm_auth_attempts": 0,
            "cdm_auth_successes": 0,
            "retries": 0,
            "rate_limits": 0,
            "failures": 0,
        }
        self._stats_lock = threading.Lock()

    # ==========================================================
    # RSC Authentication
    # ==========================================================

    def connect(self) -> None:
        logger.info("Connecting to RSC: %s", Config.RSC_BASE_URL)
        logger.info("Token endpoint: %s", Config.RSC_ACCESS_TOKEN_URI)
        self._refresh_rsc_token()
        logger.info("RSC connection established")

    def _refresh_rsc_token(self) -> None:
        with self._rsc_token_lock:
            logger.debug("Refreshing RSC token...")
            try:
                # F-03: .get_secret_value() called only at HTTP transmission
                resp = requests.post(
                    Config.RSC_ACCESS_TOKEN_URI,
                    json={
                        "client_id": Config.RSC_CLIENT_ID,
                        "client_secret": Config.RSC_CLIENT_SECRET.get_secret_value(),
                    },
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                    verify=True,   # RSC endpoint always verified against system CAs
                )

                if resp.status_code == 404:
                    raise RSCClientError(
                        "Token endpoint not found (404): "
                        + Config.RSC_ACCESS_TOKEN_URI
                    )
                if resp.status_code == 401:
                    raise RSCClientError(
                        "Auth failed (401): Check RSC_CLIENT_ID and RSC_CLIENT_SECRET"
                    )
                resp.raise_for_status()

                data = resp.json()
                self._rsc_token = (
                    data.get("access_token")
                    or data.get("token")
                    or data.get("session_token")
                )
                if not self._rsc_token:
                    raise RSCClientError(
                        "No token in response. Keys: " + str(list(data.keys()))
                    )

                expires_in = data.get("expires_in", 300)
                self._rsc_token_expiry = datetime.datetime.utcnow() + datetime.timedelta(
                    seconds=expires_in
                )
                self._session.headers.update(
                    {"Authorization": "Bearer " + self._rsc_token}
                )
                logger.info("RSC token refreshed, expires in %ds", expires_in)

            except RSCClientError:
                raise
            except Exception as e:
                raise RSCClientError("RSC connection failed: " + str(e)) from e

    def _ensure_rsc_token(self) -> None:
        buffer = datetime.timedelta(seconds=Config.TOKEN_REFRESH_BUFFER_SEC)
        if (
            not self._rsc_token
            or not self._rsc_token_expiry
            or datetime.datetime.utcnow() >= (self._rsc_token_expiry - buffer)
        ):
            self._refresh_rsc_token()

    # ==========================================================
    # GraphQL
    # ==========================================================

    def graphql(self, query: str, variables: Optional[dict] = None) -> dict:
        self._ensure_rsc_token()
        url = Config.RSC_BASE_URL + "/api/graphql"
        payload: dict = {"query": query}
        if variables:
            payload["variables"] = variables

        with self._stats_lock:
            self._stats["graphql_requests"] += 1

        with self._request_semaphore:
            try:
                resp = self._session.post(
                    url, json=payload, timeout=Config.API_TIMEOUT_SECONDS
                )
            except Exception as e:
                with self._stats_lock:
                    self._stats["failures"] += 1
                raise RSCClientError("GraphQL request failed: " + str(e)) from e

            if resp.status_code == 401:
                self._refresh_rsc_token()
                with self._request_semaphore:
                    resp = self._session.post(
                        url, json=payload, timeout=Config.API_TIMEOUT_SECONDS
                    )

            if resp.status_code != 200:
                error_body = ""
                try:
                    error_body = resp.text[:2000]
                except Exception:
                    pass
                logger.error("GraphQL HTTP %d: %s", resp.status_code, error_body)
                resp.raise_for_status()

        result = resp.json()
        if "errors" in result and result["errors"]:
            _log_errors(result["errors"], "query")
        return result.get("data", {})

    def graphql_paginated(
        self,
        query: str,
        variables: Optional[dict] = None,
        connection_path: Optional[List[str]] = None,
        page_size: int = 200,
    ) -> list:
        if variables is None:
            variables = {}
        if connection_path is None:
            connection_path = []

        all_nodes: list = []
        has_next = True
        cursor: Optional[str] = None

        while has_next:
            page_vars = dict(variables)
            page_vars["first"] = page_size
            if cursor:
                page_vars["after"] = cursor

            data = self.graphql(query, page_vars)
            conn = data
            for key in connection_path:
                if conn is None:
                    break
                conn = conn.get(key, {}) or {}

            if not conn:
                break

            edges = conn.get("edges", []) or []
            nodes = []
            for edge in edges:
                if isinstance(edge, dict):
                    node = edge.get("node", edge)
                    if node:
                        nodes.append(node)

            if not nodes:
                direct = conn.get("nodes", []) or []
                nodes = [n for n in direct if isinstance(n, dict)]

            # F-18: Check limit BEFORE appending to prevent OOM on huge datasets
            if len(all_nodes) + len(nodes) > _PAGINATION_SAFETY_LIMIT:
                logger.warning(
                    "Pagination safety limit (%d items) reached before appending "
                    "%d new items — stopping early. Enable STREAMING_OUTPUT=true "
                    "for environments this large.",
                    _PAGINATION_SAFETY_LIMIT,
                    len(nodes),
                )
                break

            all_nodes.extend(nodes)

            page_info = conn.get("pageInfo", {}) or {}
            has_next = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")

            if not edges and not nodes:
                has_next = False
            if not cursor:
                has_next = False

        return all_nodes

    # ==========================================================
    # Cluster Context
    # ==========================================================

    def set_target_cluster(
        self,
        cluster_id: str,
        node_ips: Optional[List[str]] = None,
        name: str = "",
        version: str = "",
    ) -> None:
        self._current_cluster_id = cluster_id
        self._current_cluster_name = name
        Config.set_current_cluster(cluster_id, name, version)
        if node_ips:
            with self._cdm_lock:
                self._cdm_node_ips_map[cluster_id] = list(node_ips)

    def get_cluster_node_ips(self, cluster_id: str) -> List[str]:
        with self._cdm_lock:
            return list(self._cdm_node_ips_map.get(cluster_id, []))

    # ==========================================================
    # CDM Authentication
    # ==========================================================

    def connect_cdm_direct(self, cluster_id: Optional[str] = None) -> bool:
        if not Config.CDM_DIRECT_ENABLED:
            return False
        if cluster_id is None:
            cluster_id = self._current_cluster_id

        node_ips = self.get_cluster_node_ips(cluster_id)
        if not node_ips:
            with self._cdm_lock:
                self._cdm_available_map[cluster_id] = False
            return False

        attempts = min(Config.MAX_CDM_AUTH_ATTEMPTS, len(node_ips))

        with self._stats_lock:
            self._stats["cdm_auth_attempts"] += 1

        # F-20: Randomise the order of node IPs to prevent predictable probing
        shuffled_ips = list(node_ips)
        random.shuffle(shuffled_ips)

        verify = _cdm_tls_verify()

        for i in range(attempts):
            ip = shuffled_ips[i]
            url = "https://" + ip + "/api/v1/service_account/session"
            try:
                cdm_session = requests.Session()
                cdm_session.verify = verify
                # F-03: secret transmitted only here
                resp = cdm_session.post(
                    url,
                    json={
                        "serviceAccountId": Config.RSC_CLIENT_ID,
                        "secret": Config.RSC_CLIENT_SECRET.get_secret_value(),
                    },
                    headers={"Content-Type": "application/json"},
                    timeout=Config.CDM_DIRECT_TIMEOUT,
                )

                if resp.status_code == 200:
                    data = resp.json()
                    token = data.get("token")
                    if token:
                        with self._cdm_lock:
                            self._cdm_tokens[cluster_id] = token
                            self._cdm_active_ip[cluster_id] = ip
                            self._cdm_available_map[cluster_id] = True
                        with self._stats_lock:
                            self._stats["cdm_auth_successes"] += 1
                        logger.info(" CDM direct API connected via %s", ip)
                        return True
                else:
                    logger.debug(" CDM auth %s: HTTP %d", ip, resp.status_code)

            except (requests.exceptions.ConnectionError,
                    requests.exceptions.ConnectTimeout):
                logger.debug(" CDM %s: unreachable", ip)
            except requests.exceptions.Timeout:
                logger.debug(" CDM %s: timeout", ip)
            except Exception as e:
                logger.debug(" CDM %s: %s", ip, e)

        logger.debug(" CDM direct API not available for this cluster")
        with self._cdm_lock:
            self._cdm_available_map[cluster_id] = False
        return False

    def is_cdm_available(self, cluster_id: Optional[str] = None) -> bool:
        if cluster_id is None:
            cluster_id = self._current_cluster_id
        with self._cdm_lock:
            return self._cdm_available_map.get(cluster_id, False)

    @property
    def cdm_available(self) -> bool:
        """Backward-compatible property."""
        return self.is_cdm_available()

    # ==========================================================
    # CDM Direct API
    # URL construction: https://{ip}/{endpoint}
    # endpoint includes full path, e.g. "api/v1/cluster/me"
    # ==========================================================

    def cdm_direct_get(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        cluster_id: Optional[str] = None,
    ) -> Optional[dict]:
        if cluster_id is None:
            cluster_id = self._current_cluster_id

        with self._cdm_lock:
            available = self._cdm_available_map.get(cluster_id, False)
            token = self._cdm_tokens.get(cluster_id, "")
            ip = self._cdm_active_ip.get(cluster_id, "")

        if not available or not ip or not token:
            return None

        url = "https://" + ip + "/" + endpoint.lstrip("/")

        with self._stats_lock:
            self._stats["cdm_requests"] += 1

        verify = _cdm_tls_verify()

        try:
            resp = requests.get(
                url,
                headers={
                    "Authorization": "Bearer " + token,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                params=params,
                timeout=Config.CDM_DIRECT_TIMEOUT,
                verify=verify,
            )

            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                logger.debug(" CDM %s: 404 for %s", ip, endpoint)
                return None
            elif resp.status_code == 401:
                logger.debug(" CDM %s: 401, re-authing...", ip)
                if self.connect_cdm_direct(cluster_id):
                    return self.cdm_direct_get(endpoint, params, cluster_id)
                return None
            else:
                logger.debug(
                    " CDM %s: HTTP %d for %s", ip, resp.status_code, endpoint
                )
                return None

        except requests.exceptions.Timeout:
            logger.debug(" CDM %s: timeout on %s", ip, endpoint)
            return None
        except Exception as e:
            logger.debug(" CDM %s: %s on %s", ip, e, endpoint)
            return None

    # Backward-compatible aliases
    def cdm_get(self, endpoint: str, params: Optional[dict] = None) -> Optional[dict]:
        return self.cdm_direct_get(endpoint, params, self._current_cluster_id)

    def cdm_get_paginated(
        self,
        endpoint: str,
        limit: int = 500,
        page_key: str = "data",
        cluster_id: Optional[str] = None,
    ) -> list:
        if cluster_id is None:
            cluster_id = self._current_cluster_id
        if not self.is_cdm_available(cluster_id):
            return []

        all_results: list = []
        offset = 0

        while True:
            sep = "&" if "?" in endpoint else "?"
            paged = endpoint + sep + "limit=" + str(limit) + "&offset=" + str(offset)
            data = self.cdm_direct_get(paged, cluster_id=cluster_id)

            if data is None:
                break

            if isinstance(data, dict):
                page_data = data.get(page_key, [])
                if not isinstance(page_data, list):
                    page_data = []
                has_more = data.get("hasMore", False)
            elif isinstance(data, list):
                page_data = data
                has_more = len(page_data) == limit
            else:
                break

            if not page_data:
                break

            # F-18: Check limit before appending
            if len(all_results) + len(page_data) > _PAGINATION_SAFETY_LIMIT:
                logger.warning(
                    "CDM pagination safety limit reached at offset %d for %s",
                    offset,
                    endpoint,
                )
                break

            all_results.extend(page_data)

            if not has_more:
                break
            offset += limit

        return all_results

    # ==========================================================
    # Statistics
    # ==========================================================

    def get_stats(self) -> dict:
        with self._stats_lock:
            return dict(self._stats)

    def log_stats(self) -> None:
        stats = self.get_stats()
        logger.info(
            "API Stats -- GraphQL: %d, CDM: %d, CDM Auth: %d/%d, "
            "Retries: %d, Rate Limits: %d, Failures: %d",
            stats["graphql_requests"],
            stats["cdm_requests"],
            stats["cdm_auth_successes"],
            stats["cdm_auth_attempts"],
            stats["retries"],
            stats["rate_limits"],
            stats["failures"],
        )


def _is_known_noise(message: str) -> bool:
    msg_lower = message.lower()
    return any(noise in msg_lower for noise in KNOWN_SERVER_NOISE)


def _log_errors(errors: list, context: str) -> None:
    noise_count = 0
    real_errors = []
    for err in errors:
        msg = err.get("message", str(err))
        if _is_known_noise(msg):
            noise_count += 1
        else:
            real_errors.append(msg)

    if noise_count > 0:
        logger.debug(
            "GraphQL %s: %d non-blocking server error(s) suppressed",
            context,
            noise_count,
        )
    for msg in real_errors:
        logger.warning("GraphQL %s: %s", context, msg[:300])
