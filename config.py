#!/usr/bin/env python3
"""
Rubrik CDM Pre-Upgrade Assessment — Configuration

Security remediations applied:
  F-03  RSC_CLIENT_SECRET wrapped in SecretStr to prevent accidental logging/repr
  F-04  Removed class-level cluster-context fallbacks; thread-local only (eliminates
        the race condition under ThreadPoolExecutor)
  F-09  Added documented guidance on production-safe parallelism limits and a
        validation warning when configured values exceed recommended thresholds
  F-13  setup_logging() now uses RotatingFileHandler (50 MB / 10 backups) and
        embeds a unique run_id in every log line for cross-thread correlation
"""

import os
import sys
import uuid
import logging
import logging.handlers
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


# ──────────────────────────────────────────────────────────────
# F-03: SecretStr wrapper
# Prevents RSC_CLIENT_SECRET from appearing in repr(), str(),
# log calls, or any serialisation that touches Config attributes.
# ──────────────────────────────────────────────────────────────
class SecretStr:
    """Opaque wrapper for sensitive string values."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def get_secret_value(self) -> str:
        """Return the raw secret — call only when sending to an API."""
        return self._value

    def __repr__(self) -> str:          # noqa: D105
        return "SecretStr('***')"

    def __str__(self) -> str:           # noqa: D105
        return "***"

    def __bool__(self) -> bool:         # noqa: D105
        return bool(self._value)

    def __eq__(self, other: object) -> bool:  # noqa: D105
        if isinstance(other, SecretStr):
            return self._value == other._value
        return NotImplemented


class Config:
    """
    Centralized configuration.

    Authentication fields match original tool exactly.
    Scaling fields added for parallel processing.

    Security notes
    ─────────────
    • RSC_CLIENT_SECRET is a SecretStr — never cast to str or log it.
      Always call .get_secret_value() only at the point of HTTP transmission.
    • Cluster context is stored thread-locally only.  The class-level fallback
      attributes that existed previously have been removed to prevent the
      ThreadPoolExecutor race condition (F-04).
    """

    # =========================================================
    # RSC Connection
    # =========================================================
    RSC_URL = os.environ.get("RSC_URL", "").rstrip("/")
    RSC_BASE_URL = (
        os.environ.get("RSC_BASE_URL", "").rstrip("/") or RSC_URL
    )
    RSC_ACCESS_TOKEN_URI = os.environ.get(
        "RSC_ACCESS_TOKEN_URI", ""
    ).strip()

    RSC_CLIENT_ID = os.environ.get("RSC_CLIENT_ID", "")

    # F-03: wrap in SecretStr so repr/logging never exposes the value
    RSC_CLIENT_SECRET: SecretStr = SecretStr(
        os.environ.get("RSC_CLIENT_SECRET", "")
    )

    # Auto-derive token URI if not set
    if not RSC_ACCESS_TOKEN_URI and RSC_BASE_URL:
        RSC_ACCESS_TOKEN_URI = RSC_BASE_URL + "/api/client_token"

    # =========================================================
    # TLS / CDM CA bundle (F-01 / F-02)
    # Set CDM_CA_BUNDLE to a path for custom CAs, "true" for
    # system CAs (default), or "false" ONLY in air-gapped labs
    # where you accept the MITM risk and have no CA bundle.
    # =========================================================
    _cdm_ca_raw = os.environ.get("CDM_CA_BUNDLE", "true").strip()
    if _cdm_ca_raw.lower() in ("true", "1", "yes", ""):
        CDM_CA_BUNDLE: object = True          # verify against system CAs
    elif _cdm_ca_raw.lower() in ("false", "0", "no"):
        CDM_CA_BUNDLE = False                 # INSECURE — only for isolated labs
    else:
        CDM_CA_BUNDLE = _cdm_ca_raw           # treat as path to CA bundle

    # =========================================================
    # Target CDM Version
    # =========================================================
    TARGET_CDM_VERSION = os.environ.get("TARGET_CDM_VERSION", "")

    # =========================================================
    # Cluster Filtering
    # =========================================================
    INCLUDE_CLUSTERS = [
        c.strip()
        for c in os.environ.get("INCLUDE_CLUSTERS", "").split(",")
        if c.strip()
    ]
    EXCLUDE_CLUSTERS = [
        c.strip()
        for c in os.environ.get("EXCLUDE_CLUSTERS", "").split(",")
        if c.strip()
    ]
    SKIP_DISCONNECTED = os.environ.get(
        "SKIP_DISCONNECTED_CLUSTERS", "true"
    ).lower() in ("true", "1", "yes")

    # =========================================================
    # Scaling
    # F-09: Production-safe guidance
    #   • Small  (1-20 clusters):   MAX_PARALLEL_CLUSTERS=5,  MAX_CONCURRENT_API_REQUESTS=10
    #   • Medium (20-100 clusters): MAX_PARALLEL_CLUSTERS=10, MAX_CONCURRENT_API_REQUESTS=20
    #   • Large  (100+ clusters):   MAX_PARALLEL_CLUSTERS=15, MAX_CONCURRENT_API_REQUESTS=30
    #     and enable STREAMING_OUTPUT=true
    #
    # Exceeding 20 parallel clusters may trigger RSC rate-limiting (429).
    # The validate() method emits warnings when limits exceed safe thresholds.
    # =========================================================
    MAX_PARALLEL_CLUSTERS = int(
        os.environ.get("MAX_PARALLEL_CLUSTERS", "10")
    )
    MAX_PARALLEL_ENRICHMENT = int(
        os.environ.get("MAX_PARALLEL_ENRICHMENT", "20")
    )
    MAX_CONCURRENT_API_REQUESTS = int(
        os.environ.get("MAX_CONCURRENT_API_REQUESTS", "20")
    )

    # API Resilience
    API_MAX_RETRIES = int(os.environ.get("API_MAX_RETRIES", "5"))
    API_BACKOFF_BASE = float(os.environ.get("API_BACKOFF_BASE", "1.0"))
    API_BACKOFF_MAX = float(os.environ.get("API_BACKOFF_MAX", "60.0"))
    API_BACKOFF_FACTOR = float(os.environ.get("API_BACKOFF_FACTOR", "2.0"))
    API_TIMEOUT_SECONDS = int(os.environ.get("API_TIMEOUT_SECONDS", "60"))
    API_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

    # Circuit-breaker: if the 429-rate in a sliding window exceeds this
    # fraction, MAX_PARALLEL_CLUSTERS is halved automatically.
    CIRCUIT_BREAKER_RATE_LIMIT_THRESHOLD = float(
        os.environ.get("CIRCUIT_BREAKER_RATE_LIMIT_THRESHOLD", "0.2")
    )

    # Token Management
    TOKEN_REFRESH_BUFFER_SEC = int(
        os.environ.get("TOKEN_REFRESH_BUFFER_SEC", "300")
    )

    # CDM Direct API
    CDM_DIRECT_ENABLED = os.environ.get(
        "CDM_DIRECT_ENABLED", "true"
    ).lower() in ("true", "1", "yes")
    CDM_DIRECT_TIMEOUT = int(os.environ.get("CDM_DIRECT_TIMEOUT", "10"))
    MAX_CDM_AUTH_ATTEMPTS = int(os.environ.get("MAX_CDM_AUTH_ATTEMPTS", "3"))

    # Memory Management
    STREAMING_OUTPUT = os.environ.get(
        "STREAMING_OUTPUT", "false"
    ).lower() in ("true", "1", "yes")

    # Output Settings
    OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "./output")
    LOG_DIR = os.environ.get("LOG_DIR", "./logs")
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
    REPORT_FORMATS = [
        f.strip()
        for f in os.environ.get("REPORT_FORMATS", "csv,json,html").split(",")
        if f.strip()
    ]

    # =========================================================
    # F-04: Thread-local cluster context ONLY
    # The previous class-level fallback (_current_cluster_id etc.)
    # has been removed.  All cluster context is now stored
    # exclusively in thread-local storage so parallel workers
    # cannot read each other's state.
    # =========================================================
    import threading as _threading
    _thread_local = _threading.local()

    @classmethod
    def set_current_cluster(
        cls, cluster_id: str, name: str = "", version: str = ""
    ) -> None:
        """Set current cluster context for the calling thread only."""
        cls._thread_local.cluster_id = cluster_id
        cls._thread_local.cluster_name = name
        cls._thread_local.cluster_version = version

    @classmethod
    def get_current_cluster_id(cls) -> str:
        """Return the cluster ID for the calling thread.

        Raises RuntimeError if called outside a worker thread that has not
        yet called set_current_cluster() — intentional: a missing context
        is a programming error, not a recoverable state.
        """
        val = getattr(cls._thread_local, "cluster_id", None)
        if val is None:
            raise RuntimeError(
                "Cluster context not initialised for this thread. "
                "Call Config.set_current_cluster() before accessing context."
            )
        return val

    @classmethod
    def get_current_cluster_name(cls) -> str:
        val = getattr(cls._thread_local, "cluster_name", None)
        if val is None:
            raise RuntimeError("Cluster context not initialised for this thread.")
        return val

    @classmethod
    def get_current_cluster_version(cls) -> str:
        val = getattr(cls._thread_local, "cluster_version", None)
        if val is None:
            raise RuntimeError("Cluster context not initialised for this thread.")
        return val

    @classmethod
    def validate(cls) -> list:
        """Validate required configuration and emit scaling warnings."""
        errors = []

        if not cls.RSC_BASE_URL:
            errors.append("RSC_BASE_URL (or RSC_URL) is required")
        if not cls.RSC_ACCESS_TOKEN_URI:
            errors.append(
                "RSC_ACCESS_TOKEN_URI is required — "
                "copy from RSC > Settings > Service Accounts"
            )
        if not cls.RSC_CLIENT_ID:
            errors.append("RSC_CLIENT_ID is required")
        if not cls.RSC_CLIENT_SECRET:
            errors.append("RSC_CLIENT_SECRET is required")
        if not cls.TARGET_CDM_VERSION:
            errors.append("TARGET_CDM_VERSION is required")

        # F-09: Scaling safety warnings
        if cls.MAX_PARALLEL_CLUSTERS < 1:
            errors.append("MAX_PARALLEL_CLUSTERS must be >= 1")
        if cls.MAX_PARALLEL_CLUSTERS > 20:
            errors.append(
                f"WARNING: MAX_PARALLEL_CLUSTERS={cls.MAX_PARALLEL_CLUSTERS} "
                "exceeds recommended limit of 20 and may trigger RSC rate-limiting. "
                "Consider enabling STREAMING_OUTPUT=true for large environments."
            )
        if cls.MAX_CONCURRENT_API_REQUESTS > 40:
            errors.append(
                f"WARNING: MAX_CONCURRENT_API_REQUESTS={cls.MAX_CONCURRENT_API_REQUESTS} "
                "is high — RSC may throttle requests above ~30."
            )

        # F-01/F-02: Warn loudly when TLS verification is disabled
        if cls.CDM_CA_BUNDLE is False:
            errors.append(
                "WARNING: CDM_CA_BUNDLE=false — TLS certificate verification is "
                "DISABLED for all CDM direct API calls. This exposes credentials "
                "to man-in-the-middle attacks. Only acceptable in isolated lab environments."
            )

        return errors

    @classmethod
    def summary(cls) -> dict:
        """Return a safe summary dict (never includes secret values)."""
        return {
            "rsc_base_url": cls.RSC_BASE_URL,
            "rsc_access_token_uri": cls.RSC_ACCESS_TOKEN_URI,
            "rsc_client_id": cls.RSC_CLIENT_ID,
            # F-03: deliberately omit RSC_CLIENT_SECRET
            "target_cdm_version": cls.TARGET_CDM_VERSION,
            "max_parallel_clusters": cls.MAX_PARALLEL_CLUSTERS,
            "cdm_direct_enabled": cls.CDM_DIRECT_ENABLED,
            "cdm_ca_bundle": str(cls.CDM_CA_BUNDLE),
            "streaming_output": cls.STREAMING_OUTPUT,
        }


def setup_logging() -> logging.Logger:
    """
    Configure file + console logging.

    F-13 remediations:
      • RotatingFileHandler replaces FileHandler — 50 MB max, 10 backup files
      • A unique run_id is embedded in every log line for cross-thread correlation
      • Run metadata (start time, hostname, Python version) logged at startup
    """
    import platform
    from datetime import datetime

    log_dir = Path(Config.LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = uuid.uuid4().hex[:8]
    log_file = log_dir / f"assessment_{timestamp}_{run_id}.log"

    level = getattr(logging, Config.LOG_LEVEL, logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers.clear()

    # F-13: RotatingFileHandler — always DEBUG level
    fh = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=50 * 1024 * 1024,   # 50 MB
        backupCount=10,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter(
            f"%(asctime)s [%(levelname)-7s] [%(threadName)s] "
            f"[run={run_id}] %(name)s: %(message)s"
        )
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)-7s] %(message)s",
            datefmt="%H:%M:%S",
        )
    )

    root_logger.addHandler(fh)
    root_logger.addHandler(ch)

    logger = logging.getLogger("assessment")
    # F-13: log run metadata for audit trail
    logger.info(
        "Assessment run started | run_id=%s | host=%s | python=%s | log=%s",
        run_id,
        platform.node(),
        platform.python_version(),
        log_file,
    )
    return logger
