# Rubrik CDM Pre-Upgrade Compatibility Assessment Tool

Automated pre-upgrade assessment for Rubrik CDM clusters via the RSC GraphQL API and CDM Direct REST API. Designed for environments from 1 cluster to 200+ clusters with 100K+ servers.

> **Not affiliated with Rubrik.** This is an independent, community-built tool. See [Legal & Disclaimer](#legal--disclaimer) for full details.

---

## Overview

This tool connects to your RSC instance, discovers all CDM clusters, and runs a comprehensive set of pre-upgrade checks in parallel — surfacing blockers, warnings, and informational findings as a visual HTML dashboard, CSV, and JSON report. It is purpose-built for Advisory SEs and infrastructure teams managing large, complex CDM estates.

**v1.1.0** includes a full security hardening pass reviewed against OWASP Top 10 (2021), NIST CSF 2.0, CIS Controls v8, and MITRE ATT&CK for Enterprise. See [Security](#security) for details.

---

## Features

| Feature | Details |
|---------|---------|
| 🏢 Multi-cluster parallel assessment | Assess 1–200+ clusters concurrently via `ThreadPoolExecutor` |
| 🔌 Dual API mode | RSC GraphQL for discovery + CDM Direct REST for deep cluster data |
| 🚦 Blocker / Warning / Info findings | Categorised findings with per-cluster drill-down |
| 📊 Visual HTML dashboard | Per-cluster cards, cross-cluster issues table, status badges |
| 📥 CSV + JSON export | Flat issues CSV and full JSON report for downstream tooling |
| 🔄 Streaming output mode | Disk-backed incremental writes for 100+ cluster environments |
| 🔐 SecretStr credential wrapping | Credentials never appear in logs, `repr()`, or tracebacks |
| 🔒 TLS-verified CDM API calls | Configurable CA bundle; `verify=False` removed entirely |
| 🧾 SHA-256 integrity manifest | Tamper-evident output for audit and compliance use cases |
| 🔑 RSC token auto-refresh | Thread-safe token lifecycle for multi-hour assessment runs |
| 📝 Rotating log files | 50 MB max / 10 backups with unique run ID per assessment |

---

## Supported Check Categories

| Category | Checks |
|----------|--------|
| **Blockers** | Unhealthy nodes, active live mounts, no upgrade path, EOS version, storage ≥ 95%, RSC disconnected |
| **Warnings** | Disconnected hosts, retention-locked SLAs, replication mismatches, outdated RBS agents, floating IPs, high storage |
| **Info** | Workload inventory, SLA summary, OS distribution, agent versions, network config, running jobs |

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| Python | 3.8 or higher (3.11 recommended) |
| Network | HTTPS access to your RSC instance (port 443) and CDM node IPs |
| RSC Permissions | Service account with `ViewCluster`, `ViewSLA`, `ViewInventory` |
| Disk Space | ~200 MB |
| RAM | 512 MB minimum; 2 GB recommended for 100+ cluster environments |

> You must have a valid API key and an active Rubrik Security Cloud subscription. This tool does not bypass licensing or provide unauthorised access to any Rubrik features.

---

## Quick Start

### macOS / Linux

```bash
# Clone the repository
git clone https://github.com/jacobbryce1/Rubrik_CDM_Upgrade_Assessment.git
cd Rubrik_CDM_Upgrade_Assessment

# Set up environment
chmod +x setup.sh && ./setup.sh

# Configure credentials
cp .env.example .env
# Edit .env with your RSC credentials and target version

# Run
./run.sh
```

### Windows (Command Prompt)

```bat
git clone https://github.com/jacobbryce1/Rubrik_CDM_Upgrade_Assessment.git
cd Rubrik_CDM_Upgrade_Assessment
setup.bat
copy .env.example .env
REM Edit .env with your RSC credentials and target version
run.bat
```

Reports are written to `output/assessment_YYYYMMDD_HHMMSS/` when the run completes.

---

## Configuration

### 1. Create your `.env` file

```bash
cp .env.example .env
```

```dotenv
# Required
RSC_BASE_URL=https://your-org.my.rubrik.com
RSC_ACCESS_TOKEN_URI=https://your-org.my.rubrik.com/api/client_token
RSC_CLIENT_ID=your-client-id
RSC_CLIENT_SECRET=your-client-secret
TARGET_CDM_VERSION=9.1.0
```

> ⚠️ **Never commit `.env` to version control.** It is already listed in `.gitignore`. The setup script also offers to install a `detect-secrets` pre-commit hook that blocks credential patterns before they reach Git history.

### 2. RSC Service Account Setup

1. Log into RSC → **Settings** → **Service Accounts**
2. Create a new service account
3. Assign roles: `ViewCluster`, `ViewSLA`, `ViewInventory` *(principle of least privilege)*
4. Optionally add `UPGRADE_CLUSTER` to enable live upgrade path data
5. Copy the Client ID and Secret into your `.env`

### 3. TLS Configuration

CDM direct API calls verify TLS against system CAs by default. Override with `CDM_CA_BUNDLE`:

```dotenv
CDM_CA_BUNDLE=true                  # Default — verify against system CAs
CDM_CA_BUNDLE=/path/to/bundle.pem   # Custom CA bundle for self-signed certs
CDM_CA_BUNDLE=false                 # INSECURE — isolated labs only
```

> ⚠️ Setting `CDM_CA_BUNDLE=false` exposes all CDM credentials to man-in-the-middle interception. A warning is logged on every API call when this is set.

### 4. Cluster Filtering

```dotenv
INCLUDE_CLUSTERS=cluster-01,cluster-02   # Only assess these clusters
EXCLUDE_CLUSTERS=lab-cluster-01          # Skip these clusters
SKIP_DISCONNECTED_CLUSTERS=true          # Skip clusters disconnected from RSC
```

### 5. Full Configuration Reference

#### Required

| Variable | Description |
|----------|-------------|
| `RSC_BASE_URL` | Base URL of your RSC tenant |
| `RSC_ACCESS_TOKEN_URI` | Token endpoint — copy exactly from RSC → Settings → Service Accounts |
| `RSC_CLIENT_ID` | RSC service account client ID |
| `RSC_CLIENT_SECRET` | RSC service account secret |
| `TARGET_CDM_VERSION` | CDM version to assess readiness for, e.g. `9.1.0` |

#### TLS

| Variable | Default | Description |
|----------|---------|-------------|
| `CDM_CA_BUNDLE` | `true` | `true` (system CAs), path to `.pem`, or `false` (insecure) |

#### Cluster Filtering

| Variable | Default | Description |
|----------|---------|-------------|
| `INCLUDE_CLUSTERS` | *(all)* | Comma-separated cluster names or IDs to include |
| `EXCLUDE_CLUSTERS` | *(none)* | Comma-separated cluster names or IDs to exclude |
| `SKIP_DISCONNECTED_CLUSTERS` | `true` | Skip clusters disconnected from RSC |

#### Scaling & Resilience

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_PARALLEL_CLUSTERS` | `10` | Max clusters assessed concurrently |
| `MAX_PARALLEL_ENRICHMENT` | `20` | Max enrichment operations in parallel |
| `MAX_CONCURRENT_API_REQUESTS` | `20` | Global semaphore for all API requests |
| `API_MAX_RETRIES` | `5` | Retries on transient failures (429, 5xx) |
| `API_BACKOFF_BASE` | `1.0` | Exponential backoff base (seconds) |
| `API_BACKOFF_MAX` | `60.0` | Maximum backoff wait (seconds) |
| `API_BACKOFF_FACTOR` | `2.0` | Backoff multiplier per retry |
| `API_TIMEOUT_SECONDS` | `60` | Per-request timeout |
| `TOKEN_REFRESH_BUFFER_SEC` | `300` | Seconds before expiry to proactively refresh RSC token |
| `CIRCUIT_BREAKER_RATE_LIMIT_THRESHOLD` | `0.2` | Auto-reduce parallelism when 429-rate exceeds this fraction |

#### CDM Direct API

| Variable | Default | Description |
|----------|---------|-------------|
| `CDM_DIRECT_ENABLED` | `true` | Enable per-cluster CDM direct API calls |
| `CDM_DIRECT_TIMEOUT` | `10` | Timeout for CDM node requests (seconds) |
| `MAX_CDM_AUTH_ATTEMPTS` | `3` | Max node IPs to try per cluster auth |

#### Output & Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `OUTPUT_DIR` | `./output` | Root directory for assessment output |
| `LOG_DIR` | `./logs` | Root directory for log files |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |
| `REPORT_FORMATS` | `csv,json,html` | Comma-separated formats to generate |
| `STREAMING_OUTPUT` | `false` | `true` for disk-backed streaming (large environments) |

---

## Scaling Reference

Choose values based on your environment size. Exceeding 20 parallel clusters may trigger RSC rate-limiting (429). The tool emits a startup warning when configured values exceed safe thresholds.

| Environment | `MAX_PARALLEL_CLUSTERS` | `MAX_CONCURRENT_API_REQUESTS` | Notes |
|-------------|------------------------|-------------------------------|-------|
| Small (1–20 clusters) | `5` | `10` | Default config is fine |
| Medium (20–100 clusters) | `10` | `20` | Default config |
| Large (100+ clusters) | `15` | `30` | Enable `STREAMING_OUTPUT=true` |

---

## Usage

### Running the Assessment

```bash
# Full assessment — all discovered clusters
./run.sh

# Debug logging
LOG_LEVEL=DEBUG python main.py
```

### Assessment Status Values

| Status | Meaning |
|--------|---------|
| `COMPLETED` | All collectors ran successfully |
| `PARTIAL` | One or more collectors failed — results may be incomplete |
| `FAILED` | Cluster assessment could not be completed |
| `SKIPPED` | Excluded by filters or connection state |

> ⚠️ A `PARTIAL` result must **not** be treated as a clean result. Check the log and re-run after resolving the issue before making any upgrade decisions.

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | No blockers found — clusters appear ready |
| `1` | Blockers found — do NOT proceed with upgrade |
| `2` | Assessment failures — review before proceeding |

---

## Output Files

All output files are written with `chmod 0o600` (owner read/write only). Directories are created with `chmod 0700`.

### Standard Mode (`STREAMING_OUTPUT=false`)

```
output/assessment_YYYYMMDD_HHMMSS/
  assessment_report.json    # Full JSON report
  all_issues.csv            # All issues (flat CSV)
  cluster_summary.csv       # One row per cluster, includes status field
  assessment_report.html    # Visual HTML dashboard
  manifest.sha256           # SHA-256 integrity manifest
```

### Streaming Mode (`STREAMING_OUTPUT=true`)

```
output/assessment_YYYYMMDD_HHMMSS/
  manifest.json             # Master manifest
  summary.jsonl             # One JSON line per cluster
  all_issues.csv            # Incremental issues CSV
  failures.jsonl            # Failed assessments
  skipped.jsonl             # Skipped clusters
  assessment_report.json    # Summary JSON
  cluster_summary.csv       # One row per cluster
  assessment_report.html    # Visual HTML dashboard
  manifest.sha256           # SHA-256 integrity manifest
  clusters/
    cluster_name_1.json
    cluster_name_2.json
    ...
```

### Verifying Output Integrity

The SHA-256 manifest allows you to detect any post-assessment modification to output files. Verify before acting on results:

```bash
cd output/assessment_YYYYMMDD_HHMMSS/
sha256sum -c manifest.sha256
```

All files should report `OK`. Any mismatch indicates output was modified after the assessment completed.

---

## Architecture

```
RSC GraphQL API (clusterConnection + activity queries)
         |
         | Parallel ThreadPoolExecutor
         v
+----------------------------------+
|       cluster_discovery.py       |
|  - Cluster enumeration           |
|  - Node IP / capacity enrichment |
|  - Inclusion / exclusion filters |
+----------------+-----------------+
                 |
                 v
+----------------------------------+
|         rsc_client.py            |
|  - SecretStr credential wrapping |
|  - TLS-verified requests         |
|  - Token lifecycle + auto-refresh|
|  - Rate-limit semaphore          |
|  - Exponential backoff + retry   |
+----------------+-----------------+
                 |
                 v
+----------------------------------+
|      Collector Modules           |
|  upgrade_prechecks               |
|  workload_inventory              |
|  sla_compliance                  |
|  cdm_system_status               |
|  cdm_live_mounts                 |
|  cdm_archive_replication         |
|  cdm_network_config              |
|  cdm_workloads                   |
|  host_inventory                  |
|  compatibility_validator         |
+----------------+-----------------+
                 |
                 v
+----------------------------------+
|           main.py                |
|  - Per-collector error isolation |
|  - PARTIAL status tracking       |
|  - HTML / CSV / JSON reports     |
|  - SHA-256 integrity manifest    |
|  - Secure file permissions       |
+----------------------------------+
```

---

## Project Structure

```
Rubrik_CDM_Upgrade_Assessment/
├── main.py                         # Orchestrator, report generator
├── config.py                       # SecretStr credentials, thread-local context
├── rsc_client.py                   # RSC + CDM API client, TLS-verified
├── cluster_discovery.py            # Cluster discovery and enrichment
├── models.py                       # Data models and streaming output
├── compatibility_matrix.py         # CDM compatibility matrix
├── cdm_eos_data.json               # Static EOS dates and upgrade paths
├── requirements.txt                # Pinned Python dependencies
├── .env.example                    # Configuration template
├── .gitignore                      # Excludes .env, output/, logs/
├── README.md                       # This file
├── SECURITY.md                     # Vulnerability reporting and security design
├── setup.sh / setup.bat            # Environment setup (macOS, Linux, Windows)
├── run.sh / run.bat                # Assessment runner
├── .github/
│   └── workflows/
│       └── security.yml            # CI: pip-audit + gitleaks on every push
├── collectors/
│   ├── upgrade_prechecks.py        # EOS, upgrade path, version risks
│   ├── workload_inventory.py       # VM, DB, host inventory
│   ├── sla_compliance.py           # SLA, archival, replication
│   ├── cdm_system_status.py        # Node, disk, DNS, NTP, storage
│   ├── cdm_live_mounts.py          # Active live mount detection
│   ├── cdm_archive_replication.py  # Archive / replication topology
│   ├── cdm_network_config.py       # VLAN, floating IP, proxy
│   ├── cdm_workloads.py            # Hosts, agents, filesets, AD, K8s
│   ├── host_inventory.py           # RSC host inventory + OS compat
│   └── compatibility_validator.py  # Matrix validation
├── output/                         # Assessment output (auto-created, mode 0700)
└── logs/                           # Rotating log files (auto-created, mode 0700)
```

---

## Security

v1.1.0 was reviewed against **OWASP Top 10 (2021)**, **NIST CSF 2.0**, **CIS Controls v8**, and **MITRE ATT&CK for Enterprise**. The following hardening measures are in place.

### Credential Protection

- Credentials are loaded from `.env` and immediately wrapped in a `SecretStr` type — they never appear in `repr()`, log output, or exception tracebacks.
- `RSC_CLIENT_SECRET.get_secret_value()` is called only at the exact point of HTTP transmission; the wrapped object is passed everywhere else.
- Use a **dedicated read-only service account** for assessments. Rotate the secret after each run to limit blast radius.

### TLS Verification

- All RSC API calls verify TLS against system CAs. This cannot be disabled.
- CDM direct API calls default to `CDM_CA_BUNDLE=true` (system CAs). See [TLS Configuration](#3-tls-configuration) for self-signed cert guidance.
- `urllib3.disable_warnings()` has been removed entirely — TLS warnings are never globally suppressed.

### Output File Security

- `output/` and `logs/` directories are created with `chmod 0700` (owner-only).
- Each output file is set to `chmod 0600` immediately at creation, before any data is written.
- A **SHA-256 integrity manifest** (`manifest.sha256`) is written at the end of every run. Verify before acting on results.

### Credential Leak Prevention

- `.gitignore` excludes `.env`, `output/`, and `logs/` from version control.
- `setup.sh` offers to install a `detect-secrets` pre-commit hook on supported systems.
- A GitHub Actions workflow runs `gitleaks` across the full commit history on every push and PR.

### Dependency Auditing

All dependencies are pinned to exact versions in `requirements.txt`. Run a local audit at any time:

```bash
pip install pip-audit
pip-audit -r requirements.txt
```

The CI pipeline runs `pip-audit` automatically on every push.

### Reporting Vulnerabilities

See [SECURITY.md](SECURITY.md) for the responsible disclosure process. Please do **not** open a public GitHub issue for security vulnerabilities.

---

## Updating Static Data

### EOS Dates & Upgrade Paths

Edit `cdm_eos_data.json` when Rubrik publishes new End-of-Support dates, End-of-Life dates, upgrade path changes, or version-specific known issues.

### Compatibility Matrix

Edit `compatibility_matrix.py` when Rubrik publishes new CDM version support for hypervisors, databases, or OS versions, or deprecates older component versions.

---

## Performance

| Operation | Duration |
|-----------|----------|
| Full assessment — 1 cluster | ~1–2 minutes |
| Full assessment — 10 clusters | ~2–4 minutes |
| Full assessment — 100+ clusters | 10–20 minutes (with `STREAMING_OUTPUT=true`) |

Assessment speed is primarily governed by CDM API response times. The parallel executor reduces wall-clock time by up to 10× compared to sequential assessment.

---

## Testing

Validate connectivity and RSC service account permissions:

```bash
./test.sh
```

Run the dependency security audit locally:

```bash
pip install pip-audit
pip-audit -r requirements.txt
```

---

## Troubleshooting

**"404 Not Found" on token endpoint**
`RSC_ACCESS_TOKEN_URI` is wrong. Go to RSC → **Settings** → **Service Accounts** and copy the exact "Access Token URI" value.

**"401 Unauthorized"**
`RSC_CLIENT_ID` or `RSC_CLIENT_SECRET` is incorrect. Re-copy credentials from the RSC Service Account page.

**"CDM direct API not available"**
CDM cluster nodes are not reachable from this machine. Set `CDM_DIRECT_ENABLED=false` to run in RSC-only mode, or configure network access to CDM node IPs.

**"TLS certificate verification failed"**
CDM clusters use self-signed certificates. Set `CDM_CA_BUNDLE=/path/to/your/ca-bundle.pem` in `.env`. Only use `CDM_CA_BUNDLE=false` in isolated lab environments — see [TLS Configuration](#3-tls-configuration).

**"Rate limited (429)"**
Reduce `MAX_PARALLEL_CLUSTERS` or `MAX_CONCURRENT_API_REQUESTS`. Refer to the [Scaling Reference](#scaling-reference) table for recommended values. The tool auto-retries with exponential backoff.

**"Assessment shows PARTIAL status"**
One or more data collectors failed for that cluster. Check `logs/assessment_<timestamp>_<run_id>.log` for the specific error. Common causes: network timeout, insufficient RSC permissions, CDM API version mismatch. Do not use a PARTIAL result to make upgrade decisions.

**Debug logging**

```bash
LOG_LEVEL=DEBUG python main.py
```

Full debug logs are always written to `logs/assessment_<timestamp>_<run_id>.log` regardless of console log level. The `run_id` in the filename and every log line enables cross-thread correlation in parallel runs.

---

## Updating

```bash
cd Rubrik_CDM_Upgrade_Assessment
source venv/bin/activate
git pull
pip install -r requirements.txt    # picks up any new pinned deps
./run.sh
```

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -am 'Add feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a Pull Request

Please run `pip-audit -r requirements.txt` before submitting and include test coverage for any new functionality.

---

## Legal & Disclaimer

This project is an **independent, open-source tool** and is **not affiliated with, authorized, maintained, sponsored, or endorsed by Rubrik, Inc.** in any way. All product and company names are the registered trademarks of their respective owners. The use of any trade name or trademark is for identification and reference purposes only and does not imply any affiliation with or endorsement by the trademark holder.

This software is provided **"as-is," without warranty of any kind**, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, and non-infringement. Use of this tool is entirely at your own risk. The authors and contributors are not responsible for any data loss, API rate-limit overages, account suspensions, security incidents, or other damages resulting from the use or misuse of this software.

You must have a valid API key and an active subscription or license for Rubrik Security Cloud (RSC). This software does not bypass any licensing checks or provide unauthorised access to Rubrik features.

For questions about the security design of this tool, open a GitHub Discussion. To report a vulnerability, follow the process in [SECURITY.md](SECURITY.md).

---

## License

[Apache 2.0](LICENSE)