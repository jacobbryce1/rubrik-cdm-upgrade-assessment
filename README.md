\# 🔍 Rubrik CDM Pre-Upgrade Compatibility Assessment Tool



Automated pre-upgrade assessment for Rubrik CDM clusters via the RSC GraphQL API and CDM Direct REST API. Designed for environments from 1 cluster to 200+ clusters with 100K+ servers.



> \*\*Not affiliated with Rubrik.\*\* This is an independent, community-built tool. See \[Legal \& Disclaimer](#legal--disclaimer) for details.



\---



\## Overview



This tool connects to your RSC instance, discovers all CDM clusters, and runs a comprehensive set of pre-upgrade checks in parallel — surfacing blockers, warnings, and informational findings as a visual HTML dashboard, CSV, and JSON report. It is purpose-built for Advisory SEs and infrastructure teams managing large, complex CDM estates.



\*\*v1.1.0\*\* introduces a full security hardening pass reviewed against OWASP Top 10, NIST CSF 2.0, CIS Controls v8, and MITRE ATT\&CK. See \[Security](#security) for details.



\---



\## Features



| Feature | Details |

|---|---|

| 🏢 \*\*Multi-cluster parallel assessment\*\* | Assess 1–200+ clusters concurrently via `ThreadPoolExecutor` |

| 🔌 \*\*Dual API mode\*\* | RSC GraphQL for discovery + CDM Direct REST for deep cluster data |

| 🚦 \*\*Blocker / Warning / Info findings\*\* | Categorised findings with per-cluster drill-down |

| 📊 \*\*Visual HTML dashboard\*\* | Per-cluster cards, cross-cluster issues table, status badges |

| 📥 \*\*CSV + JSON export\*\* | Flat issues CSV and full JSON report for downstream tooling |

| 🔄 \*\*Streaming output mode\*\* | Disk-backed incremental writes for 100+ cluster environments |

| 🔐 \*\*SecretStr credential wrapping\*\* | Credentials never appear in logs, `repr()`, or tracebacks |

| 🔒 \*\*TLS-verified CDM API calls\*\* | Configurable CA bundle; `verify=False` removed entirely |

| 🧾 \*\*SHA-256 integrity manifest\*\* | Tamper-evident output for audit and compliance use cases |

| 🔑 \*\*RSC token auto-refresh\*\* | Thread-safe token lifecycle for multi-hour assessment runs |

| 📝 \*\*Rotating log files\*\* | 50 MB max / 10 backups with unique run ID per assessment |



\---



\## Supported Check Categories



| Category | Checks |

|---|---|

| \*\*Blockers\*\* | Unhealthy nodes, active live mounts, no upgrade path, EOS version, storage ≥ 95%, RSC disconnected |

| \*\*Warnings\*\* | Disconnected hosts, retention-locked SLAs, replication mismatches, outdated RBS agents, floating IPs, high storage |

| \*\*Info\*\* | Workload inventory, SLA summary, OS distribution, agent versions, network config, running jobs |



\---



\## Prerequisites



| Requirement | Details |

|---|---|

| Python | 3.8 or higher (3.11 recommended) |

| Network | HTTPS access to your RSC instance (port 443) and CDM node IPs |

| RSC Permissions | Service account with `ViewCluster`, `ViewSLA`, `ViewInventory` |

| Disk Space | \~200 MB |

| RAM | 512 MB minimum; 2 GB recommended for 100+ cluster environments |



> You must have a valid API key and an active Rubrik Security Cloud subscription. This tool does not bypass licensing or provide unauthorised access to any Rubrik features.



\---



\## Quick Start



\### macOS / Linux



```bash

\# Clone or download the repo

git clone https://github.com/jacobbryce1/Rubrik\_CDM\_Upgrade\_Assessment.git

cd Rubrik\_CDM\_Upgrade\_Assessment



\# Set up environment

chmod +x setup.sh \&\& ./setup.sh



\# Configure credentials

cp .env.example .env

\# Edit .env with your RSC credentials and target version



\# Run

./run.sh

```



\### Windows (Command Prompt)



```bat

git clone https://github.com/jacobbryce1/Rubrik\_CDM\_Upgrade\_Assessment.git

cd Rubrik\_CDM\_Upgrade\_Assessment

setup.bat

copy .env.example .env

REM Edit .env with your RSC credentials and target version

run.bat

```



Reports are written to `output/assessment\_YYYYMMDD\_HHMMSS/` when the run completes.



\---



\## Configuration



\### 1. Create your `.env` file



```bash

cp .env.example .env

```



```dotenv

\# Required

RSC\_BASE\_URL=https://your-org.my.rubrik.com

RSC\_ACCESS\_TOKEN\_URI=https://your-org.my.rubrik.com/api/client\_token

RSC\_CLIENT\_ID=your-client-id

RSC\_CLIENT\_SECRET=your-client-secret

TARGET\_CDM\_VERSION=9.1.0

```



> ⚠️ \*\*Never commit `.env` to version control.\*\* It is already listed in `.gitignore`. The setup script also offers to install a `detect-secrets` pre-commit hook that blocks credential patterns before they reach Git history.



\### 2. RSC Service Account Setup



1\. Log into RSC → \*\*Settings\*\* → \*\*Service Accounts\*\*

2\. Create a new service account

3\. Assign roles: `ViewCluster`, `ViewSLA`, `ViewInventory` \*(principle of least privilege)\*

4\. Optionally add `UPGRADE\_CLUSTER` to enable live upgrade path data

5\. Copy the Client ID and Secret into your `.env`



\### 3. TLS Configuration



CDM direct API calls verify TLS against system CAs by default. Override with `CDM\_CA\_BUNDLE`:



```dotenv

CDM\_CA\_BUNDLE=true                  # Default — verify against system CAs

CDM\_CA\_BUNDLE=/path/to/bundle.pem   # Custom CA bundle for self-signed certs

CDM\_CA\_BUNDLE=false                 # INSECURE — isolated labs only

```



> Setting `CDM\_CA\_BUNDLE=false` exposes all CDM credentials to man-in-the-middle interception. A warning is logged on every call when this is set.



\### 4. Cluster Filtering



```dotenv

INCLUDE\_CLUSTERS=cluster-01,cluster-02   # Only assess these clusters

EXCLUDE\_CLUSTERS=lab-cluster-01          # Skip these clusters

SKIP\_DISCONNECTED\_CLUSTERS=true          # Skip clusters disconnected from RSC

```



\---



\## Scaling Reference



Choose values based on your environment size. Exceeding 20 parallel clusters may trigger RSC rate-limiting (429). The tool emits a startup warning when configured values exceed safe thresholds.



| Environment | `MAX\_PARALLEL\_CLUSTERS` | `MAX\_CONCURRENT\_API\_REQUESTS` | Notes |

|---|---|---|---|

| Small (1–20 clusters) | `5` | `10` | Default config is fine |

| Medium (20–100 clusters) | `10` | `20` | Default config |

| Large (100+ clusters) | `15` | `30` | Enable `STREAMING\_OUTPUT=true` |



\---



\## Full Configuration Reference



\### Required



| Variable | Description |

|---|---|

| `RSC\_BASE\_URL` | Base URL of your RSC tenant |

| `RSC\_ACCESS\_TOKEN\_URI` | Token endpoint — copy exactly from RSC > Settings > Service Accounts |

| `RSC\_CLIENT\_ID` | RSC service account client ID |

| `RSC\_CLIENT\_SECRET` | RSC service account secret |

| `TARGET\_CDM\_VERSION` | CDM version to assess readiness for, e.g. `9.1.0` |



\### TLS



| Variable | Default | Description |

|---|---|---|

| `CDM\_CA\_BUNDLE` | `true` | `true` (system CAs), path to `.pem`, or `false` (insecure) |



\### Cluster Filtering



| Variable | Default | Description |

|---|---|---|

| `INCLUDE\_CLUSTERS` | \*(all)\* | Comma-separated cluster names or IDs to include |

| `EXCLUDE\_CLUSTERS` | \*(none)\* | Comma-separated cluster names or IDs to exclude |

| `SKIP\_DISCONNECTED\_CLUSTERS` | `true` | Skip clusters disconnected from RSC |



\### Scaling \& Resilience



| Variable | Default | Description |

|---|---|---|

| `MAX\_PARALLEL\_CLUSTERS` | `10` | Max clusters assessed concurrently |

| `MAX\_PARALLEL\_ENRICHMENT` | `20` | Max enrichment operations in parallel |

| `MAX\_CONCURRENT\_API\_REQUESTS` | `20` | Global semaphore for all API requests |

| `API\_MAX\_RETRIES` | `5` | Retries on transient failures (429, 5xx) |

| `API\_BACKOFF\_BASE` | `1.0` | Exponential backoff base (seconds) |

| `API\_BACKOFF\_MAX` | `60.0` | Maximum backoff wait (seconds) |

| `API\_BACKOFF\_FACTOR` | `2.0` | Backoff multiplier per retry |

| `API\_TIMEOUT\_SECONDS` | `60` | Per-request timeout |

| `TOKEN\_REFRESH\_BUFFER\_SEC` | `300` | Seconds before expiry to proactively refresh RSC token |

| `CIRCUIT\_BREAKER\_RATE\_LIMIT\_THRESHOLD` | `0.2` | Auto-reduce parallelism when 429-rate exceeds this fraction |



\### CDM Direct API



| Variable | Default | Description |

|---|---|---|

| `CDM\_DIRECT\_ENABLED` | `true` | Enable per-cluster CDM direct API calls |

| `CDM\_DIRECT\_TIMEOUT` | `10` | Timeout for CDM node requests (seconds) |

| `MAX\_CDM\_AUTH\_ATTEMPTS` | `3` | Max node IPs to try per cluster auth |



\### Output \& Logging



| Variable | Default | Description |

|---|---|---|

| `OUTPUT\_DIR` | `./output` | Root directory for assessment output |

| `LOG\_DIR` | `./logs` | Root directory for log files |

| `LOG\_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |

| `REPORT\_FORMATS` | `csv,json,html` | Comma-separated formats to generate |

| `STREAMING\_OUTPUT` | `false` | `true` for disk-backed streaming (large environments) |



\---



\## Security



v1.1.0 was reviewed against \*\*OWASP Top 10 (2021)\*\*, \*\*NIST CSF 2.0\*\*, \*\*CIS Controls v8\*\*, and \*\*MITRE ATT\&CK for Enterprise\*\*. The following hardening measures are in place.



\### Credential Protection



\- Credentials are loaded from `.env` and wrapped in a `SecretStr` type — they never appear in `repr()`, log output, or exception tracebacks.

\- `RSC\_CLIENT\_SECRET.get\_secret\_value()` is called only at the exact point of HTTP transmission; the wrapped object is passed everywhere else.

\- Use a \*\*dedicated read-only service account\*\* for assessments. Rotate the secret after each run to limit blast radius.



\### TLS Verification



\- All RSC API calls verify TLS against system CAs. This cannot be disabled.

\- CDM direct API calls default to `CDM\_CA\_BUNDLE=true` (system CAs). See \[TLS Configuration](#3-tls-configuration) for self-signed cert guidance.

\- `urllib3.disable\_warnings()` has been removed entirely — TLS warnings are never globally suppressed.



\### Output File Security



Assessment output contains detailed infrastructure data (cluster names, node IPs, version strings, SLA policies). Treat it as sensitive.



\- `output/` and `logs/` directories are created with permissions `0700` (owner-only).

\- Each output file is set to `0600` after writing.

\- A \*\*SHA-256 integrity manifest\*\* (`manifest.sha256`) is written at the end of every run. Verify before acting on results:



```bash

cd output/assessment\_YYYYMMDD\_HHMMSS/

sha256sum -c manifest.sha256

```



\### Protecting Your Credentials



\- `.gitignore` excludes `.env`, `output/`, and `logs/` from version control.

\- `setup.sh` offers to install a `detect-secrets` pre-commit hook on supported systems.

\- A GitHub Actions workflow runs `gitleaks` across the full commit history on every push and PR.



\### Dependency Auditing



All dependencies are pinned to exact versions in `requirements.txt`. Run a local audit at any time:



```bash

pip install pip-audit

pip-audit -r requirements.txt

```



The CI pipeline runs `pip-audit` automatically on every push.



\### Files Created at Runtime



| File | Contents | Protected By |

|---|---|---|

| `.env` | RSC credentials | `.gitignore`, file permissions |

| `output/` | Assessment reports | `.gitignore`, `chmod 0700` / `0600` |

| `logs/` | Rotating log files | `.gitignore`, `chmod 0700` |



\---



\## Architecture



```

RSC GraphQL API (clusterConnection + activity queries)

&#x20;        |

&#x20;        | Parallel ThreadPoolExecutor

&#x20;        v

+----------------------------------+

|       cluster\_discovery.py       |

|  - Cluster enumeration           |

|  - Node IP / capacity enrichment |

|  - Inclusion / exclusion filters |

+----------------+-----------------+

&#x20;                |

&#x20;                v

+----------------------------------+

|         rsc\_client.py            |

|  - SecretStr credential wrapping |

|  - TLS-verified requests         |

|  - Token lifecycle + auto-refresh|

|  - Rate-limit semaphore          |

|  - Exponential backoff + retry   |

+----------------+-----------------+

&#x20;                |

&#x20;                v

+----------------------------------+

|      Collector Modules           |

|  upgrade\_prechecks               |

|  workload\_inventory              |

|  sla\_compliance                  |

|  cdm\_system\_status               |

|  cdm\_live\_mounts                 |

|  cdm\_archive\_replication         |

|  cdm\_network\_config              |

|  cdm\_workloads                   |

|  host\_inventory                  |

|  compatibility\_validator         |

+----------------+-----------------+

&#x20;                |

&#x20;                v

+----------------------------------+

|           main.py                |

|  - Per-collector error isolation |

|  - PARTIAL status tracking       |

|  - HTML / CSV / JSON reports     |

|  - SHA-256 integrity manifest    |

|  - Secure file permissions       |

+----------------------------------+

```



\---



\## Project Structure



```

Rubrik\_CDM\_Upgrade\_Assessment/

├── main.py                        # Orchestrator, report generator

├── config.py                      # SecretStr credentials, thread-local context

├── rsc\_client.py                  # RSC + CDM API client, TLS-verified

├── cluster\_discovery.py           # Cluster discovery and enrichment

├── models.py                      # Data models and streaming output

├── compatibility\_matrix.py        # CDM compatibility matrix

├── cdm\_eos\_data.json              # Static EOS dates and upgrade paths

├── requirements.txt               # Pinned Python dependencies

├── .env.example                   # Configuration template

├── .gitignore                     # Excludes .env, output/, logs/

├── README.md                      # This file

├── setup.sh / setup.bat           # Environment setup (macOS, Linux, Windows)

├── run.sh / run.bat               # Assessment runner

├── .github/

│   └── workflows/

│       └── security.yml           # CI: pip-audit + gitleaks on every push

├── collectors/

│   ├── upgrade\_prechecks.py       # EOS, upgrade path, version risks

│   ├── workload\_inventory.py      # VM, DB, host inventory

│   ├── sla\_compliance.py          # SLA, archival, replication

│   ├── cdm\_system\_status.py       # Node, disk, DNS, NTP, storage

│   ├── cdm\_live\_mounts.py         # Active live mount detection

│   ├── cdm\_archive\_replication.py # Archive / replication topology

│   ├── cdm\_network\_config.py      # VLAN, floating IP, proxy

│   ├── cdm\_workloads.py           # Hosts, agents, filesets, AD, K8s

│   ├── host\_inventory.py          # RSC host inventory + OS compat

│   └── compatibility\_validator.py # Matrix validation

├── output/                        # Assessment output (auto-created, mode 0700)

└── logs/                          # Rotating log files (auto-created, mode 0700)

```



\---



\## Output Files



\### Standard Mode (`STREAMING\_OUTPUT=false`)



```

output/assessment\_YYYYMMDD\_HHMMSS/

&#x20; assessment\_report.json    # Full JSON report

&#x20; all\_issues.csv            # All issues (flat CSV)

&#x20; cluster\_summary.csv       # One row per cluster, includes status field

&#x20; assessment\_report.html    # Visual HTML dashboard

&#x20; manifest.sha256           # SHA-256 integrity manifest

```



\### Streaming Mode (`STREAMING\_OUTPUT=true`)



```

output/assessment\_YYYYMMDD\_HHMMSS/

&#x20; manifest.json             # Master manifest

&#x20; summary.jsonl             # One JSON line per cluster

&#x20; all\_issues.csv            # Incremental issues CSV

&#x20; failures.jsonl            # Failed assessments

&#x20; skipped.jsonl             # Skipped clusters

&#x20; assessment\_report.json    # Summary JSON

&#x20; cluster\_summary.csv       # One row per cluster

&#x20; assessment\_report.html    # Visual HTML dashboard

&#x20; manifest.sha256           # SHA-256 integrity manifest

&#x20; clusters/

&#x20;   cluster\_name\_1.json

&#x20;   cluster\_name\_2.json

&#x20;   ...

```



\### Assessment Status Values



| Status | Meaning |

|---|---|

| `COMPLETED` | All collectors ran successfully |

| `PARTIAL` | One or more collectors failed — results may be incomplete |

| `FAILED` | Cluster assessment could not be completed |

| `SKIPPED` | Excluded by filters or connection state |



> ⚠️ A `PARTIAL` assessment must \*\*not\*\* be treated as a clean result. Check the log and re-run after resolving the issue before making upgrade decisions.



\---



\## Exit Codes



| Code | Meaning |

|---|---|

| `0` | No blockers found — clusters appear ready |

| `1` | Blockers found — do NOT proceed with upgrade |

| `2` | Assessment failures — review before proceeding |



\---



\## Performance



| Operation | Duration |

|---|---|

| Full assessment — 1 cluster | \~1–2 minutes |

| Full assessment — 10 clusters | \~2–4 minutes |

| Full assessment — 100+ clusters | 10–20 minutes (with `STREAMING\_OUTPUT=true`) |



Assessment speed is primarily governed by CDM API response times. The parallel executor reduces wall-clock time by up to 10× compared to sequential assessment.



\---



\## Updating Static Data



\### EOS Dates \& Upgrade Paths



Edit `cdm\_eos\_data.json` when Rubrik publishes new End-of-Support dates, End-of-Life dates, upgrade path changes, or version-specific known issues.



\### Compatibility Matrix



Edit `compatibility\_matrix.py` when Rubrik publishes new CDM version support for hypervisors, databases, or OS versions, or deprecates older component versions.



\---



\## Troubleshooting



\### "404 Not Found" on token endpoint

\- `RSC\_ACCESS\_TOKEN\_URI` is wrong

\- Go to RSC → \*\*Settings\*\* → \*\*Service Accounts\*\* and copy the exact "Access Token URI"



\### "401 Unauthorized"

\- `RSC\_CLIENT\_ID` or `RSC\_CLIENT\_SECRET` is wrong

\- Re-copy credentials from the RSC Service Account page



\### "CDM direct API not available"

\- CDM cluster nodes are not reachable from this machine

\- Set `CDM\_DIRECT\_ENABLED=false` for RSC-only mode, or configure network access to CDM node IPs



\### "TLS certificate verification failed"

\- CDM clusters use self-signed certificates

\- Set `CDM\_CA\_BUNDLE=/path/to/your/ca-bundle.pem` in `.env`

\- Only use `CDM\_CA\_BUNDLE=false` in isolated lab environments



\### "Rate limited (429)"

\- Reduce `MAX\_PARALLEL\_CLUSTERS` or `MAX\_CONCURRENT\_API\_REQUESTS`

\- Refer to the \[Scaling Reference](#scaling-reference) table for recommended values

\- The tool auto-retries with exponential backoff



\### "Assessment shows PARTIAL status"

\- One or more data collectors failed for that cluster

\- Check `logs/assessment\_<timestamp>\_<run\_id>.log` for the specific error

\- Common causes: network timeout, insufficient RSC permissions, CDM API version mismatch

\- Do not use a PARTIAL result to make upgrade decisions



\### Debug Logging



```bash

LOG\_LEVEL=DEBUG python main.py

```



Full debug logs are always written to `logs/assessment\_<timestamp>\_<run\_id>.log` regardless of console log level. The `run\_id` in the filename and every log line allows cross-thread correlation in parallel runs.



\### Verify Output Integrity



```bash

cd output/assessment\_YYYYMMDD\_HHMMSS/

sha256sum -c manifest.sha256

```



All files should report `OK`. Any mismatch indicates output was modified after the assessment completed.



\---



\## Legal \& Disclaimer



This project is an \*\*independent, open-source tool\*\* and is \*\*not affiliated with, authorized, maintained, sponsored, or endorsed by Rubrik, Inc.\*\* in any way. All product and company names are the registered trademarks of their respective owners. The use of any trade name or trademark is for identification and reference purposes only.



This software is provided \*\*"as-is," without warranty of any kind\*\*. Use of this tool is at your own risk. The authors are not responsible for any data loss, API rate-limit overages, account suspensions, or security incidents resulting from the use of this software.



You must have a valid API key and an active subscription or license for Rubrik Security Cloud (RSC). This software does not bypass any licensing checks or provide unauthorised access to Rubrik features.



\---



\## License



\[Apache 2.0](LICENSE)

