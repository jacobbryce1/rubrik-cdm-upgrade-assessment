#!/usr/bin/env bash
# ==============================================================
# Rubrik CDM Pre-Upgrade Assessment — Run Script
# Supports macOS and Linux
#
# Security remediations applied:
#   F-15  Added set -euo pipefail — any unhandled error aborts
#         immediately rather than silently continuing.
#   F-07  Enforces that output and log directories have 0700
#         permissions before starting the assessment.
#   F-11  Verifies .env is not a placeholder before running.
# ==============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo ""
echo -e "${BLUE}╔══════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     Rubrik CDM Upgrade Assessment        ║${NC}"
echo -e "${BLUE}║  $(date '+%Y-%m-%d %H:%M:%S')               ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════╝${NC}"
echo ""

# ── Locate Python ──
PYTHON=""
for cmd in python3 python; do
    if command -v "$cmd" &>/dev/null; then
        PYTHON="$cmd"
        break
    fi
done

if [ -z "${PYTHON:-}" ]; then
    echo -e "${RED}ERROR: Python 3 not found. Install Python 3.8+ or run ./setup.sh${NC}"
    exit 1
fi
echo "Python: $($PYTHON --version 2>&1)"

# ── Activate virtual environment ──
if [ ! -d ".venv" ]; then
    echo -e "${RED}ERROR: Virtual environment not found. Run ./setup.sh first.${NC}"
    exit 1
fi
# shellcheck disable=SC1091
source .venv/bin/activate

# ── Validate .env exists and is not a template ──
if [ ! -f ".env" ]; then
    echo -e "${RED}ERROR: .env not found. Run: cp .env.example .env then configure it.${NC}"
    exit 1
fi

# F-11: Abort if placeholder values are still present
if grep -qE "your-client-(id|secret)" .env 2>/dev/null; then
    echo -e "${RED}ERROR: .env still contains placeholder values.${NC}"
    echo "Edit .env with your RSC credentials before running."
    exit 1
fi

# Ensure required variables are non-empty
# F-15: -u flag causes unset variable references to fail
for var in RSC_BASE_URL RSC_CLIENT_ID RSC_CLIENT_SECRET TARGET_CDM_VERSION; do
    val=$(grep -E "^${var}=" .env 2>/dev/null | cut -d= -f2- | tr -d '"' | tr -d "'" | xargs || true)
    if [ -z "${val:-}" ]; then
        echo -e "${RED}ERROR: ${var} is not set in .env${NC}"
        exit 1
    fi
done

# F-07: Ensure output/log directories exist with secure permissions
for dir in output logs; do
    mkdir -p "$dir"
    chmod 700 "$dir" 2>/dev/null || true
done

# ── Run assessment ──
echo ""
echo "Starting assessment..."
echo ""

set +e
$PYTHON main.py
EXIT_CODE=$?
set -e

# ── Post-run summary ──
LATEST=$(ls -td output/assessment_*/ 2>/dev/null | head -1 || true)
LATEST_LOG=$(ls -t logs/assessment_*.log 2>/dev/null | head -1 || true)

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}╔══════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║   Assessment Complete — No Blockers      ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════╝${NC}"
elif [ $EXIT_CODE -eq 1 ]; then
    echo -e "${RED}╔══════════════════════════════════════════╗${NC}"
    echo -e "${RED}║   BLOCKERS FOUND — Review the Report     ║${NC}"
    echo -e "${RED}╚══════════════════════════════════════════╝${NC}"
elif [ $EXIT_CODE -eq 2 ]; then
    echo -e "${YELLOW}╔══════════════════════════════════════════╗${NC}"
    echo -e "${YELLOW}║   Some Clusters Failed — Review Errors   ║${NC}"
    echo -e "${YELLOW}╚══════════════════════════════════════════╝${NC}"
fi

echo ""
if [ -n "${LATEST:-}" ]; then
    echo -e "Reports:   ${GREEN}${LATEST}${NC}"
    HTML_REPORT="${LATEST}assessment_report.html"
    MANIFEST="${LATEST}manifest.sha256"
    if [ -f "$HTML_REPORT" ]; then
        echo -e "HTML:      ${GREEN}${HTML_REPORT}${NC}"
    fi
    if [ -f "$MANIFEST" ]; then
        echo -e "Manifest:  ${GREEN}${MANIFEST}${NC}"
        echo ""
        echo -e "${BLUE}Verify report integrity before acting on results:${NC}"
        echo "  cd ${LATEST} && sha256sum -c manifest.sha256"
    fi
    if [ -f "$HTML_REPORT" ] && [ "$(uname -s)" = "Darwin" ]; then
        read -r -p "Open HTML report in browser? (Y/n): " open_report || true
        if [ "${open_report:-Y}" != "n" ] && [ "${open_report:-Y}" != "N" ]; then
            open "$HTML_REPORT"
        fi
    fi
fi

if [ -n "${LATEST_LOG:-}" ]; then
    echo -e "Log:       ${GREEN}${LATEST_LOG}${NC}"
fi

echo ""
exit $EXIT_CODE
