#!/usr/bin/env bash
# deep-value incremental sync wrapper for cron with Uptime Kuma push.
# Usage: ./scripts/cron-sync.sh <mode>
#   mode: daily | financial | meta
#
# Requires KUMA_PUSH_URL in .env (optional; sync still runs without it).
# Format: http://<kuma-host>:3001/api/push/<token>
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_DIR"

# Load .env
if [ -f .env ]; then
  set -a; source .env; set +a
fi

TODAY="$(date +%Y%m%d)"
MODE="${1:-daily}"
DELAY_MS="${SYNC_DELAY_MS:-150}"

# ---- Uptime Kuma push helper (uses curl, no Python dependency) ----
push_kuma() {
  local status="$1"
  local message="$2"
  local push_url="${KUMA_PUSH_URL:-}"
  if [[ -z "${push_url}" ]]; then
    return 0
  fi
  # Append query params to the push URL, preserving any existing query string
  local sep="?"
  if [[ "${push_url}" == *"?"* ]]; then
    sep="&"
  fi
  local full_url="${push_url}${sep}status=${status}&msg=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "${message}" 2>/dev/null || echo "${message}")&ping="
  curl -sf --max-time 10 "${full_url}" >/dev/null 2>&1 || {
    echo "[warn] Failed to push to Uptime Kuma: status=${status}" >&2
  }
}

# ---- Trap: push down on unexpected exit ----
handle_exit() {
  local exit_code="$?"
  if [[ "${exit_code}" -ne 0 ]]; then
    push_kuma "down" "deep-value sync ${MODE} exited with code ${exit_code}"
  fi
}
trap handle_exit EXIT

# ---- Run sync ----
echo "[$(date -Iseconds)] deep-value sync --incremental --mode ${MODE} --end ${TODAY}"

SYNC_OUTPUT=""
SYNC_EXIT=0

case "$MODE" in
  daily)
    SYNC_OUTPUT=$(cargo run --release -- sync --incremental --mode daily --end "$TODAY" --delay-ms "$DELAY_MS" 2>&1) || SYNC_EXIT=$?
    ;;
  financial)
    SYNC_OUTPUT=$(cargo run --release -- sync --incremental --mode financial --end "$TODAY" --delay-ms "$DELAY_MS" 2>&1) || SYNC_EXIT=$?
    ;;
  meta)
    SYNC_OUTPUT=$(cargo run --release -- sync --incremental --mode meta --delay-ms "$DELAY_MS" 2>&1) || SYNC_EXIT=$?
    ;;
  *)
    echo "Unknown mode: $MODE (use daily | financial | meta)"
    exit 1
    ;;
esac

echo "$SYNC_OUTPUT"

# ---- Push result to Uptime Kuma ----
if [[ "$SYNC_EXIT" -eq 0 ]]; then
  # Extract stats: "Sync complete: 5 calls, 12000 rows, 10 skipped, 12.3s, 0 errors"
  STATS=$(echo "$SYNC_OUTPUT" | grep "Sync complete:" | tail -1)
  if [[ -n "$STATS" ]]; then
    push_kuma "up" "deep-value ${MODE}: ${STATS}"
  else
    push_kuma "up" "deep-value ${MODE}: completed (no stats line)"
  fi
else
  ERR_LAST=$(echo "$SYNC_OUTPUT" | tail -3 | tr '\n' ' ' | head -c 200)
  push_kuma "down" "deep-value ${MODE}: sync failed (exit=${SYNC_EXIT}): ${ERR_LAST}"
  exit "$SYNC_EXIT"
fi
