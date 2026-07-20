#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
UNIT_SOURCE_DIR="${PROJECT_DIR}/ops/systemd"
UNIT_TARGET_DIR="${XDG_CONFIG_HOME:-${HOME}/.config}/systemd/user"
ENV_FILE="${PROJECT_DIR}/.env"
START_NOW=false
DRY_RUN=false

usage() {
  echo "Usage: $0 [--start-now] [--dry-run]"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-now)
      START_NOW=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing ${ENV_FILE}; TUSHARE_TOKEN must be provided through this untracked environment file." >&2
  exit 2
fi

if [[ "${DRY_RUN}" == true ]]; then
  echo "Would install user units from ${UNIT_SOURCE_DIR} to ${UNIT_TARGET_DIR}"
  echo "Would restrict ${ENV_FILE} to mode 600"
  echo "Would enable tushare-mirror-periodic-sync.timer"
  if [[ "${START_NOW}" == true ]]; then
    echo "Would start tushare-mirror-periodic-sync.service without waiting"
  fi
  exit 0
fi

chmod 600 "${ENV_FILE}"
install -d -m 700 "${UNIT_TARGET_DIR}"
install -m 644 \
  "${UNIT_SOURCE_DIR}/tushare-mirror-periodic-sync.service" \
  "${UNIT_TARGET_DIR}/tushare-mirror-periodic-sync.service"
install -m 644 \
  "${UNIT_SOURCE_DIR}/tushare-mirror-periodic-sync.timer" \
  "${UNIT_TARGET_DIR}/tushare-mirror-periodic-sync.timer"

systemctl --user daemon-reload
systemctl --user enable --now tushare-mirror-periodic-sync.timer
if [[ "${START_NOW}" == true ]]; then
  systemctl --user start --no-block tushare-mirror-periodic-sync.service
fi

systemctl --user status tushare-mirror-periodic-sync.timer --no-pager
