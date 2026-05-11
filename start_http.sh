#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_ENV_FILE="${RUNTIME_ENV_FILE:-${ROOT_DIR}/.runtime.env}"
if [[ -f "${RUNTIME_ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${RUNTIME_ENV_FILE}"
fi
APP_FILE="${APP_FILE:-app.py}"
ADDRESS="${APP_ADDRESS:-${HOST:-0.0.0.0}}"
PORT="${1:-${APP_PORT:-${DEFAULT_HTTP_PORT:-80}}}"
PYTHON_BIN="${APP_PYTHON_BIN:-${ROOT_DIR}/.venv/bin/python}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="python3"
fi

require_command "${PYTHON_BIN}"
echo "Using Python interpreter: ${PYTHON_BIN}"

if (( PORT < 1024 )) && [[ "${EUID}" -ne 0 ]]; then
  echo "Re-running with sudo so the app can bind to port ${PORT}."
  exec sudo -E bash "$0" "$@"
fi

export APP_ADDRESS="${ADDRESS}"
export APP_PORT="${PORT}"
export HOST="${ADDRESS}"
export PORT="${PORT}"
unset APP_SSL_CERT_FILE
unset APP_SSL_KEY_FILE

exec "${PYTHON_BIN}" "${ROOT_DIR}/${APP_FILE}"
