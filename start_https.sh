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
PORT="${APP_PORT:-${DEFAULT_HTTPS_PORT:-443}}"
SSL_CN="${APP_SSL_CN:-localhost}"
SKIP_SUDO_REEXEC="${APP_SKIP_SUDO_REEXEC:-0}"
PYTHON_BIN="${APP_PYTHON_BIN:-${ROOT_DIR}/.venv/bin/python}"
CERT_DIR="${ROOT_DIR}/tls"
CERT_FILE="${APP_SSL_CERT_FILE:-${SSL_CERT_FILE:-${CERT_DIR}/heatwave-demo-selfsigned.crt}}"
KEY_FILE="${APP_SSL_KEY_FILE:-${SSL_KEY_FILE:-${CERT_DIR}/heatwave-demo-selfsigned.key}}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

generate_self_signed_cert() {
  mkdir -p "${CERT_DIR}"
  openssl req \
    -x509 \
    -nodes \
    -days 365 \
    -newkey rsa:2048 \
    -keyout "${KEY_FILE}" \
    -out "${CERT_FILE}" \
    -subj "/CN=${SSL_CN}" >/dev/null 2>&1
  chmod 600 "${KEY_FILE}"
}

require_command openssl

if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="python3"
fi

require_command "${PYTHON_BIN}"
echo "Using Python interpreter: ${PYTHON_BIN}"

if [[ ! -f "${CERT_FILE}" || ! -f "${KEY_FILE}" ]]; then
  generate_self_signed_cert
fi

if (( PORT < 1024 )) && [[ "${EUID}" -ne 0 ]] && [[ "${SKIP_SUDO_REEXEC}" != "1" ]]; then
  echo "Re-running with sudo so the app can bind to port ${PORT}."
  exec sudo -E bash "$0" "$@"
fi

export APP_ADDRESS="${ADDRESS}"
export APP_PORT="${PORT}"
export APP_SSL_CERT_FILE="${CERT_FILE}"
export APP_SSL_KEY_FILE="${KEY_FILE}"
export HOST="${ADDRESS}"
export PORT="${PORT}"
export SSL_CERT_FILE="${CERT_FILE}"
export SSL_KEY_FILE="${KEY_FILE}"

exec "${PYTHON_BIN}" "${ROOT_DIR}/${APP_FILE}"
