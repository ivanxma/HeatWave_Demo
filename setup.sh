#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_SLUG="heatwave-demo"
VENV_DIR="${APP_VENV_DIR:-${ROOT_DIR}/.venv}"
RUNTIME_ENV_FILE="${RUNTIME_ENV_FILE:-${ROOT_DIR}/.runtime.env}"
SERVICE_USER="${SERVICE_USER:-${SUDO_USER:-$(id -un)}}"
SERVICE_GROUP="${SERVICE_GROUP:-$(id -gn "${SERVICE_USER}" 2>/dev/null || id -gn)}"
HOST="${HOST:-0.0.0.0}"
OS_FAMILY="${OS_FAMILY:-}"
DEPLOY_MODE="${DEPLOY_MODE:-}"
HTTP_PORT="${HTTP_PORT:-}"
HTTPS_PORT="${HTTPS_PORT:-}"
SSL_CERT_FILE="${SSL_CERT_FILE:-}"
SSL_KEY_FILE="${SSL_KEY_FILE:-}"
SKIP_PRIVILEGED_SETUP="${SKIP_PRIVILEGED_SETUP:-0}"

if [[ -n "${1:-}" && "${1}" != --* ]]; then
  OS_FAMILY="$1"
  shift
fi
if [[ -n "${1:-}" && "${1}" != --* ]]; then
  DEPLOY_MODE="$1"
  shift
fi

usage() {
  cat <<'EOF'
Usage: ./setup.sh [ol8|ol9|ubuntu|macos] [http|https|both|none] [options]

Options:
  --host VALUE
  --http-port PORT
  --https-port PORT
  --ssl-cert-file PATH
  --ssl-key-file PATH
  --service-user USER
  --service-group GROUP
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2 ;;
    --http-port) HTTP_PORT="$2"; shift 2 ;;
    --https-port) HTTPS_PORT="$2"; shift 2 ;;
    --ssl-cert-file) SSL_CERT_FILE="$2"; shift 2 ;;
    --ssl-key-file) SSL_KEY_FILE="$2"; shift 2 ;;
    --service-user) SERVICE_USER="$2"; shift 2 ;;
    --service-group) SERVICE_GROUP="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

run_privileged() {
  if [[ "${SKIP_PRIVILEGED_SETUP}" == "1" ]]; then
    echo "Skipping privileged command: $*"
    return 0
  fi
  if [[ "${EUID}" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

detect_os_family() {
  if [[ -n "${OS_FAMILY}" ]]; then
    OS_FAMILY="$(printf '%s' "${OS_FAMILY}" | tr '[:upper:]' '[:lower:]')"
    return
  fi
  if [[ "$(uname -s)" == "Darwin" ]]; then
    OS_FAMILY="macos"
    return
  fi
  if [[ ! -r /etc/os-release ]]; then
    echo "Cannot detect OS family. Pass one of: ol8, ol9, ubuntu, macos." >&2
    exit 1
  fi
  . /etc/os-release
  local os_id="${ID:-}"
  local major="${VERSION_ID%%.*}"
  case "${os_id}:${major}" in
    ubuntu:*) OS_FAMILY="ubuntu" ;;
    ol:8|oraclelinux:8) OS_FAMILY="ol8" ;;
    ol:9|oraclelinux:9) OS_FAMILY="ol9" ;;
    *) echo "Unsupported OS: ${PRETTY_NAME:-${os_id} ${major}}" >&2; exit 1 ;;
  esac
}

prompt_value() {
  local var_name="$1"
  local label="$2"
  local current="$3"
  local required="${4:-0}"
  local value=""
  while true; do
    read -r -p "${label}${current:+ [current: ${current}]}: " value
    if [[ -n "${value}" ]]; then
      printf -v "${var_name}" '%s' "${value}"
      return
    fi
    if [[ "${required}" != "1" ]]; then
      printf -v "${var_name}" '%s' "${current}"
      return
    fi
    echo "Enter an explicit value."
  done
}

prompt_port() {
  local var_name="$1"
  local label="$2"
  local suggested="$3"
  local value=""
  while true; do
    read -r -p "${label} [suggested: ${suggested}]: " value
    if [[ "${value}" =~ ^[0-9]+$ ]] && (( value > 0 && value < 65536 )); then
      printf -v "${var_name}" '%s' "${value}"
      return
    fi
    echo "Enter an explicit numeric TCP port between 1 and 65535."
  done
}

collect_interactive_defaults() {
  if [[ ! -t 0 ]]; then
    DEPLOY_MODE="${DEPLOY_MODE:-none}"
    HTTP_PORT="${HTTP_PORT:-80}"
    HTTPS_PORT="${HTTPS_PORT:-443}"
    return
  fi

  prompt_value OS_FAMILY "OS family (ol8, ol9, ubuntu, macos)" "${OS_FAMILY}" 1
  OS_FAMILY="$(printf '%s' "${OS_FAMILY}" | tr '[:upper:]' '[:lower:]')"

  while [[ ! "${DEPLOY_MODE}" =~ ^(http|https|both|none)$ ]]; do
    prompt_value DEPLOY_MODE "Deploy mode (http, https, both, none)" "${DEPLOY_MODE:-none}" 1
    DEPLOY_MODE="$(printf '%s' "${DEPLOY_MODE}" | tr '[:upper:]' '[:lower:]')"
  done

  prompt_value HOST "Listener host" "${HOST:-0.0.0.0}" 0
  if [[ "${OS_FAMILY}" != "macos" ]]; then
    prompt_value SERVICE_USER "Linux service user" "${SERVICE_USER}" 0
    prompt_value SERVICE_GROUP "Linux service group" "${SERVICE_GROUP}" 0
  fi
  if [[ "${DEPLOY_MODE}" == "http" || "${DEPLOY_MODE}" == "both" ]]; then
    [[ -n "${HTTP_PORT}" ]] || prompt_port HTTP_PORT "HTTP listener port" "80"
  fi
  if [[ "${DEPLOY_MODE}" == "https" || "${DEPLOY_MODE}" == "both" ]]; then
    [[ -n "${HTTPS_PORT}" ]] || prompt_port HTTPS_PORT "HTTPS listener port" "443"
    prompt_value SSL_CERT_FILE "TLS certificate path (blank generates self-signed)" "${SSL_CERT_FILE}" 0
    prompt_value SSL_KEY_FILE "TLS private key path (blank generates self-signed)" "${SSL_KEY_FILE}" 0
  fi
}

install_platform_packages() {
  if [[ "${SKIP_PRIVILEGED_SETUP}" == "1" ]]; then
    echo "Skipping OS package installation in unprivileged setup mode."
    return
  fi
  case "${OS_FAMILY}" in
    ubuntu)
      run_privileged apt-get update
      run_privileged env DEBIAN_FRONTEND=noninteractive apt-get install -y git openssl python3 python3-venv python3-pip ca-certificates
      ;;
    ol8)
      run_privileged dnf install -y git openssl python3 python3-pip python3-setuptools python3-wheel
      ;;
    ol9)
      run_privileged dnf install -y git openssl python3 python3-pip python3-setuptools python3-pip-wheel
      ;;
    macos)
      command -v python3 >/dev/null 2>&1 || { echo "Install Python 3 first, for example with Homebrew."; exit 1; }
      ;;
  esac
}

install_mysql_shell_innovation() {
  local installer="${ROOT_DIR}/${OS_FAMILY}/install_mysql_shell_innovation.sh"
  if [[ ! -f "${installer}" ]]; then
    echo "MySQL Shell Innovation installer is not present for ${OS_FAMILY}: ${installer}"
    return
  fi
  chmod 755 "${installer}"
  if [[ "${SKIP_PRIVILEGED_SETUP}" == "1" ]]; then
    echo "Skipping MySQL Shell installer in unprivileged setup mode."
    return
  fi
  bash "${installer}"
}

install_python_environment() {
  python3 -m venv "${VENV_DIR}"
  "${VENV_DIR}/bin/python" -m pip install --upgrade pip setuptools wheel
  "${VENV_DIR}/bin/python" -m pip install -r "${ROOT_DIR}/requirements.txt"
}

generate_tls_if_needed() {
  if [[ ! "${DEPLOY_MODE}" =~ ^(https|both)$ ]]; then
    return
  fi
  if [[ -n "${SSL_CERT_FILE}" && -n "${SSL_KEY_FILE}" ]]; then
    return
  fi
  local tls_dir="${ROOT_DIR}/tls"
  mkdir -p "${tls_dir}"
  SSL_CERT_FILE="${tls_dir}/heatwave-demo-selfsigned.crt"
  SSL_KEY_FILE="${tls_dir}/heatwave-demo-selfsigned.key"
  if [[ ! -f "${SSL_CERT_FILE}" || ! -f "${SSL_KEY_FILE}" ]]; then
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
      -keyout "${SSL_KEY_FILE}" \
      -out "${SSL_CERT_FILE}" \
      -subj "/CN=$(hostname -f 2>/dev/null || hostname || echo localhost)" >/dev/null 2>&1
    chmod 600 "${SSL_KEY_FILE}"
  fi
}

write_runtime_env() {
  cat > "${RUNTIME_ENV_FILE}" <<EOF
APP_SLUG=${APP_SLUG}
OS_FAMILY=${OS_FAMILY}
DEPLOY_MODE=${DEPLOY_MODE}
HOST=${HOST}
DEFAULT_HTTP_PORT=${HTTP_PORT:-80}
DEFAULT_HTTPS_PORT=${HTTPS_PORT:-443}
SSL_CERT_FILE=${SSL_CERT_FILE}
SSL_KEY_FILE=${SSL_KEY_FILE}
SERVICE_USER=${SERVICE_USER}
SERVICE_GROUP=${SERVICE_GROUP}
EOF
}

prepare_runtime_files() {
  chmod 755 "${ROOT_DIR}/start_http.sh" "${ROOT_DIR}/start_https.sh"
  [[ -f "${ROOT_DIR}/profiles.json" ]] || printf '{\n  "profiles": []\n}\n' > "${ROOT_DIR}/profiles.json"
  if [[ "${OS_FAMILY}" != "macos" && "${SKIP_PRIVILEGED_SETUP}" != "1" ]]; then
    run_privileged chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${VENV_DIR}" "${ROOT_DIR}/profiles.json" "${RUNTIME_ENV_FILE}"
    [[ -d "${ROOT_DIR}/tls" ]] && run_privileged chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${ROOT_DIR}/tls"
  fi
}

unit_capability_line() {
  local port="$1"
  if [[ "${port}" =~ ^[0-9]+$ ]] && (( port < 1024 )); then
    echo "AmbientCapabilities=CAP_NET_BIND_SERVICE"
  fi
}

write_systemd_unit() {
  local unit_name="$1"
  local mode="$2"
  local port="$3"
  local start_script="$4"
  local unit_file="/etc/systemd/system/${unit_name}"
  local temp_file
  temp_file="$(mktemp)"
  cat > "${temp_file}" <<EOF
[Unit]
Description=HeatWave Demo ${mode^^} service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${ROOT_DIR}
Environment=APP_ADDRESS=${HOST}
Environment=APP_PORT=${port}
Environment=APP_SKIP_SUDO_REEXEC=1
ExecStart=/bin/bash ${ROOT_DIR}/${start_script}
Restart=on-failure
RestartSec=5
$(unit_capability_line "${port}")

[Install]
WantedBy=multi-user.target
EOF
  run_privileged install -m 0644 "${temp_file}" "${unit_file}"
  rm -f "${temp_file}"
}

configure_systemd() {
  if [[ "${OS_FAMILY}" == "macos" || "${SKIP_PRIVILEGED_SETUP}" == "1" ]]; then
    return
  fi
  command -v systemctl >/dev/null 2>&1 || { echo "systemctl is unavailable; start with ./start_http.sh or ./start_https.sh."; return; }
  [[ -d /run/systemd/system ]] || { echo "systemd is not active; start with ./start_http.sh or ./start_https.sh."; return; }

  write_systemd_unit "${APP_SLUG}-http.service" "http" "${HTTP_PORT:-80}" "start_http.sh"
  write_systemd_unit "${APP_SLUG}-https.service" "https" "${HTTPS_PORT:-443}" "start_https.sh"
  run_privileged systemctl daemon-reload

  case "${DEPLOY_MODE}" in
    http)
      run_privileged systemctl enable --now "${APP_SLUG}-http.service"
      run_privileged systemctl disable --now "${APP_SLUG}-https.service" || true
      ;;
    https)
      run_privileged systemctl disable --now "${APP_SLUG}-http.service" || true
      if [[ -f "${SSL_CERT_FILE}" && -f "${SSL_KEY_FILE}" ]]; then
        run_privileged systemctl enable --now "${APP_SLUG}-https.service"
      else
        run_privileged systemctl disable --now "${APP_SLUG}-https.service" || true
        echo "HTTPS unit installed but disabled because TLS certificate or key is missing."
      fi
      ;;
    both)
      run_privileged systemctl enable --now "${APP_SLUG}-http.service"
      if [[ -f "${SSL_CERT_FILE}" && -f "${SSL_KEY_FILE}" ]]; then
        run_privileged systemctl enable --now "${APP_SLUG}-https.service"
      else
        run_privileged systemctl disable --now "${APP_SLUG}-https.service" || true
        echo "HTTPS unit installed but disabled because TLS certificate or key is missing."
      fi
      ;;
    none)
      run_privileged systemctl disable --now "${APP_SLUG}-http.service" || true
      run_privileged systemctl disable --now "${APP_SLUG}-https.service" || true
      ;;
  esac
}

open_firewall_port() {
  local port="$1"
  [[ -n "${port}" ]] || return
  case "${OS_FAMILY}" in
    ol8|ol9)
      if command -v firewall-cmd >/dev/null 2>&1; then
        run_privileged firewall-cmd --permanent --add-port="${port}/tcp" || true
        run_privileged firewall-cmd --reload || true
      else
        echo "firewall-cmd is unavailable; manually allow TCP port ${port} if needed."
      fi
      ;;
    ubuntu)
      if command -v ufw >/dev/null 2>&1; then
        run_privileged ufw allow "${port}/tcp" || true
      else
        echo "ufw is unavailable; manually allow TCP port ${port} if needed."
      fi
      ;;
  esac
}

configure_firewall() {
  if [[ "${SKIP_PRIVILEGED_SETUP}" == "1" || "${OS_FAMILY}" == "macos" ]]; then
    return
  fi
  [[ "${DEPLOY_MODE}" == "http" || "${DEPLOY_MODE}" == "both" ]] && open_firewall_port "${HTTP_PORT:-80}"
  [[ "${DEPLOY_MODE}" == "https" || "${DEPLOY_MODE}" == "both" ]] && open_firewall_port "${HTTPS_PORT:-443}"
}

main() {
  detect_os_family
  DEPLOY_MODE="$(printf '%s' "${DEPLOY_MODE}" | tr '[:upper:]' '[:lower:]')"
  collect_interactive_defaults
  DEPLOY_MODE="${DEPLOY_MODE:-none}"
  if [[ ! "${OS_FAMILY}" =~ ^(ol8|ol9|ubuntu|macos)$ ]]; then
    echo "Unsupported OS family: ${OS_FAMILY}. Expected ol8, ol9, ubuntu, or macos." >&2
    exit 1
  fi
  if [[ ! "${DEPLOY_MODE}" =~ ^(http|https|both|none)$ ]]; then
    echo "Unsupported deploy mode: ${DEPLOY_MODE}. Expected http, https, both, or none." >&2
    exit 1
  fi
  HTTP_PORT="${HTTP_PORT:-80}"
  HTTPS_PORT="${HTTPS_PORT:-443}"
  install_platform_packages
  install_mysql_shell_innovation
  install_python_environment
  generate_tls_if_needed
  write_runtime_env
  prepare_runtime_files
  configure_systemd
  configure_firewall

  echo
  echo "Setup complete for ${OS_FAMILY}."
  echo "Runtime defaults: ${RUNTIME_ENV_FILE}"
  echo "HTTP:  ${HOST}:${HTTP_PORT}"
  echo "HTTPS: ${HOST}:${HTTPS_PORT}"
  echo "Start locally with ./start_http.sh or ./start_https.sh."
}

main "$@"
