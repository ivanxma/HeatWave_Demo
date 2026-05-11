#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARCHIVE_NAME="${AIRPORTDB_ARCHIVE:-airport-db.tar.gz}"
ARCHIVE_URL="${AIRPORTDB_URL:-https://downloads.mysql.com/docs/airport-db.tar.gz}"
ARCHIVE_PATH="${AIRPORTDB_ARCHIVE_PATH:-${ROOT_DIR}/${ARCHIVE_NAME}}"
EXTRACT_DIR="${AIRPORTDB_EXTRACT_DIR:-${ROOT_DIR}/airport-db}"
MYSQLSH_BIN="${MYSQLSH_BIN:-mysqlsh}"
LOAD_THREADS="${LOAD_THREADS:-16}"
HEATWAVE_LOAD="${HEATWAVE_LOAD:-1}"
MYSQL_USER="${1:-}"
MYSQL_HOST="${2:-}"

usage() {
  cat <<'EOF'
Usage:
  ./load_airportdb.sh <user> <ip>

Optional:
  LOAD_THREADS=16
  HEATWAVE_LOAD=1
  AIRPORTDB_ARCHIVE_PATH=/path/airport-db.tar.gz
  AIRPORTDB_EXTRACT_DIR=/path/airport-db
EOF
}

require_arg() {
  local value="$1"
  local label="$2"
  if [[ -z "${value}" ]]; then
    echo "Missing required argument: ${label}" >&2
    usage >&2
    exit 1
  fi
}

ensure_mysqlsh() {
  if command -v "${MYSQLSH_BIN}" >/dev/null 2>&1; then
    return
  fi

  if [[ ! -r /etc/os-release ]]; then
    echo "Cannot detect operating system because /etc/os-release is missing." >&2
    exit 1
  fi

  # shellcheck disable=SC1091
  . /etc/os-release
  case "${ID:-}" in
    ubuntu)
      bash "${ROOT_DIR}/ubuntu/install_mysql_shell_innovation.sh"
      ;;
    ol|oracle|oraclelinux)
      case "${VERSION_ID%%.*}" in
        8) bash "${ROOT_DIR}/ol8/install_mysql_shell_innovation.sh" ;;
        9) bash "${ROOT_DIR}/ol9/install_mysql_shell_innovation.sh" ;;
        *)
          echo "Unsupported Oracle Linux version: ${VERSION_ID:-unknown}" >&2
          exit 1
          ;;
      esac
      ;;
    *)
      echo "Unsupported operating system: ${ID:-unknown} ${VERSION_ID:-}" >&2
      exit 1
      ;;
  esac

  command -v "${MYSQLSH_BIN}" >/dev/null 2>&1 || {
    echo "mysqlsh is still not available after installation." >&2
    exit 1
  }
}

ensure_archive() {
  if [[ -f "${ARCHIVE_PATH}" ]]; then
    return
  fi
  command -v wget >/dev/null 2>&1 || {
    echo "wget is required to download ${ARCHIVE_URL}" >&2
    exit 1
  }
  wget -O "${ARCHIVE_PATH}" "${ARCHIVE_URL}"
}

extract_archive() {
  if [[ -d "${EXTRACT_DIR}" ]]; then
    return
  fi
  tar xvzf "${ARCHIVE_PATH}" -C "$(dirname "${EXTRACT_DIR}")"
}

load_dump() {
  (
    cd "${ROOT_DIR}"
    "${MYSQLSH_BIN}" --js "${MYSQL_USER}@${MYSQL_HOST}" -e \
      "util.loadDump('airport-db', {threads: ${LOAD_THREADS}, deferTableIndexes: 'all', ignoreVersion: true})"
  )
}

load_heatwave() {
  if [[ "${HEATWAVE_LOAD}" != "1" ]]; then
    return
  fi
  "${MYSQLSH_BIN}" --sql "${MYSQL_USER}@${MYSQL_HOST}" -e \
    "CALL sys.heatwave_load(JSON_ARRAY('airportdb'), NULL);"
}

main() {
  require_arg "${MYSQL_USER}" "<user>"
  require_arg "${MYSQL_HOST}" "<ip>"
  ensure_mysqlsh
  ensure_archive
  extract_archive
  load_dump
  load_heatwave
}

main "$@"
