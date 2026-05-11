#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  exec sudo -E bash "$0" "$@"
fi

export DEBIAN_FRONTEND=noninteractive
APT_CONFIG_DEB="${APT_CONFIG_DEB:-mysql-apt-config_0.8.36-1_all.deb}"
APT_CONFIG_URL="${APT_CONFIG_URL:-https://dev.mysql.com/get/${APT_CONFIG_DEB}}"
TMP_DEB="/tmp/${APT_CONFIG_DEB}"

apt-get update
apt-get install -y wget gnupg lsb-release ca-certificates tar gzip unzip
wget -O "${TMP_DEB}" "${APT_CONFIG_URL}"
dpkg -i "${TMP_DEB}" || apt-get -f install -y

CODENAME="$(. /etc/os-release && echo "${VERSION_CODENAME:-}")"
if [[ -z "${CODENAME}" ]]; then
  echo "Unable to determine Ubuntu codename." >&2
  exit 1
fi

cat >/etc/apt/sources.list.d/mysql.list <<EOF
deb http://repo.mysql.com/apt/ubuntu/ ${CODENAME} mysql-innovation mysql-tools
EOF

apt-get update
apt-get install -y mysql-shell

