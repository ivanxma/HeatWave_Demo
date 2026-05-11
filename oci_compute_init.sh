#!/bin/bash
set -euxo pipefail

APP_REPO="${APP_REPO:-https://github.com/ivanxma/HeatWave_Demo.git}"
APP_DIR="${APP_DIR:-/home/opc/HeatWave_Demo}"
APP_USER="${APP_USER:-opc}"
APP_GROUP="${APP_GROUP:-opc}"
OS_FAMILY="${OS_FAMILY:-ol9}"
DEPLOY_MODE="${DEPLOY_MODE:-both}"
HTTP_PORT="${HTTP_PORT:-80}"
HTTPS_PORT="${HTTPS_PORT:-443}"
SERVICE_NAME="${SERVICE_NAME:-heatwave-demo-https.service}"

STATE_DIR="/var/lib/heatwave-demo-init"
INSTALLING_FLAG="$STATE_DIR/installing"
INSTALLED_FLAG="$STATE_DIR/installed"
FAILED_FLAG="$STATE_DIR/failed"
SERVICE_FILE="$STATE_DIR/service-name"
LOG_FILE="/var/log/heatwave-demo-init.log"
PROFILE_BANNER="/etc/profile.d/heatwave-demo-login-banner.sh"

mkdir -p "$STATE_DIR"
chmod 0755 "$STATE_DIR"
: > "$LOG_FILE"
exec > >(tee -a "$LOG_FILE") 2>&1

touch "$INSTALLING_FLAG"
rm -f "$INSTALLED_FLAG" "$FAILED_FLAG"
printf '%s\n' "$SERVICE_NAME" > "$SERVICE_FILE"
chmod 0644 "$SERVICE_FILE"

cat > "$PROFILE_BANNER" <<'EOF'
#!/bin/bash
STATE_DIR="/var/lib/heatwave-demo-init"
INSTALLING_FLAG="$STATE_DIR/installing"
INSTALLED_FLAG="$STATE_DIR/installed"
FAILED_FLAG="$STATE_DIR/failed"
SERVICE_FILE="$STATE_DIR/service-name"
LOG_FILE="/var/log/heatwave-demo-init.log"

case $- in
  *i*) ;;
  *) return 0 ;;
esac

[ "${USER:-}" = "opc" ] || return 0

SERVICE_NAME=""
if [ -r "$SERVICE_FILE" ]; then
  SERVICE_NAME="$(head -n 1 "$SERVICE_FILE")"
fi

printf '\n'
if [ -f "$INSTALLING_FLAG" ]; then
  printf '%s\n' "Please wait until installation to be completed."
elif [ -f "$INSTALLED_FLAG" ]; then
  printf '%s\n' "The service is installed."
  if [ -n "$SERVICE_NAME" ]; then
    systemctl --no-pager --full --lines=12 status "$SERVICE_NAME" || true
  fi
elif [ -f "$FAILED_FLAG" ]; then
  printf '%s\n' "The installation finished with errors. Review $LOG_FILE."
  if [ -n "$SERVICE_NAME" ]; then
    systemctl --no-pager --full --lines=12 status "$SERVICE_NAME" || true
  fi
fi
printf '\n'
EOF
chmod 0755 "$PROFILE_BANNER"

finish_install() {
  local exit_code="$1"
  rm -f "$INSTALLING_FLAG"
  if [ "$exit_code" -eq 0 ]; then
    touch "$INSTALLED_FLAG"
    rm -f "$FAILED_FLAG"
  else
    touch "$FAILED_FLAG"
    rm -f "$INSTALLED_FLAG"
  fi
}
trap 'finish_install $?' EXIT

if command -v dnf >/dev/null 2>&1; then
  dnf install -y git
elif command -v apt-get >/dev/null 2>&1; then
  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y git
else
  echo "Unable to install git automatically." >&2
  exit 1
fi

if [ -d "$APP_DIR/.git" ]; then
  sudo -u "$APP_USER" git -C "$APP_DIR" fetch --all --prune
  sudo -u "$APP_USER" git -C "$APP_DIR" pull --ff-only
elif [ -d "$APP_DIR" ]; then
  mv "$APP_DIR" "${APP_DIR}.$(date +%Y%m%d%H%M%S)"
  sudo -u "$APP_USER" git clone "$APP_REPO" "$APP_DIR"
else
  sudo -u "$APP_USER" git clone "$APP_REPO" "$APP_DIR"
fi

cd "$APP_DIR"
sudo -u "$APP_USER" env \
  HOST=0.0.0.0 \
  SERVICE_USER="$APP_USER" \
  SERVICE_GROUP="$APP_GROUP" \
  bash ./setup.sh "$OS_FAMILY" "$DEPLOY_MODE" --http-port "$HTTP_PORT" --https-port "$HTTPS_PORT"

systemctl --no-pager --full --lines=12 status "$SERVICE_NAME" || true
