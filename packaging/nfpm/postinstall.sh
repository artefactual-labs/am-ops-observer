#!/bin/sh
set -e

SERVICE="am-ops-observer.service"
USER="am-report"
GROUP="am-report"

if ! getent group "$GROUP" >/dev/null 2>&1; then
  groupadd --system "$GROUP" >/dev/null 2>&1 || true
fi
if ! id -u "$USER" >/dev/null 2>&1; then
  useradd --system --no-create-home --home-dir /nonexistent \
    --shell /usr/sbin/nologin --gid "$GROUP" "$USER" >/dev/null 2>&1 || true
fi

DEFAULT_FILE="/etc/default/am-ops-observer"
CONFIG_DIR="/etc/am-ops-observer"
CONFIG_FILE="$CONFIG_DIR/config.env"
SECRETS_FILE="$CONFIG_DIR/secrets.env"
STATE_DIR="/var/lib/am-ops-observer"
if [ -f "$DEFAULT_FILE" ]; then
  chown root:root "$DEFAULT_FILE" >/dev/null 2>&1 || true
  chmod 0644 "$DEFAULT_FILE" >/dev/null 2>&1 || true
fi
mkdir -p "$CONFIG_DIR" >/dev/null 2>&1 || true
if [ -f "$CONFIG_FILE" ]; then
  chown root:root "$CONFIG_FILE" >/dev/null 2>&1 || true
  chmod 0644 "$CONFIG_FILE" >/dev/null 2>&1 || true
fi
if [ -f "$SECRETS_FILE" ]; then
  chown root:root "$SECRETS_FILE" >/dev/null 2>&1 || true
  chmod 0600 "$SECRETS_FILE" >/dev/null 2>&1 || true
fi
mkdir -p "$STATE_DIR" >/dev/null 2>&1 || true
chown "$USER:$GROUP" "$STATE_DIR" >/dev/null 2>&1 || true
chmod 0750 "$STATE_DIR" >/dev/null 2>&1 || true

if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
  systemctl daemon-reload >/dev/null 2>&1 || true
  systemctl enable --now "$SERVICE" >/dev/null 2>&1 || true
fi

exit 0
