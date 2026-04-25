#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="csharpdb-daemon"
INSTALL_DIR="/opt/csharpdb-daemon"
REMOVE_INSTALL_DIR=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --service-name) SERVICE_NAME="$2"; shift 2 ;;
    --install-dir) INSTALL_DIR="$2"; shift 2 ;;
    --remove-install-dir) REMOVE_INSTALL_DIR=1; shift ;;
    -h|--help)
      echo "Usage: sudo ./uninstall-csharpdb-daemon.sh [--service-name NAME] [--install-dir PATH] [--remove-install-dir]"
      exit 0
      ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Uninstalling CSharpDB.Daemon as a systemd service requires root." >&2
  exit 1
fi

SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
ENV_FILE="/etc/csharpdb-daemon/${SERVICE_NAME}.env"

systemctl stop "${SERVICE_NAME}.service" >/dev/null 2>&1 || true
systemctl disable "${SERVICE_NAME}.service" >/dev/null 2>&1 || true
rm -f "$SERVICE_FILE" "$ENV_FILE"
systemctl daemon-reload

if [[ "$REMOVE_INSTALL_DIR" -eq 1 && -d "$INSTALL_DIR" ]]; then
  rm -rf "$INSTALL_DIR"
fi

echo "Uninstalled ${SERVICE_NAME}.service"
