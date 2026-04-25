#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="com.csharpdb.daemon"
INSTALL_DIR="/usr/local/lib/csharpdb-daemon"
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
  echo "Uninstalling CSharpDB.Daemon as a launchd service requires root." >&2
  exit 1
fi

PLIST_PATH="/Library/LaunchDaemons/${SERVICE_NAME}.plist"

if [[ -f "$PLIST_PATH" ]]; then
  launchctl bootout system "$PLIST_PATH" >/dev/null 2>&1 || true
  rm -f "$PLIST_PATH"
fi

if [[ "$REMOVE_INSTALL_DIR" -eq 1 && -d "$INSTALL_DIR" ]]; then
  rm -rf "$INSTALL_DIR"
fi

echo "Uninstalled $SERVICE_NAME"
