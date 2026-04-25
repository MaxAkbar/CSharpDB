#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="com.csharpdb.daemon"
INSTALL_DIR="/usr/local/lib/csharpdb-daemon"
DATA_DIR="/usr/local/var/csharpdb"
LOG_DIR="/usr/local/var/log"
URL="http://127.0.0.1:5820"
SOURCE_DIR=""
FORCE=0
START=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --service-name) SERVICE_NAME="$2"; shift 2 ;;
    --install-dir) INSTALL_DIR="$2"; shift 2 ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --log-dir) LOG_DIR="$2"; shift 2 ;;
    --url) URL="$2"; shift 2 ;;
    --source-dir) SOURCE_DIR="$2"; shift 2 ;;
    --force) FORCE=1; shift ;;
    --start) START=1; shift ;;
    -h|--help)
      echo "Usage: sudo ./install-csharpdb-daemon.sh [--service-name NAME] [--install-dir PATH] [--data-dir PATH] [--url URL] [--force] [--start]"
      exit 0
      ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Installing CSharpDB.Daemon as a launchd service requires root." >&2
  exit 1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "$SOURCE_DIR" ]]; then
  SOURCE_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
fi

PLIST_PATH="/Library/LaunchDaemons/${SERVICE_NAME}.plist"
DATABASE_PATH="${DATA_DIR}/csharpdb.db"

if [[ -f "$PLIST_PATH" && "$FORCE" -ne 1 ]]; then
  echo "LaunchDaemon already exists: $PLIST_PATH. Re-run with --force to replace it." >&2
  exit 1
fi

if [[ -f "$PLIST_PATH" ]]; then
  launchctl bootout system "$PLIST_PATH" >/dev/null 2>&1 || true
fi

mkdir -p "$INSTALL_DIR" "$DATA_DIR" "$LOG_DIR"
cp -a "$SOURCE_DIR"/. "$INSTALL_DIR"/
chmod +x "$INSTALL_DIR/CSharpDB.Daemon"

cat > "$INSTALL_DIR/appsettings.Production.json" <<JSON
{
  "ConnectionStrings": {
    "CSharpDB": "Data Source=$DATABASE_PATH"
  },
  "CSharpDB": {
    "Daemon": {
      "EnableRestApi": true
    },
    "HostDatabase": {
      "OpenMode": "HybridIncrementalDurable",
      "ImplicitInsertExecutionMode": "ConcurrentWriteTransactions",
      "UseWriteOptimizedPreset": true,
      "HotTableNames": [],
      "HotCollectionNames": []
    }
  }
}
JSON

sed \
  -e "s|{{SERVICE_NAME}}|$SERVICE_NAME|g" \
  -e "s|{{INSTALL_DIR}}|$INSTALL_DIR|g" \
  -e "s|{{DATA_DIR}}|$DATA_DIR|g" \
  -e "s|{{DATABASE_PATH}}|$DATABASE_PATH|g" \
  -e "s|{{LOG_DIR}}|$LOG_DIR|g" \
  -e "s|{{URL}}|$URL|g" \
  "$INSTALL_DIR/service/macos/com.csharpdb.daemon.plist" > "$PLIST_PATH"

chown root:wheel "$PLIST_PATH"
chmod 0644 "$PLIST_PATH"

if [[ "$START" -eq 1 ]]; then
  launchctl bootstrap system "$PLIST_PATH"
  launchctl enable "system/${SERVICE_NAME}"
else
  echo "LaunchDaemon installed but not started. Start with: sudo launchctl bootstrap system $PLIST_PATH"
fi

echo "Installed $SERVICE_NAME"
echo "  Install directory: $INSTALL_DIR"
echo "  Data directory: $DATA_DIR"
echo "  URL: $URL"
