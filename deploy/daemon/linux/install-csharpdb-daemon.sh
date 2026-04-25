#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="csharpdb-daemon"
INSTALL_DIR="/opt/csharpdb-daemon"
DATA_DIR="/var/lib/csharpdb"
URL="http://127.0.0.1:5820"
SERVICE_USER="csharpdb"
SERVICE_GROUP="csharpdb"
SOURCE_DIR=""
FORCE=0
START=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --service-name) SERVICE_NAME="$2"; shift 2 ;;
    --install-dir) INSTALL_DIR="$2"; shift 2 ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --url) URL="$2"; shift 2 ;;
    --service-user) SERVICE_USER="$2"; shift 2 ;;
    --service-group) SERVICE_GROUP="$2"; shift 2 ;;
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
  echo "Installing CSharpDB.Daemon as a systemd service requires root." >&2
  exit 1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "$SOURCE_DIR" ]]; then
  SOURCE_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
fi

SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
ENV_DIR="/etc/csharpdb-daemon"
ENV_FILE="${ENV_DIR}/${SERVICE_NAME}.env"
DATABASE_PATH="${DATA_DIR}/csharpdb.db"

if [[ -f "$SERVICE_FILE" && "$FORCE" -ne 1 ]]; then
  echo "Service file already exists: $SERVICE_FILE. Re-run with --force to replace it." >&2
  exit 1
fi

if ! getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
  groupadd --system "$SERVICE_GROUP"
fi

if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --gid "$SERVICE_GROUP" --home-dir "$DATA_DIR" --shell /usr/sbin/nologin "$SERVICE_USER"
fi

if systemctl list-unit-files "${SERVICE_NAME}.service" >/dev/null 2>&1; then
  systemctl stop "${SERVICE_NAME}.service" >/dev/null 2>&1 || true
fi

mkdir -p "$INSTALL_DIR" "$DATA_DIR" "$ENV_DIR"
cp -a "$SOURCE_DIR"/. "$INSTALL_DIR"/
chmod +x "$INSTALL_DIR/CSharpDB.Daemon"
chown -R "$SERVICE_USER:$SERVICE_GROUP" "$DATA_DIR"

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

cat > "$ENV_FILE" <<ENV
DOTNET_ENVIRONMENT=Production
ASPNETCORE_ENVIRONMENT=Production
ASPNETCORE_URLS=$URL
ConnectionStrings__CSharpDB="Data Source=$DATABASE_PATH"
CSharpDB__Daemon__EnableRestApi=true
ENV

sed \
  -e "s|{{INSTALL_DIR}}|$INSTALL_DIR|g" \
  -e "s|{{ENV_FILE}}|$ENV_FILE|g" \
  -e "s|{{SERVICE_NAME}}|$SERVICE_NAME|g" \
  -e "s|{{SERVICE_USER}}|$SERVICE_USER|g" \
  -e "s|{{SERVICE_GROUP}}|$SERVICE_GROUP|g" \
  "$INSTALL_DIR/service/linux/csharpdb-daemon.service" > "$SERVICE_FILE"

chmod 0644 "$SERVICE_FILE"
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}.service"

if [[ "$START" -eq 1 ]]; then
  systemctl start "${SERVICE_NAME}.service"
fi

echo "Installed ${SERVICE_NAME}.service"
echo "  Install directory: $INSTALL_DIR"
echo "  Data directory: $DATA_DIR"
echo "  URL: $URL"
