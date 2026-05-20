#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="quic-av-pubsub"
BIN_PATH="/usr/local/bin/quic-av-server"
CERT_DIR="/etc/quic-av-pubsub"
LISTEN_ADDR="${LISTEN_ADDR:-:4430}"
SERVICE_USER="${SERVICE_USER:-root}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

usage() {
    cat <<EOF
Usage: $0 [options]

Install quic-av-pubsub as a systemd service.

Options:
  -b, --bin PATH       Server binary path (default: ./quic-av-server*)
  -c, --cert DIR       Certificate directory (default: ./)
  -a, --addr ADDR      Listen address (default: :4430)
  -u, --user USER      System user to run the service (default: root)
  -h, --help           Show this help

Environment variables:
  LISTEN_ADDR          Same as --addr
  SERVICE_USER         Same as --user

Examples:
  # Install from release zip (run in extracted directory)
  sudo ./install-service.sh

  # Custom install
  sudo ./install-service.sh --bin /tmp/quic-av-server-linux-amd64 --addr :8443
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -b|--bin) BIN_PATH="$2"; shift 2 ;;
        -c|--cert) CERT_DIR="$2"; shift 2 ;;
        -a|--addr) LISTEN_ADDR="$2"; shift 2 ;;
        -u|--user) SERVICE_USER="$2"; shift 2 ;;
        -h|--help) usage ;;
        *) error "Unknown option: $1"; usage ;;
    esac
done

# --- Preflight checks ---
if [ "$(id -u)" -ne 0 ]; then
    error "This script must be run as root (sudo)."
    exit 1
fi

if ! command -v systemctl &>/dev/null; then
    error "systemd not found. This script only supports systemd-based systems."
    exit 1
fi

# --- Locate binary ---
if [ ! -x "$BIN_PATH" ]; then
    found=$(find . -maxdepth 1 -type f -name 'quic-av-server*' ! -name '*.zip' ! -name '*.gz' | head -1)
    if [ -n "$found" ]; then
        BIN_PATH="$found"
        chmod +x "$BIN_PATH"
    else
        error "Binary not found at $BIN_PATH and no quic-av-server* file in current directory."
        error "Place the binary in the current directory or specify with --bin."
        exit 1
    fi
fi

info "Installing binary to /usr/local/bin/quic-av-server"
if [ "$(realpath "$BIN_PATH" 2>/dev/null)" != "/usr/local/bin/quic-av-server" ]; then
    cp "$BIN_PATH" /usr/local/bin/quic-av-server
fi
chmod 755 /usr/local/bin/quic-av-server

# --- Install certificates ---
mkdir -p "$CERT_DIR"

if [ -f server.crt ] && [ -f server.key ]; then
    info "Installing TLS certificates from current directory"
    cp server.crt server.key "$CERT_DIR/"
elif [ ! -f "$CERT_DIR/server.crt" ] || [ ! -f "$CERT_DIR/server.key" ]; then
    warn "No TLS certificates found. Generating self-signed certificates..."
    if command -v openssl &>/dev/null; then
        openssl ecparam -name prime256v1 -genkey -noout -out "$CERT_DIR/server.key"
        openssl req -new -x509 -key "$CERT_DIR/server.key" -out "$CERT_DIR/server.crt" \
            -days 3650 -subj "/CN=quic-av-pubsub" \
            -addext "subjectAltName=IP:127.0.0.1,DNS:localhost,DNS:arm2.pvpv.bid"
        info "Self-signed certificates generated"
    else
        error "openssl not found. Please provide server.crt and server.key in $CERT_DIR"
        exit 1
    fi
fi
chmod 600 "$CERT_DIR/server.key"

# --- Kernel tuning ---
info "Applying kernel UDP buffer tuning"
cat > /etc/sysctl.d/99-quic-av-pubsub.conf <<'SYSCTL'
net.core.rmem_max = 67108864
net.core.wmem_max = 67108864
net.core.rmem_default = 16777216
net.core.wmem_default = 16777216
net.core.netdev_max_backlog = 5000
net.ipv4.udp_rmem_min = 8192
net.ipv4.udp_wmem_min = 8192
SYSCTL
sysctl -p /etc/sysctl.d/99-quic-av-pubsub.conf &>/dev/null || true

# --- Create systemd service ---
info "Creating systemd service: $SERVICE_NAME"
cat > /etc/systemd/system/${SERVICE_NAME}.service <<SYSTEMD
[Unit]
Description=QUIC AV Pub/Sub Server
Documentation=https://github.com/yourorg/quic-pubsub
Wants=network-online.target
After=network.target network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
ExecStart=/usr/local/bin/quic-av-server \\
    -addr ${LISTEN_ADDR} \\
    -cert ${CERT_DIR}/server.crt \\
    -key ${CERT_DIR}/server.key \\
    -v
Restart=always
RestartSec=5
Nice=-10
LimitNOFILE=1048576
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
SYSTEMD

# --- Enable and start ---
systemctl daemon-reload
systemctl enable ${SERVICE_NAME}
systemctl restart ${SERVICE_NAME}

echo ""
info "Installation complete!"
echo ""
echo "  Service:   ${SERVICE_NAME}"
echo "  Binary:    /usr/local/bin/quic-av-server"
echo "  Config:    ${CERT_DIR}"
echo "  Listen:    UDP ${LISTEN_ADDR}"
echo ""
echo "Commands:"
echo "  sudo systemctl status ${SERVICE_NAME}"
echo "  sudo journalctl -u ${SERVICE_NAME} -f"
echo "  sudo systemctl restart ${SERVICE_NAME}"
echo ""

# --- Verify ---
sleep 2
if systemctl is-active --quiet ${SERVICE_NAME}; then
    info "Service is running."
    systemctl status ${SERVICE_NAME} --no-pager | head -8
else
    error "Service failed to start. Check logs: journalctl -u ${SERVICE_NAME} -n 50"
    exit 1
fi
