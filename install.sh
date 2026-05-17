#!/usr/bin/env bash
# hermes-agent-cluster one-click installer
# Usage: bash install.sh [--help] [--uninstall] [--role main|worker] [--token TOKEN] [--endpoint URL]

set -euo pipefail

# ─── Constants ───────────────────────────────────────────────────────────────
REPO="HughesCuit/hermes-agent-cluster"
RAW_BASE="https://raw.githubusercontent.com/${REPO}/main/plugins/hermes-agent-cluster"
RELEASE_BASE="https://github.com/${REPO}/releases/latest/download"
BINARY_NAME="hermes-cluster"
INSTALL_BIN_DIR="${HOME}/.local/bin"
INSTALL_BIN="${INSTALL_BIN_DIR}/${BINARY_NAME}"
PLUGIN_DIR="${HOME}/.hermes/plugins/hermes-agent-cluster"
CONFIG_FILE="${HOME}/.hermes/cluster.yaml"

# ─── Colors & Symbols ────────────────────────────────────────────────────────
if [[ -t 1 ]]; then
    GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[0;33m'; BOLD='\033[1m'; RESET='\033[0m'
else
    GREEN=''; RED=''; YELLOW=''; BOLD=''; RESET=''
fi
ok()   { printf "${GREEN}✓${RESET} %s\n" "$*"; }
fail() { printf "${RED}✗${RESET} %s\n" "$*" >&2; }
warn() { printf "${YELLOW}⚠${RESET} %s\n" "$*"; }
info() { printf "${BOLD}→${RESET} %s\n" "$*"; }

# ─── Usage ───────────────────────────────────────────────────────────────────
usage() {
    cat <<EOF
hermes-agent-cluster installer

Usage:
  bash install.sh [OPTIONS]

Options:
  --help              Show this help message and exit
  --uninstall         Remove binary, plugin, and config
  --role ROLE         Node role: main or worker (skips interactive prompt)
  --token TOKEN       Cluster token (auto-generated if omitted)
  --endpoint URL      Main node endpoint (required for worker role)
  --cluster-id ID     Cluster identifier (default: my-cluster)

Examples:
  bash install.sh                        # interactive install
  bash install.sh --role main            # install as main node
  bash install.sh --role worker --endpoint http://10.0.0.1:8787
EOF
}

# ─── Parse args ──────────────────────────────────────────────────────────────
ROLE=""
TOKEN=""
ENDPOINT=""
CLUSTER_ID="my-cluster"
UNINSTALL=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --help|-h)      usage; exit 0 ;;
        --uninstall)    UNINSTALL=true; shift ;;
        --role)         ROLE="$2"; shift 2 ;;
        --token)        TOKEN="$2"; shift 2 ;;
        --endpoint)     ENDPOINT="$2"; shift 2 ;;
        --cluster-id)   CLUSTER_ID="$2"; shift 2 ;;
        *)              fail "Unknown option: $1"; usage; exit 1 ;;
    esac
done

# ─── Uninstall ───────────────────────────────────────────────────────────────
if $UNINSTALL; then
    info "Uninstalling hermes-agent-cluster..."
    [[ -f "$INSTALL_BIN" ]] && rm -f "$INSTALL_BIN" && ok "Removed ${INSTALL_BIN}" || warn "Binary not found: ${INSTALL_BIN}"
    [[ -d "$PLUGIN_DIR" ]] && rm -rf "$PLUGIN_DIR" && ok "Removed ${PLUGIN_DIR}" || warn "Plugin dir not found: ${PLUGIN_DIR}"
    [[ -f "$CONFIG_FILE" ]] && rm -f "$CONFIG_FILE" && ok "Removed ${CONFIG_FILE}" || warn "Config not found: ${CONFIG_FILE}"
    ok "Uninstall complete"
    exit 0
fi

# ─── 1. Detect environment ──────────────────────────────────────────────────
info "Detecting environment..."
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$OS" in
    linux)  PLATFORM="linux" ;;
    darwin) PLATFORM="darwin" ;;
    *)      fail "Unsupported OS: $OS"; exit 1 ;;
esac
case "$ARCH" in
    x86_64)  ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *)       fail "Unsupported architecture: $ARCH"; exit 1 ;;
esac
ok "Platform: ${PLATFORM}/${ARCH}"

# ─── Check dependencies ─────────────────────────────────────────────────────
for cmd in curl; do
    if ! command -v "$cmd" &>/dev/null; then
        fail "Required command not found: $cmd"
        exit 1
    fi
done

# ─── 2. Download Go binary ──────────────────────────────────────────────────
BINARY_URL="${RELEASE_BASE}/${BINARY_NAME}-${PLATFORM}-${ARCH}"
info "Downloading ${BINARY_NAME} from ${BINARY_URL} ..."
mkdir -p "$INSTALL_BIN_DIR"
if curl -fsSL --retry 3 --retry-delay 2 -o "$INSTALL_BIN" "$BINARY_URL"; then
    chmod +x "$INSTALL_BIN"
    ok "Installed binary: ${INSTALL_BIN}"
else
    fail "Failed to download binary. Check that a release exists for ${PLATFORM}/${ARCH}"
    rm -f "$INSTALL_BIN"
    exit 1
fi

# ─── 3. Install Python plugin ───────────────────────────────────────────────
info "Installing Python plugin..."
mkdir -p "$PLUGIN_DIR"
PLUGIN_FILES=("__init__.py" "plugin.yaml" "__main__.py")
for f in "${PLUGIN_FILES[@]}"; do
    URL="${RAW_BASE}/${f}"
    if curl -fsSL --retry 3 --retry-delay 2 -o "${PLUGIN_DIR}/${f}" "$URL" 2>/dev/null; then
        ok "Downloaded ${f}"
    else
        warn "Could not download ${f} (non-fatal)"
    fi
done

# ─── 4. Generate config ─────────────────────────────────────────────────────
info "Configuring cluster..."

# Role
if [[ -z "$ROLE" ]]; then
    echo ""
    echo "  Select node role:"
    echo "    1) main   — coordination node (planning, reviewing, scheduling)"
    echo "    2) worker  — execution node (coding, browser)"
    echo ""
    read -rp "  Enter choice [1/2]: " choice
    case "$choice" in
        1) ROLE="main" ;;
        2) ROLE="worker" ;;
        *) fail "Invalid choice"; exit 1 ;;
    esac
fi
[[ "$ROLE" != "main" && "$ROLE" != "worker" ]] && fail "Role must be 'main' or 'worker'" && exit 1
ok "Role: ${ROLE}"

# Token
if [[ -z "$TOKEN" ]]; then
    if command -v uuidgen &>/dev/null; then
        TOKEN="$(uuidgen)"
    elif [[ -f /proc/sys/kernel/random/uuid ]]; then
        TOKEN="$(cat /proc/sys/kernel/random/uuid)"
    else
        TOKEN="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' | head -c 32)"
    fi
fi
ok "Token: ${TOKEN:0:8}..."

# Endpoint (worker only)
if [[ "$ROLE" == "worker" ]]; then
    if [[ -z "$ENDPOINT" ]]; then
        read -rp "  Enter main node endpoint (e.g. http://10.0.0.1:8787): " ENDPOINT
    fi
    [[ -z "$ENDPOINT" ]] && fail "Worker role requires a main node endpoint" && exit 1
    ok "Endpoint: ${ENDPOINT}"
fi

# Node ID & name
NODE_ID="node-$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n' | head -c 8)"
NODE_NAME="$(hostname 2>/dev/null || echo 'unknown')"

# Capabilities
if [[ "$ROLE" == "main" ]]; then
    CAPS=("planning" "reviewing" "scheduling")
else
    CAPS=("coding" "browser")
fi

# Build YAML
mkdir -p "$(dirname "$CONFIG_FILE")"
{
    cat <<EOF
cluster:
  id: ${CLUSTER_ID}
  role: ${ROLE}
  token: "${TOKEN}"
EOF
    if [[ "$ROLE" == "worker" ]]; then
        echo "  endpoint: \"${ENDPOINT}\""
    fi
    cat <<EOF
node:
  id: ${NODE_ID}
  name: ${NODE_NAME}
  capabilities:
EOF
    for c in "${CAPS[@]}"; do
        echo "    - ${c}"
    done
    cat <<EOF
server:
  bind: "0.0.0.0"
  port: 8787
lease:
  ttl: 60s
  scan_rate: 10s
watchdog:
  check_interval: 5s
  degraded_after: 15s
  offline_after: 30s
heartbeat:
  interval: 30s
  lease_timeout: 120s
EOF
} > "$CONFIG_FILE"

ok "Config written: ${CONFIG_FILE}"

# ─── 5. Verify ───────────────────────────────────────────────────────────────
echo ""
info "Verifying installation..."
ERRS=0
if [[ -x "$INSTALL_BIN" ]]; then
    ok "Binary executable: ${INSTALL_BIN}"
else
    fail "Binary not executable: ${INSTALL_BIN}"; ((ERRS++))
fi
if [[ -d "$PLUGIN_DIR" ]]; then
    ok "Plugin directory: ${PLUGIN_DIR}"
else
    fail "Plugin directory missing: ${PLUGIN_DIR}"; ((ERRS++))
fi
if [[ -f "$CONFIG_FILE" ]]; then
    ok "Config file: ${CONFIG_FILE}"
else
    fail "Config file missing: ${CONFIG_FILE}"; ((ERRS++))
fi

# ─── Summary ─────────────────────────────────────────────────────────────────
echo ""
if [[ $ERRS -eq 0 ]]; then
    printf "${GREEN}${BOLD}══════════════════════════════════════════════${RESET}\n"
    printf "${GREEN}${BOLD}  hermes-agent-cluster installed successfully!${RESET}\n"
    printf "${GREEN}${BOLD}══════════════════════════════════════════════${RESET}\n"
else
    printf "${RED}${BOLD}  Installation completed with ${ERRS} error(s).${RESET}\n"
fi
echo ""
echo "  Binary:   ${INSTALL_BIN}"
echo "  Plugin:   ${PLUGIN_DIR}"
echo "  Config:   ${CONFIG_FILE}"
echo "  Role:     ${ROLE}"
echo "  Node ID:  ${NODE_ID}"
echo ""
if [[ "$ROLE" == "main" ]]; then
    echo "  ${BOLD}Next steps:${RESET}"
    echo "    1. Start the cluster sidecar:"
    echo "       hermes-cluster serve"
    echo ""
    echo "    2. On worker nodes, run:"
    echo "       bash install.sh --role worker --endpoint http://$(hostname -I 2>/dev/null | awk '{print $1}' || echo 'YOUR_IP'):8787"
    echo ""
    echo "    3. Hermes Agent will auto-load the plugin."
    echo "       Use: agent_cluster_status, agent_cluster_task_create, etc."
else
    echo "  ${BOLD}Next steps:${RESET}"
    echo "    1. Start the cluster sidecar:"
    echo "       hermes-cluster serve"
    echo ""
    echo "    2. Ensure the main node is reachable at: ${ENDPOINT}"
    echo ""
    echo "    3. Hermes Agent will auto-load the plugin."
fi
echo ""
echo "  ${BOLD}Management:${RESET}"
echo "    Uninstall: bash install.sh --uninstall"
echo "    Reconfigure: edit ${CONFIG_FILE}"
echo ""
