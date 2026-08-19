#!/usr/bin/env bash
#
# Bring the cluster and its console up on a fresh Linux VM.
#
#   deploy/vm/setup.sh                          # plain HTTP on :8080
#   deploy/vm/setup.sh distkv.example.org       # HTTPS, certificate and all
#
# Installs Docker if it is missing, opens the firewall if something is
# blocking the port, and starts the stack. Safe to run more than once.
#
# It prints what it is about to do before doing it. Read it before running
# it on a machine you care about; it is short on purpose.

set -euo pipefail

DOMAIN="${1:-}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

# --- Docker ---------------------------------------------------------------

if ! command -v docker >/dev/null; then
    say "Installing Docker"
    curl -fsSL https://get.docker.com | sh
    sudo usermod -aG docker "$USER" || true
    echo "Added $USER to the docker group. If the commands below fail with a"
    echo "permission error, log out and back in, then run this script again."
fi

DOCKER="docker"
docker info >/dev/null 2>&1 || DOCKER="sudo docker"

# --- Firewall -------------------------------------------------------------

# Oracle Cloud's images (and some others) ship with an iptables INPUT chain
# that rejects everything except SSH, which is separate from — and easy to
# miss behind — the cloud provider's own security rules. A stack that comes
# up perfectly and answers nothing is almost always this.
open_port() {
    local port="$1"
    if ! command -v iptables >/dev/null; then
        return
    fi
    if sudo iptables -C INPUT -p tcp --dport "$port" -j ACCEPT 2>/dev/null; then
        return
    fi
    if ! sudo iptables -S INPUT 2>/dev/null | grep -qE '\-j (REJECT|DROP)'; then
        return # nothing is blocking; leave the rules alone
    fi
    say "Opening port $port in the host firewall"
    sudo iptables -I INPUT 1 -p tcp --dport "$port" -j ACCEPT
    if command -v netfilter-persistent >/dev/null; then
        sudo netfilter-persistent save
    elif [ -d /etc/iptables ]; then
        sudo sh -c 'iptables-save > /etc/iptables/rules.v4'
    else
        echo "Note: could not persist the rule; it will not survive a reboot."
    fi
}

# --- Start ----------------------------------------------------------------

if [ -n "$DOMAIN" ]; then
    open_port 80
    open_port 443
    say "Starting the cluster and console for https://$DOMAIN"
    DISTKV_DOMAIN="$DOMAIN" $DOCKER compose \
        -f docker-compose.yml \
        -f deploy/vm/docker-compose.tls.yml \
        up --build -d
    URL="https://$DOMAIN"
else
    open_port 8080
    say "Starting the cluster and console on port 8080"
    $DOCKER compose up --build -d
    URL="http://$(curl -fsS --max-time 5 https://api.ipify.org 2>/dev/null || echo "<this-machine>"):8080"
fi

say "Up"
$DOCKER compose ps --format 'table {{.Service}}\t{{.Status}}'

cat <<EOF

  Console:  $URL

The cluster starts empty. Write a key from the console, or from here:

  $DOCKER compose exec node1 distkv-cli -endpoints node1:7070 put city kathmandu

If the console does not answer, the port is almost certainly still closed in
your cloud provider's own firewall — that is a separate setting from the host
one this script handles. See docs/deploy.md.
EOF
