#!/usr/bin/env bash
#
# Start, manage and clean a Minikube cluster that always uses --driver=none.
# Works on:
#   • GitHub Actions ubuntu-latest runners (root via sudo is available)
#   • Any local Linux box (run the script with sudo, or let it sudo itself)
#
# Docs: https://minikube.sigs.k8s.io/docs/drivers/none/          ▲ marked “advanced users”
# None-driver with K8s ≥ 1.24 also needs containernetworking-plugins           :contentReference[oaicite:1]{index=1}
set -euo pipefail
[[ "${DEBUG:-}" == 1 ]] && set -x

# -------------------------------------------------------------------------------------------------
# Configuration knobs (feel free to override with env vars)
# -------------------------------------------------------------------------------------------------
MINIKUBE_VERSION="${MINIKUBE_VERSION:-v1.35.0}"   # 2025-01 LTS
K8S_VERSION="${K8S_VERSION:-v1.32.0}"
PROFILE="${MINIKUBE_PROFILE:-minikube}"
WAIT_TIMEOUT="${MINIKUBE_WAIT_TIMEOUT:-5m}"

# Always the none driver
DRIVER=none

# The unprivileged user that invoked the script (works in sudo context, too)
REAL_USER="${SUDO_USER:-$USER}"
REAL_HOME="$(getent passwd "$REAL_USER" | cut -d: -f6)"

# Minikube must use the caller’s home so kubeconfig is readable after chown
export MINIKUBE_HOME="$REAL_HOME"
export CHANGE_MINIKUBE_NONE_USER=true
export KUBECONFIG="$REAL_HOME/.kube/config"

# -------------------------------------------------------------------------------------------------
# Utility helpers
# -------------------------------------------------------------------------------------------------
log() { printf '\e[1;34m▶ %s\e[0m\n' "$*"; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "❌ $1 is required"; exit 1; }; }

with_sudo() {
  if [[ "$(id -u)" -ne 0 ]]; then sudo "$@"; else "$@"; fi
}

# -------------------------------------------------------------------------------------------------
# 1 – Install prerequisites
# -------------------------------------------------------------------------------------------------
k8s::install_minikube() {
  need curl
  log "Installing Linux packages needed for the bare-metal driver…"
  with_sudo apt-get update -y
  with_sudo apt-get install -y conntrack socat iptables bridge-utils \
                               containernetworking-plugins >/dev/null

  if ! command -v minikube >/dev/null; then
    log "Downloading Minikube ${MINIKUBE_VERSION}"
    curl -Lo /tmp/minikube "https://storage.googleapis.com/minikube/releases/${MINIKUBE_VERSION}/minikube-linux-amd64"
    chmod +x /tmp/minikube
    with_sudo mv /tmp/minikube /usr/local/bin/minikube
  fi
  log "Minikube $(minikube version -o short) installed."
}

# -------------------------------------------------------------------------------------------------
# 2 – Start cluster
# -------------------------------------------------------------------------------------------------
k8s::start() {
  # Kernel & sysctl tweaks kubeadm pre-flight requires
  with_sudo modprobe br_netfilter
  echo "net.bridge.bridge-nf-call-iptables=1" | with_sudo tee /etc/sysctl.d/99-k8s.conf >/dev/null
  with_sudo sysctl --system >/dev/null

  # None-driver must run as root
  CMD=(minikube start
        --profile="$PROFILE"
        --driver="$DRIVER"
        --kubernetes-version="$K8S_VERSION"
        --wait=all --timeout="$WAIT_TIMEOUT")

  log "Starting Minikube (driver=none)…"
  with_sudo env "MINIKUBE_HOME=$MINIKUBE_HOME" "${CMD[@]}"

  # Give the config & profile dirs back to the real user
  with_sudo chown -R "$REAL_USER":"$REAL_USER" "$REAL_HOME/.kube" "$REAL_HOME/.minikube"

  log "Cluster is up"
  kubectl config use-context "$PROFILE"
  kubectl get nodes -o wide
}

# -------------------------------------------------------------------------------------------------
# 3 – Load image & apply test manifests
# -------------------------------------------------------------------------------------------------
wait_job() {
  local job="$1"
  log "Waiting for job/$job to succeed (60 s)…"
  kubectl wait --for=condition=complete "job/$job" --timeout=60s
}

k8s::apply() {
  log "Building admission-controller test image…"
  docker build -t admission-controller-tests:latest .
  minikube image load admission-controller-tests:latest --profile="$PROFILE"

  log "Applying manifests…"
  kubectl apply -f tests/k8s/rbac.yaml
  kubectl apply -f tests/k8s/preinstall-job.yaml
  wait_job admission-controller-lib-preinstall
  kubectl apply -f tests/k8s/admission-controller-deployment.yaml
  kubectl apply -f tests/k8s/postinstall-job.yaml
  wait_job admission-controller-lib-postinstall
}

# -------------------------------------------------------------------------------------------------
# 4 – Cleanup helpers
# -------------------------------------------------------------------------------------------------
k8s::clean() {
  log "Deleting manifests…"
  kubectl delete -f tests/k8s/postinstall-job.yaml || true
  kubectl delete -f tests/k8s/admission-controller-deployment.yaml || true
  kubectl delete -f tests/k8s/preinstall-job.yaml || true
  kubectl delete -f tests/k8s/rbac.yaml || true
}

k8s::stop() {
  log "Stopping / deleting Minikube profile $PROFILE"
  with_sudo minikube stop --profile="$PROFILE" || true
  with_sudo minikube delete --profile="$PROFILE" || true
  with_sudo rm -rf "$REAL_HOME/.minikube" "$REAL_HOME/.kube"
}

# -------------------------------------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------------------------------------
case "${1:-}" in
  install) k8s::install_minikube ;;
  start)   k8s::start ;;
  apply)   k8s::apply ;;
  clean)   k8s::clean ;;
  stop)    k8s::stop ;;
  *) echo "Usage: $0 {install|start|apply|clean|stop}"; exit 1 ;;
esac
