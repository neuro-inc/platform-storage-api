#!/usr/bin/env bash
set -euo pipefail
[[ "${DEBUG:-}" == 1 ]] && set -x

# ---------------------------------------------------------------------------
# Config – override with env vars if you need
# ---------------------------------------------------------------------------
MINIKUBE_VERSION="${MINIKUBE_VERSION:-v1.35.0}"   # Jan 2025 LTS
K8S_VERSION="${K8S_VERSION:-v1.32.0}"
CRICTL_VERSION="${CRICTL_VERSION:-v1.32.0}"        # match K8s minor
PROFILE="${MINIKUBE_PROFILE:-minikube}"
WAIT_TIMEOUT="${MINIKUBE_WAIT_TIMEOUT:-5m}"
DRIVER="none"

REAL_USER="${SUDO_USER:-$USER}"
REAL_HOME="$(getent passwd "$REAL_USER" | cut -d: -f6)"
export MINIKUBE_HOME="$REAL_HOME"
export CHANGE_MINIKUBE_NONE_USER=true
export KUBECONFIG="$REAL_HOME/.kube/config"

log()       { printf '\e[1;34m▶ %s\e[0m\n' "$*"; }
need()      { command -v "$1" >/dev/null 2>&1 || { echo "❌ $1 is required"; exit 1; }; }
with_sudo() { if [[ "$(id -u)" -ne 0 ]]; then sudo "$@"; else "$@"; fi; }

# ---------------------------------------------------------------------------
# 1 – Install / upgrade Minikube + runtime deps
# ---------------------------------------------------------------------------
install_crictl() {
  if command -v crictl >/dev/null 2>&1; then
    log "crictl $(crictl --version | awk '{print $3}')" already present
    return 0
  fi

  log "Downloading crictl ${CRICTL_VERSION}"
  ARCH=$(uname -m)
  TAR="crictl-${CRICTL_VERSION}-linux-${ARCH}.tar.gz"
  URL="https://github.com/kubernetes-sigs/cri-tools/releases/download/${CRICTL_VERSION}/${TAR}"
  curl -L "$URL" -o "/tmp/${TAR}"
  with_sudo tar -C /usr/local/bin -xzf "/tmp/${TAR}"
}

k8s::install_minikube() {
  need curl
  log "Installing Linux packages needed for the bare-metal driver…"
  # Try to install cri-tools; ignore if the package is missing (Ubuntu 24.04)
  with_sudo apt-get update -y
  with_sudo apt-get install -y \
        conntrack socat iptables bridge-utils \
        containernetworking-plugins cri-tools || true

  install_crictl

  # Replace any older minikube binary
  if command -v minikube >/dev/null 2>&1; then
    CURRENT="$(minikube version --short | sed 's/^v//')"
    TARGET="$(echo "$MINIKUBE_VERSION" | sed 's/^v//')"
    if [[ "$CURRENT" != "$TARGET" ]]; then
      log "Replacing minikube $CURRENT → $TARGET"
      with_sudo rm -f "$(command -v minikube)"
    else
      log "Minikube $CURRENT already present"
      return 0
    fi
  fi

  log "Downloading Minikube $MINIKUBE_VERSION…"
  curl -Lo /tmp/minikube "https://storage.googleapis.com/minikube/releases/${MINIKUBE_VERSION}/minikube-linux-amd64"
  chmod +x /tmp/minikube
  with_sudo mv /tmp/minikube /usr/local/bin/minikube
  log "Minikube $(minikube version --short) installed."
}

# ---------------------------------------------------------------------------
# 2 – Kernel tweaks + cluster start
# ---------------------------------------------------------------------------
k8s::prepare_kernel() {
  with_sudo modprobe br_netfilter
  echo "net.bridge.bridge-nf-call-iptables=1" | with_sudo tee /etc/sysctl.d/99-k8s.conf >/dev/null
  with_sudo sysctl --system >/dev/null
  with_sudo swapoff -a || true
}

k8s::start() {
  k8s::prepare_kernel

  log "Starting Minikube (driver=none)…"
  with_sudo env "MINIKUBE_HOME=$MINIKUBE_HOME" minikube start \
      --profile="$PROFILE" \
      --driver="$DRIVER" \
      --kubernetes-version="$K8S_VERSION" \
      --wait=all \
      --wait-timeout="$WAIT_TIMEOUT"

  with_sudo chown -R "$REAL_USER":"$REAL_USER" "$REAL_HOME/.kube" "$REAL_HOME/.minikube"

  log "Cluster is up"
  kubectl config use-context "$PROFILE"
  kubectl get nodes -o wide
}

# ---------------------------------------------------------------------------
# 3 – Load test image & apply manifests
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# 4 – Cleanup helpers
# ---------------------------------------------------------------------------
k8s::clean() {
  log "Deleting manifests…"
  kubectl delete -f tests/k8s/postinstall-job.yaml           || true
  kubectl delete -f tests/k8s/admission-controller-deployment.yaml || true
  kubectl delete -f tests/k8s/preinstall-job.yaml            || true
  kubectl delete -f tests/k8s/rbac.yaml                      || true
}

k8s::stop() {
  log "Stopping Minikube…"
  with_sudo minikube stop   --profile="$PROFILE" || true
  with_sudo minikube delete --profile="$PROFILE" || true
  with_sudo rm -rf "$REAL_HOME/.minikube" "$REAL_HOME/.kube"
}

# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
case "${1:-}" in
  install) k8s::install_minikube ;;
  start)   k8s::start ;;
  apply)   k8s::apply ;;
  clean)   k8s::clean ;;
  stop)    k8s::stop ;;
  *) echo "Usage: $0 {install|start|apply|clean|stop}" >&2; exit 1 ;;
esac
