#!/usr/bin/env bash
set -euo pipefail
[[ "${DEBUG:-}" == 1 ]] && set -x

# ──────────────────────────────────────────────────────────────────────────────
# Config – override via env if you need
# ──────────────────────────────────────────────────────────────────────────────
MINIKUBE_VERSION="${MINIKUBE_VERSION:-v1.35.0}"
K8S_VERSION="${K8S_VERSION:-v1.32.0}"
CRICTL_VERSION="${CRICTL_VERSION:-v1.32.0}"
PROFILE="${MINIKUBE_PROFILE:-minikube}"
WAIT_TIMEOUT="${MINIKUBE_WAIT_TIMEOUT:-5m}"
DRIVER="none"

REAL_USER="${SUDO_USER:-$USER}"
REAL_HOME="$(getent passwd "$REAL_USER" | cut -d: -f6)"
export MINIKUBE_HOME="$REAL_HOME"
export CHANGE_MINIKUBE_NONE_USER=true
export KUBECONFIG="$REAL_HOME/.kube/config"

log()       { printf '\e[1;34m▶ %s\e[0m\n' "$*"; }
with_sudo() { if [[ "$(id -u)" -ne 0 ]]; then sudo "$@"; else "$@"; fi; }

# ──────────────────────────────────────────────────────────────────────────────
# 1 – Install runtime deps
# ──────────────────────────────────────────────────────────────────────────────
install_crictl() {
  command -v crictl >/dev/null && return

  log "Downloading crictl ${CRICTL_VERSION}"
  ARCH="$(uname -m)"
  [[ "$ARCH" == "x86_64" ]]  && ARCH=amd64
  [[ "$ARCH" == "aarch64" ]] && ARCH=arm64
  TAR="crictl-${CRICTL_VERSION}-linux-${ARCH}.tar.gz"
  URL="https://github.com/kubernetes-sigs/cri-tools/releases/download/${CRICTL_VERSION}/${TAR}"

  curl -sSL --retry 3 -o "/tmp/${TAR}" "$URL"
  with_sudo tar -C /usr/local/bin -xzf "/tmp/${TAR}" crictl
}

k8s::install_minikube() {
  log "Installing system packages…"
  with_sudo apt-get update -y
  # core packages (must succeed)
  with_sudo apt-get install -y conntrack socat iptables bridge-utils containernetworking-plugins
  # optional cri-tools meta-package (may not exist on Ubuntu 24.04)
  with_sudo apt-get install -y cri-tools || true
  install_crictl

  # upgrade / install Minikube
  if ! command -v minikube >/dev/null || [[ "$(minikube version --short)" != "$MINIKUBE_VERSION" ]]; then
    log "Installing Minikube ${MINIKUBE_VERSION}"
    curl -sSL -o /tmp/minikube \
      "https://storage.googleapis.com/minikube/releases/${MINIKUBE_VERSION}/minikube-linux-amd64"
    chmod +x /tmp/minikube
    with_sudo mv /tmp/minikube /usr/local/bin/minikube
  fi
  log "Minikube $(minikube version --short) ready."
}

# ──────────────────────────────────────────────────────────────────────────────
# 2 – Kernel tweaks + cluster start
# ──────────────────────────────────────────────────────────────────────────────
prepare_kernel() {
  with_sudo modprobe br_netfilter
  echo "net.bridge.bridge-nf-call-iptables=1" | with_sudo tee /etc/sysctl.d/99-k8s.conf >/dev/null
  with_sudo sysctl --system >/dev/null
  with_sudo swapoff -a || true
}

k8s::start() {
  prepare_kernel
  log "Starting Minikube (driver=none)…"
  with_sudo env MINIKUBE_HOME="$MINIKUBE_HOME" minikube start \
      --profile="$PROFILE" \
      --driver="$DRIVER" \
      --kubernetes-version="$K8S_VERSION" \
      --wait=all \
      --wait-timeout="$WAIT_TIMEOUT"

  with_sudo chown -R "$REAL_USER:$REAL_USER" "$REAL_HOME/.kube" "$REAL_HOME/.minikube"
  kubectl config use-context "$PROFILE"
  kubectl get nodes -o wide
}

# ──────────────────────────────────────────────────────────────────────────────
# 3 – Image load + manifests
# ──────────────────────────────────────────────────────────────────────────────
wait_job() { kubectl wait --for=condition=complete "job/$1" --timeout=60s; }

k8s::apply() {
  docker build -t admission-controller-tests:latest .
  minikube image load admission-controller-tests:latest --profile="$PROFILE"

  kubectl apply -f tests/k8s/rbac.yaml
  kubectl apply -f tests/k8s/preinstall-job.yaml; wait_job admission-controller-lib-preinstall
  kubectl apply -f tests/k8s/admission-controller-deployment.yaml
  kubectl apply -f tests/k8s/postinstall-job.yaml;  wait_job admission-controller-lib-postinstall
}

# ──────────────────────────────────────────────────────────────────────────────
# 4 – Cleanup
# ──────────────────────────────────────────────────────────────────────────────
k8s::clean() {
  kubectl delete -f tests/k8s/postinstall-job.yaml            || true
  kubectl delete -f tests/k8s/admission-controller-deployment.yaml || true
  kubectl delete -f tests/k8s/preinstall-job.yaml             || true
  kubectl delete -f tests/k8s/rbac.yaml                       || true
}

k8s::stop() {
  with_sudo minikube stop --profile="$PROFILE"   || true
  with_sudo minikube delete --profile="$PROFILE" || true
  with_sudo rm -rf "$REAL_HOME/.minikube" "$REAL_HOME/.kube"
}

# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────
case "${1:-}" in
  install) k8s::install_minikube ;;
  start)   k8s::start ;;
  apply)   k8s::apply ;;
  clean)   k8s::clean ;;
  stop)    k8s::stop ;;
  *) echo "Usage: $0 {install|start|apply|clean|stop}" >&2; exit 1 ;;
esac
