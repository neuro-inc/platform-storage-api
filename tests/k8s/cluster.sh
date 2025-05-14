#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# GLOBALS
###############################################################################
WS="${GITHUB_WORKSPACE:-$PWD}"
export MINIKUBE_HOME="$WS/.minikube"        # keep state inside repo
export KUBECONFIG="$WS/.kube/config"        # pytest & kubectl read this
MK="minikube kubectl --"                    # embedded kubectl shortcut

###############################################################################
# 1. Install Minikube + conntrack
###############################################################################
k8s::install_minikube() {
  local VER="v1.25.2"
  sudo apt-get update
  sudo apt-get install -y conntrack
  curl -Lo minikube "https://storage.googleapis.com/minikube/releases/${VER}/minikube-linux-amd64"
  chmod +x minikube
  sudo mv minikube /usr/local/bin/
}

###############################################################################
# 2. Start cluster (Docker driver, **no sudo**)
###############################################################################
k8s::start() {
  rm -rf "$MINIKUBE_HOME" "$WS/.kube"
  mkdir -p  "$MINIKUBE_HOME" "$(dirname "$KUBECONFIG")"

  minikube start \
    --driver=docker \
    --kubernetes-version=stable \
    --wait=all \
    --wait-timeout=5m

  # dump full kube-config for subsequent steps & pytest
  minikube -p minikube kubeconfig > "$KUBECONFIG"
}

###############################################################################
# 3. Apply manifests & wait for jobs
###############################################################################
k8s::apply_all_configurations() {
  echo "→ loading images into Minikube ..."
  minikube image load ghcr.io/neuro-inc/admission-controller-lib:latest
  make dist
  docker build -t admission-controller-tests:latest .
  docker image save -o ac.tar admission-controller-tests:latest
  minikube image load ac.tar

  echo "→ applying RBAC / jobs / deployment ..."
  $MK apply -f tests/k8s/rbac.yaml
  $MK apply -f tests/k8s/preinstall-job.yaml
  wait_job admission-controller-lib-preinstall
  $MK apply -f tests/k8s/admission-controller-deployment.yaml
  $MK apply -f tests/k8s/postinstall-job.yaml
  wait_job admission-controller-lib-postinstall
}

###############################################################################
# 4. Wait helper
###############################################################################
wait_job() {
  local JOB="$1"
  echo "   waiting for job/$JOB ..."
  $MK wait --for=condition=complete job/"$JOB" --timeout=60s
  echo "✓ job/$JOB completed"
}

###############################################################################
# 5. Optional clean / stop
###############################################################################
k8s::clean() {
  $MK delete -f tests/k8s/postinstall-job.yaml              || true
  $MK delete -f tests/k8s/admission-controller-deployment.yaml || true
  $MK delete -f tests/k8s/preinstall-job.yaml               || true
  $MK delete -f tests/k8s/rbac.yaml                         || true
}
k8s::stop() { minikube stop || true; minikube delete || true; }

###############################################################################
# Dispatcher
###############################################################################
case "${1:-}" in
  install) k8s::install_minikube          ;;
  start)   k8s::start                     ;;
  apply)   k8s::apply_all_configurations  ;;
  clean)   k8s::clean                     ;;
  stop)    k8s::stop                      ;;
  *)
    echo "Usage: $0 {install|start|apply|clean|stop}" >&2
    exit 1
    ;;
esac
