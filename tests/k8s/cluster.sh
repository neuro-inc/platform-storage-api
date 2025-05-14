#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# Globals (shared across every function)
###############################################################################
WS="${GITHUB_WORKSPACE:-$PWD}"          # repo workspace
export MINIKUBE_HOME="$WS/.minikube"    # keep state inside the repo dir
export KUBECONFIG="$WS/.kube/config"    # pytest & kubectl will read this
MK="minikube kubectl --"                # shortcut to embedded kubectl

###############################################################################
# 1. Install Minikube binary + conntrack
###############################################################################
k8s::install_minikube() {
  local VER="v1.25.2"                   # pick a known-good version
  sudo apt-get update
  sudo apt-get install -y conntrack     # required by kubelet
  curl -Lo minikube \
    "https://storage.googleapis.com/minikube/releases/${VER}/minikube-linux-amd64"
  chmod +x minikube
  sudo mv minikube /usr/local/bin/
}

###############################################################################
# 2. Start a Docker-driver Minikube cluster (no sudo)
#    – writes ~/.kube/config inside $WS/.kube/config
###############################################################################
k8s::start() {
  rm -rf "$MINIKUBE_HOME" "$WS/.kube"            # clean slate
  mkdir -p  "$MINIKUBE_HOME" "$(dirname "$KUBECONFIG")"

  # Spin up cluster unprivileged
  minikube start \
    --driver=docker \
    --kubernetes-version=stable \
    --wait=all \
    --wait-timeout=5m

  # Dump a complete kubeconfig for later steps / pytest
  $MK config view --raw > "$KUBECONFIG"

  # Label the single node (your tests expect this)
  $MK label node minikube platform.neuromation.io/nodepool=minikube --overwrite
}

###############################################################################
# 3. Apply all manifests & wait for pre/post-install jobs
###############################################################################
k8s::apply_all_configurations() {
  echo "→ Loading test images into Minikube’s Docker ..."
  minikube image load ghcr.io/neuro-inc/admission-controller-lib:latest
  make dist                                 # your build step
  docker build -t admission-controller-tests:latest .
  docker image save -o ac.tar admission-controller-tests:latest
  minikube image load ac.tar

  echo "→ Applying RBAC + Jobs + Deployment ..."
  $MK apply -f tests/k8s/rbac.yaml
  $MK apply -f tests/k8s/preinstall-job.yaml
  wait_job admission-controller-lib-preinstall
  $MK apply -f tests/k8s/admission-controller-deployment.yaml
  $MK apply -f tests/k8s/postinstall-job.yaml
  wait_job admission-controller-lib-postinstall
}

###############################################################################
# 4. Utility: wait for a Kubernetes Job to `Complete`
###############################################################################
wait_job() {
  local JOB="$1"
  echo "   waiting for job/$JOB ..."
  if ! $MK wait --for=condition=complete job/"$JOB" --timeout=60s; then
    echo "✖ job/$JOB did not finish in 60 s – dumping events"
    $MK get events --sort-by=.metadata.creationTimestamp
    exit 1
  fi
  echo "✓ job/$JOB succeeded"
}

###############################################################################
# 5. Clean & Stop helpers (optional in CI but handy locally)
###############################################################################
k8s::clean() {
  $MK delete -f tests/k8s/postinstall-job.yaml  || true
  $MK delete -f tests/k8s/admission-controller-deployment.yaml || true
  $MK delete -f tests/k8s/preinstall-job.yaml   || true
  $MK delete -f tests/k8s/rbac.yaml             || true
}
k8s::stop()  { minikube stop  || true; minikube delete || true; }

###############################################################################
# Dispatcher
###############################################################################
case "${1:-}" in
  install) k8s::install_minikube               ;;
  start)   k8s::start                          ;;
  apply)   k8s::apply_all_configurations       ;;
  clean)   k8s::clean                          ;;
  stop)    k8s::stop                           ;;
  *)
    echo "Usage: $0 {install|start|apply|clean|stop}" >&2
    exit 1
    ;;
esac
