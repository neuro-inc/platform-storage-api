#!/usr/bin/env bash
set -euo pipefail

# k8s cluster management script for CI (GitHub Actions)

# shorthand for invoking Minikube’s embedded kubectl
MK="minikube kubectl --"

# Install Minikube in CI environment
echo "Installing Minikube..."
function k8s::install_minikube {
    local minikube_version="v1.25.2"
    sudo apt-get update
    sudo apt-get install -y conntrack
    curl -Lo minikube "https://storage.googleapis.com/minikube/releases/${minikube_version}/minikube-linux-amd64"
    chmod +x minikube
    sudo mv minikube /usr/local/bin/
}

# Start Minikube cluster and write kubeconfig for tests
echo "Starting Minikube..."
function k8s::start {
    # Force HOME into the workspace so Minikube writes its config there
    local WS="${GITHUB_WORKSPACE:-$PWD}"
    export HOME="$WS"

    # Prepare workspace kubeconfig path stub
    local STUB="$WS/.kube/config"
    rm -f "$STUB"
    mkdir -p "$(dirname "$STUB")"

    # Ensure conntrack is installed
    sudo apt-get update && sudo apt-get install -y conntrack

    # Start Minikube unprivileged with Docker driver
    minikube start \
      --driver=docker \
      --kubernetes-version=stable \
      --wait=all \
      --wait-timeout=5m

    # Dump a complete kubeconfig using Minikube's embedded kubectl (ignore env KUBECONFIG)
    (unset KUBECONFIG; $MK config view --raw > "$STUB")

    # Export for downstream steps (pytest, kubectl)
    export KUBECONFIG="$STUB"
}

# Apply Kubernetes manifests for integration tests
echo "Applying Kubernetes configurations..."
function k8s::apply_all_configurations {
    # Ensure context and label the node
    $MK config use-context minikube
    $MK label node minikube platform.neuromation.io/nodepool=minikube --overwrite

    # Load controller and test images
    minikube image load ghcr.io/neuro-inc/admission-controller-lib:latest
    make dist
    docker build -t admission-controller-tests:latest .
    docker image save -o ac.tar admission-controller-tests:latest
    minikube image load ac.tar

    # Apply manifests and wait for jobs
    $MK apply -f tests/k8s/rbac.yaml
    $MK apply -f tests/k8s/preinstall-job.yaml
    wait_job admission-controller-lib-preinstall

    $MK apply -f tests/k8s/admission-controller-deployment.yaml
    $MK apply -f tests/k8s/postinstall-job.yaml
    wait_job admission-controller-lib-postinstall
}

# Clean up Kubernetes resources
echo "Cleaning up Kubernetes resources..."
function k8s::clean {
    $MK delete -f tests/k8s/postinstall-job.yaml || true
    $MK delete -f tests/k8s/admission-controller-deployment.yaml || true
    $MK delete -f tests/k8s/preinstall-job.yaml || true
    $MK delete -f tests/k8s/rbac.yaml || true
}

# Stop and delete Minikube cluster
echo "Stopping Minikube cluster..."
function k8s::stop {
    minikube stop || true
    minikube delete || true
    rm -rf "${GITHUB_WORKSPACE:-$PWD}/.kube"
    rm -rf "${GITHUB_WORKSPACE:-$PWD}/.minikube"
}

# Wait for a Kubernetes Job to complete
echo "Waiting for Kubernetes job completion..."
function wait_job() {
    local JOB_NAME="$1"
    echo "Waiting up to 60s for job/$JOB_NAME to complete..."
    if ! $MK wait \
         --for=condition=complete \
         job/"$JOB_NAME" \
         --timeout=60s
    then
        echo "ERROR: Job '$JOB_NAME' did not complete in time."
        echo "Events:"
        $MK get events --sort-by=.metadata.creationTimestamp
        exit 1
    fi
    echo "job/$JOB_NAME succeeded; logs:"
    $MK logs -l app=admission-controller
}

# Entrypoint for k8s operations
case "${1:-}" in
    install)
        k8s::install_minikube
        ;;
    start)
        k8s::start
        ;;
    apply)
        k8s::apply_all_configurations
        ;;
    clean)
        k8s::clean
        ;;
    stop)
        k8s::stop
        ;;
    *)
        echo "Usage: $0 {install|start|apply|clean|stop}"
        exit 1
        ;;
esac
