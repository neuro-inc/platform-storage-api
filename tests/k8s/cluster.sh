#!/usr/bin/env bash
set -euo pipefail

# k8s cluster management script for CI (GitHub Actions)

# Install Minikube in CI environment
function k8s::install_minikube {
    local minikube_version="v1.25.2"
    sudo apt-get update
    sudo apt-get install -y conntrack
    curl -Lo minikube "https://storage.googleapis.com/minikube/releases/${minikube_version}/minikube-linux-amd64"
    chmod +x minikube
    sudo mv minikube /usr/local/bin/
}

# Start Minikube cluster and expose kubeconfig for tests
function k8s::start {
    # Force HOME into the workspace so Minikube writes config where we expect
    local WS="${GITHUB_WORKSPACE:-$PWD}"
    export HOME="$WS"

    # Define kubeconfig path for pytest
    export KUBECONFIG="$WS/.kube/config"
    mkdir -p "$(dirname "$KUBECONFIG")"

    # Ensure conntrack is installed
    sudo apt-get update && sudo apt-get install -y conntrack

    # Start Minikube unprivileged using Docker driver
    minikube start \
      --driver=docker \
      --kubernetes-version=stable \
      --wait=all \
      --wait-timeout=5m

    # Dump full kubeconfig via Minikube's embedded kubectl
    minikube kubectl -- config view --raw > "$KUBECONFIG"

    # Label the node for your tests
    minikube kubectl -- label node minikube \
        platform.neuromation.io/nodepool=minikube --overwrite

    # Load test images into Minikube's Docker
    minikube image load ghcr.io/neuro-inc/admission-controller-lib:latest
}

# Apply all Kubernetes configurations for integration tests
function k8s::apply_all_configurations {
    echo "Applying Kubernetes configurations..."
    # Use Minikube's kubectl to operate against the cluster
    minikube kubectl -- apply -f tests/k8s/rbac.yaml
    minikube kubectl -- apply -f tests/k8s/preinstall-job.yaml
    wait_job admission-controller-lib-preinstall

    minikube kubectl -- apply -f tests/k8s/admission-controller-deployment.yaml
    minikube kubectl -- apply -f tests/k8s/postinstall-job.yaml
    wait_job admission-controller-lib-postinstall
}

# Clean up Kubernetes resources
function k8s::clean {
    echo "Cleaning up Kubernetes resources..."
    minikube kubectl -- delete -f tests/k8s/postinstall-job.yaml || true
    minikube kubectl -- delete -f tests/k8s/admission-controller-deployment.yaml || true
    minikube kubectl -- delete -f tests/k8s/preinstall-job.yaml || true
    minikube kubectl -- delete -f tests/k8s/rbac.yaml || true
}

# Stop and remove Minikube cluster
function k8s::stop {
    echo "Stopping Minikube..."
    minikube stop || true
    minikube delete || true
    rm -rf "${GITHUB_WORKSPACE:-$PWD}/.kube"
    rm -rf "${GITHUB_WORKSPACE:-$PWD}/.minikube"
}

# Wait for a Kubernetes Job to complete
function wait_job() {
    local JOB_NAME="$1"
    echo "Waiting up to 60s for job/$JOB_NAME to complete..."
    if ! minikube kubectl -- wait \
         --for=condition=complete \
         job/"$JOB_NAME" \
         --timeout=60s
    then
        echo "ERROR: Job '$JOB_NAME' did not complete in time."
        echo "Events:"
        minikube kubectl -- get events --sort-by=.metadata.creationTimestamp
        exit 1
    fi

    echo "job/$JOB_NAME succeeded; logs:"
    minikube kubectl -- logs -l app=admission-controller
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
