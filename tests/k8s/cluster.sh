#!/usr/bin/env bash
set -euo pipefail

# Install Minikube in CI environment
function k8s::install_minikube {
    local minikube_version="v1.25.2"
    sudo apt-get update
    sudo apt-get install -y conntrack
    curl -Lo minikube "https://storage.googleapis.com/minikube/releases/${minikube_version}/minikube-linux-amd64"
    chmod +x minikube
    sudo mv minikube /usr/local/bin/
}

# Start Minikube and make its kubeconfig available to tests
function k8s::start {
    # Define where tests expect the kubeconfig
    export KUBECONFIG="${GITHUB_WORKSPACE:-$PWD}/.kube/config"
    mkdir -p "$(dirname "$KUBECONFIG")"

    # Ensure conntrack is present
    sudo apt-get update && sudo apt-get install -y conntrack

    # Start Minikube unprivileged, Docker driver
    minikube start \
      --driver=docker \
      --kubernetes-version=stable \
      --wait=all \
      --wait-timeout=5m

    # Copy the real config from the runner's home into the workspace file
    cp "$HOME/.kube/config" "$KUBECONFIG"
}

# Apply all k8s manifests for integration tests
function k8s::apply_all_configurations {
    echo "Applying Kubernetes configurations..."
    kubectl --kubeconfig="$KUBECONFIG" apply -f tests/k8s/rbac.yaml
    kubectl --kubeconfig="$KUBECONFIG" apply -f tests/k8s/preinstall-job.yaml
    wait_job admission-controller-lib-preinstall
    kubectl --kubeconfig="$KUBECONFIG" apply -f tests/k8s/admission-controller-deployment.yaml
    kubectl --kubeconfig="$KUBECONFIG" apply -f tests/k8s/postinstall-job.yaml
    wait_job admission-controller-lib-postinstall
}

# Clean up k8s resources
function k8s::clean {
    echo "Cleaning up Kubernetes resources..."
    kubectl --kubeconfig="$KUBECONFIG" delete -f tests/k8s/postinstall-job.yaml
    kubectl --kubeconfig="$KUBECONFIG" delete -f tests/k8s/admission-controller-deployment.yaml
    kubectl --kubeconfig="$KUBECONFIG" delete -f tests/k8s/preinstall-job.yaml
    kubectl --kubeconfig="$KUBECONFIG" delete -f tests/k8s/rbac.yaml
}

# Stop and delete Minikube cluster data
function k8s::stop {
    echo "Stopping Minikube..."
    minikube stop || true
    minikube delete || true
    rm -rf "${GITHUB_WORKSPACE:-$PWD}/.kube"
    rm -rf "${GITHUB_WORKSPACE:-$PWD}/.minikube"
}

# Wait for a job to complete
function wait_job() {
    local JOB_NAME="$1"
    echo "Waiting up to 60s for job/$JOB_NAME to complete..."
    if ! kubectl --kubeconfig="$KUBECONFIG" wait \
         --for=condition=complete \
         job/"$JOB_NAME" \
         --timeout=60s
    then
        echo "ERROR: Job '$JOB_NAME' did not complete in time."
        echo "Events:"
        kubectl --kubeconfig="$KUBECONFIG" get events --sort-by=.metadata.creationTimestamp
        exit 1
    fi
    echo "job/$JOB_NAME succeeded; logs:"
    kubectl --kubeconfig="$KUBECONFIG" logs -l app=admission-controller
}

# Entry point
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
