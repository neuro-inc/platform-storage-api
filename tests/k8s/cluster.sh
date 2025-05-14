#!/usr/bin/env bash
set -euo pipefail

function k8s::install_minikube {
    local minikube_version="v1.25.2"
    sudo apt-get update
    sudo apt-get install -y conntrack
    curl -Lo minikube https://storage.googleapis.com/minikube/releases/${minikube_version}/minikube-linux-amd64
    chmod +x minikube
    sudo mv minikube /usr/local/bin/
}

function k8s::start {
    # Let Minikube manage the config by default
    unset KUBECONFIG

    # Optionally export if you want to use it later
    export MINIKUBE_HOME="${GITHUB_WORKSPACE:-$PWD}/.minikube"
    mkdir -p "$MINIKUBE_HOME"

    export MINIKUBE_WANTUPDATENOTIFICATION=false
    export MINIKUBE_WANTREPORTERRORPROMPT=false
    export CHANGE_MINIKUBE_NONE_USER=true

    sudo -E minikube start \
        --driver=docker \
        --wait=all \
        --wait-timeout=5m

    # Save kubeconfig for use in later steps
    mkdir -p "$PWD/.kube"
    minikube config view --format='{{.ConfigPath}}' | xargs cat > "$PWD/.kube/config"
    export KUBECONFIG="$PWD/.kube/config"

    kubectl config use-context minikube
    kubectl get nodes -o name | xargs -I {} kubectl label {} --overwrite \
        platform.neuromation.io/nodepool=minikube
}

function k8s::apply_all_configurations {
    echo "Applying configurations..."
    kubectl config use-context minikube

    make dist
    docker build -t admission-controller-tests:latest .
    docker image save -o ac.tar admission-controller-tests:latest
    minikube image load ac.tar

    kubectl apply -f tests/k8s/rbac.yaml
    kubectl apply -f tests/k8s/preinstall-job.yaml
    wait_job admission-controller-lib-preinstall
    kubectl apply -f tests/k8s/admission-controller-deployment.yaml
    kubectl apply -f tests/k8s/postinstall-job.yaml
    wait_job admission-controller-lib-postinstall
}

function k8s::clean {
    echo "Cleaning up..."
    kubectl config use-context minikube
    kubectl delete -f tests/k8s/postinstall-job.yaml || true
    kubectl delete -f tests/k8s/admission-controller-deployment.yaml || true
    kubectl delete -f tests/k8s/preinstall-job.yaml || true
    kubectl delete -f tests/k8s/rbac.yaml || true
}

function k8s::stop {
    echo "Stopping Minikube..."
    sudo -E minikube stop || true
    sudo -E minikube delete || true
    sudo rm -rf ~/.minikube
    sudo rm -rf /root/.minikube
}

function wait_job() {
    local job_name=$1
    echo "Waiting for job $job_name to complete..."
    if ! kubectl wait --for=condition=complete job/$job_name --timeout=60s; then
        echo "ERROR: Job '$job_name' did not complete."
        kubectl get events --sort-by=.metadata.creationTimestamp
        exit 1
    fi
    echo "Job $job_name completed successfully"
    kubectl logs -l app=admission-controller || true
}

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
