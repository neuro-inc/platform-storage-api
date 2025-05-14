#!/usr/bin/env bash
set -euo pipefail

# shorthand for invoking Minikube’s embedded kubectl
MK="minikube kubectl --"

function k8s::install_minikube {
    local minikube_version="v1.25.2"
    sudo apt-get update
    sudo apt-get install -y conntrack
    curl -Lo minikube "https://storage.googleapis.com/minikube/releases/${minikube_version}/minikube-linux-amd64"
    chmod +x minikube
    sudo mv minikube /usr/local/bin/
}

function k8s::start {
    # 1) write kubeconfig into the repo workspace
    export KUBECONFIG="${GITHUB_WORKSPACE:-$PWD}/.kube/config"
    mkdir -p "$(dirname "$KUBECONFIG")"

    # 2) ensure conntrack
    sudo apt-get update && sudo apt-get install -y conntrack

    # 3) start Minikube unprivileged with Docker driver
    minikube start \
      --driver=docker \
      --kubernetes-version=stable \
      --wait=all \
      --wait-timeout=5m

    # 4) dump the merged kubeconfig
    $MK config view --raw > "$KUBECONFIG"

    # 5) configure context & label node
    $MK config use-context minikube
    $MK get nodes -o name \
      | xargs -I {} $MK label {} \
          platform.neuromation.io/nodepool=minikube --overwrite

    # 6) load test image
    minikube image load ghcr.io/neuro-inc/admission-controller-lib:latest
}

function k8s::apply_all_configurations {
    echo "Applying Kubernetes configurations..."
    # ensure we’re talking to the right cluster
    $MK config use-context minikube

    make dist
    docker build -t admission-controller-tests:latest .
    docker image save -o ac.tar admission-controller-tests:latest
    minikube image load ac.tar

    $MK apply -f tests/k8s/rbac.yaml
    $MK apply -f tests/k8s/preinstall-job.yaml
    wait_job admission-controller-lib-preinstall

    $MK apply -f tests/k8s/admission-controller-deployment.yaml
    $MK apply -f tests/k8s/postinstall-job.yaml
    wait_job admission-controller-lib-postinstall
}

function k8s::clean {
    echo "Cleaning up Kubernetes resources..."
    $MK config use-context minikube
    $MK delete -f tests/k8s/postinstall-job.yaml
    $MK delete -f tests/k8s/admission-controller-deployment.yaml
    $MK delete -f tests/k8s/preinstall-job.yaml
    $MK delete -f tests/k8s/rbac.yaml
}

function k8s::stop {
    echo "Stopping Minikube..."
    minikube stop || true
    minikube delete || true
    rm -rf "${GITHUB_WORKSPACE:-$PWD}/.kube"
    rm -rf "${GITHUB_WORKSPACE:-$PWD}/.minikube"
}

function wait_job() {
    local JOB_NAME="$1"
    echo "Waiting up to 60s for job/$JOB_NAME to complete..."
    if ! $MK wait \
         --for=condition=complete \
         job/"$JOB_NAME" \
         --timeout=60s
    then
        echo "ERROR: Job '$JOB_NAME' did not complete in time."
        echo "All events:"
        $MK get events --sort-by=.metadata.creationTimestamp
        exit 1
    fi

    echo "job/$JOB_NAME succeeded; logs:"
    $MK logs -l app=admission-controller
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
