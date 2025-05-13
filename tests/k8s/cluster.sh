#!/usr/bin/env bash
set -euo verbose pipefail

# based on
# https://github.com/kubernetes/minikube#linux-continuous-integration-without-vm-support

function k8s::install_minikube {
    local minikube_version="v1.25.2"
    sudo apt-get update
    sudo apt-get install -y conntrack
    curl -Lo minikube https://storage.googleapis.com/minikube/releases/${minikube_version}/minikube-linux-amd64
    chmod +x minikube
    sudo mv minikube /usr/local/bin/
    sudo -E minikube config set WantReportErrorPrompt false
    sudo -E minikube config set WantNoneDriverWarning false
}

function k8s::start {
    # point kubectl (and minikube) at a workspace-local kubeconfig
    export KUBECONFIG=${GITHUB_WORKSPACE:-$PWD}/.kube/config
    mkdir -p "$(dirname "$KUBECONFIG")"

    # ensure conntrack is present (needed by minikube)
    sudo apt-get update && sudo apt-get install -y conntrack

    # start Minikube in Docker mode (no sudo, no permission tricks)
    minikube start \
      --driver=docker \
      --kubernetes-version=stable \
      --kubeconfig="$KUBECONFIG" \
      --wait=all \
      --wait-timeout=5m

    # load your test image into Minikube’s Docker daemon
    minikube image load ghcr.io/neuro-inc/admission-controller-lib:latest

    # set the kubectl context
    kubectl config use-context minikube
    kubectl get nodes -o name \
      | xargs -I {} kubectl label {} --overwrite \
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
    kubectl delete -f tests/k8s/postinstall-job.yaml
    kubectl delete -f tests/k8s/admission-controller-deployment.yaml
    kubectl delete -f tests/k8s/preinstall-job.yaml
    kubectl delete -f tests/k8s/rbac.yaml
}


function k8s::stop {
  echo "Stopping minikube..."
    sudo -E minikube stop || :
    sudo -E minikube delete || :
    sudo -E rm -rf ~/.minikube
    sudo rm -rf /root/.minikube
}


function wait_job() {
  local JOB_NAME=$1
  echo "Waiting up to 60 seconds for $JOB_NAME job to succeed..."
  if ! kubectl wait \
       --for=condition=complete \
       job/$JOB_NAME \
       --timeout="60s"
  then
    echo "ERROR: Job '$JOB_NAME' did not complete within 60 seconds."
    echo "----- Displaying all Kubernetes events: -----"
    kubectl get events --sort-by=.metadata.creationTimestamp
    exit 1
  fi

  echo "job/$JOB_NAME succeeded"
  kubectl logs -l app=admission-controller
}


function k8s::apply {
    minikube status
    k8s::apply_all_configurations
}

case "${1:-}" in
    install)
        k8s::install_minikube
        ;;
    start)
        k8s::start
        ;;
    apply)
        k8s::apply
        ;;
    clean)
        k8s::clean
        ;;
    stop)
        k8s::stop
        ;;
esac
