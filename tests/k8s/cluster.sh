#!/usr/bin/env bash
set -euo pipefail

# based on
# https://github.com/kubernetes/minikube#linux-continuous-integration-without-vm-support

function k8s::install_minikube {
    local minikube_version="v1.25.2"
    sudo apt-get update
    sudo apt-get install -y conntrack
    curl -Lo minikube https://storage.googleapis.com/minikube/releases/${minikube_version}/minikube-linux-amd64
    chmod +x minikube
    sudo mv minikube /usr/local/bin/
}

function k8s::start {
    # 1) Point both HOME and KUBECONFIG into the workspace
    local WS="${GITHUB_WORKSPACE:-$PWD}"
    export HOME="$WS"
    export KUBECONFIG="$WS/.kube/config"
    mkdir -p "$(dirname "$KUBECONFIG")"

    # 2) Ensure conntrack (still required)
    sudo apt-get update && sudo apt-get install -y conntrack

    # 3) Start Minikube un-privileged with the Docker driver
    minikube start \
      --driver=docker \
      --kubernetes-version=stable \
      --wait=all \
      --wait-timeout=5m

    # 4) **Dump a complete, raw kube-config** from Minikube’s kubectl
    minikube kubectl -- config view --raw > "$KUBECONFIG"

    # 5) Now *use* that same embedded kubectl for everything:
    minikube kubectl -- config use-context minikube
    minikube kubectl -- get nodes -o name \
      | xargs -I {} minikube kubectl -- label {} \
          platform.neuromation.io/nodepool=minikube --overwrite

    # 6) (Optional) Load your test image into Minikube’s Docker
    minikube image load ghcr.io/neuro-inc/admission-controller-lib:latest
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
