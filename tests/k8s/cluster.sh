#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o pipefail
set -o verbose

# Based on:
# https://github.com/kubernetes/minikube#linux-continuous-integration-without-vm-support

function k8s::install_minikube {
    local minikube_version="v1.25.2"
    local crictl_version="v1.28.0"

    sudo apt-get update
    sudo apt-get install -y conntrack socat ebtables iptables containerd curl

    if ! command -v conntrack >/dev/null; then
      echo "conntrack not found, something is wrong"
      exit 1
    fi
    CONNTRACK_PATH=$(command -v conntrack)
    sudo ln -s "$CONNTRACK_PATH" /usr/bin/conntrack 2>/dev/null || true

    # Install crictl
    curl -LO "https://github.com/kubernetes-sigs/cri-tools/releases/download/${crictl_version}/crictl-${crictl_version}-linux-amd64.tar.gz"
    sudo tar -C /usr/local/bin -xzf "crictl-${crictl_version}-linux-amd64.tar.gz"
    rm "crictl-${crictl_version}-linux-amd64.tar.gz"

    # Install minikube
    curl -Lo minikube https://storage.googleapis.com/minikube/releases/${minikube_version}/minikube-linux-amd64
    chmod +x minikube
    sudo mv minikube /usr/local/bin/

    # Skip deprecated config set commands
    echo "Minikube installed"
}

function k8s::start {
    export KUBECONFIG=$HOME/.kube/config
    mkdir -p "$(dirname "$KUBECONFIG")"
    touch "$KUBECONFIG"

    export MINIKUBE_WANTUPDATENOTIFICATION=false
    export MINIKUBE_WANTREPORTERRORPROMPT=false
    export MINIKUBE_HOME=$HOME
    export CHANGE_MINIKUBE_NONE_USER=true

    # Fix permissions before starting
    sudo chown -R "$USER:$USER" "$HOME/.minikube" || true
    chmod -R u+rwX "$HOME/.minikube" || true
    sudo sysctl fs.protected_regular=0

    echo "Starting minikube..."
    sudo -E minikube start \
        --driver=docker \
        --container-runtime=containerd \
        --wait=all \
        --wait-timeout=5m

    if ! minikube status | grep -q "host: Running"; then
        echo "❌ Minikube did not start successfully"
        minikube status
        exit 1
    fi

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
    echo "Stopping minikube..."
    sudo -E minikube stop || true
    sudo -E minikube delete || true
    sudo rm -rf "$HOME/.minikube"
    sudo rm -rf /root/.minikube
}

function wait_job() {
  local JOB_NAME=$1
  echo "Waiting up to 60 seconds for $JOB_NAME job to succeed..."
  if ! kubectl wait --for=condition=complete job/$JOB_NAME --timeout="60s"; then
    echo "ERROR: Job '$JOB_NAME' did not complete within 60 seconds."
    echo "----- Displaying all Kubernetes events: -----"
    kubectl get events --sort-by=.metadata.creationTimestamp
    exit 1
  fi
  echo "job/$JOB_NAME succeeded"
  kubectl logs -l app=admission-controller || true
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
    *)
        echo "Usage: $0 {install|start|apply|clean|stop}"
        exit 1
        ;;
esac
