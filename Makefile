IMAGE_NAME ?= platformstorageapi
IMAGE_TAG ?= latest
IMAGE ?= $(IMAGE_NAME):$(IMAGE_TAG)
IMAGE_K8S ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME)


build:
	docker build -t $(IMAGE) .

run:
	docker run -it --rm --name platformstorageapi \
	    -p 8080:8080 \
	    -v /tmp/np_storage:/tmp/np_storage \
	    -e NP_STORAGE_LOCAL_BASE_PATH=/tmp/np_storage \
	    $(IMAGE)
gke_login:
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet components update --version 204.0.0
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet components update --version 204.0.0 kubectl
	@echo $(GKE_ACCT_AUTH) | base64 --decode > $(HOME)//gcloud-service-key.json
	sudo /opt/google-cloud-sdk/bin/gcloud auth activate-service-account --key-file $(HOME)/gcloud-service-key.json
	sudo /opt/google-cloud-sdk/bin/gcloud config set project $(GKE_PROJECT_ID)
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet config set container/cluster $(GKE_CLUSTER_NAME)
	sudo /opt/google-cloud-sdk/bin/gcloud config set compute/zone $(GKE_COMPUTE_ZONE)
	curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash
		
gke_docker_push: build
	docker tag $(IMAGE) $(IMAGE_K8S):latest
	docker tag $(IMAGE_K8S):latest $(IMAGE_K8S):$(CIRCLE_SHA1)
	sudo /opt/google-cloud-sdk/bin/gcloud docker -- push $(IMAGE_K8S)

gke_k8s_deploy_dev:
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet container clusters get-credentials $(GKE_CLUSTER_NAME)
	sudo chown -R circleci: $(HOME)/.kube
	helm --set "global.env=dev" --set "IMAGE.dev=$(IMAGE_K8S):$(CIRCLE_SHA1)" upgrade platformstorageapi deploy/platformstorageapi/ --wait --timeout 600
	
gke_k8s_deploy_staging:
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet container clusters get-credentials $(GKE_STAGE_CLUSTER_NAME)
	sudo chown -R circleci: $(HOME)/.kube
	helm --set "global.env=staging" --set "IMAGE.staging=$(IMAGE_K8S):$(CIRCLE_SHA1)" upgrade platformstorageapi deploy/platformstorageapi/ --wait --timeout 600
