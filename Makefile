IMAGE_NAME ?= platformstorageapi
IMAGE_TAG ?= $(GITHUB_SHA)
ARTIFACTORY_TAG ?= $(shell echo $${GITHUB_REF\#refs/tags/v})
IMAGE ?= $(IMAGE_NAME):$(IMAGE_TAG)

IMAGE_REPO_gke   ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)
IMAGE_REPO_aws   ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
IMAGE_REPO_azure ?= $(AZURE_ACR_NAME).azurecr.io

export IMAGE_REPO  ?= ${IMAGE_REPO_${CLOUD_PROVIDER}}
CLOUD_IMAGE  ?=$(IMAGE_REPO)/$(IMAGE_NAME)

export PIP_INDEX_URL ?= $(shell python pip_extra_index_url.py)

setup:
	pip install -r requirements-dev.txt
	pre-commit install

build:
	python setup.py sdist
	docker build -f Dockerfile -t $(IMAGE) \
	--build-arg PIP_INDEX_URL \
	--build-arg DIST_FILENAME=`python setup.py --fullname`.tar.gz .
	docker tag $(IMAGE) $(IMAGE_NAME):latest

pull:
	-docker-compose --project-directory=`pwd` -p platformregistryapi \
	    -f tests/docker/e2e.compose.yml pull

format:
ifdef CI_LINT_RUN
	pre-commit run --all-files --show-diff-on-failure
else
	pre-commit run --all-files
endif


lint: format
	mypy platform_storage_api tests

test_unit:
	pytest -vv tests/unit

test_integration: build_test test_integration_built

build_test: build
	docker build -t platformstorageapi-test -f tests/Dockerfile .

test_integration_built: pull
	docker-compose --project-directory=`pwd` -f tests/docker/e2e.compose.yml run test make _test_integration; \
	exit_code=$$?; \
	docker-compose --project-directory=`pwd` \
	    -f tests/docker/e2e.compose.yml kill; \
	docker-compose --project-directory=`pwd` \
	    -f tests/docker/e2e.compose.yml rm -f; \
	exit $$exit_code

_test_integration:
	pytest -vv tests/integration

run:
	docker run -it --rm --name platformstorageapi \
	    -p 8080:8080 \
	    -v /tmp/np_storage:/tmp/np_storage \
	    -e NP_STORAGE_LOCAL_BASE_PATH=/tmp/np_storage \
	    $(IMAGE)

gke_login:
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet components update --version 204.0.0
	sudo /opt/google-cloud-sdk/bin/gcloud --quiet components update --version 204.0.0 kubectl
	sudo chown circleci:circleci -R $$HOME
	@echo $(GKE_ACCT_AUTH) | base64 --decode > $(HOME)/gcloud-service-key.json
	gcloud auth activate-service-account --key-file $(HOME)/gcloud-service-key.json
	gcloud config set project $(GKE_PROJECT_ID)
	gcloud --quiet config set container/cluster $(GKE_CLUSTER_NAME)
	gcloud config set $(SET_CLUSTER_ZONE_REGION)
	gcloud auth configure-docker

aws_k8s_login:
	pip install --upgrade awscli
	aws eks --region $(AWS_REGION) update-kubeconfig --name $(CLUSTER_NAME)

azure_k8s_login:
	az aks get-credentials --resource-group $(AZURE_RG_NAME) --name $(CLUSTER_NAME)

helm_install:
	curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash -s -- -v $(HELM_VERSION)
	helm init --client-only

docker_push: build
	docker tag $(IMAGE) $(CLOUD_IMAGE):latest
	docker tag $(CLOUD_IMAGE):latest $(CLOUD_IMAGE):$(IMAGE_TAG)
	docker push  $(CLOUD_IMAGE):latest
	docker push  $(CLOUD_IMAGE):$(IMAGE_TAG)

helm_deploy:
	helm -f deploy/platformstorageapi/values-$(HELM_ENV)-$(CLOUD_PROVIDER).yaml --set "IMAGE=$(CLOUD_IMAGE):$(IMAGE_TAG)" upgrade --install platformstorageapi deploy/platformstorageapi/ --namespace platform --wait --timeout 600

artifactory_docker_push: build
	docker tag $(IMAGE) $(ARTIFACTORY_DOCKER_REPO)/$(IMAGE_NAME):$(ARTIFACTORY_TAG)
	docker login $(ARTIFACTORY_DOCKER_REPO) --username=$(ARTIFACTORY_USERNAME) --password=$(ARTIFACTORY_PASSWORD)
	docker push $(ARTIFACTORY_DOCKER_REPO)/$(IMAGE_NAME):$(ARTIFACTORY_TAG)

artifactory_helm_push: helm_install
	mkdir -p temp_deploy/platformstorageapi
	cp -Rf deploy/platformstorageapi/. temp_deploy/platformstorageapi
	cp temp_deploy/platformstorageapi/values-template.yaml temp_deploy/platformstorageapi/values.yaml
	sed -i "s/IMAGE_TAG/$(ARTIFACTORY_TAG)/g" temp_deploy/platformstorageapi/values.yaml
	find temp_deploy/platformstorageapi -type f -name 'values-*' -delete
	helm package --app-version=$(ARTIFACTORY_TAG) --version=$(ARTIFACTORY_TAG) temp_deploy/platformstorageapi/
	helm plugin install https://github.com/belitre/helm-push-artifactory-plugin
	helm push-artifactory $(IMAGE_NAME)-$(ARTIFACTORY_TAG).tgz $(ARTIFACTORY_HELM_REPO) --username $(ARTIFACTORY_USERNAME) --password $(ARTIFACTORY_PASSWORD)
