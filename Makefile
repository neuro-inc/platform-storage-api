AWS_ACCOUNT_ID ?= 771188043543
AWS_REGION ?= us-east-1

AZURE_RG_NAME ?= dev
AZURE_ACR_NAME ?= crc570d91c95c6aac0ea80afb1019a0c6f

ARTIFACTORY_DOCKER_REPO ?= neuro-docker-local-public.jfrog.io
ARTIFACTORY_HELM_REPO ?= https://neuro.jfrog.io/artifactory/helm-local-public

HELM_ENV ?= dev

TAG ?= latest

IMAGE_NAME ?= platformstorageapi
IMAGE ?= $(IMAGE_NAME):$(TAG)

CLOUD_IMAGE_REPO_gke   ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)
CLOUD_IMAGE_REPO_aws   ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
CLOUD_IMAGE_REPO_azure ?= $(AZURE_ACR_NAME).azurecr.io
CLOUD_IMAGE_REPO_BASE  ?= ${CLOUD_IMAGE_REPO_${CLOUD_PROVIDER}}
CLOUD_IMAGE_REPO       ?= $(CLOUD_IMAGE_REPO_BASE)/$(IMAGE_NAME)
CLOUD_IMAGE            ?= $(CLOUD_IMAGE_REPO):$(TAG)

ARTIFACTORY_IMAGE_REPO = $(ARTIFACTORY_DOCKER_REPO)/$(IMAGE_NAME)
ARTIFACTORY_IMAGE = $(ARTIFACTORY_IMAGE_REPO):$(TAG)

HELM_CHART = platformstorageapi

export CLOUD_IMAGE_REPO_BASE

setup:
	pip install -U pip
	pip install -r requirements-dev.txt
	pre-commit install

build:
	python setup.py sdist
	docker build -f Dockerfile -t $(IMAGE) \
	--build-arg DIST_FILENAME=`python setup.py --fullname`.tar.gz .
	docker tag $(IMAGE) $(IMAGE_NAME):latest

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

test_integration:
	docker-compose -f tests/docker/e2e.compose.yml up -d
	pytest -vv tests/integration; \
	exit_code=$$?; \
	docker-compose -f tests/docker/e2e.compose.yml kill; \
	docker-compose -f tests/docker/e2e.compose.yml rm -f; \
	exit $$exit_code

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
	curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash -s -- -v $(HELM_VERSION)
	helm plugin install https://github.com/belitre/helm-push-artifactory-plugin --version 1.0.2

docker_push: build
	docker tag $(IMAGE) $(CLOUD_IMAGE)
	docker push $(CLOUD_IMAGE)

_helm_fetch:
	rm -rf temp_deploy/$(HELM_CHART)
	mkdir -p temp_deploy/$(HELM_CHART)
	cp -Rf deploy/$(HELM_CHART) temp_deploy/
	find temp_deploy/$(HELM_CHART) -type f -name 'values*' -delete

_helm_expand_vars:
	export IMAGE_REPO=$(ARTIFACTORY_IMAGE_REPO); \
	export IMAGE_TAG=$(TAG); \
	cat deploy/$(HELM_CHART)/values-template.yaml | envsubst > temp_deploy/$(HELM_CHART)/values.yaml

helm_deploy: _helm_fetch _helm_expand_vars
	helm upgrade $(HELM_CHART) temp_deploy/$(HELM_CHART) \
		-f deploy/$(HELM_CHART)/values-$(HELM_ENV).yaml \
		--set "image.repository=$(CLOUD_IMAGE_REPO)" \
		--namespace platform --install --wait --timeout 600s

artifactory_docker_push: build
	docker tag $(IMAGE) $(ARTIFACTORY_IMAGE)
	docker push $(ARTIFACTORY_IMAGE)

artifactory_helm_push: _helm_fetch _helm_expand_vars
	helm package --app-version=$(TAG) --version=$(TAG) temp_deploy/$(HELM_CHART)
	helm push-artifactory $(HELM_CHART)-$(TAG).tgz $(ARTIFACTORY_HELM_REPO) \
		--username $(ARTIFACTORY_USERNAME) \
		--password $(ARTIFACTORY_PASSWORD)
