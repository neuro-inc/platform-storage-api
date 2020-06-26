IMAGE_NAME ?= platformstorageapi
IMAGE_TAG ?= $(GITHUB_SHA)
ARTIFACTORY_TAG ?=$(shell echo "$(GITHUB_SHA)" | awk -F/ '{print $$2}')
IMAGE ?= $(IMAGE_NAME):$(IMAGE_TAG)
IMAGE_K8S_GKE ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME)
IMAGE_K8S_AWS ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(IMAGE_NAME)

export PIP_INDEX_URL ?= $(shell python pip_extra_index_url.py)
#export IMAGE_REPO ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com

build:
	@docker build --build-arg PIP_INDEX_URL -t $(IMAGE) .
	docker tag $(IMAGE) $(IMAGE_NAME):latest

pull:
	-docker-compose --project-directory=`pwd` -p platformregistryapi \
	    -f tests/docker/e2e.compose.yml pull

format:
	isort -rc platform_storage_api tests setup.py
	black platform_storage_api tests setup.py pip_extra_index_url.py

lint: build_test lint_built

test_unit: build_test test_unit_built

test_integration: build_test test_integration_built

build_test: build
	docker build -t platformstorageapi-test -f tests/Dockerfile .

lint_built:
	docker run --rm platformstorageapi-test make _lint

test_integration_built: pull
	docker-compose --project-directory=`pwd` -f tests/docker/e2e.compose.yml run test make _test_integration; \
	exit_code=$$?; \
	docker-compose --project-directory=`pwd` \
	    -f tests/docker/e2e.compose.yml kill; \
	docker-compose --project-directory=`pwd` \
	    -f tests/docker/e2e.compose.yml rm -f; \
	exit $$exit_code

test_unit_built:
	docker run --rm platformstorageapi-test make _test_unit

_test_unit:
	pytest -vv tests/unit

_test_integration:
	pytest -vv tests/integration

_lint:
	black --check platform_storage_api tests setup.py
	@if ! isort -c -rc platform_storage_api tests setup.py; then \
            echo "Import sort errors, run 'make format' to fix them!!!"; \
            isort --diff -rc platform_storage_api tests setup.py; \
            false; \
        fi
	flake8 platform_storage_api tests setup.py
	mypy platform_storage_api tests

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

eks_login:
	pip install --upgrade awscli
	aws eks --region $(AWS_REGION) update-kubeconfig --name $(AWS_CLUSTER_NAME)

_helm:
	curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash -s -- -v v2.11.0

docker_push: build
	docker tag $(IMAGE) $(IMAGE_K8S_AWS):latest
	docker tag $(IMAGE_K8S_AWS):latest $(IMAGE_K8S_AWS):$(IMAGE_TAG)
	docker push  $(IMAGE_K8S_AWS):latest
	docker push  $(IMAGE_K8S_AWS):$(IMAGE_TAG)

helm_deploy:
	#helm -f deploy/platformstorageapi/values-$(HELM_ENV)-aws.yaml --set "IMAGE=$(IMAGE_K8S_AWS):$(IMAGE_TAG)" upgrade --install platformstorageapi deploy/platformstorageapi/ --namespace platform --wait --timeout 600
	helm -f deploy/platformstorageapi/values-$(HELM_ENV)-aws.yaml --set "IMAGE=$(IMAGE_K8S_AWS):$(IMAGE_TAG)" upgrade --install platformstorageapi deploy/platformstorageapi/ --namespace platform --debug --dry-run


artifactory_docker_push: build
	docker tag $(IMAGE) $(ARTIFACTORY_DOCKER_REPO)/$(IMAGE_NAME):$(ARTIFACTORY_TAG)
	docker login $(ARTIFACTORY_DOCKER_REPO) --username=$(ARTIFACTORY_USERNAME) --password=$(ARTIFACTORY_PASSWORD)
	docker push $(ARTIFACTORY_DOCKER_REPO)/$(IMAGE_NAME):$(ARTIFACTORY_TAG)

artifactory_helm_push: _helm
	mkdir -p temp_deploy/platformstorageapi
	cp -Rf deploy/platformstorageapi/. temp_deploy/platformstorageapi
	cp temp_deploy/platformstorageapi/values-template.yaml temp_deploy/platformstorageapi/values.yaml
	sed -i "s/IMAGE_TAG/$(ARTIFACTORY_TAG)/g" temp_deploy/platformstorageapi/values.yaml
	find temp_deploy/platformstorageapi -type f -name 'values-*' -delete
	helm init --client-only
	helm package --app-version=$(ARTIFACTORY_TAG) --version=$(ARTIFACTORY_TAG) temp_deploy/platformstorageapi/
	helm plugin install https://github.com/belitre/helm-push-artifactory-plugin
	helm push-artifactory $(IMAGE_NAME)-$(ARTIFACTORY_TAG).tgz $(ARTIFACTORY_HELM_REPO) --username $(ARTIFACTORY_USERNAME) --password $(ARTIFACTORY_PASSWORD)

