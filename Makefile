IMAGE_NAME ?= platformstorageapi
IMAGE_TAG ?= latest
IMAGE ?= $(IMAGE_NAME):$(IMAGE_TAG)
IMAGE_K8S ?= $(GKE_DOCKER_REGISTRY)/$(GKE_PROJECT_ID)/$(IMAGE_NAME)

ifdef CIRCLECI
    PIP_INDEX_URL ?= "https://$(DEVPI_USER):$(DEVPI_PASS)@$(DEVPI_HOST)/$(DEVPI_USER)/$(DEVPI_INDEX)"
else
    PIP_INDEX_URL ?= "$(shell python pip_extra_index_url.py)"
endif

build:
	@docker build --build-arg PIP_INDEX_URL="$(PIP_INDEX_URL)" -t $(IMAGE) .

pull:
	-docker-compose --project-directory=`pwd` -p platformregistryapi \
	    -f tests/docker/e2e.compose.yml pull

format:
	isort -rc platform_storage_api tests
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
	flake8 platform_storage_api tests

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
	@if [ "$(CLUSTER_TYPE)" = "regional" ]; then\
		gcloud config set compute/region $(GKE_CLUSTER_REGION)
	else \
		gcloud config set compute/zone $(GKE_COMPUTE_ZONE)
	fi
	gcloud auth configure-docker

_helm:
	curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash -s -- -v v2.11.0


gke_docker_push: build
	docker tag $(IMAGE) $(IMAGE_K8S):latest
	docker tag $(IMAGE_K8S):latest $(IMAGE_K8S):$(CIRCLE_SHA1)
	sudo /opt/google-cloud-sdk/bin/gcloud docker -- push $(IMAGE_K8S)

gke_k8s_deploy: _helm
	@if [ "$(CLUSTER_TYPE)" = "regional" ]; then\
		sudo /opt/google-cloud-sdk/bin/gcloud --quiet container clusters get-credentials $(GKE_CLUSTER_NAME) --region $(GKE_CLUSTER_REGION)
	else \
		sudo /opt/google-cloud-sdk/bin/gcloud --quiet container clusters get-credentials $(GKE_CLUSTER_NAME) --zone $(GKE_COMPUTE_ZONE)
	fi
	sudo chown -R circleci: $(HOME)/.kube
	helm --set "global.env=$(HELM_ENV)" --set "IMAGE.$(HELM_ENV)=$(IMAGE_K8S):$(CIRCLE_SHA1)" upgrade --install platformstorageapi deploy/platformstorageapi/ --wait --timeout 600
