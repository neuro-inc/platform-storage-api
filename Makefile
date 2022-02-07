AWS_REGION ?= us-east-1

GITHUB_OWNER ?= neuro-inc

IMAGE_TAG ?= latest

IMAGE_REPO_aws    = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
IMAGE_REPO_github = ghcr.io/$(GITHUB_OWNER)

IMAGE_REGISTRY ?= aws

IMAGE_NAME      = platformstorageapi
IMAGE_REPO_BASE = $(IMAGE_REPO_$(IMAGE_REGISTRY))
IMAGE_REPO      = $(IMAGE_REPO_BASE)/$(IMAGE_NAME)

HELM_ENV           ?= dev
HELM_CHART          = platform-storage
HELM_CHART_VERSION ?= 1.0.0
HELM_APP_VERSION   ?= 1.0.0

export IMAGE_REPO_BASE

setup:
	pip install -U pip
	pip install -e .[dev]
	pre-commit install

format:
ifdef CI
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
		platformstorageapi:latest

docker_build:
	rm -rf build dist
	pip install -U build
	python -m build
	docker build -t $(IMAGE_NAME):latest .
