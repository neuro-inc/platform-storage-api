.PHONY: all test clean
all test clean:

.PHONY: venv
venv:
	poetry lock
	poetry install --with dev;

.PHONY: build
build: venv poetry-plugins

.PHONY: poetry-plugins
poetry-plugins:
	poetry self add "poetry-dynamic-versioning[plugin]"; \
    poetry self add "poetry-plugin-export";

.PHONY: setup
setup: venv
	poetry run pre-commit install;

.PHONY: lint
lint: format
	poetry run mypy --show-error-codes src tests

.PHONY: format
format:
ifdef CI
	poetry run pre-commit run --all-files --show-diff-on-failure
else
	poetry run pre-commit run --all-files
endif

.PHONY: test_unit
test_unit:
	poetry run pytest -vv --log-level=INFO --cov-config=pyproject.toml --cov=platform_storage --cov-report xml:.coverage.unit.xml tests/unit

.PHONY: test_integration
test_integration:
	poetry run pytest -vv --log-level=INFO --cov-config=pyproject.toml --cov=platform_storage --cov-report xml:.coverage.integration.xml

.PHONY: clean-dist
clean-dist:
	rm -rf dist

IMAGE_NAME = platformstorageapi

.PHONY: build/image
build/image: dist
	docker build \
		--build-arg PY_VERSION=$$(cat .python-version) \
		-t $(IMAGE_NAME):latest .

.python-version:
	@echo "Error: .python-version file is missing!" && exit 1

.PHONY: dist
dist: build
	rm -rf build dist; \
	poetry export -f requirements.txt --without-hashes -o requirements.txt; \
	poetry build -f wheel;

.PHONY: build/test-image
build/test-image: dist
	docker build \
		--build-arg PY_VERSION=$$(cat .python-version) \
		-t admission-controller-tests:latest .

.PHONY: docker_pull_test_images
docker_pull_test_images:
ifeq ($(MINIKUBE_DRIVER),none)
	make _docker_pull_test_images
else
	@eval $$(minikube docker-env); \
	make _docker_pull_test_images
endif

.PHONY: _docker_pull_test_images
_docker_pull_test_images:
	docker pull ghcr.io/neuro-inc/admission-controller-lib:latest; \


include k8s.mk
