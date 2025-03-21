.PHONY: all test clean
all test clean:

venv:
	python -m venv venv
	. venv/bin/activate; \
	python -m pip install --upgrade pip

.PHONY: setup
setup: venv
	. venv/bin/activate; \
	pip install -e .[dev]; \
	pre-commit install

.PHONY: lint
lint:
	. venv/bin/activate; \
	python -m pre_commit run --all-files
	. venv/bin/activate; \
	python -m mypy src tests

.PHONY: test_unit
test_unit:
	. venv/bin/activate; \
	pytest -vv tests/unit

.PHONY: test_integration
test_integration:
	. venv/bin/activate; \
	pytest -vv tests/integration

dist: venv setup.cfg pyproject.toml $(shell find src -type f)
	make clean-dist
	. venv/bin/activate; \
	pip install -U build; \
	python -m build --wheel ./;

.PHONY: clean-dist
clean-dist:
	rm -rf dist

IMAGE_NAME = platformstorageapi

build/image: .dockerignore Dockerfile dist
	docker build \
		--build-arg PY_VERSION=$$(cat .python-version) \
		-t $(IMAGE_NAME):latest .
	mkdir -p build
	docker image inspect $(IMAGE_NAME):latest -f '{{ .ID }}' > $@


build/test-image:
	docker build \
		--build-arg PY_VERSION=$$(cat .python-version) \
		-t admission-controller-tests:latest .

docker_pull_test_images:
ifeq ($(MINIKUBE_DRIVER),none)
	make _docker_pull_test_images
else
	@eval $$(minikube docker-env); \
	make _docker_pull_test_images
endif

_docker_pull_test_images:
	docker pull ghcr.io/neuro-inc/admission-controller-lib:latest; \


include k8s.mk
