DOCKER_REGISTRY ?= registry.neuromation.io
DOCKER_REPO ?= $(DOCKER_REGISTRY)/neuromationorg
IMAGE_NAME ?= platformstorageapi
IMAGE_TAG ?= latest
IMAGE ?= $(DOCKER_REPO)/$(IMAGE_NAME):$(IMAGE_TAG)

_docker_login:
	@docker login -u "$(DOCKER_USER)" -p "$(DOCKER_PASS)" $(DOCKER_REGISTRY)

build:
	docker build -t $(IMAGE) .

push: _docker_login
	docker push $(IMAGE)
