REGISTRY?=icr.io/ai-services-private
IMAGE=litellm
# 1. Must run update-patch after updating the Litellm version
# 2. Manually regenerate the patches/watsonx-passthrough.patch
LITELLM_VERSION?=1.89.3
IMAGE_REVISION?=1
TAG?=v$(LITELLM_VERSION)-$(IMAGE_REVISION)
CREDS_ARG := $(if $(and $(REGISTRY_USER),$(REGISTRY_PASSWORD)),--creds="$(REGISTRY_USER):$(REGISTRY_PASSWORD)")

CONTAINER_BUILDER?=podman

# Dev env vars — override on the command line or export before running
INSTRUCT_MODEL_NAME?=my-instruct-model
INSTRUCT_MODEL?=watsonx/ibm/granite-4-h-small
WATSONX_APIKEY?=
WATSONX_PROJECT_ID?=
WATSONX_URL?=https://us-south.ml.cloud.ibm.com

run:
	$(CONTAINER_BUILDER) run -d \
		--name litellm-dev \
		--net host \
		-e INSTRUCT_MODEL_NAME="$(INSTRUCT_MODEL_NAME)" \
		-e INSTRUCT_MODEL="$(INSTRUCT_MODEL)" \
		-e WATSONX_APIKEY="$(WATSONX_APIKEY)" \
		-e WATSONX_PROJECT_ID="$(WATSONX_PROJECT_ID)" \
		-e WATSONX_URL="$(WATSONX_URL)" \
		$(REGISTRY)/$(IMAGE):$(TAG) \
		litellm -c /app/config.yaml --detailed_debug
.PHONY: run

build:
	$(CONTAINER_BUILDER) build --platform linux/ppc64le -t $(REGISTRY)/$(IMAGE):$(TAG) --build-arg VERSION=$(LITELLM_VERSION) .
.PHONY: build

push:
	$(CONTAINER_BUILDER) push $(CREDS_ARG) $(REGISTRY)/$(IMAGE):$(TAG)
.PHONY: push

image-tag:
	@echo $(TAG)
.PHONY: image-tag

update-patch:
	cd ./patches && ./pyproject-toml.sh $(LITELLM_VERSION)
.PHONY: update-patch
