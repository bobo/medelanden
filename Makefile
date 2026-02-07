.PHONY: build test vet kind-create kind-delete kind-load kind-deploy kind-wait kind-test test-e2e-kind

CLUSTER_NAME ?= medelanden-test
IMAGE_TAG    ?= medelanden:test
KIND_CONFIG  := e2e/manifests/kind-config.yaml
K8S_MANIFEST := e2e/manifests/medelanden.yaml

build:
	go build -v ./...

vet:
	go vet ./...

test:
	go test -v -short -count=1 -timeout 60s ./broker/...

test-integration:
	go test -v -count=1 -timeout 180s -run 'TestIntegration' ./broker/...

## Kind e2e targets

kind-create:
	@echo "Creating Kind cluster $(CLUSTER_NAME)..."
	kind delete cluster --name $(CLUSTER_NAME) 2>/dev/null || true
	kind create cluster --name $(CLUSTER_NAME) --config $(KIND_CONFIG) --wait 60s

kind-delete:
	kind delete cluster --name $(CLUSTER_NAME)

kind-load: build
	@echo "Building Docker image $(IMAGE_TAG)..."
	docker build -t $(IMAGE_TAG) .
	@echo "Loading image into Kind cluster..."
	kind load docker-image $(IMAGE_TAG) --name $(CLUSTER_NAME)

kind-deploy:
	@echo "Deploying medelanden into Kind cluster..."
	kubectl --context kind-$(CLUSTER_NAME) apply -f $(K8S_MANIFEST)

kind-wait:
	@echo "Waiting for pods to be ready..."
	kubectl --context kind-$(CLUSTER_NAME) -n medelanden rollout status statefulset/medelanden --timeout=120s

kind-test:
	@echo "Running Kind e2e tests..."
	go test -v -count=1 -timeout 300s -tags e2e ./e2e/... \
		-kind-context kind-$(CLUSTER_NAME) \
		-namespace medelanden

test-e2e-kind: kind-create kind-load kind-deploy kind-wait kind-test
	@echo "Kind e2e tests complete."

install-kind:
	go install sigs.k8s.io/kind@v0.27.0
