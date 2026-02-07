.PHONY: build test vet bench bench-short bench-compare kind-create kind-delete kind-load kind-deploy kind-wait kind-test test-e2e-kind \
	generate manifests operator-build operator-docker-build install-controller-gen

CLUSTER_NAME ?= medelanden-test
IMAGE_TAG    ?= medelanden:test
OPERATOR_TAG ?= medelanden-operator:test
KIND_CONFIG  := e2e/manifests/kind-config.yaml
K8S_MANIFEST := e2e/manifests/medelanden.yaml

# Tool versions
CONTROLLER_TOOLS_VERSION ?= v0.17.2

# Tool binaries
LOCALBIN ?= $(shell pwd)/bin
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen

build:
	go build -v ./...

vet:
	go vet ./...

test:
	go test -v -short -count=1 -timeout 60s ./broker/...

test-integration:
	go test -v -count=1 -timeout 180s -run 'TestIntegration' ./broker/...

bench:
	go test -bench=. -benchmem -count=1 -timeout 300s -run='^$$' ./broker/...

bench-short:
	go test -bench='Benchmark(NodePublish|WALAppend|ProtocolParse)' -benchmem -count=1 -timeout 120s -run='^$$' ./broker/...

bench-compare:
	go test -bench=. -benchmem -count=1 -timeout 300s -run='TestThroughputComparison' -v ./bench/...

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

## Operator targets

$(LOCALBIN):
	mkdir -p $(LOCALBIN)

install-controller-gen: $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

generate: install-controller-gen ## Generate deepcopy methods.
	$(CONTROLLER_GEN) object paths="./api/..."

manifests: install-controller-gen ## Generate CRD and RBAC manifests.
	$(CONTROLLER_GEN) crd rbac:roleName=medelanden-operator-role paths="./..." output:crd:artifacts:config=config/crd/bases output:rbac:dir=config/rbac

operator-build: generate ## Build the operator binary.
	go build -v -o bin/medelanden-operator ./cmd/operator

operator-docker-build: ## Build the operator Docker image.
	docker build -t $(OPERATOR_TAG) -f Dockerfile.operator .
