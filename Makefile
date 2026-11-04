.DEFAULT_GOAL := all
GO ?= go
NODE ?= node
PNPM ?= pnpm
MISE ?=
NATIVE_SDK_ROOT ?=
PKG_CONFIG ?= pkg-config
JOBS ?= 2
WITH_RCLONE ?= 0
INFRA_ROOT ?= /develop/infra
DATA_PLANE_ROOT ?= /develop/network/data-plane-worker
RCLONE_ROOT ?= /develop/network/rclone
BUILD_DIR ?= $(abspath $(CURDIR)/../build-output/storage-gateway)
SOURCE_REFERENCES ?=
export GOMAXPROCS ?= $(JOBS)
export GOMEMLIMIT ?= 768MiB
GO_RUN = $(if $(strip $(MISE)),$(MISE) exec go@1.27.3 -- go,$(GO))
DERIVE = ../build/derive/main.go ../build/derive/remote_inputs.go ../build/derive/remote_release.go ../build/derive/native_build.go
DERIVE_TESTS = $(addprefix ../,$(wildcard build/derive/*_test.go))
SOURCE_TOOL = cd go && GOWORK=$(BUILD_DIR)/bootstrap/go.work GOTOOLCHAIN=local $(GO_RUN) run $(DERIVE) -root $(CURDIR) -out $(BUILD_DIR) -infra-root $(INFRA_ROOT) -data-plane-root $(DATA_PLANE_ROOT) -rclone-root $(RCLONE_ROOT) -source-references "$(SOURCE_REFERENCES)" -go "$(GO)" -node "$(NODE)" -pnpm "$(PNPM)" -mise "$(MISE)" -native-sdk-root "$(NATIVE_SDK_ROOT)" -pkg-config "$(PKG_CONFIG)" -jobs $(JOBS) -with-rclone=$(WITH_RCLONE)
.PHONY: all build bootstrap source prepare ui control-plane plugin worker materializer rclone transfer admin test test-build test-derived test-core image-inputs
bootstrap:
	bash build/bootstrap-workspace.sh "$(BUILD_DIR)" "$(GO)" "$(MISE)" "$(CURDIR)" "$(INFRA_ROOT)" "$(DATA_PLANE_ROOT)" "$(RCLONE_ROOT)"
source: bootstrap
	$(SOURCE_TOOL) source
prepare: source
	$(SOURCE_TOOL) prepare
all build: source
	$(SOURCE_TOOL) build
ui: source
	$(SOURCE_TOOL) ui
control-plane plugin worker transfer admin: all
materializer rclone:
	$(MAKE) -C "$(RCLONE_ROOT)" GO="$(GO)" MISE="$(MISE)" JOBS=$(JOBS) NATIVE_SDK_ROOT="$(NATIVE_SDK_ROOT)" PKG_CONFIG="$(PKG_CONFIG)" INFRA_ROOT="$(INFRA_ROOT)" DATA_PLANE_ROOT="$(DATA_PLANE_ROOT)" BUILD_DIR="$(BUILD_DIR)/materializer" all
test-build: bootstrap
	cd go && GOWORK=$(BUILD_DIR)/bootstrap/go.work GOTOOLCHAIN=local $(GO_RUN) test -p=$(JOBS) -race $(DERIVE) $(DERIVE_TESTS)
test-derived: source
	$(SOURCE_TOOL) test
test-core: all
	cd "$(DATA_PLANE_ROOT)/go" && GOWORK=$(BUILD_DIR)/go.work GOTOOLCHAIN=local REMOTE_GATEWAY_REAPI_PLUGIN=$(BUILD_DIR)/bin/transfer-reapi.so $(GO_RUN) test -p=$(JOBS) -race -tags dataplane_l7,infra_grpc ./...
test: all
	cd go && GOWORK=$(BUILD_DIR)/go.work GOTOOLCHAIN=local REMOTE_GATEWAY_BUNDLE_BIN=$(BUILD_DIR)/bin/gateway REMOTE_GATEWAY_WORKER_BIN=$(BUILD_DIR)/bin/storage-worker REMOTE_GATEWAY_OPENLIST_PLUGIN=$(BUILD_DIR)/bin/client-openlist.so REMOTE_GATEWAY_RCLONE_PLUGIN=$(BUILD_DIR)/bin/client-rclone.so $(if $(filter 1 true,$(WITH_RCLONE)),REMOTE_GATEWAY_RCLONE_BIN=$(BUILD_DIR)/bin/rclone) REMOTE_GATEWAY_REAPI_PLUGIN=$(BUILD_DIR)/bin/transfer-reapi.so $(GO_RUN) test -p=$(JOBS) -race -tags dataplane_l7,infra_grpc ./...
image-inputs: all
	$(SOURCE_TOOL) -release inputs
