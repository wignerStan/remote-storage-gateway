.DEFAULT_GOAL := all
MISE ?= mise
INFRA_ROOT ?= /develop/infra
DATA_PLANE_ROOT ?= /develop/network/data-plane-worker
RCLONE_ROOT ?= /develop/network/rclone
BUILD_DIR ?= /tmp/trade-remote-gateway-build
export GATEWAY_BUILD_INPUTS = $(BUILD_DIR)/build-inputs/manifest.json
export RCLONE_BUILD_INPUTS = $(BUILD_DIR)/materializer/build-inputs/manifest.json
GO = $(MISE) exec go@1.27.3 -- go
DERIVE = ../build/derive/main.go ../build/derive/remote_inputs.go ../build/derive/remote_release.go
SOURCE_REFERENCES ?= /develop/storage/remote-storage-gateway/third_party
SOURCE_TOOL = cd go && GOWORK=$(BUILD_DIR)/bootstrap/go.work $(GO) run $(DERIVE) -root $(CURDIR) -out $(BUILD_DIR) -infra-root $(INFRA_ROOT) -data-plane-root $(DATA_PLANE_ROOT) -rclone-root $(RCLONE_ROOT) -source-references $(SOURCE_REFERENCES) -mise $(MISE)
.PHONY: all bootstrap source prepare ui control-plane plugin worker materializer rclone transfer admin test test-build test-derived test-core image-inputs images
bootstrap:
	sh build/bootstrap-workspace.sh "$(CURDIR)" "$(INFRA_ROOT)" "$(BUILD_DIR)" "$(MISE)" "$(DATA_PLANE_ROOT)" "$(RCLONE_ROOT)"
source: bootstrap
	$(SOURCE_TOOL) source
	$(MAKE) -C $(RCLONE_ROOT) MISE=$(MISE) INFRA_ROOT=$(INFRA_ROOT) DATA_PLANE_ROOT=$(DATA_PLANE_ROOT) BUILD_DIR=$(BUILD_DIR)/materializer prepare
prepare: source
	$(SOURCE_TOOL) remote-inputs
	$(SOURCE_TOOL) verify-inputs
all: prepare
	$(SOURCE_TOOL) collect
ui: prepare
	$(SOURCE_TOOL) ui
control-plane plugin worker materializer rclone transfer admin: all
test-build: bootstrap
	cd go && GOWORK=$(BUILD_DIR)/bootstrap/go.work $(GO) test -race $(DERIVE) ../build/derive/main_test.go ../build/derive/remote_inputs_test.go ../build/derive/remote_release_test.go
test-derived: source
	$(SOURCE_TOOL) test
test-core: all
	cd $(DATA_PLANE_ROOT)/go && GOWORK=$(BUILD_DIR)/go.work REMOTE_GATEWAY_REAPI_PLUGIN=$(BUILD_DIR)/bin/transfer-reapi.so $(GO) test -race -tags dataplane_l7 ./...
test: all
	cd go && GOWORK=$(BUILD_DIR)/go.work REMOTE_GATEWAY_OPENLIST_PLUGIN=$(BUILD_DIR)/bin/client-openlist.so REMOTE_GATEWAY_RCLONE_PLUGIN=$(BUILD_DIR)/bin/client-rclone.so REMOTE_GATEWAY_RCLONE_BIN=$(BUILD_DIR)/bin/rclone REMOTE_GATEWAY_REAPI_PLUGIN=$(BUILD_DIR)/bin/transfer-reapi.so $(GO) test -race -tags dataplane_l7 ./...
image-inputs: all
	$(SOURCE_TOOL) -release inputs
images: prepare
	$(SOURCE_TOOL) -release images
