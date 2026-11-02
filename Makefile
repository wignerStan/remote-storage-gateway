.DEFAULT_GOAL := all

MISE ?= mise
INFRA_ROOT ?= /develop/infra
DATA_PLANE_ROOT ?= /develop/network/data-plane-worker
RCLONE_ROOT ?= /develop/network/rclone
BUILD_DIR ?= /tmp/trade-remote-gateway-build
BAZEL ?= bazel
BAZEL_FLAGS ?= --config=buildbarn --lockfile_mode=off
GOCACHE = $(BUILD_DIR)/tool-cache
export GOCACHE
GO = $(MISE) exec go@1.27.3 -- go
SOURCE_TOOL = cd go && GOWORK=$(BUILD_DIR)/bootstrap/go.work $(GO) run ../build/derive/main.go -root $(CURDIR) -out $(BUILD_DIR) -infra-root $(INFRA_ROOT) -data-plane-root $(DATA_PLANE_ROOT) -rclone-root $(RCLONE_ROOT) -mise $(MISE)
CGO_CFLAGS = -I$(INFRA_ROOT)/rust/crates/interop/native-plugin-protocol/include -I$(INFRA_ROOT)/c/plugin-interop/include
export CGO_CFLAGS

.PHONY: all bootstrap source ui control-plane plugin worker materializer rclone transfer admin test test-build test-derived test-core image-inputs images
all: control-plane plugin worker materializer rclone transfer admin

bootstrap:
	sh build/bootstrap-workspace.sh "$(CURDIR)" "$(INFRA_ROOT)" "$(BUILD_DIR)" "$(MISE)" "$(DATA_PLANE_ROOT)" "$(RCLONE_ROOT)"
source: bootstrap
	$(SOURCE_TOOL) source
ui: source
	$(SOURCE_TOOL) ui
control-plane: ui
	$(SOURCE_TOOL) cp
plugin: source
	$(SOURCE_TOOL) plugin
worker: source
	mkdir -p $(BUILD_DIR)/bin
	cd go && GOWORK=$(BUILD_DIR)/go.work $(GO) build -trimpath -buildvcs=false -tags dataplane_l7 -o $(BUILD_DIR)/bin/storage-worker ./cmd/storage-worker
rclone: source
	mkdir -p $(BUILD_DIR)/bin
	plugin_build_digest="$$($(SOURCE_TOOL) fingerprint)"; cd go && GOWORK=$(BUILD_DIR)/go.work CGO_ENABLED=1 $(GO) build -trimpath -buildvcs=false -buildmode=c-shared -ldflags "-X main.buildDigest=$$plugin_build_digest" -o $(BUILD_DIR)/bin/client-rclone.so ./cmd/plugin-rclone
materializer:
	$(MAKE) -C $(RCLONE_ROOT) MISE=$(MISE) INFRA_ROOT=$(INFRA_ROOT) DATA_PLANE_ROOT=$(DATA_PLANE_ROOT) BUILD_DIR=$(BUILD_DIR)/materializer all
	mkdir -p $(BUILD_DIR)/bin
	cp $(BUILD_DIR)/materializer/bin/rclone $(BUILD_DIR)/bin/rclone
transfer: source
	mkdir -p $(BUILD_DIR)/bin
	plugin_build_digest="$$($(SOURCE_TOOL) fingerprint)"; cd go && GOWORK=$(BUILD_DIR)/go.work CGO_ENABLED=1 $(GO) build -trimpath -buildvcs=false -buildmode=c-shared -ldflags "-X main.buildDigest=$$plugin_build_digest" -o $(BUILD_DIR)/bin/transfer-reapi.so ./cmd/plugin-reapi
admin: source
	mkdir -p $(BUILD_DIR)/bin
	cd go && GOWORK=$(BUILD_DIR)/go.work $(GO) build -trimpath -buildvcs=false -o $(BUILD_DIR)/bin/gateway-admin ./cmd/gateway-admin
test-build: bootstrap
	cd go && GOWORK=$(BUILD_DIR)/bootstrap/go.work $(GO) test -race ../build/derive/main.go ../build/derive/main_test.go
test-derived: source
	$(SOURCE_TOOL) test
test-core: source transfer
	cd $(DATA_PLANE_ROOT)/go && GOWORK=$(BUILD_DIR)/go.work REMOTE_GATEWAY_REAPI_PLUGIN=$(BUILD_DIR)/bin/transfer-reapi.so $(GO) test -race -tags dataplane_l7 ./...
test: test-build test-derived rclone transfer test-core materializer
	cd go && GOWORK=$(BUILD_DIR)/go.work REMOTE_GATEWAY_RCLONE_PLUGIN=$(BUILD_DIR)/bin/client-rclone.so REMOTE_GATEWAY_RCLONE_BIN=$(BUILD_DIR)/bin/rclone REMOTE_GATEWAY_REAPI_PLUGIN=$(BUILD_DIR)/bin/transfer-reapi.so $(GO) test -race -tags dataplane_l7 ./...
image-inputs: all
	$(SOURCE_TOOL) -release inputs
images: image-inputs
	GATEWAY_IMAGE_INPUTS=$(BUILD_DIR)/image-inputs/gateway CONTROL_PLANE_IMAGE_INPUTS=$(BUILD_DIR)/image-inputs/control-plane $(BAZEL) --output_user_root=$(BUILD_DIR)/bazel-user-root build $(BAZEL_FLAGS) --output_groups=+oci_tarball,+digest //:gateway_image //:control_plane_image
