package provider

/*
#cgo LDFLAGS: -pthread
#include "provider.h"
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"runtime/cgo"
	"sync"
	"time"
	"unsafe"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/nativeclient"
)

var factoryMu sync.Mutex
var factory client.Factory

// RegisterFactory is called once by the concrete plugin during package init.
// The native entry waits for Go initialization before reading this identity.
func RegisterFactory(value client.Factory) {
	if value == nil {
		panic("native client factory required")
	}
	factoryMu.Lock()
	defer factoryMu.Unlock()
	if factory != nil {
		panic("native client factory already registered")
	}
	factory = value
}

//export worker_client_go_package
func worker_client_go_package() C.InfraBytes {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	if factory == nil {
		return C.InfraBytes{}
	}
	name := factory.Descriptor().PluginID
	return C.InfraBytes{ptr: (*C.uint8_t)(C.CBytes([]byte(name))), len: C.size_t(len(name))}
}

type serviceOwner struct {
	service *nativeclient.Service
	mu      sync.Mutex
	cancel  map[uint64]context.CancelFunc
}

func owner(h C.uint64_t) *serviceOwner { return cgo.Handle(h).Value().(*serviceOwner) }

//export worker_client_go_new
func worker_client_go_new() C.uint64_t {
	s, e := nativeclient.NewService(factory)
	if e != nil {
		return 0
	}
	return C.uint64_t(cgo.NewHandle(&serviceOwner{service: s, cancel: map[uint64]context.CancelFunc{}}))
}

//export worker_client_go_call
func worker_client_go_call(h, op C.uint64_t, method, input C.InfraBytes, meta C.InfraCallMeta, token unsafe.Pointer, out *C.InfraOwnedBytes) (result C.uint32_t) {
	defer func() {
		if recover() != nil {
			result = C.INFRA_FAILED
		}
	}()
	s := owner(h)
	timeout := time.Duration(meta.timeout_ms) * time.Millisecond
	if timeout <= 0 || timeout > 5*time.Minute {
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	s.mu.Lock()
	s.cancel[uint64(op)] = cancel
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.cancel, uint64(op)); s.mu.Unlock() }()
	if C.worker_client_operation_cancelled(token) != 0 {
		cancel()
	}
	m := C.GoStringN((*C.char)(unsafe.Pointer(method.ptr)), C.int(method.len))
	b := C.GoBytes(unsafe.Pointer(input.ptr), C.int(input.len))
	reply, e := s.service.Call(ctx, m, b)
	if e != nil {
		return C.uint32_t(nativeclient.ErrorCode(e))
	}
	if len(reply) > 1<<20 {
		return C.INFRA_FAILED
	}
	if len(reply) > 0 {
		out.ptr = (*C.uint8_t)(C.CBytes(reply))
		out.len = C.size_t(len(reply))
	}
	return C.INFRA_OK
}

//export worker_client_go_cancel
func worker_client_go_cancel(h, op C.uint64_t) {
	s := owner(h)
	s.mu.Lock()
	f := s.cancel[uint64(op)]
	s.mu.Unlock()
	if f != nil {
		f()
	}
}

//export worker_client_go_close
func worker_client_go_close(h C.uint64_t) C.uint32_t {
	s := owner(h)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if s.service.Close(ctx) != nil {
		return C.INFRA_FAILED
	}
	return C.INFRA_OK
}

//export worker_client_go_delete
func worker_client_go_delete(h C.uint64_t) { cgo.Handle(h).Delete() }
