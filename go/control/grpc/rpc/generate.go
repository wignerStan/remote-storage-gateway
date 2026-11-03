// Package rpc contains generated operational bindings for the public control IDL.
// The authoritative schema stays under contracts/control/rpc.
package rpc

//go:generate protoc -I../../.. --go_out=../../.. --go_opt=module=github.com/wignerStan/remote-storage-gateway/go --go-grpc_out=../../.. --go-grpc_opt=module=github.com/wignerStan/remote-storage-gateway/go contracts/control/rpc/control.proto
