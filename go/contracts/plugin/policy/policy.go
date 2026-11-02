// Package policy owns separate bounded L4 and L7 policy ports.
package policy

import "context"

const L4Contract = "network-data-plane-worker/l4-policy/v1"
const L7Contract = "network-data-plane-worker/l7-policy/v1"

type L4Facts struct {
	Network, LocalAddress, PeerAddress, SNI, ALPN string
	PreRead                                       []byte
}
type L4Decision struct {
	Allow   bool
	RouteID string
}
type L4 interface {
	Admit(context.Context, L4Facts) (L4Decision, error)
	Close(context.Context) error
}

// GrantedHeaders includes only explicitly authorized bounded metadata. Raw
// queries, bodies, native connections, and handles are absent from this port.
type L7Facts struct {
	Method, Host, Path string
	GrantedHeaders     map[string][]string
}
type L7Decision struct {
	Allow   bool
	RouteID string
}
type L7 interface {
	Admit(context.Context, L7Facts) (L7Decision, error)
	Close(context.Context) error
}
