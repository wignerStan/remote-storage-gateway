package worker

import dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"

// EncodedOptions binds an approved executable to a metadata broker and a finite
// operator-installed upstream inventory. The broker never supplies addresses or
// provider credentials, and a desired-state update cannot install executable code.
type EncodedOptions struct {
	Artifact          dp.Artifact       `json:"artifact"`
	SourceID          string            `json:"source_id"`
	TargetID          string            `json:"target_id"`
	MetadataNames     []string          `json:"metadata_names,omitempty"`
	MaxSessions       int               `json:"max_sessions,omitempty"`
	MaxAggregateBytes int64             `json:"max_aggregate_bytes,omitempty"`
	Upstreams         []EncodedUpstream `json:"upstreams"`
}
type EncodedUpstream struct {
	ID         string `json:"id"`
	Address    string `json:"address"`
	TLS        bool   `json:"tls"`
	ServerName string `json:"server_name,omitempty"`
	CAFile     string `json:"ca_file,omitempty"`
	AuthSecret string `json:"auth_secret,omitempty"`
}
