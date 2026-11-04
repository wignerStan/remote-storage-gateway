// Package control declares finite control-plane assignments shared with bundle assembly.
package control

import "github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"

type Rule struct {
	InstanceID string          `json:"instance_id"`
	Authority  string          `json:"authority"`
	SourceID   string          `json:"source_id,omitempty"`
	TargetID   string          `json:"target_id,omitempty"`
	Decision   broker.Decision `json:"decision"`
}
type WorkerEndpoint struct {
	ID            string        `json:"id"`
	ControlSocket string        `json:"control_socket,omitempty"`
	BrokerSocket  string        `json:"broker_socket,omitempty"`
	DataEndpoint  string        `json:"data_endpoint"`
	Remote        *RemoteWorker `json:"remote,omitempty"`
}

// TLSIdentity uses a distinct certificate identity per process role. URI SANs
// are checked against exact operator-assigned identities at the receiving port.
type TLSIdentity struct {
	CAFile          string `json:"ca_file"`
	CertificateFile string `json:"certificate_file"`
	PrivateKeyFile  string `json:"private_key_file"`
	ServerName      string `json:"server_name,omitempty"`
	PeerURI         string `json:"peer_uri,omitempty"`
}
type RemoteWorker struct {
	Address string      `json:"address"`
	TLS     TLSIdentity `json:"tls"`
}
type RPCServer struct {
	Listen string            `json:"listen"`
	TLS    TLSIdentity       `json:"tls"`
	Peers  map[string]string `json:"peers"`
}
type Config struct {
	RPC              *RPCServer       `json:"rpc,omitempty"`
	RuntimeDirectory string           `json:"runtime_directory,omitempty"`
	Workers          []WorkerEndpoint `json:"workers"`
	Rules            []Rule           `json:"rules,omitempty"`
	MetadataNames    []string         `json:"metadata_names,omitempty"`
}
