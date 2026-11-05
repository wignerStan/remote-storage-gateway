// Package worker owns passive product worker configuration.
package worker

import (
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	cpconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
)

type Mount struct {
	Name         string `json:"name"`
	Client       string `json:"client"`
	RemoteClient string `json:"remote_client,omitempty"`
	ReadOnly     bool   `json:"read_only,omitempty"`
}
type TLSOptions struct {
	CertificateFile  string `json:"certificate_file"`
	PrivateKeySecret string `json:"private_key_secret"`
}
type ControlPlaneRoute struct {
	Host     string `json:"host"`
	Upstream string `json:"upstream"`
}
type RuntimeConfig struct {
	StorageIPCSocket   string             `json:"storage_ipc_socket,omitempty"`
	StorageOfferSocket string             `json:"storage_offer_socket,omitempty"`
	Encoded            *EncodedOptions    `json:"encoded,omitempty"`
	ClientSecretRefs   []string           `json:"client_secret_refs,omitempty"`
	Listen             string             `json:"listen"`
	Username           string             `json:"username"`
	AuthSecret         string             `json:"auth_secret"`
	Worker             dp.Bootstrap       `json:"worker"`
	Mounts             []Mount            `json:"mounts"`
	TLS                *TLSOptions        `json:"tls,omitempty"`
	ControlPlane       *ControlPlaneRoute `json:"control_plane,omitempty"`
}

// ProcessConfig adds child-control metadata to the same runtime JSON shape.
type ProcessConfig struct {
	RemoteBroker *cpconfig.RemoteWorker `json:"remote_broker,omitempty"`
	RPC          *cpconfig.RPCServer    `json:"rpc,omitempty"`
	RuntimeConfig
	InstanceID    string `json:"instance_id,omitempty"`
	ControlSocket string `json:"control_socket,omitempty"`
	BrokerSocket  string `json:"broker_socket,omitempty"`
	DataEndpoint  string `json:"data_endpoint,omitempty"`
}
