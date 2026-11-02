// Package vfs describes the independent, data-plane-only materializer process.
package vfs

type ProcessConfig struct {
	WorkerLocalDataSocket string `json:"worker_local_data_socket,omitempty"`
	LocalUsername         string `json:"local_username"`
	LocalPasswordFile     string `json:"local_password_file"`
	WorkerControlSocket   string `json:"worker_control_socket"`
	WorkerInstanceID      string `json:"worker_instance_id"`
	InstanceID            string `json:"instance_id"`
	ControlSocket         string `json:"control_socket"`
	Listen                string `json:"listen"`
	DataEndpoint          string `json:"data_endpoint"`
	DataUsername          string `json:"data_username"`
	DataPasswordFile      string `json:"data_password_file"`
	Directory             string `json:"directory"`
	MaxBytes              int64  `json:"max_bytes"`
	MaxAgeSeconds         int    `json:"max_age_seconds"`
}
