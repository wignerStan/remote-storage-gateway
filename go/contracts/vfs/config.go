// Package vfs describes the independent, data-plane-only materializer process.
package vfs

// DefaultMaxConcurrentRequests bounds active DAV requests before authentication.
const DefaultMaxConcurrentRequests = 64

// MaxConcurrentRequests is the supported per-materializer admission ceiling.
const MaxConcurrentRequests = 1024

type ProcessConfig struct {
	MaxOpenFiles          int    `json:"max_open_files,omitempty" minimum:"0" maximum:"4096"`
	MaxCacheEntries       int    `json:"max_cache_entries,omitempty" minimum:"0" maximum:"1000000"`
	MaxDirectoryEntries   int    `json:"max_directory_entries,omitempty" minimum:"0" maximum:"10000"`
	MaxConcurrentRequests int    `json:"max_concurrent_requests,omitempty" minimum:"0" maximum:"1024"`
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
