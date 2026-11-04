//go:build dataplane_l7 && linux && cgo

package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	transport "github.com/wignerStan/network-data-plane-worker/go/clienttransport/ttrpc"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/storageipc"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/network-data-plane-worker/go/nativeclient"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	config "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
	"golang.org/x/net/webdav"
)

func TestStorageIPCWorkerNativeGenerationAndBrokerScope(t *testing.T) {
	artifactPath := os.Getenv("REMOTE_GATEWAY_RCLONE_PLUGIN")
	if artifactPath == "" {
		t.Skip("actual client-rclone.so required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	data, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	artifact := dp.Artifact{Path: artifactPath, SHA256: hex.EncodeToString(digest[:]), PluginID: "client.rclone", Language: "go"}
	record, err := nativeclient.Inspect(ctx, artifact.Path, artifact.SHA256, artifact.PluginID, artifact.Language)
	if err != nil {
		t.Fatal(err)
	}
	artifact.DescriptorDigest = record.Digest
	dir, err := os.MkdirTemp("/tmp", "trade-reapi-vfs-runtime-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	originDir := filepath.Join(dir, "origin")
	if err = os.Mkdir(originDir, 0700); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("actual-native-shared-memory\x00"), 16000)
	if err = os.WriteFile(filepath.Join(originDir, "object"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	origin := httptest.NewServer(&webdav.Handler{FileSystem: webdav.Dir(originDir), LockSystem: webdav.NewMemLS()})
	defer origin.Close()
	auth := filepath.Join(dir, "auth")
	if err = os.WriteFile(auth, []byte(password), 0600); err != nil {
		t.Fatal(err)
	}
	var decisions atomic.Int64
	service := brokerFunc(func(_ context.Context, intent broker.TransferIntent) (broker.Decision, error) {
		if intent.Principal != "ipc-worker" || intent.Method != storageipc.AuthorizeMethod || intent.Authority != storageipc.Authority || intent.SourceID != "docs" || intent.TargetID != "raw" || len(intent.Metadata) != 0 {
			return broker.Decision{}, broker.ErrDenied
		}
		decisions.Add(1)
		return broker.Decision{RouteID: "raw-docs", WorkerDataEndpoint: "http://127.0.0.1:1", ApprovedUpstreamID: "raw", TTLSeconds: 30, MaxMessageBytes: 65536, MaxInflightBytes: 8 * 65536, Methods: []string{storageipc.AuthorizeMethod}}, nil
	})
	brokerServer, err := control.NewDecisionServer(service, control.DecisionOptions{Principal: func(context.Context) (string, error) { return "ipc-worker", nil }})
	if err != nil {
		t.Fatal(err)
	}
	brokerPath := filepath.Join(dir, "broker")
	listener, err := net.Listen("unix", brokerPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(brokerPath, 0600); err != nil {
		t.Fatal(err)
	}
	go brokerServer.Serve(listener)
	defer brokerServer.Close(context.Background())
	public, _ := json.Marshal(map[string]string{"url": origin.URL})
	selected := dp.Selection{Name: "raw", PluginID: artifact.PluginID, DescriptorDigest: record.Digest, ImplementationID: "webdav", Public: public}
	cfg := config.ProcessConfig{InstanceID: "ipc-worker", BrokerSocket: brokerPath, ControlSocket: filepath.Join(dir, "control"), RuntimeConfig: config.RuntimeConfig{StorageIPCSocket: filepath.Join(dir, "storage"), StorageOfferSocket: filepath.Join(dir, "offer"), Listen: "127.0.0.1:0", Username: "operator", AuthSecret: "ingress", Worker: dp.Bootstrap{Schema: dp.BootstrapSchema, Artifacts: []dp.Artifact{artifact}, Clients: []dp.Selection{selected}, SecretFiles: map[string]string{"ingress": auth}}, Mounts: []config.Mount{{Name: "docs", Client: "raw", RemoteClient: "raw"}}}}
	runtime, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	if err = runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := runtime.Info(ctx)
	if err != nil || !info.Ready || info.StorageSocket != cfg.StorageIPCSocket || info.OfferSocket != cfg.StorageOfferSocket || info.Incarnation == "" {
		t.Fatalf("IPC discovery %+v %v", info, err)
	}
	client, err := transport.Dial(ctx, transport.Config{Endpoint: storageipc.Endpoint{Socket: info.StorageSocket, OfferSocket: info.OfferSocket, Namespace: "docs", ExpectedWorkerID: info.ID, ExpectedIncarnation: info.Incarnation}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	reader, _, err := client.Open(ctx, "/object", 7, 200123)
	if err != nil {
		t.Fatal(err)
	}
	old := runtime.Manager.Current("raw")
	version, err := runtime.Replace(ctx, "raw", old, selected)
	if err != nil || version == old {
		t.Fatal("generation replacement", err)
	}
	if err = client.Check(ctx); !errors.Is(err, storageipc.ErrStale) {
		t.Fatal("cache-hit generation fence", err)
	}
	actual, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(actual, payload[7:200130]) {
		t.Fatalf("pinned native read %d %v", len(actual), err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	if decisions.Load() != 1 {
		t.Fatalf("CP called per payload buffer, calls=%d", decisions.Load())
	}
	after, err := runtime.Info(ctx)
	if err != nil || after.Incarnation != info.Incarnation {
		t.Fatal("session confused process incarnation with client generation")
	}
}
