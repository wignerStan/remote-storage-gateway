package grpc

import (
	"context"
	"encoding/json"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	cfg "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	pb "github.com/wignerStan/remote-storage-gateway/go/contracts/control/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net"
	"time"
)

func connect(c cfg.RemoteWorker) (*grpc.ClientConn, error) {
	if _, _, e := net.SplitHostPort(c.Address); e != nil {
		return nil, broker.ErrInvalid
	}
	t, e := tlsConfig(c.TLS, false)
	if e != nil {
		return nil, e
	}
	return grpc.NewClient("dns:///"+c.Address, grpc.WithTransportCredentials(credentials.NewTLS(t)), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(descriptor.MaxBytes+1024), grpc.MaxCallSendMsgSize(descriptor.MaxBytes+1024)))
}
func encode(v any) (*pb.Metadata, error) {
	b, e := json.Marshal(v)
	if e != nil || len(b) > descriptor.MaxBytes {
		return nil, broker.ErrInvalid
	}
	return &pb.Metadata{Json: b}, nil
}
func decode(b *pb.Metadata, e error, out any) error {
	if e != nil {
		return e
	}
	if b == nil || len(b.Json) == 0 || len(b.Json) > descriptor.MaxBytes || admission.StrictJSON(b.Json, out) != nil {
		return broker.ErrInvalid
	}
	return nil
}

type WorkerClient struct {
	conn   *grpc.ClientConn
	client pb.WorkerClient
}

func DialWorker(c cfg.RemoteWorker) (*WorkerClient, error) {
	conn, e := connect(c)
	if e != nil {
		return nil, e
	}
	return &WorkerClient{conn, pb.NewWorkerClient(conn)}, nil
}
func (c *WorkerClient) Close(context.Context) error { return c.conn.Close() }
func (c *WorkerClient) Info(ctx context.Context) (broker.InstanceInfo, error) {
	var out broker.InstanceInfo
	b, e := c.client.Info(ctx, &pb.Empty{})
	e = decode(b, e, &out)
	if e == nil && (out.Contract != broker.Contract || out.ID == "") {
		e = broker.ErrInvalid
	}
	return out, e
}
func (c *WorkerClient) Manifest(ctx context.Context) (dp.Manifest, error) {
	var out dp.Manifest
	b, e := c.client.Manifest(ctx, &pb.Empty{})
	e = decode(b, e, &out)
	if e == nil && (out.Schema != dp.CapabilitySchema || !descriptor.ValidDigest(out.BuildDigest)) {
		e = broker.ErrInvalid
	}
	return out, e
}
func (c *WorkerClient) Observed(ctx context.Context) ([]dp.Observed, error) {
	var out []dp.Observed
	b, e := c.client.Observed(ctx, &pb.Empty{})
	e = decode(b, e, &out)
	if e == nil && len(out) > 4096 {
		e = broker.ErrInvalid
	}
	return out, e
}
func (c *WorkerClient) Descriptor(ctx context.Context, digest string) (descriptor.Record, error) {
	if !descriptor.ValidDigest(digest) {
		return descriptor.Record{}, broker.ErrInvalid
	}
	b, e := c.client.Descriptor(ctx, &pb.DescriptorRequest{Digest: digest})
	if e != nil {
		return descriptor.Record{}, e
	}
	if b == nil || len(b.Json) > descriptor.MaxBytes {
		return descriptor.Record{}, broker.ErrInvalid
	}
	out, e := admission.Decode(b.Json)
	if e != nil || out.Digest != digest {
		return descriptor.Record{}, broker.ErrInvalid
	}
	return out, nil
}
func (c *WorkerClient) Replace(ctx context.Context, name string, expected dp.Version, selection dp.Selection) (dp.Version, error) {
	var out dp.Version
	if name == "" || name != selection.Name {
		return out, broker.ErrInvalid
	}
	req, e := encode(struct {
		Expected  dp.Version   `json:"expected"`
		Selection dp.Selection `json:"selection"`
	}{expected, selection})
	if e != nil {
		return out, e
	}
	b, e := c.client.Replace(ctx, &pb.ReplaceRequest{Name: name, Json: req.Json})
	e = decode(b, e, &out)
	if e == nil && (out.Incarnation != expected.Incarnation || out.Generation <= expected.Generation || out.DescriptorDigest != selection.DescriptorDigest || (out.ArtifactDigest != "" && !descriptor.ValidDigest(out.ArtifactDigest))) {
		e = broker.ErrInvalid
	}
	return out, e
}

type DecisionClient struct {
	conn   *grpc.ClientConn
	client pb.DecisionsClient
	names  map[string]bool
}

func DialDecision(c cfg.RemoteWorker, names []string) (*DecisionClient, error) {
	conn, e := connect(c)
	if e != nil {
		return nil, e
	}
	allowed := map[string]bool{}
	for _, n := range names {
		allowed[n] = true
	}
	return &DecisionClient{conn, pb.NewDecisionsClient(conn), allowed}, nil
}
func (c *DecisionClient) Close(context.Context) error { return c.conn.Close() }
func (c *DecisionClient) Decide(ctx context.Context, in broker.TransferIntent) (broker.Decision, error) {
	var out broker.Decision
	if ctx == nil || broker.ValidateIntent(in, c.names) != nil {
		return out, broker.ErrInvalid
	}
	req, e := encode(in)
	if e != nil {
		return out, e
	}
	started := time.Now()
	b, e := c.client.Decide(ctx, req)
	e = decode(b, e, &out)
	if e == nil {
		e = broker.ValidateDecision(in, out)
	}
	deadline, _ := ctx.Deadline()
	if e == nil && !time.Now().Before(out.ExpiresAt(started, deadline)) {
		e = context.DeadlineExceeded
	}
	return out, e
}
