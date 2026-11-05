package grpc

import (
	"context"
	"errors"

	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Only finite control categories cross the transport boundary. In particular,
// provider errors and credential-bearing diagnostic strings remain in Worker.
func wireError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "control operation canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "control deadline exceeded")
	case errors.Is(err, broker.ErrInvalid):
		return status.Error(codes.InvalidArgument, "invalid control metadata")
	case errors.Is(err, broker.ErrDenied):
		return status.Error(codes.PermissionDenied, "control operation denied")
	case errors.Is(err, broker.ErrRejected):
		return status.Error(codes.FailedPrecondition, "desired worker state rejected")
	case errors.Is(err, broker.ErrUnavailable):
		return status.Error(codes.Unavailable, "worker control unavailable")
	default:
		return status.Error(codes.Internal, "control operation failed")
	}
}

func controlError(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.Canceled:
		return context.Canceled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	case codes.InvalidArgument, codes.NotFound:
		return broker.ErrInvalid
	case codes.PermissionDenied, codes.Unauthenticated:
		return broker.ErrDenied
	case codes.FailedPrecondition, codes.Aborted:
		return broker.ErrRejected
	default:
		return broker.ErrUnavailable
	}
}
