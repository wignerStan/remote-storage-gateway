package grpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

func TestControlErrorsPreserveCategoriesAndHideProviderDetails(t *testing.T) {
	for _, category := range []error{broker.ErrInvalid, broker.ErrDenied, broker.ErrUnavailable, broker.ErrRejected, context.Canceled, context.DeadlineExceeded} {
		wire := wireError(fmt.Errorf("private-provider-detail: %w", category))
		if strings.Contains(wire.Error(), "private-provider-detail") {
			t.Fatal("provider diagnostic crossed control transport")
		}
		if err := controlError(wire); !errors.Is(err, category) {
			t.Fatalf("lost category %v: %v", category, err)
		}
	}
	if err := controlError(wireError(errors.New("private-provider-detail"))); !errors.Is(err, broker.ErrUnavailable) {
		t.Fatalf("unknown provider failure was exposed: %v", err)
	}
}
