package manager

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStartRejectsCancelledContextBeforeRuntimeChanges(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "cancelled"
		if expired {
			name = "expired"
		}
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "runtime")
			m, err := New(Config{RuntimeDirectory: dir})
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close(context.Background())
			ctx, cancel := context.WithCancel(context.Background())
			want := context.Canceled
			if expired {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Unix(1, 0))
				want = context.DeadlineExceeded
			}
			cancel()
			if err = m.Start(ctx); !errors.Is(err, want) {
				t.Fatalf("Start() = %v, want %v", err, want)
			}
			if m.Ready() {
				t.Fatal("cancelled startup reported ready")
			}
			if _, err = os.Stat(dir); !os.IsNotExist(err) {
				t.Fatalf("cancelled startup changed runtime directory: %v", err)
			}
			if err = m.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
