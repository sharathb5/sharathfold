package fold

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/sharathb5/sharathfold/store/postgres"
)

func TestResizeHandoffHOLEnforcementPostgres(t *testing.T) {
	t.Run("grow_2_to_3_hol_on_zero_inversions", func(t *testing.T) {
		inv, n := runResizeHandoff(t, resizeHandoffOpts{
			from: 2, to: 3, disableHOL: false, store: openResizePG(t),
		})
		t.Logf("resize 2→3 HOL on (postgres): deliveries=%d inversions=%d", n, inv)
		if inv != 0 {
			t.Fatalf("expected 0 inversions with HOL on, got %d", inv)
		}
	})
	t.Run("grow_2_to_3_hol_off_nonzero_inversions", func(t *testing.T) {
		inv, n := runResizeHandoff(t, resizeHandoffOpts{
			from: 2, to: 3, disableHOL: true, store: openResizePG(t),
		})
		t.Logf("resize 2→3 HOL off (postgres): deliveries=%d inversions=%d", n, inv)
		if inv == 0 {
			t.Fatalf("expected nonzero inversions with HOL off (test is toothless), got 0 over %d deliveries", n)
		}
	})
	t.Run("shrink_3_to_2_hol_on_zero_inversions", func(t *testing.T) {
		inv, n := runResizeHandoff(t, resizeHandoffOpts{
			from: 3, to: 2, disableHOL: false, store: openResizePG(t),
		})
		t.Logf("resize 3→2 HOL on (postgres): deliveries=%d inversions=%d", n, inv)
		if inv != 0 {
			t.Fatalf("expected 0 inversions with HOL on, got %d", inv)
		}
	})
	t.Run("shrink_3_to_2_hol_off_nonzero_inversions", func(t *testing.T) {
		inv, n := runResizeHandoff(t, resizeHandoffOpts{
			from: 3, to: 2, disableHOL: true, store: openResizePG(t),
		})
		t.Logf("resize 3→2 HOL off (postgres): deliveries=%d inversions=%d", n, inv)
		if inv == 0 {
			t.Fatalf("expected nonzero inversions with HOL off (test is toothless), got 0 over %d deliveries", n)
		}
	})
}

func openResizePG(t *testing.T) *postgres.Store {
	t.Helper()
	dsn := os.Getenv("FOLD_PG_TEST_DSN")
	inCI := os.Getenv("CI") != ""
	if dsn == "" {
		if inCI {
			t.Fatal("FOLD_PG_TEST_DSN is required in CI — Postgres tests must not be skipped")
		}
		dsn = "postgres://localhost/fold_test?sslmode=disable"
	}
	ctx := context.Background()
	s, err := postgres.Open(ctx, dsn)
	if err != nil {
		msg := fmt.Sprintf("postgres unavailable: %v", err)
		if inCI {
			t.Fatal(msg)
		}
		if os.Getenv("FOLD_PG_ALLOW_SKIP") == "1" {
			t.Skip(msg)
		}
		t.Fatalf("%s\n(set FOLD_PG_ALLOW_SKIP=1 to skip instead of fail)", msg)
	}
	t.Cleanup(func() { s.Close() })
	release, err := s.AcquireTestDBLock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	if err := s.TruncateForTest(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}
