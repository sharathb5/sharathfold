package fold

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold/store"
	"github.com/sharathb5/sharathfold/store/memory"
	"github.com/sharathb5/sharathfold/store/postgres"
)

func TestCrashRecoverOrdering(t *testing.T) {
	t.Run("recover_on_zero_inversions", func(t *testing.T) {
		inv, n, pending := runCrashRecover(t, crashRecoverOpts{recover: true})
		t.Logf("crash recover on (memory): deliveries=%d inversions=%d stranded_pending_or_inflight=%d", n, inv, pending)
		if n == 0 {
			t.Fatal("expected deliveries after recovery")
		}
		if inv != 0 {
			t.Fatalf("expected 0 inversions after recovery, got %d", inv)
		}
		if pending != 0 {
			t.Fatalf("expected queue drained, still have %d pending/in_flight", pending)
		}
	})
	t.Run("recover_off_stranded", func(t *testing.T) {
		inv, n, pending := runCrashRecover(t, crashRecoverOpts{recover: false})
		t.Logf("crash recover off (memory): deliveries=%d inversions=%d stranded_pending_or_inflight=%d", n, inv, pending)
		if n != 0 {
			t.Fatalf("expected 0 deliveries with recovery off (seq 1 stranded blocks HOL), got %d", n)
		}
		if pending == 0 {
			t.Fatal("expected stranded work still pending/in_flight (test is toothless)")
		}
		_ = inv
	})
}

func TestCrashRecoverOrderingPostgres(t *testing.T) {
	t.Run("recover_on_zero_inversions", func(t *testing.T) {
		inv, n, pending := runCrashRecover(t, crashRecoverOpts{recover: true, store: openCrashPG(t)})
		t.Logf("crash recover on (postgres): deliveries=%d inversions=%d stranded_pending_or_inflight=%d", n, inv, pending)
		if n == 0 {
			t.Fatal("expected deliveries after recovery")
		}
		if inv != 0 {
			t.Fatalf("expected 0 inversions after recovery, got %d", inv)
		}
		if pending != 0 {
			t.Fatalf("expected queue drained, still have %d pending/in_flight", pending)
		}
	})
	t.Run("recover_off_stranded", func(t *testing.T) {
		inv, n, pending := runCrashRecover(t, crashRecoverOpts{recover: false, store: openCrashPG(t)})
		t.Logf("crash recover off (postgres): deliveries=%d inversions=%d stranded_pending_or_inflight=%d", n, inv, pending)
		if n != 0 {
			t.Fatalf("expected 0 deliveries with recovery off (seq 1 stranded blocks HOL), got %d", n)
		}
		if pending == 0 {
			t.Fatal("expected stranded work still pending/in_flight (test is toothless)")
		}
		_ = inv
	})
}

type crashRecoverOpts struct {
	recover bool
	store   store.Store // nil → memory
}

type pender interface{ PendingOrInFlight() int }

type claimBackdater interface {
	BackdateClaimForTest(id string, claimedAt time.Time) error
}

type claimBackdaterCtx interface {
	BackdateClaimForTest(ctx context.Context, id string, claimedAt time.Time) error
}

func runCrashRecover(t *testing.T, opts crashRecoverOpts) (inversions, deliveries, stillQueued int) {
	t.Helper()

	const (
		subID     = "crash-sub"
		partition = 7
		events    = 12
		deadOwner = "dead-worker"
		deadGen   = uint64(1)
	)

	st := opts.store
	if st == nil {
		st = memory.New()
	}
	ctx := context.Background()

	rows := make([]store.Delivery, events)
	for i := 0; i < events; i++ {
		rows[i] = store.Delivery{
			ID:           fmt.Sprintf("crash_dlg_%d", i+1),
			EventID:      fmt.Sprintf("evt-%04d", i),
			SubscriberID: subID,
			URL:          "https://example.test/hook",
			Partition:    partition,
			Payload:      []byte(fmt.Sprintf(`{"n":%d}`, i)),
			EventType:    "test.event",
		}
	}
	if err := st.Enqueue(ctx, rows); err != nil {
		t.Fatal(err)
	}

	claimed, err := st.Claim(ctx, deadOwner, deadGen, []int{partition}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].Sequence != 1 {
		t.Fatalf("claimed=%v want seq 1", claimed)
	}
	abandonedAt := time.Now().Add(-time.Hour)
	switch b := st.(type) {
	case claimBackdater:
		if err := b.BackdateClaimForTest(claimed[0].ID, abandonedAt); err != nil {
			t.Fatal(err)
		}
	case claimBackdaterCtx:
		if err := b.BackdateClaimForTest(ctx, claimed[0].ID, abandonedAt); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("store %T cannot backdate claims for test", st)
	}

	rec := &RecordingTransport{}
	staleAge := time.Minute // anything under the 1h backdate
	if !opts.recover {
		staleAge = -1 // disable Start recovery
	}
	d, err := New(Config{
		Store:         st,
		Transport:     rec,
		Workers:       2,
		Partitions:    32,
		StaleClaimAge: staleAge,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p, ok := st.(pender); ok && p.PendingOrInFlight() == 0 {
			break
		}
		if !opts.recover {
			time.Sleep(100 * time.Millisecond)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	closeTimeout := 5 * time.Second
	if !opts.recover {
		closeTimeout = 200 * time.Millisecond // queue will not drain
	}
	closeCtx, cancel := context.WithTimeout(ctx, closeTimeout)
	defer cancel()
	_ = d.Close(closeCtx)

	calls := rec.Calls()
	for i := 1; i < len(calls); i++ {
		if calls[i].Sequence <= calls[i-1].Sequence {
			inversions++
		}
	}
	stillQueued = 0
	if p, ok := st.(pender); ok {
		stillQueued = p.PendingOrInFlight()
	}
	return inversions, len(calls), stillQueued
}

func openCrashPG(t *testing.T) *postgres.Store {
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
