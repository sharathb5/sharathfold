package fold_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/store/postgres"
)

func openPG(t *testing.T) *postgres.Store {
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

func TestOrderingHOLEnforcementPostgres(t *testing.T) {
	t.Run("hol_on_zero_inversions", func(t *testing.T) {
		inv, n := runOrderingRacePG(t, false)
		t.Logf("HOL on (postgres): deliveries=%d inversions=%d", n, inv)
		if inv != 0 {
			t.Fatalf("expected 0 inversions with HOL on, got %d", inv)
		}
	})
	t.Run("hol_off_nonzero_inversions", func(t *testing.T) {
		inv, n := runOrderingRacePG(t, true)
		t.Logf("HOL off (postgres): deliveries=%d inversions=%d", n, inv)
		if inv == 0 {
			t.Fatalf("expected nonzero inversions with HOL off (test is toothless), got 0 over %d deliveries", n)
		}
	})
}

func runOrderingRacePG(t *testing.T, disableHOL bool) (inversions, deliveries int) {
	t.Helper()

	const (
		subID  = "sub-race-pg"
		events = 20
	)

	pg := openPG(t)
	pg.DisableHOL = disableHOL

	hold := fold.NewHoldingTransport(subID)
	d, err := fold.New(fold.Config{
		Store:             pg,
		Transport:         hold,
		Workers:           2,
		Partitions:        32,
		OverlapPartitions: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}

	subs := []fold.Subscriber{{ID: subID, URL: "https://example.test/hook"}}
	payload0, _ := json.Marshal(map[string]int{"n": 0})
	if err := d.Dispatch(ctx, fold.Event{
		ID: "evt-0000", Type: "test.event", Payload: payload0,
	}, subs); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	select {
	case <-hold.Held():
	case <-time.After(5 * time.Second):
		hold.Release()
		_ = d.Close(context.Background())
		t.Fatal("timed out waiting for hold on first delivery")
	}

	for e := 1; e < events; e++ {
		payload, _ := json.Marshal(map[string]int{"n": e})
		if err := d.Dispatch(ctx, fold.Event{
			ID:      fmt.Sprintf("evt-%04d", e),
			Type:    "test.event",
			Payload: payload,
		}, subs); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
	}

	if disableHOL {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if len(hold.Calls()) >= 1 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if len(hold.Calls()) < 1 {
			hold.Release()
			_ = d.Close(context.Background())
			t.Fatal("HOL off but peer recorded nothing while seq 1 was held")
		}
	}

	hold.Release()

	closeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := d.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	calls := hold.Calls()
	if len(calls) != events {
		t.Fatalf("recorded %d deliveries, want %d", len(calls), events)
	}

	inv := 0
	for i := 1; i < len(calls); i++ {
		if calls[i].Sequence <= calls[i-1].Sequence {
			inv++
			t.Logf("inversion at i=%d: seq %d (%s) then %d (%s)",
				i, calls[i-1].Sequence, calls[i-1].EventID, calls[i].Sequence, calls[i].EventID)
		}
	}
	seen := map[int64]bool{}
	for _, c := range calls {
		if c.SubscriberID != subID {
			t.Fatalf("unexpected subscriber %s", c.SubscriberID)
		}
		seen[c.Sequence] = true
	}
	for seq := int64(1); seq <= int64(events); seq++ {
		if !seen[seq] {
			t.Fatalf("missing sequence %d", seq)
		}
	}
	return inv, len(calls)
}

func TestSuspendResumeOrderingPostgres(t *testing.T) {
	t.Run("gate_on_no_delivery_while_suspended_then_ordered_replay", func(t *testing.T) {
		inv, during, after := runSuspendResumePG(t, false)
		t.Logf("suspend gate on (postgres): during_suspend=%d after_resume=%d inversions=%d", during, after, inv)
		if during != 0 {
			t.Fatalf("expected 0 deliveries while suspended, got %d", during)
		}
		if inv != 0 {
			t.Fatalf("expected 0 inversions after resume, got %d", inv)
		}
		if after == 0 {
			t.Fatal("expected deliveries after resume")
		}
	})
	t.Run("gate_off_delivers_while_suspended", func(t *testing.T) {
		_, during, _ := runSuspendResumePG(t, true)
		t.Logf("suspend gate off (postgres): during_suspend=%d", during)
		if during == 0 {
			t.Fatal("expected deliveries while suspended with gate off (test is toothless)")
		}
	})
}

func runSuspendResumePG(t *testing.T, skipSuspendGate bool) (inversions, duringSuspend, afterResume int) {
	t.Helper()

	const subID = "sub-sr-pg"
	pg := openPG(t)
	pg.SkipSuspendGate = skipSuspendGate

	failOnce := &failFirstThenRecord{}
	d, err := fold.New(fold.Config{
		Store:       pg,
		Transport:   failOnce,
		Workers:     2,
		Partitions:  16,
		MaxAttempts: 1,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}

	subs := []fold.Subscriber{{ID: subID, URL: "https://example.test/hook"}}
	payload, _ := json.Marshal(map[string]int{"n": 0})
	if err := d.Dispatch(ctx, fold.Event{ID: "evt-0000", Type: "t", Payload: payload}, subs); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ok, _ := d.Suspended(ctx, subID)
		if ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	ok, _ := d.Suspended(ctx, subID)
	if !ok {
		_ = d.Close(context.Background())
		t.Fatal("subscriber never suspended")
	}

	failOnce.ResetRecording()
	const extra = 12
	for i := 1; i <= extra; i++ {
		p, _ := json.Marshal(map[string]int{"n": i})
		if err := d.Dispatch(ctx, fold.Event{
			ID:      fmt.Sprintf("evt-%04d", i),
			Type:    "t",
			Payload: p,
		}, subs); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(150 * time.Millisecond)
	duringSuspend = len(failOnce.Calls())

	if !skipSuspendGate {
		res, err := d.Resume(ctx, subID)
		if err != nil {
			t.Fatal(err)
		}
		if res.Gap {
			t.Fatalf("unexpected gap: %+v", res)
		}

		closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := d.Close(closeCtx); err != nil {
			t.Fatalf("Close: %v", err)
		}

		calls := failOnce.Calls()
		afterResume = len(calls)
		for i := 1; i < len(calls); i++ {
			if calls[i].Sequence <= calls[i-1].Sequence {
				inversions++
			}
		}
		return inversions, duringSuspend, afterResume
	}

	time.Sleep(100 * time.Millisecond)
	duringSuspend = len(failOnce.Calls())
	_ = d.Close(context.Background())
	return 0, duringSuspend, 0
}
