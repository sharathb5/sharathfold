package fold_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/store/memory"
)

func TestOrderingHOLEnforcement(t *testing.T) {
	t.Run("hol_on_zero_inversions", func(t *testing.T) {
		inv, n := runOrderingRace(t, false)
		t.Logf("HOL on: deliveries=%d inversions=%d", n, inv)
		if inv != 0 {
			t.Fatalf("expected 0 inversions with HOL on, got %d", inv)
		}
	})
	t.Run("hol_off_nonzero_inversions", func(t *testing.T) {
		inv, n := runOrderingRace(t, true)
		t.Logf("HOL off: deliveries=%d inversions=%d", n, inv)
		if inv == 0 {
			t.Fatalf("expected nonzero inversions with HOL off (test is toothless), got 0 over %d deliveries", n)
		}
	})
}

// runOrderingRace starts two overlapping workers and a hold-open transport so a
// peer can claim and record a later sequence while seq 1 is held before record.
func runOrderingRace(t *testing.T, disableHOL bool) (inversions, deliveries int) {
	t.Helper()

	const (
		subID  = "sub-race"
		events = 20
	)

	mem := memory.New()
	mem.DisableHOL = disableHOL

	hold := fold.NewHoldingTransport(subID)
	d, err := fold.New(fold.Config{
		Store:             mem,
		Transport:         hold,
		Workers:           2,
		Partitions:        32,
		OverlapPartitions: true, // exclusive ownership cannot produce a peer race
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}

	subs := []fold.Subscriber{{ID: subID, URL: "https://example.test/hook"}}
	// Enqueue seq 1, wait for hold, then enqueue the rest so a peer can claim
	// later sequences while seq 1 is still in flight (HOL-off tooth).
	payload0, _ := json.Marshal(map[string]int{"n": 0})
	if err := d.Dispatch(ctx, fold.Event{
		ID: "evt-0000", Type: "test.event", Payload: payload0,
	}, subs); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	select {
	case <-hold.Held():
	case <-time.After(3 * time.Second):
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
		// Peer should record at least one later sequence while seq 1 is held.
		deadline := time.Now().Add(2 * time.Second)
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

	closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
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
