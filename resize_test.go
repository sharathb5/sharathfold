package fold

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold/internal/hash"
	"github.com/sharathb5/sharathfold/store"
	"github.com/sharathb5/sharathfold/store/memory"
	"github.com/sharathb5/sharathfold/store/postgres"
)

func TestResizeHandoffHOLEnforcement(t *testing.T) {
	t.Run("grow_2_to_3_hol_on_zero_inversions", func(t *testing.T) {
		inv, n := runResizeHandoff(t, resizeHandoffOpts{from: 2, to: 3, disableHOL: false})
		t.Logf("resize 2→3 HOL on (memory): deliveries=%d inversions=%d", n, inv)
		if inv != 0 {
			t.Fatalf("expected 0 inversions with HOL on, got %d", inv)
		}
	})
	t.Run("grow_2_to_3_hol_off_nonzero_inversions", func(t *testing.T) {
		inv, n := runResizeHandoff(t, resizeHandoffOpts{from: 2, to: 3, disableHOL: true})
		t.Logf("resize 2→3 HOL off (memory): deliveries=%d inversions=%d", n, inv)
		if inv == 0 {
			t.Fatalf("expected nonzero inversions with HOL off (test is toothless), got 0 over %d deliveries", n)
		}
	})
	t.Run("shrink_3_to_2_hol_on_zero_inversions", func(t *testing.T) {
		inv, n := runResizeHandoff(t, resizeHandoffOpts{from: 3, to: 2, disableHOL: false})
		t.Logf("resize 3→2 HOL on (memory): deliveries=%d inversions=%d", n, inv)
		if inv != 0 {
			t.Fatalf("expected 0 inversions with HOL on, got %d", inv)
		}
	})
	t.Run("shrink_3_to_2_hol_off_nonzero_inversions", func(t *testing.T) {
		inv, n := runResizeHandoff(t, resizeHandoffOpts{from: 3, to: 2, disableHOL: true})
		t.Logf("resize 3→2 HOL off (memory): deliveries=%d inversions=%d", n, inv)
		if inv == 0 {
			t.Fatalf("expected nonzero inversions with HOL off (test is toothless), got 0 over %d deliveries", n)
		}
	})
}

type resizeHandoffOpts struct {
	from, to   int
	disableHOL bool
	store      store.Store // nil → memory
}

// runResizeHandoff holds an old owner's first delivery open, resizes so that
// subscriber's partition moves to a new owner, and lets the new owner claim
// concurrently. HOL on → no inversions; HOL off → inversions (proves the race).
func runResizeHandoff(t *testing.T, opts resizeHandoffOpts) (inversions, deliveries int) {
	t.Helper()

	const (
		partitions = 32
		events     = 20
	)

	subID, part, oldOwner, newOwner := findMovingSubscriber(opts.from, opts.to, partitions)
	t.Logf("subscriber=%s partition=%d %s → %s (workers %d→%d)",
		subID, part, oldOwner, newOwner, opts.from, opts.to)

	st := opts.store
	if st == nil {
		mem := memory.New()
		mem.DisableHOL = opts.disableHOL
		st = mem
	} else {
		switch s := st.(type) {
		case *memory.Store:
			s.DisableHOL = opts.disableHOL
		case *postgres.Store:
			s.DisableHOL = opts.disableHOL
		default:
			t.Fatalf("unsupported store type %T", st)
		}
	}

	hold := NewHoldingTransport(subID)
	d, err := New(Config{
		Store:      st,
		Transport:  hold,
		Workers:    opts.from,
		Partitions: partitions,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}

	subs := []Subscriber{{ID: subID, URL: "https://example.test/hook"}}
	// Enqueue seq 1 first and wait for the old owner to hold it before enqueueing
	// the rest. Otherwise DisableHOL lets one Claim swallow every row and the
	// handoff race never appears.
	payload0, _ := json.Marshal(map[string]int{"n": 0})
	if err := d.Dispatch(ctx, Event{
		ID: "evt-0000", Type: "test.event", Payload: payload0,
	}, subs); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	select {
	case <-hold.Held():
	case <-time.After(5 * time.Second):
		hold.Release()
		_ = d.Close(context.Background())
		t.Fatal("timed out waiting for hold on first delivery (old owner)")
	}

	for e := 1; e < events; e++ {
		payload, _ := json.Marshal(map[string]int{"n": e})
		if err := d.Dispatch(ctx, Event{
			ID:      fmt.Sprintf("evt-%04d", e),
			Type:    "test.event",
			Payload: payload,
		}, subs); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
	}

	if err := d.Resize(ctx, opts.to); err != nil {
		hold.Release()
		_ = d.Close(context.Background())
		t.Fatalf("Resize: %v", err)
	}

	if opts.disableHOL {
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
			t.Fatal("HOL off but new owner recorded nothing while old owner held seq 1 — race not reached")
		}
	} else {
		// With HOL on, new owner must not record while seq 1 is in flight.
		time.Sleep(150 * time.Millisecond)
		if n := len(hold.Calls()); n != 0 {
			hold.Release()
			_ = d.Close(context.Background())
			t.Fatalf("HOL on but %d deliveries recorded while old owner held seq 1", n)
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

func findMovingSubscriber(fromWorkers, toWorkers, partitions int) (subID string, part int, oldOwner, newOwner string) {
	for i := 0; i < 100000; i++ {
		id := fmt.Sprintf("resize-sub-%d", i)
		p := hash.Partition(id, partitions)
		old := p % fromWorkers
		neu := p % toWorkers
		if old != neu {
			return id, p, ownerID(old), ownerID(neu)
		}
	}
	panic("no subscriber remapped between worker counts")
}
