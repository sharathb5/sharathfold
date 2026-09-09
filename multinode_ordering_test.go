package fold_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
)

// TestMultiNodeHOLEnforcement answers D12's proof question: with two processes
// sharing Postgres, can the ordering test still fail with HOL disabled?
//
// Production Manifold assignment uses disjoint PartitionsClaim — a peer node
// cannot claim the same subscriber, so HOL-off does not produce cross-node
// peer inversions (exclusivity already prevents them). The tooth that proves
// the store gate still works is intentional overlap of PartitionsClaim, the
// multi-node analogue of OverlapPartitions.
func TestMultiNodeHOLEnforcement(t *testing.T) {
	t.Run("exclusive_assignment_hol_off_still_zero_peer_inversions", func(t *testing.T) {
		inv, n := runMultiNodeRace(t, false, true)
		t.Logf("exclusive+HOL-off: deliveries=%d inversions=%d", n, inv)
		if inv != 0 {
			t.Fatalf("exclusive claim should prevent peer inversions even with HOL off; got %d", inv)
		}
	})
	t.Run("overlap_hol_on_zero_inversions", func(t *testing.T) {
		inv, n := runMultiNodeRace(t, true, false)
		t.Logf("overlap+HOL-on: deliveries=%d inversions=%d", n, inv)
		if inv != 0 {
			t.Fatalf("expected 0 inversions with HOL on, got %d", inv)
		}
	})
	t.Run("overlap_hol_off_nonzero_inversions", func(t *testing.T) {
		inv, n := runMultiNodeRace(t, true, true)
		t.Logf("overlap+HOL-off: deliveries=%d inversions=%d", n, inv)
		if inv == 0 {
			t.Fatalf("expected nonzero inversions with overlapped claims and HOL off (gate toothless), got 0 over %d", n)
		}
	})
}

func runMultiNodeRace(t *testing.T, overlapClaims, disableHOL bool) (inversions, deliveries int) {
	t.Helper()

	const (
		subID      = "sub-multinode-race"
		events     = 20
		partitions = 32
		nodeCount  = 2
	)

	pg := openPG(t)
	pg.DisableHOL = disableHOL

	// Prefer a subscriber hashing onto node 0's exclusive slice (even partitions).
	chosen := ""
	part := -1
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("%s-%d", subID, i)
		p := fold.NodeIndex(id, partitions, partitions) // == hash.Partition
		if p%nodeCount == 0 {
			chosen = id
			part = p
			break
		}
	}
	if chosen == "" {
		t.Fatal("could not find subscriber hashing to even partition")
	}
	t.Logf("subscriber=%s partition=%d", chosen, part)

	hold := fold.NewHoldingTransport(chosen)

	var claim0, claim1 []int
	if !overlapClaims {
		claim0 = fold.PartitionsForNode(partitions, nodeCount, 0)
		claim1 = fold.PartitionsForNode(partitions, nodeCount, 1)
	}

	d0, err := fold.New(fold.Config{
		Store:           pg,
		Transport:       hold,
		Workers:         1,
		Partitions:      partitions,
		PartitionsClaim: claim0,
	})
	if err != nil {
		t.Fatal(err)
	}
	d1, err := fold.New(fold.Config{
		Store:           pg,
		Transport:       hold,
		Workers:         1,
		Partitions:      partitions,
		PartitionsClaim: claim1,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := d0.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d1.Start(ctx); err != nil {
		t.Fatal(err)
	}

	subs := []fold.Subscriber{{ID: chosen, URL: "https://example.test/hook"}}
	// Enqueue only on owning node (Manifold would route here). Both nodes' Claim
	// loops still run; overlap is what lets the peer race.
	payload0, _ := json.Marshal(map[string]int{"n": 0})
	if err := d0.Dispatch(ctx, fold.Event{ID: "evt-0000", Type: "t", Payload: payload0}, subs); err != nil {
		t.Fatal(err)
	}

	select {
	case <-hold.Held():
	case <-time.After(5 * time.Second):
		hold.Release()
		_ = d0.Close(context.Background())
		_ = d1.Close(context.Background())
		t.Fatal("timed out waiting for hold")
	}

	for e := 1; e < events; e++ {
		payload, _ := json.Marshal(map[string]int{"n": e})
		if err := d0.Dispatch(ctx, fold.Event{
			ID: fmt.Sprintf("evt-%04d", e), Type: "t", Payload: payload,
		}, subs); err != nil {
			t.Fatal(err)
		}
	}

	if disableHOL && overlapClaims {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if len(hold.Calls()) >= 1 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if len(hold.Calls()) < 1 {
			hold.Release()
			_ = d0.Close(context.Background())
			_ = d1.Close(context.Background())
			t.Fatal("HOL off + overlap but peer recorded nothing while seq 1 held")
		}
	}

	hold.Release()

	closeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := d0.Close(closeCtx); err != nil {
		t.Fatalf("d0.Close: %v", err)
	}
	if err := d1.Close(closeCtx); err != nil {
		t.Fatalf("d1.Close: %v", err)
	}

	calls := hold.Calls()
	if len(calls) != events {
		t.Fatalf("recorded %d deliveries, want %d", len(calls), events)
	}
	inv := 0
	for i := 1; i < len(calls); i++ {
		if calls[i].Sequence <= calls[i-1].Sequence {
			inv++
			t.Logf("inversion at i=%d: seq %d then %d", i, calls[i-1].Sequence, calls[i].Sequence)
		}
	}
	return inv, len(calls)
}
