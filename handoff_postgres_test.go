package fold_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/store"
)

// Graceful handoff: hold seq 1 on A, BeginHandoff → draining, release, CompleteHandoff,
// B finishes remaining sequences with 0 inversions.
//
// Tooth: drop only Claim's state='active' gate (keep owner_node match) and disable HOL.
// While draining, owner_node is still A, so A skip-ahead claims seq 2+ → nonzero inversions.
func TestGracefulHandoffPostgres(t *testing.T) {
	t.Run("graceful_handoff_zero_inversions", func(t *testing.T) {
		inv, n, finalOwner, finalGen := runGracefulHandoff(t, gracefulHandoffOpts{})
		t.Logf("graceful: deliveries=%d inversions=%d owner=%s gen=%d", n, inv, finalOwner, finalGen)
		if inv != 0 {
			t.Fatalf("expected 0 inversions with draining gate intact, got %d", inv)
		}
		if n != gracefulHandoffEvents {
			t.Fatalf("recorded %d deliveries, want %d", n, gracefulHandoffEvents)
		}
		if finalOwner != "B" {
			t.Fatalf("final owner_node=%q, want B", finalOwner)
		}
		if finalGen != 2 {
			t.Fatalf("final generation=%d, want 2 (seeded 1, bumped on Complete)", finalGen)
		}
	})
	t.Run("tooth_bypass_draining_gate_hol_off_nonzero_inversions", func(t *testing.T) {
		inv, n, _, _ := runGracefulHandoff(t, gracefulHandoffOpts{
			bypassDrainingGate: true,
			disableHOL:         true,
		})
		t.Logf("tooth: deliveries=%d inversions=%d", n, inv)
		if inv == 0 {
			t.Fatalf("expected nonzero inversions with draining gate bypassed + HOL off (toothless), got 0 over %d", n)
		}
	})
}

const gracefulHandoffEvents = 20

type gracefulHandoffOpts struct {
	bypassDrainingGate bool
	disableHOL         bool
}

func runGracefulHandoff(t *testing.T, opts gracefulHandoffOpts) (inversions, deliveries int, finalOwner string, finalGen uint64) {
	t.Helper()

	const (
		partitions = 32
		nodeA      = "A"
		nodeB      = "B"
	)

	pg := openPG(t)
	pg.DisableHOL = opts.disableHOL
	pg.BypassDrainingClaimGate = opts.bypassDrainingGate

	po, ok := any(pg).(store.PartitionOwnership)
	if !ok {
		t.Fatal("postgres.Store must implement store.PartitionOwnership")
	}

	ctx := context.Background()
	if err := po.EnsureOwners(ctx, partitions, nodeA); err != nil {
		t.Fatalf("EnsureOwners: %v", err)
	}

	// Any subscriber works: EnsureOwners seeds every partition under A.
	subID := "sub-handoff-graceful"
	part := fold.NodeIndex(subID, partitions, partitions)
	t.Logf("subscriber=%s partition=%d", subID, part)

	hold := fold.NewHoldingTransport(subID)

	// Both nodes list P as a claim hint; authoritative ownership is in Postgres.
	// A runs two overlapped workers so the tooth can show same-owner skip-ahead
	// while worker-0 holds seq 1 (one worker alone cannot Claim during Deliver).
	claim := []int{part}
	dA, err := fold.New(fold.Config{
		Store:             pg,
		Transport:         hold,
		Workers:           2,
		Partitions:        partitions,
		PartitionsClaim:   claim,
		OverlapPartitions: true,
		NodeID:            nodeA,
		IDPrefix:          "a-",
	})
	if err != nil {
		t.Fatal(err)
	}
	dB, err := fold.New(fold.Config{
		Store:           pg,
		Transport:       hold,
		Workers:         1,
		Partitions:      partitions,
		PartitionsClaim: claim,
		NodeID:          nodeB,
		IDPrefix:        "b-",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := dA.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := dB.Start(ctx); err != nil {
		t.Fatal(err)
	}

	subs := []fold.Subscriber{{ID: subID, URL: "https://example.test/hook"}}
	payload0, _ := json.Marshal(map[string]int{"n": 0})
	if err := dA.Dispatch(ctx, fold.Event{ID: "evt-0000", Type: "t", Payload: payload0}, subs); err != nil {
		t.Fatal(err)
	}

	select {
	case <-hold.Held():
	case <-time.After(5 * time.Second):
		hold.Release()
		_ = dA.Close(context.Background())
		_ = dB.Close(context.Background())
		t.Fatal("timed out waiting for hold on seq 1")
	}

	if err := po.BeginHandoff(ctx, part, nodeA, nodeB); err != nil {
		hold.Release()
		_ = dA.Close(context.Background())
		_ = dB.Close(context.Background())
		t.Fatalf("BeginHandoff: %v", err)
	}

	owner, state, next, gen, err := pg.OwnerRow(ctx, part)
	if err != nil {
		hold.Release()
		_ = dA.Close(context.Background())
		_ = dB.Close(context.Background())
		t.Fatalf("OwnerRow after Begin: %v", err)
	}
	if owner != nodeA || state != "draining" || next != nodeB || gen != 1 {
		hold.Release()
		_ = dA.Close(context.Background())
		_ = dB.Close(context.Background())
		t.Fatalf("after Begin: owner=%s state=%s next=%s gen=%d; want A/draining/B/1", owner, state, next, gen)
	}

	for e := 1; e < gracefulHandoffEvents; e++ {
		payload, _ := json.Marshal(map[string]int{"n": e})
		if err := dA.Dispatch(ctx, fold.Event{
			ID: fmt.Sprintf("evt-%04d", e), Type: "t", Payload: payload,
		}, subs); err != nil {
			hold.Release()
			_ = dA.Close(context.Background())
			_ = dB.Close(context.Background())
			t.Fatalf("Dispatch %d: %v", e, err)
		}
	}

	if !opts.bypassDrainingGate {
		// With the gate intact, neither node may record further deliveries while held.
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			if len(hold.Calls()) > 0 {
				hold.Release()
				_ = dA.Close(context.Background())
				_ = dB.Close(context.Background())
				t.Fatalf("recorded %d deliveries while draining+held; want 0", len(hold.Calls()))
			}
			time.Sleep(10 * time.Millisecond)
		}
	} else {
		// Tooth path: A must skip-ahead under HOL-off while draining.
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if len(hold.Calls()) >= 1 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if len(hold.Calls()) < 1 {
			hold.Release()
			_ = dA.Close(context.Background())
			_ = dB.Close(context.Background())
			t.Fatal("tooth: expected A to claim ahead while draining+held with gate bypassed")
		}
	}

	hold.Release()

	if opts.bypassDrainingGate {
		// Tooth only needs inversions while draining+held. Drain workers and
		// skip Complete — with the gate bypassed, Claim can race Complete.
		closeCtx, cancelClose := context.WithTimeout(ctx, 15*time.Second)
		defer cancelClose()
		_ = dA.Close(closeCtx)
		_ = dB.Close(closeCtx)
		calls := hold.Calls()
		inv := 0
		for i := 1; i < len(calls); i++ {
			if calls[i].Sequence <= calls[i-1].Sequence {
				inv++
				t.Logf("inversion at i=%d: seq %d then %d", i, calls[i-1].Sequence, calls[i].Sequence)
			}
		}
		owner, _, _, gen, err := pg.OwnerRow(ctx, part)
		if err != nil {
			t.Fatalf("OwnerRow: %v", err)
		}
		return inv, len(calls), owner, gen
	}

	// Wait for seq 1 in_flight to clear, then Complete.
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		n, err := pg.InFlightCount(waitCtx, part)
		if err != nil {
			_ = dA.Close(context.Background())
			_ = dB.Close(context.Background())
			t.Fatalf("InFlightCount: %v", err)
		}
		if n == 0 {
			break
		}
		select {
		case <-waitCtx.Done():
			_ = dA.Close(context.Background())
			_ = dB.Close(context.Background())
			t.Fatal("timed out waiting for in_flight=0 before CompleteHandoff")
		case <-time.After(5 * time.Millisecond):
		}
	}

	if err := po.CompleteHandoff(ctx, part); err != nil {
		_ = dA.Close(context.Background())
		_ = dB.Close(context.Background())
		t.Fatalf("CompleteHandoff: %v", err)
	}

	closeCtx, cancelClose := context.WithTimeout(ctx, 15*time.Second)
	defer cancelClose()
	if err := dA.Close(closeCtx); err != nil {
		t.Fatalf("dA.Close: %v", err)
	}
	if err := dB.Close(closeCtx); err != nil {
		t.Fatalf("dB.Close: %v", err)
	}

	finalOwner, state, next, finalGen, err = pg.OwnerRow(ctx, part)
	if err != nil {
		t.Fatalf("OwnerRow final: %v", err)
	}
	if state != "active" || next != "" {
		t.Fatalf("final state=%s next=%q; want active/empty", state, next)
	}

	calls := hold.Calls()
	inv := 0
	for i := 1; i < len(calls); i++ {
		if calls[i].Sequence <= calls[i-1].Sequence {
			inv++
			t.Logf("inversion at i=%d: seq %d then %d", i, calls[i-1].Sequence, calls[i].Sequence)
		}
	}
	return inv, len(calls), finalOwner, finalGen
}

// ForceTakeover after A abandons an in_flight claim; B reclaims under HOL with
// 0 durable inversions.
//
// Tooth: SkipInFlightReset → seq 1 stays in_flight → B never progresses.
func TestForcedTakeoverPostgres(t *testing.T) {
	t.Run("forced_takeover_durable_order", func(t *testing.T) {
		inv, n, stuck := runForcedTakeover(t, forcedTakeoverOpts{})
		t.Logf("forced: deliveries=%d inversions=%d stuck=%v", n, inv, stuck)
		if stuck {
			t.Fatal("expected B to progress after ForceTakeover reset")
		}
		if n != forcedTakeoverEvents {
			t.Fatalf("recorded %d deliveries, want %d", n, forcedTakeoverEvents)
		}
		if inv != 0 {
			t.Fatalf("expected 0 durable inversions, got %d", inv)
		}
	})
	t.Run("tooth_skip_inflight_reset_stuck", func(t *testing.T) {
		inv, n, stuck := runForcedTakeover(t, forcedTakeoverOpts{skipInFlightReset: true})
		t.Logf("tooth: deliveries=%d inversions=%d stuck=%v", n, inv, stuck)
		_ = inv
		if !stuck || n != 0 {
			t.Fatalf("expected stuck with 0 deliveries when reset skipped (toothless); stuck=%v n=%d", stuck, n)
		}
	})
}

const forcedTakeoverEvents = 12

type forcedTakeoverOpts struct {
	skipInFlightReset bool
}

func runForcedTakeover(t *testing.T, opts forcedTakeoverOpts) (inversions, deliveries int, stuck bool) {
	t.Helper()

	const (
		partitions = 32
		nodeA      = "A"
		nodeB      = "B"
	)

	pg := openPG(t)
	pg.SkipInFlightReset = opts.skipInFlightReset

	po, ok := any(pg).(store.PartitionOwnership)
	if !ok {
		t.Fatal("postgres.Store must implement store.PartitionOwnership")
	}

	ctx := context.Background()
	if err := po.EnsureOwners(ctx, partitions, nodeA); err != nil {
		t.Fatalf("EnsureOwners: %v", err)
	}

	subID := "sub-handoff-forced"
	part := fold.NodeIndex(subID, partitions, partitions)
	t.Logf("subscriber=%s partition=%d", subID, part)

	// Enqueue all sequences, then abandon seq 1 in_flight under A's token
	// (process unavailable — no Mark).
	rows := make([]store.Delivery, forcedTakeoverEvents)
	for i := 0; i < forcedTakeoverEvents; i++ {
		rows[i] = store.Delivery{
			ID:           fmt.Sprintf("force_dlg_%d", i+1),
			EventID:      fmt.Sprintf("evt-%04d", i),
			SubscriberID: subID,
			URL:          "https://example.test/hook",
			Partition:    part,
			Payload:      []byte(fmt.Sprintf(`{"n":%d}`, i)),
			EventType:    "t",
		}
	}
	if err := pg.Enqueue(ctx, rows); err != nil {
		t.Fatal(err)
	}

	abandonedOwner := "A/abandoned-inc/worker-0"
	claimed, err := pg.Claim(ctx, abandonedOwner, 1, []int{part}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].Sequence != 1 {
		t.Fatalf("claimed=%v want seq 1", claimed)
	}
	_, _, _, genBefore, err := pg.OwnerRow(ctx, part)
	if err != nil {
		t.Fatal(err)
	}
	if genBefore != 1 {
		t.Fatalf("generation before Force=%d, want 1", genBefore)
	}

	if err := po.ForceTakeover(ctx, part, nodeB, genBefore); err != nil {
		t.Fatalf("ForceTakeover: %v", err)
	}
	owner, state, next, genAfter, err := pg.OwnerRow(ctx, part)
	if err != nil {
		t.Fatal(err)
	}
	if owner != nodeB || state != "active" || next != "" || genAfter != genBefore+1 {
		t.Fatalf("after Force: owner=%s state=%s next=%q gen=%d", owner, state, next, genAfter)
	}

	rec := &fold.RecordingTransport{}
	dB, err := fold.New(fold.Config{
		Store:           pg,
		Transport:       rec,
		Workers:         1,
		Partitions:      partitions,
		PartitionsClaim: []int{part},
		NodeID:          nodeB,
		IDPrefix:        "b-",
		StaleClaimAge:   -1, // Force owns recovery; do not age-steal
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dB.Start(ctx); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if pg.PendingOrInFlight() == 0 {
			break
		}
		if opts.skipInFlightReset {
			time.Sleep(100 * time.Millisecond)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	closeTimeout := 10 * time.Second
	if opts.skipInFlightReset {
		closeTimeout = 200 * time.Millisecond
	}
	closeCtx, cancel := context.WithTimeout(ctx, closeTimeout)
	defer cancel()
	_ = dB.Close(closeCtx)

	calls := rec.Calls()
	inv := 0
	for i := 1; i < len(calls); i++ {
		if calls[i].Sequence <= calls[i-1].Sequence {
			inv++
			t.Logf("inversion at i=%d: seq %d then %d", i, calls[i-1].Sequence, calls[i].Sequence)
		}
	}
	inflight, err := pg.InFlightCount(ctx, part)
	if err != nil {
		t.Fatal(err)
	}
	stuck = opts.skipInFlightReset && (len(calls) == 0 && inflight >= 1)
	if opts.skipInFlightReset && inflight < 1 {
		// Tooth requires the abandoned row to still block HOL.
		stuck = false
	}
	return inv, len(calls), stuck
}

// After ForceTakeover, the old owner's MarkDelivered with the pre-Force stamp
// is rejected; B's Mark wins.
//
// Tooth: SkipMarkOwnerGenCheck → old Mark succeeds against B's in_flight row.
func TestStaleMarkAfterForcePostgres(t *testing.T) {
	t.Run("stale_mark_rejected", func(t *testing.T) {
		oldOK, bOK, status := runStaleMarkAfterForce(t, staleMarkOpts{})
		t.Logf("oldMarkOK=%v bMarkOK=%v final_status=%s", oldOK, bOK, status)
		if oldOK {
			t.Fatal("old owner's MarkDelivered succeeded after Force; want rejection")
		}
		if !bOK {
			t.Fatal("B's MarkDelivered failed; want success")
		}
		if status != string(store.StatusDelivered) {
			t.Fatalf("final status=%s, want delivered", status)
		}
	})
	t.Run("tooth_skip_mark_stamp_check_corrupts", func(t *testing.T) {
		oldOK, bOK, status := runStaleMarkAfterForce(t, staleMarkOpts{skipMarkStamp: true})
		t.Logf("tooth: oldMarkOK=%v bMarkOK=%v final_status=%s", oldOK, bOK, status)
		if !oldOK {
			t.Fatal("expected old Mark to succeed when stamp check disabled (toothless)")
		}
		if bOK {
			t.Fatal("expected B's Mark to fail after corrupt old Mark stole in_flight")
		}
		if status != string(store.StatusDelivered) {
			t.Fatalf("tooth final status=%s, want delivered (corrupt mark)", status)
		}
	})
}

type staleMarkOpts struct {
	skipMarkStamp bool
}

func runStaleMarkAfterForce(t *testing.T, opts staleMarkOpts) (oldMarkOK, bMarkOK bool, finalStatus string) {
	t.Helper()

	const (
		partitions = 32
		nodeA      = "A"
		nodeB      = "B"
	)

	pg := openPG(t)
	pg.SkipMarkOwnerGenCheck = opts.skipMarkStamp

	po, ok := any(pg).(store.PartitionOwnership)
	if !ok {
		t.Fatal("postgres.Store must implement store.PartitionOwnership")
	}

	ctx := context.Background()
	if err := po.EnsureOwners(ctx, partitions, nodeA); err != nil {
		t.Fatalf("EnsureOwners: %v", err)
	}

	subID := "sub-handoff-stale-mark"
	part := fold.NodeIndex(subID, partitions, partitions)
	t.Logf("subscriber=%s partition=%d", subID, part)

	hold := fold.NewHoldingTransport(subID)
	dA, err := fold.New(fold.Config{
		Store:           pg,
		Transport:       hold,
		Workers:         1,
		Partitions:      partitions,
		PartitionsClaim: []int{part},
		NodeID:          nodeA,
		IDPrefix:        "a-",
		StaleClaimAge:   -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dA.Start(ctx); err != nil {
		t.Fatal(err)
	}

	subs := []fold.Subscriber{{ID: subID, URL: "https://example.test/hook"}}
	payload, _ := json.Marshal(map[string]int{"n": 0})
	if err := dA.Dispatch(ctx, fold.Event{ID: "evt-0000", Type: "t", Payload: payload}, subs); err != nil {
		t.Fatal(err)
	}

	select {
	case <-hold.Held():
	case <-time.After(5 * time.Second):
		hold.Release()
		_ = dA.Close(context.Background())
		t.Fatal("timed out waiting for hold")
	}

	id, oldOwner, oldGen, err := pg.InFlightStamp(ctx, part)
	if err != nil {
		hold.Release()
		_ = dA.Close(context.Background())
		t.Fatalf("InFlightStamp: %v", err)
	}
	if oldOwner == "" || oldGen == 0 {
		hold.Release()
		_ = dA.Close(context.Background())
		t.Fatalf("empty stamp owner=%q gen=%d", oldOwner, oldGen)
	}
	_, _, _, genBefore, err := pg.OwnerRow(ctx, part)
	if err != nil {
		hold.Release()
		_ = dA.Close(context.Background())
		t.Fatal(err)
	}

	if err := po.ForceTakeover(ctx, part, nodeB, genBefore); err != nil {
		hold.Release()
		_ = dA.Close(context.Background())
		t.Fatalf("ForceTakeover: %v", err)
	}

	// B claims under a fresh incarnation token while A's HTTP is still held.
	bOwner := "B/new-incarnation/worker-0"
	claimed, err := pg.Claim(ctx, bOwner, 1, []int{part}, 1)
	if err != nil {
		hold.Release()
		_ = dA.Close(context.Background())
		t.Fatalf("B Claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != id {
		hold.Release()
		_ = dA.Close(context.Background())
		t.Fatalf("B claimed %+v, want id=%s", claimed, id)
	}
	bGen := claimed[0].Generation
	if bGen != genBefore+1 {
		hold.Release()
		_ = dA.Close(context.Background())
		t.Fatalf("B claim generation=%d, want %d", bGen, genBefore+1)
	}

	// Old process finishes POST and Marks with the pre-Force stamp.
	err = pg.MarkDelivered(ctx, id, oldOwner, oldGen)
	oldMarkOK = err == nil

	err = pg.MarkDelivered(ctx, id, bOwner, bGen)
	bMarkOK = err == nil

	hold.Release()
	closeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = dA.Close(closeCtx)

	finalStatus, err = pg.DeliveryStatus(ctx, id)
	if err != nil {
		t.Fatalf("DeliveryStatus: %v", err)
	}
	return oldMarkOK, bMarkOK, finalStatus
}
