package fold_test

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/store"
	"github.com/sharathb5/sharathfold/store/postgres"
)

func pgDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("FOLD_PG_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost/fold_test?sslmode=disable"
	}
	return dsn
}

// Distributed-ownership authority and fail-closed Claim regressions.

func TestLegacyClaimRejectedWhenDistributedOwnership(t *testing.T) {
	pg := openPG(t)
	ctx := context.Background()
	po := store.PartitionOwnership(pg)
	if err := po.EnsureOwners(ctx, 8, "A"); err != nil {
		t.Fatal(err)
	}
	if !pg.HasDistributedOwnership() {
		t.Fatal("EnsureOwners must enable DistributedOwnership explicitly")
	}

	if err := pg.Enqueue(ctx, []store.Delivery{{
		ID: "leg_1", EventID: "e", SubscriberID: "sub-legacy-bypass",
		URL: "https://example.test/h", Partition: 0, Payload: []byte(`{}`),
	}}); err != nil {
		t.Fatal(err)
	}

	// Legacy worker-N token must not fall back past fold_partition_owners.
	_, err := pg.Claim(ctx, "worker-0", 1, []int{0}, 1)
	if err == nil {
		t.Fatal("legacy Claim succeeded under DistributedOwnership; authority bypass")
	}
}

func TestEmptyNodeIDRejectedWhenDistributedOwnership(t *testing.T) {
	pg := openPG(t)
	ctx := context.Background()
	if err := store.PartitionOwnership(pg).EnsureOwners(ctx, 8, "A"); err != nil {
		t.Fatal(err)
	}
	_, err := fold.New(fold.Config{
		Store:      pg,
		Workers:    1,
		Partitions: 8,
	})
	if err == nil {
		t.Fatal("New without NodeID succeeded under DistributedOwnership")
	}
}

func TestSlashContainingNodeIDRejected(t *testing.T) {
	pg := openPG(t)
	ctx := context.Background()
	if err := store.PartitionOwnership(pg).EnsureOwners(ctx, 8, "A"); err != nil {
		t.Fatal(err)
	}
	_, err := fold.New(fold.Config{
		Store:      pg,
		Workers:    1,
		Partitions: 8,
		NodeID:     "us-east/go0",
	})
	if err == nil {
		t.Fatal("New accepted NodeID containing '/'")
	}
	if !strings.Contains(err.Error(), "/") {
		t.Fatalf("error should mention '/': %v", err)
	}
}

func TestEnsureOwnersRejectsConflictingBootstrap(t *testing.T) {
	pg := openPG(t)
	ctx := context.Background()
	po := store.PartitionOwnership(pg)
	if err := po.EnsureOwners(ctx, 16, "A"); err != nil {
		t.Fatal(err)
	}
	err := po.EnsureOwners(ctx, 16, "B")
	if err == nil {
		t.Fatal("EnsureOwners(B) succeeded after EnsureOwners(A); want conflict")
	}

	// Concurrent conflicting seeds on a clean table must not produce a mixed ring.
	// Reuse the same store (openPG's advisory lock is per-test, not re-entrant).
	if err := pg.TruncateForTest(ctx); err != nil {
		t.Fatal(err)
	}
	var (
		wg   sync.WaitGroup
		errA error
		errB error
		mu   sync.Mutex
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		e := po.EnsureOwners(ctx, 32, "nodeA")
		mu.Lock()
		errA = e
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		e := po.EnsureOwners(ctx, 32, "nodeB")
		mu.Lock()
		errB = e
		mu.Unlock()
	}()
	wg.Wait()

	if errA == nil && errB == nil {
		t.Fatal("both concurrent EnsureOwners succeeded; want one conflict")
	}
	if errA != nil && errB != nil {
		t.Fatalf("both failed (%v / %v); want exactly one winner", errA, errB)
	}

	owners := map[string]int{}
	for p := 0; p < 32; p++ {
		owner, _, _, _, err := pg.OwnerRow(ctx, p)
		if err != nil {
			t.Fatalf("OwnerRow %d: %v", p, err)
		}
		owners[owner]++
	}
	if len(owners) != 1 {
		t.Fatalf("mixed ownership after concurrent EnsureOwners: %v", owners)
	}
}

func TestForceTakeoverStaleAndIdempotent(t *testing.T) {
	pg := openPG(t)
	ctx := context.Background()
	po := store.PartitionOwnership(pg)
	if err := po.EnsureOwners(ctx, 8, "A"); err != nil {
		t.Fatal(err)
	}

	// Force to self at expected gen must not bump or reset.
	if err := pg.Enqueue(ctx, []store.Delivery{{
		ID: "slf_1", EventID: "e", SubscriberID: "sub-force-self",
		URL: "https://example.test/h", Partition: 1, Payload: []byte(`{}`),
	}}); err != nil {
		t.Fatal(err)
	}
	claimed, err := pg.Claim(ctx, "A/inc-self/worker-0", 1, []int{1}, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim before self-force: %v %+v", err, claimed)
	}
	if err := po.ForceTakeover(ctx, 1, "A", 1); err != nil {
		t.Fatalf("Force to current owner: %v", err)
	}
	owner, state, _, gen, err := pg.OwnerRow(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if owner != "A" || state != "active" || gen != 1 {
		t.Fatalf("self-force mutated ownership: owner=%s state=%s gen=%d", owner, state, gen)
	}
	st, err := pg.DeliveryStatus(ctx, "slf_1")
	if err != nil {
		t.Fatal(err)
	}
	if st != string(store.StatusInFlight) {
		t.Fatalf("self-force reset in_flight; status=%s", st)
	}
	if err := pg.MarkDelivered(ctx, "slf_1", "A/inc-self/worker-0", 1); err != nil {
		t.Fatalf("mark after self-force: %v", err)
	}

	// Real takeover A→B.
	if err := pg.Enqueue(ctx, []store.Delivery{{
		ID: "stale_1", EventID: "e2", SubscriberID: "sub-force-stale",
		URL: "https://example.test/h", Partition: 2, Payload: []byte(`{}`),
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.Claim(ctx, "A/inc-a/worker-0", 1, []int{2}, 1); err != nil {
		t.Fatal(err)
	}
	if err := po.ForceTakeover(ctx, 2, "B", 1); err != nil {
		t.Fatalf("Force A→B: %v", err)
	}
	_, _, _, gen2, err := pg.OwnerRow(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if gen2 != 2 {
		t.Fatalf("gen after Force=%d, want 2", gen2)
	}

	// Idempotent retry at expectedGen+1 with target owner.
	if err := po.ForceTakeover(ctx, 2, "B", 1); err != nil {
		t.Fatalf("idempotent Force retry: %v", err)
	}
	_, _, _, genRetry, err := pg.OwnerRow(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if genRetry != 2 {
		t.Fatalf("idempotent retry bumped gen to %d", genRetry)
	}

	// Stale generation (neither expected nor expected+1) must conflict.
	err = po.ForceTakeover(ctx, 2, "B", 99)
	if err == nil {
		t.Fatal("stale ForceTakeover succeeded; want conflict")
	}
	err = po.ForceTakeover(ctx, 2, "C", 1)
	if err == nil {
		t.Fatal("Force with stale expectedGen for other node succeeded; want conflict")
	}
}

func TestHandoffProgressDespiteStaleLocalClaimList(t *testing.T) {
	pg := openPG(t)
	ctx := context.Background()
	po := store.PartitionOwnership(pg)
	const partitions = 32
	if err := po.EnsureOwners(ctx, partitions, "A"); err != nil {
		t.Fatal(err)
	}

	subID := "sub-stale-claim-list"
	part := fold.NodeIndex(subID, partitions, partitions)
	t.Logf("subscriber=%s partition=%d", subID, part)

	rec := &fold.RecordingTransport{}
	// B's PartitionsClaim deliberately omits the handed-off partition (and may
	// list an unrelated one). Durable ownership must still let B claim.
	staleClaim := []int{(part + 1) % partitions}
	if staleClaim[0] == part {
		staleClaim[0] = (part + 2) % partitions
	}

	dB, err := fold.New(fold.Config{
		Store:           pg,
		Transport:       rec,
		Workers:         1,
		Partitions:      partitions,
		PartitionsClaim: staleClaim,
		NodeID:          "B",
		IDPrefix:        "b-",
		StaleClaimAge:   -1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Seed work under A, abandon in_flight, Force to B.
	if err := pg.Enqueue(ctx, []store.Delivery{{
		ID: "stale_claim_1", EventID: "e", SubscriberID: subID,
		URL: "https://example.test/h", Partition: part, Payload: []byte(`{"n":0}`),
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.Claim(ctx, "A/dead/worker-0", 1, []int{part}, 1); err != nil {
		t.Fatal(err)
	}
	if err := po.ForceTakeover(ctx, part, "B", 1); err != nil {
		t.Fatal(err)
	}

	if err := dB.Start(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(rec.Calls()) < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = dB.Close(closeCtx)

	if len(rec.Calls()) != 1 {
		t.Fatalf("B recorded %d deliveries with stale PartitionsClaim; want 1 (stranded)", len(rec.Calls()))
	}
}

func TestMissingOwnershipMetadataIsError(t *testing.T) {
	pg := openPG(t)
	pg.SetDistributedOwnership(true)
	ctx := context.Background()

	if err := pg.Enqueue(ctx, []store.Delivery{{
		ID: "miss_1", EventID: "e", SubscriberID: "sub-missing-owner",
		URL: "https://example.test/h", Partition: 3, Payload: []byte(`{}`),
	}}); err != nil {
		t.Fatal(err)
	}
	_, err := pg.Claim(ctx, "A/inc/worker-0", 1, []int{3}, 1)
	if err == nil {
		t.Fatal("Claim with missing fold_partition_owners row succeeded; want error")
	}
}

func TestNodeIDRequiresDistributedOwnership(t *testing.T) {
	pg := openPG(t) // DistributedOwnership remains false
	_, err := fold.New(fold.Config{
		Store:      pg,
		Workers:    1,
		Partitions: 8,
		NodeID:     "A",
	})
	if err == nil {
		t.Fatal("New with NodeID succeeded without DistributedOwnership enabled")
	}
}

// TestOwnershipRowsEnforceEvenIfFlagCleared: durable fold_partition_owners must
// fail-closed. Clearing the in-memory flag (or reconnecting without EnsureOwners)
// must not re-enable legacy Claim bypass.
func TestOwnershipRowsEnforceEvenIfFlagCleared(t *testing.T) {
	pg := openPG(t)
	ctx := context.Background()
	if err := store.PartitionOwnership(pg).EnsureOwners(ctx, 8, "A"); err != nil {
		t.Fatal(err)
	}
	if err := pg.Enqueue(ctx, []store.Delivery{{
		ID: "flag_1", EventID: "e", SubscriberID: "sub-flag-clear",
		URL: "https://example.test/h", Partition: 0, Payload: []byte(`{}`),
	}}); err != nil {
		t.Fatal(err)
	}

	pg.SetDistributedOwnership(false) // simulate lost in-memory mode after misconfig
	_, err := pg.Claim(ctx, "worker-0", 1, []int{0}, 1)
	if err == nil {
		t.Fatal("legacy Claim succeeded after clearing DistributedOwnership with ownership rows present")
	}
	if !pg.HasDistributedOwnership() {
		t.Fatal("Claim/sync should re-enable DistributedOwnership when ownership rows exist")
	}
}

func TestOpenDetectsExistingOwnershipRows(t *testing.T) {
	pg := openPG(t)
	ctx := context.Background()
	if err := store.PartitionOwnership(pg).EnsureOwners(ctx, 8, "A"); err != nil {
		t.Fatal(err)
	}
	// Second connection to the same DB (no Truncate / no second advisory lock).
	pg2, err := postgres.Open(ctx, pgDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg2.Close)
	// Do not call EnsureOwners — mode must come from durable rows.
	if !pg2.HasDistributedOwnership() {
		t.Fatal("Open did not enable DistributedOwnership when fold_partition_owners has rows")
	}
	_, err = fold.New(fold.Config{Store: pg2, Workers: 1, Partitions: 8})
	if err == nil {
		t.Fatal("New without NodeID should fail when Open detected distributed ownership")
	}
}

func TestForceTakeoverDuringDrainingToCurrentOwnerAbortsHandoff(t *testing.T) {
	pg := openPG(t)
	ctx := context.Background()
	po := store.PartitionOwnership(pg)
	if err := po.EnsureOwners(ctx, 8, "A"); err != nil {
		t.Fatal(err)
	}
	if err := po.BeginHandoff(ctx, 0, "A", "B"); err != nil {
		t.Fatal(err)
	}
	// Force back to current owner must not bump/reset; should abort drain.
	if err := po.ForceTakeover(ctx, 0, "A", 1); err != nil {
		t.Fatalf("Force to current owner during drain: %v", err)
	}
	owner, state, next, gen, err := pg.OwnerRow(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if owner != "A" || state != "active" || next != "" || gen != 1 {
		t.Fatalf("want active A gen=1 empty next; got owner=%s state=%s next=%q gen=%d", owner, state, next, gen)
	}
}
