package postgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold/store"
	"github.com/sharathb5/sharathfold/store/postgres"
)

func openStore(t *testing.T) *postgres.Store {
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

func TestMarkDeliveredRejectsStaleGeneration(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	if err := s.Enqueue(ctx, []store.Delivery{{
		ID:           "dlg_1",
		EventID:      "evt_1",
		SubscriberID: "sub_1",
		URL:          "https://example.test/hook",
		Partition:    0,
		Payload:      []byte(`{}`),
	}}); err != nil {
		t.Fatal(err)
	}

	claimed, err := s.Claim(ctx, "worker-a", 7, []int{0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d, want 1", len(claimed))
	}

	err = s.MarkDelivered(ctx, "dlg_1", "worker-a", 6)
	if err == nil {
		t.Fatal("MarkDelivered accepted stale generation; want rejection")
	}

	err = s.MarkDelivered(ctx, "dlg_1", "worker-a", 0)
	if err == nil {
		t.Fatal("MarkDelivered accepted zero generation; want rejection")
	}

	err = s.MarkDelivered(ctx, "dlg_1", "worker-a", 7)
	if err != nil {
		t.Fatalf("MarkDelivered with matching generation: %v", err)
	}
}

func TestClaimRejectsZeroGeneration(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	if err := s.Enqueue(ctx, []store.Delivery{{
		ID:           "dlg_2",
		EventID:      "evt_2",
		SubscriberID: "sub_2",
		URL:          "https://example.test/hook",
		Partition:    1,
		Payload:      []byte(`{}`),
	}}); err != nil {
		t.Fatal(err)
	}
	_, err := s.Claim(ctx, "worker-a", 0, []int{1}, 1)
	if err == nil {
		t.Fatal("Claim accepted zero generation; want rejection")
	}
}

func TestMarkDeadLetterRejectsWrongOwner(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	if err := s.Enqueue(ctx, []store.Delivery{{
		ID:           "dlg_3",
		EventID:      "evt_3",
		SubscriberID: "sub_3",
		URL:          "https://example.test/hook",
		Partition:    2,
		Payload:      []byte(`{}`),
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, "worker-a", 3, []int{2}, 1); err != nil {
		t.Fatal(err)
	}
	err := s.MarkDeadLetter(ctx, "dlg_3", "worker-b", 3, "nope")
	if err == nil {
		t.Fatal("MarkDeadLetter accepted wrong owner")
	}
	if err := s.MarkFailed(ctx, "dlg_3", "worker-a", 3, 1, time.Now().Add(time.Hour), "park"); err != nil {
		t.Fatalf("MarkFailed with matching stamp: %v", err)
	}
}

func TestClaimEmptyPartitionsReturnsNothing(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	if err := s.Enqueue(ctx, []store.Delivery{{
		ID:           "dlg_4",
		EventID:      "evt_4",
		SubscriberID: "sub_4",
		URL:          "https://example.test/hook",
		Partition:    3,
		Payload:      []byte(`{}`),
	}}); err != nil {
		t.Fatal(err)
	}
	out, err := s.Claim(ctx, "worker-a", 1, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("empty partitions claimed %d", len(out))
	}
}

func TestClaimSkipsSuspendedAndRespectsHOL(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	rows := []store.Delivery{
		{ID: "a1", EventID: "e1", SubscriberID: "sub-h", URL: "https://example.test/h", Partition: 5, Payload: []byte(`1`)},
		{ID: "a2", EventID: "e2", SubscriberID: "sub-h", URL: "https://example.test/h", Partition: 5, Payload: []byte(`2`)},
	}
	if err := s.Enqueue(ctx, rows); err != nil {
		t.Fatal(err)
	}
	c1, err := s.Claim(ctx, "w", 1, []int{5}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(c1) != 1 || c1[0].Sequence != 1 {
		t.Fatalf("HOL claim: %+v", c1)
	}
	c2, err := s.Claim(ctx, "w", 1, []int{5}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(c2) != 0 {
		t.Fatalf("claimed while head in_flight: %+v", c2)
	}
	if err := s.ExhaustAndSuspend(ctx, c1[0].ID, "w", 1, "boom"); err != nil {
		t.Fatal(err)
	}
	// Remaining pending should be retained; further claim empty.
	c3, err := s.Claim(ctx, "w", 1, []int{5}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(c3) != 0 {
		t.Fatalf("claimed while suspended: %+v", c3)
	}
	n, err := s.RetainedCount(ctx, "sub-h")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("retained=%d want 1", n)
	}
}
