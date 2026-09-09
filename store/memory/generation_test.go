package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold/store"
	"github.com/sharathb5/sharathfold/store/memory"
)

// Store Mark* signatures require a generation argument (so omitting it is a
// compile error), but a caller can still pass a stale or zero value.
// requireOwner must reject those at runtime — this test is the tooth.
func TestMarkDeliveredRejectsStaleGeneration(t *testing.T) {
	s := memory.New()
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

	err = s.MarkDelivered(ctx, "dlg_1", "worker-a", 6) // stale generation
	if err == nil {
		t.Fatal("MarkDelivered accepted stale generation; want rejection")
	}

	err = s.MarkDelivered(ctx, "dlg_1", "worker-a", 0) // zero generation
	if err == nil {
		t.Fatal("MarkDelivered accepted zero generation; want rejection")
	}

	err = s.MarkDelivered(ctx, "dlg_1", "worker-a", 7)
	if err != nil {
		t.Fatalf("MarkDelivered with matching generation: %v", err)
	}
}

func TestClaimRejectsZeroGeneration(t *testing.T) {
	s := memory.New()
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
	s := memory.New()
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
	// Row still in flight — matching stamp should still work.
	if err := s.MarkFailed(ctx, "dlg_3", "worker-a", 3, 1, time.Now().Add(time.Hour), "park"); err != nil {
		t.Fatalf("MarkFailed with matching stamp: %v", err)
	}
}
