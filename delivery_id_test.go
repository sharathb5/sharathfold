package fold_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/store/memory"
)

// Simulated restart: two Dispatchers on one durable store must not reuse
// process-local dlg_<n> IDs (Enqueue would collide after restart).
func TestGeneratedDeliveryIDsSurviveRestart(t *testing.T) {
	mem := memory.New()
	sub := fold.Subscriber{ID: "sub-id-restart", URL: "https://example.test/h"}

	idA := dispatchOneID(t, mem, "evt-a", sub)
	// B is a new process against the same durable store (A's dlg_1 row remains).
	idB := dispatchOneID(t, mem, "evt-b", sub)

	if idA == idB {
		t.Fatalf("restart reused delivery ID %q", idA)
	}
	if strings.HasPrefix(idA, "dlg_") || strings.HasPrefix(idB, "dlg_") {
		t.Fatalf("generated IDs still process-local counters: %q %q", idA, idB)
	}
	if len(idA) < 32 || len(idB) < 32 {
		t.Fatalf("want opaque random IDs (≥128-bit hex), got %q %q", idA, idB)
	}
}

func dispatchOneID(t *testing.T, mem *memory.Store, eventID string, sub fold.Subscriber) string {
	t.Helper()
	rec := &fold.RecordingTransport{}
	d, err := fold.New(fold.Config{
		Store:         mem,
		Transport:     rec,
		Workers:       1,
		Partitions:    8,
		StaleClaimAge: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.Dispatch(ctx, fold.Event{ID: eventID, Type: "t", Payload: []byte(`{}`)}, []fold.Subscriber{sub}); err != nil {
		t.Fatalf("Dispatch %s: %v", eventID, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(rec.Calls()) == 0 {
		time.Sleep(2 * time.Millisecond)
	}
	calls := rec.Calls()
	if len(calls) != 1 {
		_ = d.Close(ctx)
		t.Fatalf("want 1 delivery for %s, got %d", eventID, len(calls))
	}
	id := calls[0].ID
	closeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := d.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return id
}
