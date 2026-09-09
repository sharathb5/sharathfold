package fold_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/store"
	"github.com/sharathb5/sharathfold/store/memory"
)

// tripwireTransport fails the test if Deliver is called while armed.
type tripwireTransport struct {
	t     *testing.T
	armed atomic.Bool
	calls atomic.Int64
}

func (tr *tripwireTransport) Deliver(ctx context.Context, d store.Delivery) fold.Result {
	tr.calls.Add(1)
	if tr.armed.Load() {
		tr.t.Errorf("transport.Deliver called on Dispatch path (delivery %s)", d.ID)
	}
	return fold.Result{Outcome: fold.OutcomeSuccess}
}

// gatedStore forwards to memory but can refuse Claim so workers cannot race
// Dispatch's return with a Deliver.
type gatedStore struct {
	inner      *memory.Store
	allowClaim atomic.Bool
}

func (g *gatedStore) Enqueue(ctx context.Context, ds []store.Delivery) error {
	return g.inner.Enqueue(ctx, ds)
}
func (g *gatedStore) Claim(ctx context.Context, owner string, generation uint64, partitions []int, limit int) ([]store.Delivery, error) {
	if !g.allowClaim.Load() {
		return nil, nil
	}
	return g.inner.Claim(ctx, owner, generation, partitions, limit)
}
func (g *gatedStore) MarkDelivered(ctx context.Context, id, owner string, generation uint64) error {
	return g.inner.MarkDelivered(ctx, id, owner, generation)
}
func (g *gatedStore) MarkFailed(ctx context.Context, id, owner string, generation uint64, attempt int, next time.Time, errMsg string) error {
	return g.inner.MarkFailed(ctx, id, owner, generation, attempt, next, errMsg)
}
func (g *gatedStore) MarkDeadLetter(ctx context.Context, id, owner string, generation uint64, errMsg string) error {
	return g.inner.MarkDeadLetter(ctx, id, owner, generation, errMsg)
}
func (g *gatedStore) ExhaustAndSuspend(ctx context.Context, id, owner string, generation uint64, errMsg string) error {
	return g.inner.ExhaustAndSuspend(ctx, id, owner, generation, errMsg)
}
func (g *gatedStore) Resume(ctx context.Context, subscriberID string) (store.GapInfo, error) {
	return g.inner.Resume(ctx, subscriberID)
}
func (g *gatedStore) IsSuspended(ctx context.Context, subscriberID string) (bool, error) {
	return g.inner.IsSuspended(ctx, subscriberID)
}
func (g *gatedStore) RecoverStale(ctx context.Context, olderThan time.Duration) (int, error) {
	return g.inner.RecoverStale(ctx, olderThan)
}

func TestDispatchDoesNotInvokeTransport(t *testing.T) {
	mem := memory.New()
	gs := &gatedStore{inner: mem}
	tr := &tripwireTransport{t: t}
	tr.armed.Store(true)

	d, err := fold.New(fold.Config{
		Store:      gs,
		Transport:  tr,
		Workers:    2,
		Partitions: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}

	payload, _ := json.Marshal(map[string]string{"k": "v"})
	err = d.Dispatch(ctx, fold.Event{
		ID:      "evt-dispatch-path",
		Type:    "test.event",
		Payload: payload,
	}, []fold.Subscriber{{ID: "sub-1", URL: "https://example.test/hook"}})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if n := tr.calls.Load(); n != 0 {
		t.Fatalf("transport invoked %d times during Dispatch; want 0", n)
	}

	// Let workers drain so Close can finish.
	tr.armed.Store(false)
	gs.allowClaim.Store(true)
	closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := d.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
