package fold_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/store"
	"github.com/sharathb5/sharathfold/store/memory"
)

// bareStore implements store.Store but not drain counting.
type bareStore struct {
	inner *memory.Store
}

func (b *bareStore) Enqueue(ctx context.Context, ds []store.Delivery) error {
	return b.inner.Enqueue(ctx, ds)
}
func (b *bareStore) Claim(ctx context.Context, owner string, generation uint64, partitions []int, limit int) ([]store.Delivery, error) {
	return b.inner.Claim(ctx, owner, generation, partitions, limit)
}
func (b *bareStore) MarkDelivered(ctx context.Context, id, owner string, generation uint64) error {
	return b.inner.MarkDelivered(ctx, id, owner, generation)
}
func (b *bareStore) MarkFailed(ctx context.Context, id, owner string, generation uint64, attempt int, next time.Time, errMsg string) error {
	return b.inner.MarkFailed(ctx, id, owner, generation, attempt, next, errMsg)
}
func (b *bareStore) MarkDeadLetter(ctx context.Context, id, owner string, generation uint64, errMsg string) error {
	return b.inner.MarkDeadLetter(ctx, id, owner, generation, errMsg)
}
func (b *bareStore) ExhaustAndSuspend(ctx context.Context, id, owner string, generation uint64, errMsg string) error {
	return b.inner.ExhaustAndSuspend(ctx, id, owner, generation, errMsg)
}
func (b *bareStore) Resume(ctx context.Context, subscriberID string) (store.GapInfo, error) {
	return b.inner.Resume(ctx, subscriberID)
}
func (b *bareStore) IsSuspended(ctx context.Context, subscriberID string) (bool, error) {
	return b.inner.IsSuspended(ctx, subscriberID)
}
func (b *bareStore) RecoverStale(ctx context.Context, olderThan time.Duration) (int, error) {
	return b.inner.RecoverStale(ctx, olderThan)
}

func TestCloseRejectsStoreWithoutDrainCapability(t *testing.T) {
	d, err := fold.New(fold.Config{
		Store:         &bareStore{inner: memory.New()},
		Transport:     &fold.RecordingTransport{},
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
	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err = d.Close(closeCtx)
	if err == nil {
		t.Fatal("Close succeeded without drain capability; want error")
	}
}

func TestCloseDrainsMemoryStore(t *testing.T) {
	mem := memory.New()
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
	if err := d.Dispatch(ctx, fold.Event{ID: "e-drain", Type: "t", Payload: []byte(`{}`)},
		[]fold.Subscriber{{ID: "sub-drain", URL: "https://example.test/h"}}); err != nil {
		t.Fatal(err)
	}
	closeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := d.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(rec.Calls()) != 1 {
		t.Fatalf("deliveries=%d want 1", len(rec.Calls()))
	}
	n, err := mem.PendingOrInFlight(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("pending=%d after Close", n)
	}
}

type errCountStore struct {
	*memory.Store
}

func (e *errCountStore) PendingOrInFlight(ctx context.Context) (int, error) {
	return 0, errors.New("injected: count failed")
}

func TestClosePropagatesPendingCountError(t *testing.T) {
	d, err := fold.New(fold.Config{
		Store:         &errCountStore{Store: memory.New()},
		Transport:     &fold.RecordingTransport{},
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
	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err = d.Close(closeCtx)
	if err == nil {
		t.Fatal("Close swallowed PendingOrInFlight error")
	}
}
