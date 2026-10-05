package fold_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/store"
	"github.com/sharathb5/sharathfold/store/memory"
)

// markFailStore fails one class of post-Deliver mutation.
type markFailStore struct {
	*memory.Store
	failMarkDelivered bool
	failMarkFailed    bool
	failExhaust       bool
}

func (s *markFailStore) MarkDelivered(ctx context.Context, id, owner string, generation uint64) error {
	if s.failMarkDelivered {
		return errors.New("injected: MarkDelivered failed")
	}
	return s.Store.MarkDelivered(ctx, id, owner, generation)
}

func (s *markFailStore) MarkFailed(ctx context.Context, id, owner string, generation uint64, attempt int, next time.Time, errMsg string) error {
	if s.failMarkFailed {
		return errors.New("injected: MarkFailed failed")
	}
	return s.Store.MarkFailed(ctx, id, owner, generation, attempt, next, errMsg)
}

func (s *markFailStore) ExhaustAndSuspend(ctx context.Context, id, owner string, generation uint64, errMsg string) error {
	if s.failExhaust {
		return errors.New("injected: ExhaustAndSuspend failed")
	}
	return s.Store.ExhaustAndSuspend(ctx, id, owner, generation, errMsg)
}

func TestOnDeliveryErrorMarkDelivered(t *testing.T) {
	var (
		mu  sync.Mutex
		got []fold.DeliveryMutationError
	)
	st := &markFailStore{Store: memory.New(), failMarkDelivered: true}
	d, err := fold.New(fold.Config{
		Store:         st,
		Transport:     &fold.RecordingTransport{},
		Workers:       1,
		Partitions:    8,
		StaleClaimAge: -1,
		OnDeliveryError: func(e fold.DeliveryMutationError) {
			mu.Lock()
			got = append(got, e)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.Dispatch(ctx, fold.Event{ID: "e1", Type: "t", Payload: []byte(`{}`)},
		[]fold.Subscriber{{ID: "sub-err-mark", URL: "https://example.test/h"}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_ = d.Close(closeCtx)

	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 {
		t.Fatal("OnDeliveryError not called after MarkDelivered failure")
	}
	if got[0].Op != "MarkDelivered" {
		t.Fatalf("Op=%q want MarkDelivered", got[0].Op)
	}
	if got[0].Err == nil || got[0].Err.Error() == "" {
		t.Fatal("missing Err on DeliveryMutationError")
	}
	if got[0].SubscriberID != "sub-err-mark" || got[0].EventID != "e1" || got[0].DeliveryID == "" {
		t.Fatalf("incomplete context: %+v", got[0])
	}
}

func TestOnDeliveryErrorMarkFailed(t *testing.T) {
	var (
		mu  sync.Mutex
		got []fold.DeliveryMutationError
	)
	st := &markFailStore{Store: memory.New(), failMarkFailed: true}
	tr := &failOnceTransport{}
	d, err := fold.New(fold.Config{
		Store:         st,
		Transport:     tr,
		Workers:       1,
		Partitions:    8,
		MaxAttempts:   5,
		BaseBackoff:   time.Millisecond,
		StaleClaimAge: -1,
		OnDeliveryError: func(e fold.DeliveryMutationError) {
			mu.Lock()
			got = append(got, e)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.Dispatch(ctx, fold.Event{ID: "e-fail", Type: "t", Payload: []byte(`{}`)},
		[]fold.Subscriber{{ID: "sub-err-fail", URL: "https://example.test/h"}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_ = d.Close(closeCtx)

	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 {
		t.Fatal("OnDeliveryError not called after MarkFailed failure")
	}
	if got[0].Op != "MarkFailed" {
		t.Fatalf("Op=%q want MarkFailed", got[0].Op)
	}
}

func TestOnDeliveryErrorExhaustAndSuspend(t *testing.T) {
	var (
		mu  sync.Mutex
		got []fold.DeliveryMutationError
	)
	st := &markFailStore{Store: memory.New(), failExhaust: true}
	d, err := fold.New(fold.Config{
		Store:         st,
		Transport:     &permFailTransport{},
		Workers:       1,
		Partitions:    8,
		StaleClaimAge: -1,
		OnDeliveryError: func(e fold.DeliveryMutationError) {
			mu.Lock()
			got = append(got, e)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.Dispatch(ctx, fold.Event{ID: "e-ex", Type: "t", Payload: []byte(`{}`)},
		[]fold.Subscriber{{ID: "sub-err-ex", URL: "https://example.test/h"}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_ = d.Close(closeCtx)

	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 {
		t.Fatal("OnDeliveryError not called after ExhaustAndSuspend failure")
	}
	if got[0].Op != "ExhaustAndSuspend" {
		t.Fatalf("Op=%q want ExhaustAndSuspend", got[0].Op)
	}
}

type failOnceTransport struct{}

func (f *failOnceTransport) Deliver(ctx context.Context, d store.Delivery) fold.Result {
	return fold.Result{Outcome: fold.OutcomeRetryable, Err: errors.New("transient")}
}

type permFailTransport struct{}

func (p *permFailTransport) Deliver(ctx context.Context, d store.Delivery) fold.Result {
	return fold.Result{Outcome: fold.OutcomePermanent, Err: errors.New("gone")}
}
