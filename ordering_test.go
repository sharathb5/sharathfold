package fold_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/store"
)

func TestOrderingUnderConcurrentDispatch(t *testing.T) {
	const (
		subscribers = 50
		events      = 40
	)

	rec := &fold.RecordingTransport{}
	d, err := fold.New(fold.Config{Transport: rec})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}

	subs := make([]fold.Subscriber, subscribers)
	for i := 0; i < subscribers; i++ {
		subs[i] = fold.Subscriber{
			ID:  fmt.Sprintf("sub-%03d", i),
			URL: fmt.Sprintf("https://example.test/hooks/%d", i),
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, events)
	for e := 0; e < events; e++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			payload, _ := json.Marshal(map[string]int{"n": n})
			err := d.Dispatch(ctx, fold.Event{
				ID:      fmt.Sprintf("evt-%04d", n),
				Type:    "test.event",
				Payload: payload,
			}, subs)
			if err != nil {
				errCh <- err
			}
		}(e)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("Dispatch: %v", err)
	}

	closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := d.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	calls := rec.Calls()
	want := subscribers * events
	if len(calls) != want {
		t.Fatalf("recorded %d deliveries, want %d", len(calls), want)
	}

	bySub := make(map[string][]store.Delivery, subscribers)
	for _, c := range calls {
		bySub[c.SubscriberID] = append(bySub[c.SubscriberID], c)
	}

	inversions := 0
	for sub, list := range bySub {
		if len(list) != events {
			t.Fatalf("subscriber %s: got %d deliveries, want %d", sub, len(list), events)
		}
		for i := 1; i < len(list); i++ {
			if list[i].Sequence <= list[i-1].Sequence {
				inversions++
				t.Errorf("subscriber %s: sequence inversion at i=%d: %d then %d (events %s -> %s)",
					sub, i, list[i-1].Sequence, list[i].Sequence, list[i-1].EventID, list[i].EventID)
			}
		}
		// Sequences must be exactly 1..events for this subscriber (accept order).
		seen := make(map[int64]bool, events)
		for _, d := range list {
			seen[d.Sequence] = true
		}
		for seq := int64(1); seq <= int64(events); seq++ {
			if !seen[seq] {
				t.Errorf("subscriber %s: missing sequence %d", sub, seq)
			}
		}
	}

	t.Logf("subscribers=%d events=%d deliveries=%d inversions=%d", subscribers, events, len(calls), inversions)
	if inversions != 0 {
		t.Fatalf("ordering violated: %d per-subscriber sequence inversions", inversions)
	}
}
