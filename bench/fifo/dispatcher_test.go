package fifo_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/bench/fifo"
)

func TestFIFODeliversInOrder(t *testing.T) {
	var mu sync.Mutex
	var order []string
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		mu.Lock()
		order = append(order, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	d, err := fifo.New(fifo.Config{
		Transport: fold.NewHTTPTransport(2*time.Second, srv.Client()),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}

	subs := []fold.Subscriber{
		{ID: "a", URL: srv.URL + "/a"},
		{ID: "b", URL: srv.URL + "/b"},
	}
	for i := 0; i < 5; i++ {
		payload, _ := json.Marshal(map[string]int{"n": i})
		if err := d.Dispatch(ctx, fold.Event{
			ID: fmt.Sprintf("e-%d", i), Type: "t", Payload: payload,
		}, subs); err != nil {
			t.Fatal(err)
		}
	}
	closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := d.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	if got := hits.Load(); got != 10 {
		t.Fatalf("hits=%d want 10", got)
	}

	// Per-subscriber paths must appear in dispatch order (interleaved globally
	// is fine; inversions within a subscriber are not).
	pos := map[string][]int{}
	mu.Lock()
	for i, p := range order {
		pos[p] = append(pos[p], i)
	}
	mu.Unlock()
	for _, path := range []string{"/a", "/b"} {
		idxs := pos[path]
		if len(idxs) != 5 {
			t.Fatalf("%s: got %d hits, want 5", path, len(idxs))
		}
		for i := 1; i < len(idxs); i++ {
			if idxs[i] < idxs[i-1] {
				t.Fatalf("%s: arrival positions not monotonic: %v", path, idxs)
			}
		}
	}
}
