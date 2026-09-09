package naive_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/bench/naive"
)

func TestNaiveDelivers(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	d, err := naive.New(naive.Config{
		Transport: fold.NewHTTPTransport(2*time.Second, srv.Client()),
		Workers:   4,
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
}
