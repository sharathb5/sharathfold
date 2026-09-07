package fold

import (
	"context"
	"sync"

	"github.com/sharathb5/sharathfold/store"
)

// Transport delivers one claimed delivery. Implementations must not mutate
// d.Payload; the bytes are shared read-only across subscribers of an event.
type Transport interface {
	Deliver(ctx context.Context, d store.Delivery) error
}

// RecordingTransport is a fake Transport that appends each call in order.
type RecordingTransport struct {
	mu    sync.Mutex
	calls []store.Delivery
}

// Deliver records a copy of the delivery.
func (r *RecordingTransport) Deliver(ctx context.Context, d store.Delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cp := d
	if d.Payload != nil {
		cp.Payload = append([]byte(nil), d.Payload...)
	}
	r.mu.Lock()
	r.calls = append(r.calls, cp)
	r.mu.Unlock()
	return nil
}

// Calls returns a snapshot of recorded deliveries in attempt order.
func (r *RecordingTransport) Calls() []store.Delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]store.Delivery, len(r.calls))
	copy(out, r.calls)
	return out
}
