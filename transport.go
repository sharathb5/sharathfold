package fold

import (
	"context"
	"fmt"
	"sync"

	"github.com/sharathb5/sharathfold/store"
)

// Outcome classifies a delivery attempt. The transport only reports what
// happened; retry scheduling and suspension live in the worker.
type Outcome int

const (
	OutcomeSuccess Outcome = iota
	OutcomeRetryable
	OutcomePermanent
)

// Result is the outcome of one Transport.Deliver call.
type Result struct {
	Outcome    Outcome
	StatusCode int // 0 when no HTTP response was received
	Err        error
}

// Error returns a short description suitable for store LastError, or "".
func (r Result) Error() string {
	if r.Outcome == OutcomeSuccess {
		return ""
	}
	if r.Err != nil {
		if r.StatusCode > 0 {
			return fmt.Sprintf("status %d: %v", r.StatusCode, r.Err)
		}
		return r.Err.Error()
	}
	if r.StatusCode > 0 {
		return fmt.Sprintf("status %d", r.StatusCode)
	}
	return "delivery failed"
}

// Transport delivers one claimed delivery. Implementations must not mutate
// d.Payload; the bytes are shared read-only across subscribers of an event.
type Transport interface {
	Deliver(ctx context.Context, d store.Delivery) Result
}

// RecordingTransport is a fake Transport that appends each call in order.
type RecordingTransport struct {
	mu    sync.Mutex
	calls []store.Delivery
}

// Deliver records a copy of the delivery.
func (r *RecordingTransport) Deliver(ctx context.Context, d store.Delivery) Result {
	if err := ctx.Err(); err != nil {
		return Result{Outcome: OutcomeRetryable, Err: err}
	}
	r.record(d)
	return Result{Outcome: OutcomeSuccess}
}

func (r *RecordingTransport) record(d store.Delivery) {
	cp := d
	if d.Payload != nil {
		cp.Payload = append([]byte(nil), d.Payload...)
	}
	r.mu.Lock()
	r.calls = append(r.calls, cp)
	r.mu.Unlock()
}

// Calls returns a snapshot of recorded deliveries in attempt order.
func (r *RecordingTransport) Calls() []store.Delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]store.Delivery, len(r.calls))
	copy(out, r.calls)
	return out
}

// HoldingTransport blocks the first Deliver for HoldSubscriber before
// recording it, so a peer worker can claim and record a later sequence first.
// That is the observable inversion HOL is meant to prevent.
type HoldingTransport struct {
	RecordingTransport

	HoldSubscriber string

	mu          sync.Mutex
	holding     bool
	held        chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

// NewHoldingTransport prepares a transport that holds the first attempt for sub.
func NewHoldingTransport(sub string) *HoldingTransport {
	return &HoldingTransport{
		HoldSubscriber: sub,
		held:           make(chan struct{}),
		release:        make(chan struct{}),
	}
}

// Held returns a channel closed when the hold begins (before recording).
func (h *HoldingTransport) Held() <-chan struct{} {
	return h.held
}

// Release unblocks the held delivery.
func (h *HoldingTransport) Release() {
	h.releaseOnce.Do(func() { close(h.release) })
}

// Deliver may block before recording the first HoldSubscriber attempt.
func (h *HoldingTransport) Deliver(ctx context.Context, d store.Delivery) Result {
	if err := ctx.Err(); err != nil {
		return Result{Outcome: OutcomeRetryable, Err: err}
	}

	if d.SubscriberID == h.HoldSubscriber {
		h.mu.Lock()
		startHold := !h.holding
		if startHold {
			h.holding = true
		}
		h.mu.Unlock()
		if startHold {
			close(h.held)
			select {
			case <-h.release:
			case <-ctx.Done():
				return Result{Outcome: OutcomeRetryable, Err: ctx.Err()}
			}
		}
	}

	h.record(d)
	return Result{Outcome: OutcomeSuccess}
}
