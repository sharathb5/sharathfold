// Package fifo is a strict single-threaded webhook dispatcher: one worker,
// one in-flight delivery at a time, global FIFO claim order with a
// head-of-line gate so retries cannot skip ahead.
//
// It is the "serialize everything" ordering baseline (the shape Svix ships as
// a separate strict mode) — same serialize-once accept path, same HTTP
// transport, same retry/backoff schedule as fold and naive. The only
// intentional difference is that it never parallelizes across subscribers.
package fifo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/internal/backoff"
	"github.com/sharathb5/sharathfold/store"
)

const (
	defaultMaxAttempts = fold.DefaultMaxAttempts
	defaultBaseBackoff = fold.DefaultBaseBackoff
	defaultMaxBackoff  = fold.DefaultMaxBackoff
	generation         = 1
	ownerID            = "fifo-worker"
)

// Config configures a fifo Dispatcher.
type Config struct {
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration

	Transport       fold.Transport
	DeliveryTimeout time.Duration
	HTTPClient      *http.Client
}

// Dispatcher delivers webhooks on a single worker thread.
type Dispatcher struct {
	queue     *queue
	transport fold.Transport

	maxAttempts int
	baseBackoff time.Duration
	maxBackoff  time.Duration

	mu         sync.Mutex
	started    bool
	closed     bool
	workerCtx  context.Context
	cancel     context.CancelFunc
	workerDone chan struct{}

	idSeq atomic.Uint64
}

// New builds a Dispatcher. Call Start before Dispatch.
func New(cfg Config) (*Dispatcher, error) {
	transport := cfg.Transport
	if transport == nil {
		transport = fold.NewHTTPTransport(cfg.DeliveryTimeout, cfg.HTTPClient)
	}
	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}
	baseBackoff := cfg.BaseBackoff
	if baseBackoff <= 0 {
		baseBackoff = defaultBaseBackoff
	}
	maxBackoff := cfg.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = defaultMaxBackoff
	}

	return &Dispatcher{
		queue:       newQueue(),
		transport:   transport,
		maxAttempts: maxAttempts,
		baseBackoff: baseBackoff,
		maxBackoff:  maxBackoff,
	}, nil
}

// Start launches the single worker.
func (d *Dispatcher) Start(ctx context.Context) error {
	_ = ctx
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return fmt.Errorf("fifo: dispatcher closed")
	}
	if d.started {
		return fmt.Errorf("fifo: already started")
	}

	wctx, cancel := context.WithCancel(context.Background())
	d.workerCtx = wctx
	d.cancel = cancel
	d.workerDone = make(chan struct{})
	d.started = true

	go func() {
		defer close(d.workerDone)
		d.runWorker(wctx)
	}()
	return nil
}

// Dispatch encodes the event once, enqueues one row per subscriber, and returns.
// No HTTP on this path.
func (d *Dispatcher) Dispatch(ctx context.Context, ev fold.Event, subs []fold.Subscriber) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	started, closed := d.started, d.closed
	d.mu.Unlock()
	if closed {
		return fmt.Errorf("fifo: dispatcher closed")
	}
	if !started {
		return fmt.Errorf("fifo: dispatcher not started")
	}
	if ev.ID == "" {
		return fmt.Errorf("fifo: event ID required")
	}
	if len(subs) == 0 {
		return fmt.Errorf("fifo: at least one subscriber required")
	}
	for _, s := range subs {
		if s.ID == "" {
			return fmt.Errorf("fifo: subscriber ID required")
		}
		if s.URL == "" {
			return fmt.Errorf("fifo: subscriber URL required")
		}
	}

	payload, err := encodeOnce(ev)
	if err != nil {
		return err
	}

	rows := make([]store.Delivery, len(subs))
	for i, s := range subs {
		var secret []byte
		if len(s.Secret) > 0 {
			secret = append([]byte(nil), s.Secret...)
		}
		rows[i] = store.Delivery{
			ID:           d.nextID(),
			EventID:      ev.ID,
			SubscriberID: s.ID,
			URL:          s.URL,
			Payload:      payload,
			EventType:    ev.Type,
			Secret:       secret,
		}
	}
	return d.queue.Enqueue(ctx, rows)
}

// Close drains pending work, then stops the worker.
func (d *Dispatcher) Close(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	cancel := d.cancel
	done := d.workerDone
	started := d.started
	d.mu.Unlock()

	if !started {
		return nil
	}
	if err := d.waitDrained(ctx); err != nil {
		if cancel != nil {
			cancel()
		}
		if done != nil {
			<-done
		}
		return err
	}
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (d *Dispatcher) runWorker(ctx context.Context) {
	idle := time.NewTimer(time.Hour)
	defer idle.Stop()

	for {
		if ctx.Err() != nil {
			return
		}
		// One delivery at a time — never a batch.
		batch, err := d.queue.Claim(ctx, ownerID, generation, 1)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !sleep(ctx, 5*time.Millisecond) {
				return
			}
			continue
		}
		if len(batch) == 0 {
			if !d.waitForWork(ctx, idle) {
				return
			}
			continue
		}
		d.deliverOne(ctx, batch[0])
	}
}

func (d *Dispatcher) deliverOne(ctx context.Context, del store.Delivery) {
	res := d.transport.Deliver(ctx, del)
	switch res.Outcome {
	case fold.OutcomeSuccess:
		_ = d.queue.MarkDelivered(ctx, del.ID, ownerID, generation)
	case fold.OutcomePermanent:
		_ = d.queue.MarkDeadLetter(ctx, del.ID, ownerID, generation, res.Error())
	default:
		if del.Attempt >= d.maxAttempts {
			_ = d.queue.MarkDeadLetter(ctx, del.ID, ownerID, generation, res.Error())
			return
		}
		next := time.Now().Add(backoff.Delay(del.Attempt, d.baseBackoff, d.maxBackoff))
		_ = d.queue.MarkFailed(ctx, del.ID, ownerID, generation, del.Attempt, next, res.Error())
	}
}

func (d *Dispatcher) waitDrained(ctx context.Context) error {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		if d.queue.PendingOrInFlight() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.queue.Notify():
		case <-ticker.C:
		}
	}
}

func (d *Dispatcher) waitForWork(ctx context.Context, idle *time.Timer) bool {
	if !idle.Stop() {
		select {
		case <-idle.C:
		default:
		}
	}
	idle.Reset(10 * time.Millisecond)
	select {
	case <-ctx.Done():
		return false
	case <-d.queue.Notify():
		return true
	case <-idle.C:
		return true
	}
}

func (d *Dispatcher) nextID() string {
	n := d.idSeq.Add(1)
	return fmt.Sprintf("fifo_%d", n)
}

func encodeOnce(ev fold.Event) ([]byte, error) {
	type wire struct {
		ID   string          `json:"id"`
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	data := ev.Payload
	if data == nil {
		data = json.RawMessage("null")
	}
	return json.Marshal(wire{ID: ev.ID, Type: ev.Type, Data: data})
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
