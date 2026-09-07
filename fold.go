package fold

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sharathb5/sharathfold/store"
	"github.com/sharathb5/sharathfold/store/memory"
)

// Config configures a Dispatcher. This step uses a single worker and an
// injected Transport; HTTP, partitions, and retries come later.
type Config struct {
	Store     store.Store // nil → memory.New()
	Transport Transport   // required until an HTTP default exists
}

// Dispatcher accepts events and delivers them via a worker claiming from Store.
type Dispatcher struct {
	store     store.Store
	transport Transport
	mem       *memory.Store // non-nil when using memory (default or injected)

	owner      string
	generation uint64

	mu      sync.Mutex
	started bool
	closed  bool

	workerCancel context.CancelFunc
	workerDone   chan struct{}

	idSeq atomic.Uint64
}

// New builds a Dispatcher. Call Start before Dispatch.
func New(cfg Config) (*Dispatcher, error) {
	if cfg.Transport == nil {
		return nil, fmt.Errorf("fold: Transport is required")
	}
	st := cfg.Store
	var mem *memory.Store
	if st == nil {
		mem = memory.New()
		st = mem
	} else if m, ok := st.(*memory.Store); ok {
		mem = m
	}
	return &Dispatcher{
		store:      st,
		transport:  cfg.Transport,
		mem:        mem,
		owner:      "worker-0",
		generation: 1,
	}, nil
}

// Start launches the single claim/deliver worker.
func (d *Dispatcher) Start(ctx context.Context) error {
	_ = ctx
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return fmt.Errorf("fold: dispatcher closed")
	}
	if d.started {
		return fmt.Errorf("fold: already started")
	}
	wctx, cancel := context.WithCancel(context.Background())
	d.workerCancel = cancel
	d.workerDone = make(chan struct{})
	d.started = true
	go d.runWorker(wctx)
	return nil
}

// Dispatch encodes the event payload once, enqueues one delivery per
// subscriber, and returns. It does not perform delivery.
func (d *Dispatcher) Dispatch(ctx context.Context, ev Event, subs []Subscriber) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	started, closed := d.started, d.closed
	d.mu.Unlock()
	if closed {
		return fmt.Errorf("fold: dispatcher closed")
	}
	if !started {
		return fmt.Errorf("fold: dispatcher not started")
	}
	if ev.ID == "" {
		return fmt.Errorf("fold: event ID required")
	}
	if len(subs) == 0 {
		return fmt.Errorf("fold: at least one subscriber required")
	}
	for _, s := range subs {
		if s.ID == "" {
			return fmt.Errorf("fold: subscriber ID required")
		}
		if s.URL == "" {
			return fmt.Errorf("fold: subscriber URL required")
		}
	}

	payload, err := encodeOnce(ev)
	if err != nil {
		return err
	}

	rows := make([]store.Delivery, len(subs))
	for i, s := range subs {
		rows[i] = store.Delivery{
			ID:           d.nextID(),
			EventID:      ev.ID,
			SubscriberID: s.ID,
			URL:          s.URL,
			Partition:    0, // partitioning comes later; single worker owns all
			Payload:      payload,
			EventType:    ev.Type,
		}
	}
	return d.store.Enqueue(ctx, rows)
}

// Close stops accepting work, waits for the queue to drain, then stops the worker.
func (d *Dispatcher) Close(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	cancel := d.workerCancel
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

func (d *Dispatcher) waitDrained(ctx context.Context) error {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		if d.pendingCount() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.notifyCh():
		case <-ticker.C:
		}
	}
}

func (d *Dispatcher) pendingCount() int {
	if d.mem != nil {
		return d.mem.PendingOrInFlight()
	}
	return 0
}

func (d *Dispatcher) notifyCh() <-chan struct{} {
	if d.mem != nil {
		return d.mem.Notify()
	}
	ch := make(chan struct{})
	return ch
}

func (d *Dispatcher) nextID() string {
	n := d.idSeq.Add(1)
	return fmt.Sprintf("dlg_%d", n)
}

func encodeOnce(ev Event) ([]byte, error) {
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
