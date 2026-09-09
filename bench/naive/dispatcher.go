// Package naive is an unordered webhook dispatcher: a shared queue where any
// worker may claim any delivery. No partition ownership, no head-of-line gate.
//
// It exists as a fair baseline for fold's ordering claim — same serialize-once
// accept path, same HTTP transport, same retry/backoff schedule. The only
// intentional difference is work assignment.
package naive

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/internal/backoff"
	"github.com/sharathb5/sharathfold/store"
)

const (
	defaultWorkers     = 32
	defaultMaxAttempts = fold.DefaultMaxAttempts
	defaultBaseBackoff = fold.DefaultBaseBackoff
	defaultMaxBackoff  = fold.DefaultMaxBackoff
	claimBatch         = 64
	generation         = 1 // no resize in the naive baseline
)

// Config configures a naive Dispatcher.
type Config struct {
	Workers     int
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration

	Transport       fold.Transport
	DeliveryTimeout time.Duration
	HTTPClient      *http.Client
}

// Dispatcher fans out webhooks via a shared queue and an unpartitioned worker pool.
type Dispatcher struct {
	queue     *queue
	transport fold.Transport

	maxAttempts int
	baseBackoff time.Duration
	maxBackoff  time.Duration
	workers     int

	mu         sync.Mutex
	started    bool
	closed     bool
	workerCtx  context.Context
	cancel     context.CancelFunc
	workerWG   sync.WaitGroup
	workerDone chan struct{}

	idSeq atomic.Uint64

	// Per-owner delivery/idle counters for load-balance diagnostics.
	workerStats sync.Map // owner string -> *workerCounters
}

// New builds a Dispatcher. Call Start before Dispatch.
func New(cfg Config) (*Dispatcher, error) {
	transport := cfg.Transport
	if transport == nil {
		transport = fold.NewHTTPTransport(cfg.DeliveryTimeout, cfg.HTTPClient)
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = defaultWorkers
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
		workers:     workers,
		maxAttempts: maxAttempts,
		baseBackoff: baseBackoff,
		maxBackoff:  maxBackoff,
	}, nil
}

// Start launches the worker pool.
func (d *Dispatcher) Start(ctx context.Context) error {
	_ = ctx
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return fmt.Errorf("naive: dispatcher closed")
	}
	if d.started {
		return fmt.Errorf("naive: already started")
	}

	wctx, cancel := context.WithCancel(context.Background())
	d.workerCtx = wctx
	d.cancel = cancel
	d.workerDone = make(chan struct{})
	d.started = true

	for w := 0; w < d.workers; w++ {
		owner := fmt.Sprintf("naive-worker-%d", w)
		d.workerWG.Add(1)
		go func(owner string) {
			defer d.workerWG.Done()
			d.runWorker(wctx, owner)
		}(owner)
	}
	go func() {
		d.workerWG.Wait()
		close(d.workerDone)
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
		return fmt.Errorf("naive: dispatcher closed")
	}
	if !started {
		return fmt.Errorf("naive: dispatcher not started")
	}
	if ev.ID == "" {
		return fmt.Errorf("naive: event ID required")
	}
	if len(subs) == 0 {
		return fmt.Errorf("naive: at least one subscriber required")
	}
	for _, s := range subs {
		if s.ID == "" {
			return fmt.Errorf("naive: subscriber ID required")
		}
		if s.URL == "" {
			return fmt.Errorf("naive: subscriber URL required")
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

// Close drains pending work, then stops workers.
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

func (d *Dispatcher) runWorker(ctx context.Context, owner string) {
	idle := time.NewTimer(time.Hour)
	defer idle.Stop()
	stats := d.workerCounter(owner)

	for {
		if ctx.Err() != nil {
			return
		}
		batch, err := d.queue.Claim(ctx, owner, generation, claimBatch)
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
			if !d.waitForWork(ctx, idle, stats) {
				return
			}
			continue
		}
		for _, del := range batch {
			if ctx.Err() != nil {
				return
			}
			d.deliverOne(ctx, owner, del)
			stats.deliveries.Add(1)
		}
	}
}

func (d *Dispatcher) deliverOne(ctx context.Context, owner string, del store.Delivery) {
	res := d.transport.Deliver(ctx, del)
	switch res.Outcome {
	case fold.OutcomeSuccess:
		_ = d.queue.MarkDelivered(ctx, del.ID, owner, generation)
	case fold.OutcomePermanent:
		_ = d.queue.MarkDeadLetter(ctx, del.ID, owner, generation, res.Error())
	default:
		if del.Attempt >= d.maxAttempts {
			_ = d.queue.MarkDeadLetter(ctx, del.ID, owner, generation, res.Error())
			return
		}
		next := time.Now().Add(backoff.Delay(del.Attempt, d.baseBackoff, d.maxBackoff))
		_ = d.queue.MarkFailed(ctx, del.ID, owner, generation, del.Attempt, next, res.Error())
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

func (d *Dispatcher) waitForWork(ctx context.Context, idle *time.Timer, stats *workerCounters) bool {
	if !idle.Stop() {
		select {
		case <-idle.C:
		default:
		}
	}
	idle.Reset(10 * time.Millisecond)
	start := time.Now()
	defer func() {
		if stats != nil {
			stats.idleNanos.Add(time.Since(start).Nanoseconds())
		}
	}()
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
	return fmt.Sprintf("naive_%d", n)
}

// ClaimStats returns cumulative Claim counters from the shared queue.
// Safe to call after Close.
func (d *Dispatcher) ClaimStats() (calls, rows, nonempty int64) {
	return d.queue.claimStats()
}

// WorkerStat is cumulative work for one owner. Safe to read after Close.
type WorkerStat struct {
	Owner      string
	Deliveries int64
	Idle       time.Duration
}

type workerCounters struct {
	deliveries atomic.Int64
	idleNanos  atomic.Int64
}

func (d *Dispatcher) workerCounter(owner string) *workerCounters {
	if v, ok := d.workerStats.Load(owner); ok {
		return v.(*workerCounters)
	}
	c := &workerCounters{}
	actual, _ := d.workerStats.LoadOrStore(owner, c)
	return actual.(*workerCounters)
}

// WorkerStats returns per-owner delivery counts and idle time.
// Idle is time spent in waitForWork after an empty claim. Call after Close.
func (d *Dispatcher) WorkerStats() []WorkerStat {
	var out []WorkerStat
	d.workerStats.Range(func(key, val any) bool {
		c := val.(*workerCounters)
		out = append(out, WorkerStat{
			Owner:      key.(string),
			Deliveries: c.deliveries.Load(),
			Idle:       time.Duration(c.idleNanos.Load()),
		})
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Owner < out[j].Owner })
	return out
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
