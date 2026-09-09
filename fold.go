package fold

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sharathb5/sharathfold/internal/hash"
	"github.com/sharathb5/sharathfold/store"
	"github.com/sharathb5/sharathfold/store/memory"
)

const (
	DefaultMaxAttempts       = 8
	DefaultBaseBackoff       = time.Second
	DefaultMaxBackoff        = 15 * time.Minute
	DefaultRetentionMaxCount = memory.DefaultRetentionMaxCount
	DefaultRetentionMaxAge   = memory.DefaultRetentionMaxAge
	// DefaultStaleClaimAge is how old an in_flight row must be before Start's
	// RecoverStale resets it. Must exceed DefaultDeliveryTimeout so a live
	// attempt is not stolen mid-POST (see DECISIONS D11).
	DefaultStaleClaimAge = 2 * DefaultDeliveryTimeout
)

// Config configures a Dispatcher.
type Config struct {
	Store      store.Store // nil → memory.New()
	Transport  Transport   // nil → HTTPTransport with DeliveryTimeout
	Workers    int         // default 1; production fan-out uses >1
	Partitions int         // default 256; fixed, never worker count

	// MaxAttempts caps delivery tries before suspending the subscriber.
	// Default DefaultMaxAttempts.
	MaxAttempts int
	// BaseBackoff / MaxBackoff configure exponential backoff with jitter.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration

	// RetentionMaxCount / RetentionMaxAge bound retained backlog while a
	// subscriber is suspended. Applied on the memory store when fold creates
	// or is given one; other Store implementations configure themselves.
	RetentionMaxCount int
	RetentionMaxAge   time.Duration

	// OnSuspend is called after a subscriber is suspended (retry exhaustion or
	// permanent failure). The host app should expose this state so the
	// subscriber can pull-resume (see DECISIONS D9).
	OnSuspend func(subscriberID string)

	// DeliveryTimeout bounds each HTTP attempt. Default 30s. Ignored when
	// Transport is supplied by the caller.
	DeliveryTimeout time.Duration

	// HTTPClient is used by the default HTTPTransport. nil → shared-friendly
	// client with connection reuse. Ignored when Transport is set.
	HTTPClient *http.Client

	// OverlapPartitions gives every worker every partition. Production leaves
	// this false (exclusive ownership). The HOL ordering proof sets it true so
	// a peer can attempt a later sequence for the same subscriber while one
	// delivery is held open — exclusive ownership would make that race impossible.
	OverlapPartitions bool

	// StaleClaimAge controls Start's RecoverStale call. in_flight rows with
	// claimed_at older than this are reset to pending. Zero selects
	// DefaultStaleClaimAge. Negative disables recovery (tests only).
	StaleClaimAge time.Duration
}

// ResumeResult is returned by Dispatcher.Resume.
type ResumeResult struct {
	Gap     bool
	Dropped int
	Marker  string
}

// Dispatcher accepts events and delivers them via workers claiming from Store.
type Dispatcher struct {
	store     store.Store
	transport Transport
	mem       *memory.Store

	maxAttempts int
	baseBackoff time.Duration
	maxBackoff  time.Duration
	onSuspend   func(subscriberID string)

	staleClaimAge time.Duration // <0 disables; 0 means use DefaultStaleClaimAge at New

	// claimGate serializes Resize against Claim: Resize takes the write lock
	// so every in-flight Claim finishes under the old ownership map, then the
	// map and generation flip, then new Claims begin. Deliveries already past
	// Claim (in deliverOne) continue under their claim-time generation — that
	// overlap is the HOL resize window (DECISIONS D7).
	claimGate sync.RWMutex

	mu         sync.Mutex
	own        ownership
	generation uint64
	workers    int
	started    bool
	closed     bool

	workerCtx    context.Context
	workerCancel context.CancelFunc
	workerWG     sync.WaitGroup
	workerDone   chan struct{}

	idSeq atomic.Uint64

	// Per-owner delivery/idle counters for load-balance diagnostics.
	workerStats sync.Map // owner string -> *workerCounters
}

// New builds a Dispatcher. Call Start before Dispatch.
func New(cfg Config) (*Dispatcher, error) {
	transport := cfg.Transport
	if transport == nil {
		transport = NewHTTPTransport(cfg.DeliveryTimeout, cfg.HTTPClient)
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = 1
	}
	partitions := cfg.Partitions
	if partitions <= 0 {
		partitions = hash.DefaultPartitions
	}
	own, err := buildOwnership(workers, partitions, cfg.OverlapPartitions)
	if err != nil {
		return nil, err
	}

	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}
	baseBackoff := cfg.BaseBackoff
	if baseBackoff <= 0 {
		baseBackoff = DefaultBaseBackoff
	}
	maxBackoff := cfg.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = DefaultMaxBackoff
	}

	st := cfg.Store
	var mem *memory.Store
	if st == nil {
		mem = memory.New()
		st = mem
	} else if m, ok := st.(*memory.Store); ok {
		mem = m
	}
	if mem != nil {
		if cfg.RetentionMaxCount > 0 {
			mem.RetentionMaxCount = cfg.RetentionMaxCount
		}
		if cfg.RetentionMaxAge > 0 {
			mem.RetentionMaxAge = cfg.RetentionMaxAge
		}
	}

	return &Dispatcher{
		store:         st,
		transport:     transport,
		mem:           mem,
		own:           own,
		generation:    1,
		workers:       workers,
		maxAttempts:   maxAttempts,
		baseBackoff:   baseBackoff,
		maxBackoff:    maxBackoff,
		onSuspend:     cfg.OnSuspend,
		staleClaimAge: cfg.StaleClaimAge,
	}, nil
}

// Start runs RecoverStale, then launches the worker pool.
func (d *Dispatcher) Start(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return fmt.Errorf("fold: dispatcher closed")
	}
	if d.started {
		return fmt.Errorf("fold: already started")
	}

	if age := d.effectiveStaleAge(); age > 0 {
		if _, err := d.store.RecoverStale(ctx, age); err != nil {
			return fmt.Errorf("fold: recover stale: %w", err)
		}
	}

	wctx, cancel := context.WithCancel(context.Background())
	d.workerCtx = wctx
	d.workerCancel = cancel
	d.workerDone = make(chan struct{})
	d.started = true

	for w := 0; w < d.workers; w++ {
		d.spawnWorkerLocked(ownerID(w))
	}
	go func() {
		d.workerWG.Wait()
		close(d.workerDone)
	}()
	return nil
}

func (d *Dispatcher) effectiveStaleAge() time.Duration {
	if d.staleClaimAge < 0 {
		return 0
	}
	if d.staleClaimAge == 0 {
		return DefaultStaleClaimAge
	}
	return d.staleClaimAge
}

// Resize changes the worker count, recomputes partition ownership, and bumps
// the generation stamp. Workers losing partitions stop claiming them before
// new owners begin Claim; in-flight deliveries from the old owner may still
// be running — HOL prevents the new owner from racing ahead of those sequences.
func (d *Dispatcher) Resize(ctx context.Context, workers int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if workers <= 0 {
		return fmt.Errorf("fold: Workers must be >= 1")
	}

	// Block new Claims; wait for any Claim holding the read lock to finish.
	d.claimGate.Lock()
	defer d.claimGate.Unlock()

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return fmt.Errorf("fold: dispatcher closed")
	}
	if !d.started {
		return fmt.Errorf("fold: dispatcher not started")
	}
	if d.own.overlap {
		return fmt.Errorf("fold: Resize requires exclusive partition ownership")
	}
	if workers == d.workers {
		return nil
	}

	newOwn, err := buildOwnership(workers, d.own.partitions, false)
	if err != nil {
		return err
	}

	oldWorkers := d.workers
	d.own = newOwn
	d.workers = workers
	d.generation++

	// Grow: spawn owners that did not exist under the old count.
	for w := oldWorkers; w < workers; w++ {
		d.spawnWorkerLocked(ownerID(w))
	}
	// Shrink: excess workers see index >= d.workers and exit their claim loop.
	return nil
}

func (d *Dispatcher) spawnWorkerLocked(owner string) {
	d.workerWG.Add(1)
	go func() {
		defer d.workerWG.Done()
		d.runWorker(d.workerCtx, owner)
	}()
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
		var secret []byte
		if len(s.Secret) > 0 {
			secret = append([]byte(nil), s.Secret...)
		}
		rows[i] = store.Delivery{
			ID:           d.nextID(),
			EventID:      ev.ID,
			SubscriberID: s.ID,
			URL:          s.URL,
			Partition:    hash.Partition(s.ID, d.own.partitions),
			Payload:      payload,
			EventType:    ev.Type,
			Secret:       secret,
		}
	}
	return d.store.Enqueue(ctx, rows)
}

// Suspended reports whether the subscriber is currently suspended.
func (d *Dispatcher) Suspended(ctx context.Context, subscriberID string) (bool, error) {
	return d.store.IsSuspended(ctx, subscriberID)
}

// Resume clears suspension for subscriberID and requeues retained deliveries
// for ordered replay. Reports whether a retention gap occurred (D10).
func (d *Dispatcher) Resume(ctx context.Context, subscriberID string) (ResumeResult, error) {
	if subscriberID == "" {
		return ResumeResult{}, fmt.Errorf("fold: subscriber ID required")
	}
	gap, err := d.store.Resume(ctx, subscriberID)
	if err != nil {
		return ResumeResult{}, err
	}
	return ResumeResult{
		Gap:     gap.Occurred,
		Dropped: gap.Dropped,
		Marker:  gap.Marker,
	}, nil
}

// Close stops accepting work, waits for the queue to drain, then stops workers.
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
	type pender interface{ PendingOrInFlight() int }
	if p, ok := d.store.(pender); ok {
		return p.PendingOrInFlight()
	}
	return 0
}

func (d *Dispatcher) notifyCh() <-chan struct{} {
	type notifier interface{ Notify() <-chan struct{} }
	if n, ok := d.store.(notifier); ok {
		return n.Notify()
	}
	return nil
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
