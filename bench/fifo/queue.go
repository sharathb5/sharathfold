package fifo

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sharathb5/sharathfold/store"
)

// queue is a global FIFO store with a head-of-line gate. The single worker
// claims at most one due, HOL-eligible row at a time — never overlapping HTTP
// across subscribers, and never skipping ahead of a backing-off earlier
// sequence for the same subscriber.
type queue struct {
	mu     sync.Mutex
	byID   map[string]*store.Delivery
	seq    map[string]int64
	notify chan struct{}
}

func newQueue() *queue {
	return &queue{
		byID:   make(map[string]*store.Delivery),
		seq:    make(map[string]int64),
		notify: make(chan struct{}, 1),
	}
}

func (q *queue) Notify() <-chan struct{} { return q.notify }

func (q *queue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *queue) Enqueue(ctx context.Context, ds []store.Delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	for i := range ds {
		d := ds[i]
		if d.ID == "" {
			return fmt.Errorf("fifo queue: delivery missing ID")
		}
		if d.SubscriberID == "" {
			return fmt.Errorf("fifo queue: delivery %s missing SubscriberID", d.ID)
		}
		if _, exists := q.byID[d.ID]; exists {
			return fmt.Errorf("fifo queue: duplicate delivery ID %s", d.ID)
		}

		q.seq[d.SubscriberID]++
		seq := q.seq[d.SubscriberID]

		cp := d
		if d.Payload != nil {
			cp.Payload = append([]byte(nil), d.Payload...)
		}
		if d.Secret != nil {
			cp.Secret = append([]byte(nil), d.Secret...)
		}
		cp.Sequence = seq
		cp.Attempt = 0
		cp.NextAttempt = now
		cp.CreatedAt = now
		cp.Status = store.StatusPending
		cp.Owner = ""
		cp.Generation = 0
		cp.ClaimedAt = time.Time{}
		cp.LastError = ""
		cp.Partition = 0

		q.byID[cp.ID] = &cp
	}
	q.signal()
	return nil
}

// Claim returns up to limit due, HOL-eligible rows in global created_at order.
func (q *queue) Claim(ctx context.Context, owner string, generation uint64, limit int) ([]store.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, fmt.Errorf("fifo queue: claim requires owner")
	}
	if generation == 0 {
		return nil, fmt.Errorf("fifo queue: claim requires non-zero generation")
	}
	if limit <= 0 {
		return nil, nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()

	// Earliest unfinished sequence per subscriber (pending or in_flight).
	head := make(map[string]int64)
	for _, d := range q.byID {
		if d.Status != store.StatusPending && d.Status != store.StatusInFlight {
			continue
		}
		if cur, ok := head[d.SubscriberID]; !ok || d.Sequence < cur {
			head[d.SubscriberID] = d.Sequence
		}
	}

	type cand struct {
		id  string
		at  time.Time
		seq int64
	}
	var candidates []cand
	for id, d := range q.byID {
		if d.Status != store.StatusPending {
			continue
		}
		if !d.NextAttempt.IsZero() && d.NextAttempt.After(now) {
			continue
		}
		if head[d.SubscriberID] != d.Sequence {
			continue
		}
		candidates = append(candidates, cand{id: id, at: d.CreatedAt, seq: d.Sequence})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].at.Equal(candidates[j].at) {
			return candidates[i].at.Before(candidates[j].at)
		}
		return candidates[i].seq < candidates[j].seq
	})

	out := make([]store.Delivery, 0, limit)
	claimedSubs := make(map[string]struct{})
	for _, c := range candidates {
		if len(out) >= limit {
			break
		}
		d := q.byID[c.id]
		if head[d.SubscriberID] != d.Sequence {
			continue
		}
		if _, taken := claimedSubs[d.SubscriberID]; taken {
			continue
		}
		d.Status = store.StatusInFlight
		d.Owner = owner
		d.Generation = generation
		d.ClaimedAt = now
		d.Attempt++
		claimedSubs[d.SubscriberID] = struct{}{}
		out = append(out, cloneDelivery(d))
	}
	return out, nil
}

func (q *queue) MarkDelivered(ctx context.Context, id, owner string, generation uint64) error {
	return q.markTerminal(ctx, id, owner, generation, store.StatusDelivered, "")
}

func (q *queue) MarkDeadLetter(ctx context.Context, id, owner string, generation uint64, errMsg string) error {
	return q.markTerminal(ctx, id, owner, generation, store.StatusDeadLettered, errMsg)
}

func (q *queue) MarkFailed(ctx context.Context, id, owner string, generation uint64, attempt int, next time.Time, errMsg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	d, err := q.requireOwner(id, owner, generation)
	if err != nil {
		return err
	}
	d.Status = store.StatusPending
	d.Attempt = attempt
	d.NextAttempt = next
	d.LastError = errMsg
	d.Owner = ""
	d.Generation = 0
	d.ClaimedAt = time.Time{}
	q.signal()
	return nil
}

func (q *queue) PendingOrInFlight() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, d := range q.byID {
		if d.Status == store.StatusPending || d.Status == store.StatusInFlight {
			n++
		}
	}
	return n
}

func (q *queue) markTerminal(ctx context.Context, id, owner string, generation uint64, status store.Status, errMsg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	d, err := q.requireOwner(id, owner, generation)
	if err != nil {
		return err
	}
	d.Status = status
	d.LastError = errMsg
	d.Owner = ""
	d.Generation = 0
	d.ClaimedAt = time.Time{}
	q.signal()
	return nil
}

func (q *queue) requireOwner(id, owner string, generation uint64) (*store.Delivery, error) {
	if generation == 0 {
		return nil, fmt.Errorf("fifo queue: mark requires non-zero generation")
	}
	d, ok := q.byID[id]
	if !ok {
		return nil, fmt.Errorf("fifo queue: unknown delivery %s", id)
	}
	if d.Status != store.StatusInFlight {
		return nil, fmt.Errorf("fifo queue: delivery %s not in_flight", id)
	}
	if d.Owner != owner || d.Generation != generation {
		return nil, fmt.Errorf("fifo queue: stale claim on %s", id)
	}
	return d, nil
}

func cloneDelivery(d *store.Delivery) store.Delivery {
	cp := *d
	if d.Payload != nil {
		cp.Payload = append([]byte(nil), d.Payload...)
	}
	if d.Secret != nil {
		cp.Secret = append([]byte(nil), d.Secret...)
	}
	return cp
}
