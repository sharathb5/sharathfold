package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sharathb5/sharathfold/store"
)

// Store is an in-memory map-and-mutex Store.
type Store struct {
	mu     sync.Mutex
	byID   map[string]*store.Delivery
	seq    map[string]int64 // next sequence per subscriber
	notify chan struct{}

	// DisableHOL skips head-of-line gating. For invariant tests only.
	DisableHOL bool
}

// New returns an empty memory store.
func New() *Store {
	return &Store{
		byID:   make(map[string]*store.Delivery),
		seq:    make(map[string]int64),
		notify: make(chan struct{}, 1),
	}
}

// Notify returns a channel that receives a signal after Enqueue and terminal
// marks. Workers may select on it to avoid busy-polling. Never closed.
func (s *Store) Notify() <-chan struct{} {
	return s.notify
}

func (s *Store) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// Enqueue implements store.Store.
func (s *Store) Enqueue(ctx context.Context, ds []store.Delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for i := range ds {
		d := ds[i]
		if d.ID == "" {
			return fmt.Errorf("memory store: delivery missing ID")
		}
		if d.SubscriberID == "" {
			return fmt.Errorf("memory store: delivery %s missing SubscriberID", d.ID)
		}
		if _, exists := s.byID[d.ID]; exists {
			return fmt.Errorf("memory store: duplicate delivery ID %s", d.ID)
		}

		s.seq[d.SubscriberID]++
		seq := s.seq[d.SubscriberID]

		cp := d
		if d.Payload != nil {
			cp.Payload = append([]byte(nil), d.Payload...)
		}
		cp.Sequence = seq
		cp.Status = store.StatusPending
		cp.Attempt = 0
		cp.NextAttempt = now
		cp.CreatedAt = now
		cp.Owner = ""
		cp.Generation = 0
		cp.ClaimedAt = time.Time{}
		cp.LastError = ""

		s.byID[cp.ID] = &cp
	}
	s.signal()
	return nil
}

type candidate struct {
	id  string
	seq int64
	sub string
}

// Claim implements store.Store with head-of-line gating per subscriber.
func (s *Store) Claim(ctx context.Context, owner string, generation uint64, partitions []int, limit int) ([]store.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, fmt.Errorf("memory store: claim requires owner")
	}
	if limit <= 0 {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	partSet := partitionSet(partitions)
	if partSet == nil {
		// Workers must pass the partitions they own. Empty means claim nothing
		// (avoids accidental all-partition claims under a multi-worker pool).
		return nil, nil
	}

	// Earliest unfinished sequence per subscriber (pending or in_flight).
	head := make(map[string]int64)
	if !s.DisableHOL {
		for _, d := range s.byID {
			if d.Status != store.StatusPending && d.Status != store.StatusInFlight {
				continue
			}
			if cur, ok := head[d.SubscriberID]; !ok || d.Sequence < cur {
				head[d.SubscriberID] = d.Sequence
			}
		}
	}

	var candidates []candidate
	for id, d := range s.byID {
		if d.Status != store.StatusPending {
			continue
		}
		if !d.NextAttempt.IsZero() && d.NextAttempt.After(now) {
			continue
		}
		if _, ok := partSet[d.Partition]; !ok {
			continue
		}
		if !s.DisableHOL && head[d.SubscriberID] != d.Sequence {
			continue
		}
		candidates = append(candidates, candidate{id: id, seq: d.Sequence, sub: d.SubscriberID})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].sub != candidates[j].sub {
			return candidates[i].sub < candidates[j].sub
		}
		return candidates[i].seq < candidates[j].seq
	})

	out := make([]store.Delivery, 0, limit)
	claimedSubs := make(map[string]struct{})
	for _, c := range candidates {
		if len(out) >= limit {
			break
		}
		if !s.DisableHOL {
			if _, taken := claimedSubs[c.sub]; taken {
				continue
			}
		}
		d := s.byID[c.id]
		if !s.DisableHOL && head[d.SubscriberID] != d.Sequence {
			continue
		}
		d.Status = store.StatusInFlight
		d.Owner = owner
		d.Generation = generation
		d.ClaimedAt = now
		d.Attempt++
		if !s.DisableHOL {
			claimedSubs[d.SubscriberID] = struct{}{}
		}
		out = append(out, cloneDelivery(d))
	}
	return out, nil
}

// MarkDelivered implements store.Store.
func (s *Store) MarkDelivered(ctx context.Context, id string, owner string, generation uint64) error {
	return s.markTerminal(ctx, id, owner, generation, store.StatusDelivered, "")
}

// MarkDeadLetter implements store.Store.
func (s *Store) MarkDeadLetter(ctx context.Context, id string, owner string, generation uint64, errMsg string) error {
	return s.markTerminal(ctx, id, owner, generation, store.StatusDeadLettered, errMsg)
}

// MarkFailed implements store.Store — returns the row to pending with a retry time.
func (s *Store) MarkFailed(ctx context.Context, id string, owner string, generation uint64, attempt int, next time.Time, errMsg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	d, err := s.requireOwner(id, owner, generation)
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
	s.signal()
	return nil
}

// RecoverStale implements store.Store.
func (s *Store) RecoverStale(ctx context.Context, olderThan time.Duration) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)
	n := 0
	for _, d := range s.byID {
		if d.Status != store.StatusInFlight {
			continue
		}
		if d.ClaimedAt.IsZero() || d.ClaimedAt.After(cutoff) {
			continue
		}
		d.Status = store.StatusPending
		d.Owner = ""
		d.Generation = 0
		d.ClaimedAt = time.Time{}
		n++
	}
	if n > 0 {
		s.signal()
	}
	return n, nil
}

// PendingOrInFlight reports how many non-terminal deliveries remain.
func (s *Store) PendingOrInFlight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, d := range s.byID {
		if d.Status == store.StatusPending || d.Status == store.StatusInFlight {
			n++
		}
	}
	return n
}

func (s *Store) markTerminal(ctx context.Context, id, owner string, generation uint64, status store.Status, errMsg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	d, err := s.requireOwner(id, owner, generation)
	if err != nil {
		return err
	}
	d.Status = status
	d.LastError = errMsg
	d.Owner = ""
	d.Generation = 0
	d.ClaimedAt = time.Time{}
	s.signal()
	return nil
}

func (s *Store) requireOwner(id, owner string, generation uint64) (*store.Delivery, error) {
	d, ok := s.byID[id]
	if !ok {
		return nil, fmt.Errorf("memory store: unknown delivery %s", id)
	}
	if d.Status != store.StatusInFlight {
		return nil, fmt.Errorf("memory store: delivery %s not in_flight", id)
	}
	if d.Owner != owner || d.Generation != generation {
		return nil, fmt.Errorf("memory store: stale claim on %s (owner/generation mismatch)", id)
	}
	return d, nil
}

func partitionSet(partitions []int) map[int]struct{} {
	if len(partitions) == 0 {
		return nil
	}
	m := make(map[int]struct{}, len(partitions))
	for _, p := range partitions {
		m[p] = struct{}{}
	}
	return m
}

func cloneDelivery(d *store.Delivery) store.Delivery {
	cp := *d
	if d.Payload != nil {
		cp.Payload = append([]byte(nil), d.Payload...)
	}
	return cp
}
