package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sharathb5/sharathfold/store"
)

const (
	// DefaultRetentionMaxCount caps retained deliveries per suspended subscriber.
	DefaultRetentionMaxCount = 1000
	// DefaultRetentionMaxAge drops retained deliveries older than this.
	DefaultRetentionMaxAge = 24 * time.Hour
)

type subMeta struct {
	suspended bool
	gap       bool
	dropped   int
	marker    string
}

// Store is an in-memory map-and-mutex Store.
type Store struct {
	mu     sync.Mutex
	byID   map[string]*store.Delivery
	seq    map[string]int64 // next sequence per subscriber
	subs   map[string]*subMeta
	notify chan struct{}

	// RetentionMaxCount / RetentionMaxAge bound retained backlog per
	// suspended subscriber. Zero selects the defaults above.
	RetentionMaxCount int
	RetentionMaxAge   time.Duration

	// DisableHOL skips head-of-line gating. For invariant tests only.
	DisableHOL bool

	// SkipSuspendGate makes Claim ignore suspension and Enqueue always insert
	// pending rows. For invariant tests only — proves "nothing while suspended."
	SkipSuspendGate bool

	// ClaimCalls / ClaimRows / ClaimNonEmpty accumulate Claim traffic for
	// benchmarks. Safe to read after workers have stopped.
	ClaimCalls    int64
	ClaimRows     int64
	ClaimNonEmpty int64
}

// New returns an empty memory store.
func New() *Store {
	return &Store{
		byID:   make(map[string]*store.Delivery),
		seq:    make(map[string]int64),
		subs:   make(map[string]*subMeta),
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

func (s *Store) meta(subscriberID string) *subMeta {
	m, ok := s.subs[subscriberID]
	if !ok {
		m = &subMeta{}
		s.subs[subscriberID] = m
	}
	return m
}

func (s *Store) retentionCount() int {
	if s.RetentionMaxCount > 0 {
		return s.RetentionMaxCount
	}
	return DefaultRetentionMaxCount
}

func (s *Store) retentionAge() time.Duration {
	if s.RetentionMaxAge > 0 {
		return s.RetentionMaxAge
	}
	return DefaultRetentionMaxAge
}

// Enqueue implements store.Store.
func (s *Store) Enqueue(ctx context.Context, ds []store.Delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	touched := map[string]struct{}{}
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
		if d.Secret != nil {
			cp.Secret = append([]byte(nil), d.Secret...)
		}
		cp.Sequence = seq
		cp.Attempt = 0
		cp.NextAttempt = now
		cp.CreatedAt = now
		cp.Owner = ""
		cp.Generation = 0
		cp.ClaimedAt = time.Time{}
		cp.LastError = ""

		if !s.SkipSuspendGate && s.meta(d.SubscriberID).suspended {
			cp.Status = store.StatusRetained
			touched[d.SubscriberID] = struct{}{}
		} else {
			cp.Status = store.StatusPending
		}

		s.byID[cp.ID] = &cp
	}
	for sub := range touched {
		s.applyRetentionLocked(sub, now)
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
	if generation == 0 {
		return nil, fmt.Errorf("memory store: claim requires non-zero generation")
	}
	if limit <= 0 {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	s.ClaimCalls++
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
		if !s.SkipSuspendGate && s.meta(d.SubscriberID).suspended {
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
	s.ClaimRows += int64(len(out))
	if len(out) > 0 {
		s.ClaimNonEmpty++
	}
	return out, nil
}

// ClaimStats returns cumulative Claim counters (calls, rows, nonempty calls).
func (s *Store) ClaimStats() (calls, rows, nonempty int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ClaimCalls, s.ClaimRows, s.ClaimNonEmpty
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

// ExhaustAndSuspend implements store.Store.
func (s *Store) ExhaustAndSuspend(ctx context.Context, id string, owner string, generation uint64, errMsg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	d, err := s.requireOwner(id, owner, generation)
	if err != nil {
		return err
	}
	subID := d.SubscriberID
	d.Status = store.StatusDeadLettered
	d.LastError = errMsg
	d.Owner = ""
	d.Generation = 0
	d.ClaimedAt = time.Time{}

	m := s.meta(subID)
	m.suspended = true

	now := time.Now()
	for _, row := range s.byID {
		if row.SubscriberID != subID {
			continue
		}
		if row.Status == store.StatusPending {
			row.Status = store.StatusRetained
			row.Owner = ""
			row.Generation = 0
			row.ClaimedAt = time.Time{}
		}
	}
	s.applyRetentionLocked(subID, now)
	s.signal()
	return nil
}

// Resume implements store.Store.
func (s *Store) Resume(ctx context.Context, subscriberID string) (store.GapInfo, error) {
	if err := ctx.Err(); err != nil {
		return store.GapInfo{}, err
	}
	if subscriberID == "" {
		return store.GapInfo{}, fmt.Errorf("memory store: resume requires subscriberID")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	s.applyRetentionLocked(subscriberID, now)

	m := s.meta(subscriberID)
	gap := store.GapInfo{
		Occurred: m.gap,
		Dropped:  m.dropped,
		Marker:   m.marker,
	}

	var retained []*store.Delivery
	for _, d := range s.byID {
		if d.SubscriberID == subscriberID && d.Status == store.StatusRetained {
			retained = append(retained, d)
		}
	}
	sort.Slice(retained, func(i, j int) bool {
		return retained[i].Sequence < retained[j].Sequence
	})

	for _, d := range retained {
		d.Status = store.StatusPending
		d.NextAttempt = now
		d.Owner = ""
		d.Generation = 0
		d.ClaimedAt = time.Time{}
	}

	m.suspended = false
	m.gap = false
	m.dropped = 0
	m.marker = ""

	s.signal()
	return gap, nil
}

// IsSuspended implements store.Store.
func (s *Store) IsSuspended(ctx context.Context, subscriberID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.subs[subscriberID]
	if !ok {
		return false, nil
	}
	return m.suspended, nil
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

// PendingOrInFlight reports how many non-terminal, non-retained deliveries remain.
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

// RetainedCount reports how many deliveries are held for suspended subscribers.
func (s *Store) RetainedCount(subscriberID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, d := range s.byID {
		if d.SubscriberID == subscriberID && d.Status == store.StatusRetained {
			n++
		}
	}
	return n
}

func (s *Store) applyRetentionLocked(subscriberID string, now time.Time) {
	maxCount := s.retentionCount()
	maxAge := s.retentionAge()
	cutoff := now.Add(-maxAge)

	var retained []*store.Delivery
	for _, d := range s.byID {
		if d.SubscriberID != subscriberID || d.Status != store.StatusRetained {
			continue
		}
		if !d.CreatedAt.IsZero() && d.CreatedAt.Before(cutoff) {
			s.dropRetainedLocked(d, "age")
			continue
		}
		retained = append(retained, d)
	}

	sort.Slice(retained, func(i, j int) bool {
		return retained[i].Sequence < retained[j].Sequence
	})
	for len(retained) > maxCount {
		s.dropRetainedLocked(retained[0], "count")
		retained = retained[1:]
	}
}

func (s *Store) dropRetainedLocked(d *store.Delivery, reason string) {
	m := s.meta(d.SubscriberID)
	m.gap = true
	m.dropped++
	m.marker = fmt.Sprintf("%s:dropped=%d:through_seq=%d:%s", d.SubscriberID, m.dropped, d.Sequence, reason)
	delete(s.byID, d.ID)
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
	if generation == 0 {
		return nil, fmt.Errorf("memory store: mark requires non-zero generation")
	}
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
	if d.Secret != nil {
		cp.Secret = append([]byte(nil), d.Secret...)
	}
	return cp
}

// BackdateClaimForTest sets claimed_at on an in_flight row. For crash-recovery tests.
func (s *Store) BackdateClaimForTest(id string, claimedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.byID[id]
	if !ok {
		return fmt.Errorf("memory store: unknown delivery %s", id)
	}
	if d.Status != store.StatusInFlight {
		return fmt.Errorf("memory store: %s is not in_flight", id)
	}
	d.ClaimedAt = claimedAt
	return nil
}
