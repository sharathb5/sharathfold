package fold

import (
	"context"
	"sort"
	"sync/atomic"
	"time"

	"github.com/sharathb5/sharathfold/internal/backoff"
	"github.com/sharathb5/sharathfold/store"
)

const claimBatch = 64

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

func (d *Dispatcher) runWorker(ctx context.Context, owner string) {
	idle := time.NewTimer(time.Hour)
	defer idle.Stop()
	stats := d.workerCounter(owner)

	for {
		if ctx.Err() != nil {
			return
		}

		batch, err, active := d.claimBatch(ctx, owner)
		if !active {
			return
		}
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

// claimBatch reads ownership and Claims under claimGate so Resize cannot flip
// the map between the read and the Claim.
func (d *Dispatcher) claimBatch(ctx context.Context, owner string) ([]store.Delivery, error, bool) {
	d.claimGate.RLock()
	defer d.claimGate.RUnlock()

	d.mu.Lock()
	idx, ok := parseOwnerIndex(owner)
	if !ok || idx < 0 || idx >= d.workers {
		d.mu.Unlock()
		return nil, nil, false
	}
	parts := append([]int(nil), d.own.partitionsFor(owner)...)
	gen := d.generation
	d.mu.Unlock()

	batch, err := d.store.Claim(ctx, owner, gen, parts, claimBatch)
	return batch, err, true
}

func (d *Dispatcher) deliverOne(ctx context.Context, owner string, del store.Delivery) {
	res := d.transport.Deliver(ctx, del)
	// Marks use claim-time generation so an in-flight delivery can complete
	// after Resize bumps the dispatcher's generation (D7 handoff window).
	gen := del.Generation
	switch res.Outcome {
	case OutcomeSuccess:
		_ = d.store.MarkDelivered(ctx, del.ID, owner, gen)
	case OutcomePermanent:
		d.exhaust(ctx, owner, del, res.Error())
	default:
		if del.Attempt >= d.maxAttempts {
			d.exhaust(ctx, owner, del, res.Error())
			return
		}
		next := time.Now().Add(backoff.Delay(del.Attempt, d.baseBackoff, d.maxBackoff))
		_ = d.store.MarkFailed(ctx, del.ID, owner, gen, del.Attempt, next, res.Error())
	}
}

func (d *Dispatcher) exhaust(ctx context.Context, owner string, del store.Delivery, errMsg string) {
	if err := d.store.ExhaustAndSuspend(ctx, del.ID, owner, del.Generation, errMsg); err != nil {
		return
	}
	if d.onSuspend != nil {
		d.onSuspend(del.SubscriberID)
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
	case <-d.notifyCh():
		return true
	case <-idle.C:
		return true
	}
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
