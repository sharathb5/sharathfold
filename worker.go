package fold

import (
	"context"
	"time"

	"github.com/sharathb5/sharathfold/store"
)

const claimBatch = 64

func (d *Dispatcher) runWorker(ctx context.Context) {
	defer close(d.workerDone)

	idle := time.NewTimer(time.Hour)
	defer idle.Stop()

	for {
		if ctx.Err() != nil {
			return
		}

		batch, err := d.store.Claim(ctx, d.owner, d.generation, nil, claimBatch)
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

		for _, del := range batch {
			if ctx.Err() != nil {
				return
			}
			d.deliverOne(ctx, del)
		}
	}
}

func (d *Dispatcher) deliverOne(ctx context.Context, del store.Delivery) {
	err := d.transport.Deliver(ctx, del)
	if err != nil {
		// Retries are a later step. Park the row so HOL is not jammed forever
		// if a test injects transport errors.
		_ = d.store.MarkFailed(ctx, del.ID, d.owner, d.generation, del.Attempt, time.Now().Add(time.Hour), err.Error())
		return
	}
	_ = d.store.MarkDelivered(ctx, del.ID, d.owner, d.generation)
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
