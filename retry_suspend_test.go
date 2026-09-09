package fold_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/store"
	"github.com/sharathb5/sharathfold/store/memory"
)

// flakyTransport fails the first failTimes attempts for a subscriber, then succeeds.
type flakyTransport struct {
	mu        sync.Mutex
	failTimes int
	seen      map[string]int
	calls     []store.Delivery
}

func newFlakyTransport(failTimes int) *flakyTransport {
	return &flakyTransport{
		failTimes: failTimes,
		seen:      make(map[string]int),
	}
}

func (f *flakyTransport) Deliver(ctx context.Context, d store.Delivery) fold.Result {
	if err := ctx.Err(); err != nil {
		return fold.Result{Outcome: fold.OutcomeRetryable, Err: err}
	}
	f.mu.Lock()
	n := f.seen[d.ID]
	f.seen[d.ID] = n + 1
	cp := d
	if d.Payload != nil {
		cp.Payload = append([]byte(nil), d.Payload...)
	}
	f.calls = append(f.calls, cp)
	fail := n < f.failTimes
	f.mu.Unlock()
	if fail {
		return fold.Result{Outcome: fold.OutcomeRetryable, Err: fmt.Errorf("flaky")}
	}
	return fold.Result{Outcome: fold.OutcomeSuccess}
}

func (f *flakyTransport) Calls() []store.Delivery {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.Delivery, len(f.calls))
	copy(out, f.calls)
	return out
}

type permanentTransport struct {
	calls atomic.Int64
}

func (p *permanentTransport) Deliver(ctx context.Context, d store.Delivery) fold.Result {
	p.calls.Add(1)
	return fold.Result{Outcome: fold.OutcomePermanent, Err: fmt.Errorf("gone"), StatusCode: 410}
}

func TestRetryThenSucceed(t *testing.T) {
	mem := memory.New()
	tr := newFlakyTransport(2) // fail twice, succeed on 3rd
	d, err := fold.New(fold.Config{
		Store:       mem,
		Transport:   tr,
		Workers:     1,
		Partitions:  8,
		MaxAttempts: 5,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}

	payload, _ := json.Marshal(map[string]int{"n": 1})
	if err := d.Dispatch(ctx, fold.Event{ID: "evt-1", Type: "t", Payload: payload},
		[]fold.Subscriber{{ID: "sub-1", URL: "https://example.test/hook"}}); err != nil {
		t.Fatal(err)
	}

	closeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := d.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	calls := tr.Calls()
	if len(calls) != 3 {
		t.Fatalf("attempts=%d want 3", len(calls))
	}
	suspended, err := d.Suspended(ctx, "sub-1")
	if err != nil {
		t.Fatal(err)
	}
	if suspended {
		t.Fatal("subscriber should not be suspended after success")
	}
}

func TestExhaustSuspendsSubscriber(t *testing.T) {
	mem := memory.New()
	var suspendedID string
	tr := newFlakyTransport(100) // always fail
	d, err := fold.New(fold.Config{
		Store:       mem,
		Transport:   tr,
		Workers:     1,
		Partitions:  8,
		MaxAttempts: 3,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  2 * time.Millisecond,
		OnSuspend:   func(id string) { suspendedID = id },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}

	payload, _ := json.Marshal(map[string]int{"n": 1})
	subs := []fold.Subscriber{{ID: "sub-ex", URL: "https://example.test/hook"}}
	if err := d.Dispatch(ctx, fold.Event{ID: "evt-1", Type: "t", Payload: payload}, subs); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ok, _ := d.Suspended(ctx, "sub-ex")
		if ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	ok, err := d.Suspended(ctx, "sub-ex")
	if err != nil || !ok {
		t.Fatalf("want suspended; ok=%v err=%v", ok, err)
	}
	if suspendedID != "sub-ex" {
		t.Fatalf("OnSuspend got %q", suspendedID)
	}

	// Further dispatches must not be delivered.
	before := len(tr.Calls())
	for i := 0; i < 5; i++ {
		p, _ := json.Marshal(map[string]int{"n": i + 2})
		if err := d.Dispatch(ctx, fold.Event{ID: fmt.Sprintf("evt-%d", i+2), Type: "t", Payload: p}, subs); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	after := len(tr.Calls())
	if after != before {
		t.Fatalf("delivered while suspended: before=%d after=%d", before, after)
	}
	if mem.RetainedCount("sub-ex") != 5 {
		t.Fatalf("retained=%d want 5", mem.RetainedCount("sub-ex"))
	}

	_ = d.Close(context.Background())
}

func TestPermanentFailureSuspends(t *testing.T) {
	mem := memory.New()
	tr := &permanentTransport{}
	d, err := fold.New(fold.Config{
		Store:      mem,
		Transport:  tr,
		Workers:    1,
		Partitions: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]int{"n": 1})
	if err := d.Dispatch(ctx, fold.Event{ID: "evt-p", Type: "t", Payload: payload},
		[]fold.Subscriber{{ID: "sub-p", URL: "https://example.test/hook"}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ok, _ := d.Suspended(ctx, "sub-p")
		if ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	ok, _ := d.Suspended(ctx, "sub-p")
	if !ok {
		t.Fatal("expected suspend after permanent failure")
	}
	_ = d.Close(context.Background())
}

func TestRetentionGapOnResume(t *testing.T) {
	mem := memory.New()
	mem.RetentionMaxCount = 2
	tr := &permanentTransport{}
	d, err := fold.New(fold.Config{
		Store:             mem,
		Transport:         tr,
		Workers:           1,
		Partitions:        4,
		RetentionMaxCount: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	subs := []fold.Subscriber{{ID: "sub-gap", URL: "https://example.test/hook"}}
	payload, _ := json.Marshal(map[string]int{"n": 0})
	if err := d.Dispatch(ctx, fold.Event{ID: "evt-0", Type: "t", Payload: payload}, subs); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ok, _ := d.Suspended(ctx, "sub-gap")
		if ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	for i := 1; i <= 5; i++ {
		p, _ := json.Marshal(map[string]int{"n": i})
		if err := d.Dispatch(ctx, fold.Event{ID: fmt.Sprintf("evt-%d", i), Type: "t", Payload: p}, subs); err != nil {
			t.Fatal(err)
		}
	}
	if got := mem.RetainedCount("sub-gap"); got != 2 {
		t.Fatalf("retained=%d want 2", got)
	}

	res, err := d.Resume(ctx, "sub-gap")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Gap || res.Dropped < 1 || res.Marker == "" {
		t.Fatalf("want gap with marker; got %+v", res)
	}
	_ = d.Close(context.Background())
}

func TestSuspendResumeOrdering(t *testing.T) {
	t.Run("gate_on_no_delivery_while_suspended_then_ordered_replay", func(t *testing.T) {
		inv, during, after := runSuspendResume(t, false)
		t.Logf("suspend gate on: during_suspend=%d after_resume=%d inversions=%d", during, after, inv)
		if during != 0 {
			t.Fatalf("expected 0 deliveries while suspended, got %d", during)
		}
		if inv != 0 {
			t.Fatalf("expected 0 inversions after resume, got %d", inv)
		}
		if after == 0 {
			t.Fatal("expected deliveries after resume")
		}
	})
	t.Run("gate_off_delivers_while_suspended", func(t *testing.T) {
		_, during, _ := runSuspendResume(t, true)
		t.Logf("suspend gate off: during_suspend=%d", during)
		if during == 0 {
			t.Fatal("expected deliveries while suspended with gate off (test is toothless)")
		}
	})
}

func runSuspendResume(t *testing.T, skipSuspendGate bool) (inversions, duringSuspend, afterResume int) {
	t.Helper()

	const subID = "sub-sr"
	mem := memory.New()
	mem.SkipSuspendGate = skipSuspendGate

	// First event fails permanently → suspend. Then we dispatch more.
	failOnce := &failFirstThenRecord{}
	d, err := fold.New(fold.Config{
		Store:       mem,
		Transport:   failOnce,
		Workers:     2,
		Partitions:  16,
		MaxAttempts: 1,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}

	subs := []fold.Subscriber{{ID: subID, URL: "https://example.test/hook"}}
	payload, _ := json.Marshal(map[string]int{"n": 0})
	if err := d.Dispatch(ctx, fold.Event{ID: "evt-0000", Type: "t", Payload: payload}, subs); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ok, _ := d.Suspended(ctx, subID)
		if ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	ok, _ := d.Suspended(ctx, subID)
	if !ok {
		_ = d.Close(context.Background())
		t.Fatal("subscriber never suspended")
	}

	failOnce.ResetRecording()
	const extra = 12
	for i := 1; i <= extra; i++ {
		p, _ := json.Marshal(map[string]int{"n": i})
		if err := d.Dispatch(ctx, fold.Event{
			ID:      fmt.Sprintf("evt-%04d", i),
			Type:    "t",
			Payload: p,
		}, subs); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(80 * time.Millisecond)
	duringSuspend = len(failOnce.Calls())

	if !skipSuspendGate {
		res, err := d.Resume(ctx, subID)
		if err != nil {
			t.Fatal(err)
		}
		if res.Gap {
			t.Fatalf("unexpected gap: %+v", res)
		}

		closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := d.Close(closeCtx); err != nil {
			t.Fatalf("Close: %v", err)
		}

		calls := failOnce.Calls()
		afterResume = len(calls)
		for i := 1; i < len(calls); i++ {
			if calls[i].Sequence <= calls[i-1].Sequence {
				inversions++
			}
		}
		return inversions, duringSuspend, afterResume
	}

	// Gate off: wait briefly for leak deliveries, then close.
	time.Sleep(50 * time.Millisecond)
	duringSuspend = len(failOnce.Calls())
	_ = d.Close(context.Background())
	return 0, duringSuspend, 0
}

// failFirstThenRecord permanently fails the first Deliver, then records successes.
type failFirstThenRecord struct {
	mu        sync.Mutex
	failed    bool
	recording bool
	calls     []store.Delivery
}

func (f *failFirstThenRecord) Deliver(ctx context.Context, d store.Delivery) fold.Result {
	if err := ctx.Err(); err != nil {
		return fold.Result{Outcome: fold.OutcomeRetryable, Err: err}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.failed {
		f.failed = true
		return fold.Result{Outcome: fold.OutcomePermanent, Err: fmt.Errorf("boom")}
	}
	if f.recording {
		cp := d
		if d.Payload != nil {
			cp.Payload = append([]byte(nil), d.Payload...)
		}
		f.calls = append(f.calls, cp)
	}
	return fold.Result{Outcome: fold.OutcomeSuccess}
}

func (f *failFirstThenRecord) ResetRecording() {
	f.mu.Lock()
	f.recording = true
	f.calls = nil
	f.mu.Unlock()
}

func (f *failFirstThenRecord) Calls() []store.Delivery {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.Delivery, len(f.calls))
	copy(out, f.calls)
	return out
}
