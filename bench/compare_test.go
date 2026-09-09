package bench_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/bench/fifo"
	"github.com/sharathb5/sharathfold/bench/naive"
	"github.com/sharathb5/sharathfold/store/memory"
)

// Workload sized to surface ordering races under endpoint latency variance
// without making CI painful. Latency delays are per-profile (see latencyProfile).
const (
	benchSubscribers = 64
	benchEvents      = 40 // total deliveries = 2560
	benchWorkers     = 16
	benchFailEvery   = 17

	// Default mix used by TestCompareFoldVsNaive (yesterday's setup).
	benchSlowEvery = 4 // every Nth subscriber is slow
	benchSlowDelay = 25 * time.Millisecond
	benchFastDelay = time.Millisecond
)

// latencyProfile controls endpoint sleep. SlowEvery == 0 means every subscriber
// uses FastDelay (uniform). SlowDelay is ignored in that case.
type latencyProfile struct {
	Name      string
	FastDelay time.Duration
	SlowDelay time.Duration
	SlowEvery int
}

func (p latencyProfile) delayFor(sub string) time.Duration {
	if p.isSlow(sub) {
		return p.SlowDelay
	}
	return p.FastDelay
}

func (p latencyProfile) isSlow(sub string) bool {
	if p.SlowEvery <= 0 {
		return false
	}
	var n int
	_, _ = fmt.Sscanf(sub, "sub-%d", &n)
	return n%p.SlowEvery == 0
}

func (p latencyProfile) slowCount(nSubs int) int {
	if p.SlowEvery <= 0 {
		return 0
	}
	n := 0
	for i := 0; i < nSubs; i++ {
		if p.isSlow(fmt.Sprintf("sub-%d", i)) {
			n++
		}
	}
	return n
}

// latencySweep walks from uniform endpoints through increasing slow/fast
// spread. Fast delay, subscriber fraction, and all other workload knobs stay
// fixed — only SlowDelay grows. Not tuned for a particular curve shape.
var latencySweep = []latencyProfile{
	{Name: "uniform-1ms", FastDelay: time.Millisecond, SlowDelay: time.Millisecond, SlowEvery: 0},
	{Name: "spread-5ms", FastDelay: time.Millisecond, SlowDelay: 5 * time.Millisecond, SlowEvery: 4},
	{Name: "spread-12ms", FastDelay: time.Millisecond, SlowDelay: 12 * time.Millisecond, SlowEvery: 4},
	{Name: "spread-25ms", FastDelay: time.Millisecond, SlowDelay: 25 * time.Millisecond, SlowEvery: 4}, // current mix
	{Name: "spread-50ms", FastDelay: time.Millisecond, SlowDelay: 50 * time.Millisecond, SlowEvery: 4},
	{Name: "spread-100ms", FastDelay: time.Millisecond, SlowDelay: 100 * time.Millisecond, SlowEvery: 4},
}

type receipt struct {
	sub     string
	seq     int64
	arrived time.Time
}

type recorder struct {
	mu       sync.Mutex
	receipts []receipt
	fails    sync.Map // "sub:seq" → already failed once
	profile  latencyProfile
}

func (rec *recorder) serve(w http.ResponseWriter, r *http.Request) {
	sub := r.URL.Path[len("/hook/"):]
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read", http.StatusBadRequest)
		return
	}
	var wire struct {
		Data struct {
			Seq int64 `json:"seq"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		http.Error(w, "json", http.StatusBadRequest)
		return
	}
	seq := wire.Data.Seq
	if sub == "" || seq <= 0 {
		http.Error(w, "missing sub/seq", http.StatusBadRequest)
		return
	}

	key := fmt.Sprintf("%s:%d", sub, seq)
	if benchFailEvery > 0 && (hashKey(key)%benchFailEvery) == 0 {
		if _, seen := rec.fails.LoadOrStore(key, true); !seen {
			http.Error(w, "transient", http.StatusInternalServerError)
			return
		}
	}

	time.Sleep(rec.profile.delayFor(sub))

	arrived := time.Now()
	rec.mu.Lock()
	rec.receipts = append(rec.receipts, receipt{sub: sub, seq: seq, arrived: arrived})
	rec.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func hashKey(s string) int {
	h := 0
	for i := 0; i < len(s); i++ {
		h = 31*h + int(s[i])
	}
	if h < 0 {
		h = -h
	}
	return h
}

type runResult struct {
	Name       string
	Profile    string
	Deliveries int
	Duration   time.Duration
	Throughput float64 // deliveries / second
	Inversions int
	LatencyP50 time.Duration
	LatencyP99 time.Duration
	SlowSubs   int

	ClaimCalls    int64
	ClaimRows     int64
	ClaimNonEmpty int64
}

func (r runResult) rowsPerClaim() float64 {
	if r.ClaimNonEmpty == 0 {
		return 0
	}
	return float64(r.ClaimRows) / float64(r.ClaimNonEmpty)
}

func (r runResult) String() string {
	base := fmt.Sprintf(
		"%s [%s]: deliveries=%d duration=%s throughput=%.1f/s inversions=%d p50=%s p99=%s",
		r.Name, r.Profile, r.Deliveries, r.Duration.Round(time.Millisecond), r.Throughput,
		r.Inversions, r.LatencyP50.Round(time.Microsecond), r.LatencyP99.Round(time.Microsecond),
	)
	if r.ClaimCalls == 0 {
		return base
	}
	return fmt.Sprintf("%s claims=%d nonempty=%d rows/claim=%.1f",
		base, r.ClaimCalls, r.ClaimNonEmpty, r.rowsPerClaim())
}

type dispatcher interface {
	Start(ctx context.Context) error
	Dispatch(ctx context.Context, ev fold.Event, subs []fold.Subscriber) error
	Close(ctx context.Context) error
}

func TestCompareFoldVsNaive(t *testing.T) {
	profile := latencyProfile{
		Name:      "spread-25ms",
		FastDelay: benchFastDelay,
		SlowDelay: benchSlowDelay,
		SlowEvery: benchSlowEvery,
	}
	foldRes := runWorkload(t, "fold", profile, startFold)
	naiveRes := runWorkload(t, "naive", profile, startNaive)

	t.Logf("--- benchmark report ---")
	t.Log(foldRes.String())
	t.Log(naiveRes.String())
	if foldRes.Throughput > 0 && naiveRes.Throughput > 0 {
		t.Logf("throughput ratio naive/fold = %.2fx", naiveRes.Throughput/foldRes.Throughput)
		pct := 100 * foldRes.Throughput / naiveRes.Throughput
		if foldRes.Throughput < naiveRes.Throughput*0.85 {
			t.Logf("note: fold is %.0f%% of naive throughput — ordering has a measurable cost here", pct)
		} else {
			t.Logf("note: fold throughput within ~15%% of naive (%.0f%%)", pct)
		}
	}

	if foldRes.Inversions != 0 {
		t.Errorf("fold: expected 0 inversions, got %d — ordering claim broken", foldRes.Inversions)
	}
	if naiveRes.Inversions == 0 {
		t.Errorf("naive: expected nonzero inversions under this workload (slow endpoints + shared queue); got 0 — workload is not stressing ordering")
	}
}

type sweepRow struct {
	profile latencyProfile
	fold    runResult
	naive   runResult
	fifo    runResult
}

// TestLatencyVarianceSweep runs fold, naive, and strict-FIFO across a latency
// spread progression. Subscriber/event/worker/fail knobs are fixed; only the
// slow-endpoint delay changes (uniform → beyond the default mix).
func TestLatencyVarianceSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("latency variance sweep is long-running")
	}

	rows := make([]sweepRow, 0, len(latencySweep))
	for _, profile := range latencySweep {
		profile := profile
		t.Run(profile.Name, func(t *testing.T) {
			foldRes := runWorkload(t, "fold", profile, startFold)
			naiveRes := runWorkload(t, "naive", profile, startNaive)
			fifoRes := runWorkload(t, "fifo", profile, startFIFO)
			if foldRes.Inversions != 0 {
				t.Errorf("fold: expected 0 inversions, got %d", foldRes.Inversions)
			}
			if fifoRes.Inversions != 0 {
				t.Errorf("fifo: expected 0 inversions, got %d", fifoRes.Inversions)
			}
			rows = append(rows, sweepRow{profile: profile, fold: foldRes, naive: naiveRes, fifo: fifoRes})
		})
	}

	t.Logf("--- latency variance sweep (fold / naive / fifo) ---")
	t.Log("\n" + formatSweepTable(rows))

	if len(rows) < 1 {
		return
	}
	// Premise check: fold should substantially beat FIFO if overlapping
	// across subscribers is worth anything.
	for _, r := range rows {
		if r.fifo.Throughput <= 0 {
			continue
		}
		ratio := r.fold.Throughput / r.fifo.Throughput
		t.Logf("%s: fold/fifo throughput = %.2fx (fold=%.0f/s fifo=%.0f/s)",
			r.profile.Name, ratio, r.fold.Throughput, r.fifo.Throughput)
	}
	uniform := rows[0]
	if uniform.fifo.Throughput > 0 {
		ff := uniform.fold.Throughput / uniform.fifo.Throughput
		if ff < 1.5 {
			t.Logf("finding: fold only %.2fx FIFO under uniform latency — premise undercut if this holds across the sweep", ff)
		} else {
			t.Logf("finding: fold is %.2fx FIFO under uniform latency — ordered parallelism beats strict serialize-everything", ff)
		}
	}
}

func formatSweepTable(rows []sweepRow) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-14s %8s %8s %8s %7s %7s %7s %8s %8s %8s %8s %8s %8s\n",
		"config", "fold/s", "naive/s", "fifo/s", "foldInv", "naiveInv", "fifoInv",
		"foldP50", "foldP99", "naiveP50", "naiveP99", "fifoP50", "fifoP99")
	for _, r := range rows {
		fmt.Fprintf(&b, "%-14s %8.1f %8.1f %8.1f %7d %7d %7d %8s %8s %8s %8s %8s %8s\n",
			r.profile.Name,
			r.fold.Throughput, r.naive.Throughput, r.fifo.Throughput,
			r.fold.Inversions, r.naive.Inversions, r.fifo.Inversions,
			r.fold.LatencyP50.Round(time.Millisecond), r.fold.LatencyP99.Round(time.Millisecond),
			r.naive.LatencyP50.Round(time.Millisecond), r.naive.LatencyP99.Round(time.Millisecond),
			r.fifo.LatencyP50.Round(time.Millisecond), r.fifo.LatencyP99.Round(time.Millisecond),
		)
	}
	return b.String()
}

// TestUniformOverheadAttribution separates HOL-predicate cost from claim
// batching under uniform 1ms endpoints. Three runs: fold as-is, fold with
// DisableHOL (ownership intact), naive baseline. Also reports rows/claim.
func TestUniformOverheadAttribution(t *testing.T) {
	if testing.Short() {
		t.Skip("overhead attribution is long-running")
	}

	profile := latencyProfile{
		Name:      "uniform-1ms",
		FastDelay: time.Millisecond,
		SlowDelay: time.Millisecond,
		SlowEvery: 0,
	}

	var results []attributionRow
	run := func(name string, disableHOL bool, useNaive bool) runResult {
		t.Helper()
		var mem *memory.Store
		var nd *naive.Dispatcher
		build := func(transport fold.Transport, workers int) (dispatcher, error) {
			if useNaive {
				d, err := naive.New(naive.Config{
					Transport:   transport,
					Workers:     workers,
					MaxAttempts: 5,
					BaseBackoff: 2 * time.Millisecond,
					MaxBackoff:  40 * time.Millisecond,
				})
				if err != nil {
					return nil, err
				}
				nd = d
				return d, nil
			}
			mem = memory.New()
			mem.DisableHOL = disableHOL
			return fold.New(fold.Config{
				Store:       mem,
				Transport:   transport,
				Workers:     workers,
				Partitions:  256,
				MaxAttempts: 5,
				BaseBackoff: 2 * time.Millisecond,
				MaxBackoff:  40 * time.Millisecond,
			})
		}
		res := runWorkload(t, name, profile, build)
		switch {
		case useNaive && nd != nil:
			res.ClaimCalls, res.ClaimRows, res.ClaimNonEmpty = nd.ClaimStats()
		case mem != nil:
			res.ClaimCalls, res.ClaimRows, res.ClaimNonEmpty = mem.ClaimStats()
		}
		t.Log(res.String())
		results = append(results, attributionRow{name: name, res: res})
		return res
	}

	foldOn := run("fold", false, false)
	foldOff := run("fold-no-HOL", true, false)
	naiveRes := run("naive", false, true)

	if foldOn.Inversions != 0 {
		t.Errorf("fold: expected 0 inversions, got %d", foldOn.Inversions)
	}
	// With failure injection, HOL-off lets a worker claim seq N+1 while seq N
	// is pending on backoff — exclusive ownership does not prevent that.
	// Record it; do not treat as a harness failure.
	if foldOff.Inversions == 0 {
		t.Logf("note: fold-no-HOL got 0 inversions (failure injection may not have raced)")
	} else {
		t.Logf("note: fold-no-HOL inversions=%d (same-owner skip-ahead during retry backoff)", foldOff.Inversions)
	}

	t.Logf("--- uniform overhead attribution ---")
	t.Log("\n" + formatAttributionTable(results))

	if naiveRes.Throughput <= 0 {
		t.Fatal("naive throughput is zero")
	}
	onPct := 100 * foldOn.Throughput / naiveRes.Throughput
	offPct := 100 * foldOff.Throughput / naiveRes.Throughput
	t.Logf("fold as %% of naive: HOL on → %.0f%%; HOL off → %.0f%%", onPct, offPct)
	t.Logf("rows/claim: fold=%.1f fold-no-HOL=%.1f naive=%.1f",
		foldOn.rowsPerClaim(), foldOff.rowsPerClaim(), naiveRes.rowsPerClaim())
	t.Logf("claim calls: fold=%d (empty=%d) fold-no-HOL=%d (empty=%d) naive=%d (empty=%d)",
		foldOn.ClaimCalls, foldOn.ClaimCalls-foldOn.ClaimNonEmpty,
		foldOff.ClaimCalls, foldOff.ClaimCalls-foldOff.ClaimNonEmpty,
		naiveRes.ClaimCalls, naiveRes.ClaimCalls-naiveRes.ClaimNonEmpty)

	holClosed := offPct-onPct > 5 && offPct >= 95
	holHelped := offPct-onPct > 5
	batchGap := foldOn.rowsPerClaim() > 0 && naiveRes.rowsPerClaim()/foldOn.rowsPerClaim() >= 2
	residual := 100 - offPct

	switch {
	case holClosed:
		t.Logf("finding: disabling HOL closes most of the gap — HOL's one-per-subscriber batch limit is the cost")
	case holHelped && batchGap && residual > 5:
		t.Logf("finding: HOL's batch limit is real (%.0f→%.0f%%, rows/claim %.1f→%.1f ≈ naive %.1f), but %.0f%% residual remains after HOL is off — not only the gate",
			onPct, offPct, foldOn.rowsPerClaim(), foldOff.rowsPerClaim(), naiveRes.rowsPerClaim(), residual)
	case holHelped && batchGap:
		t.Logf("finding: HOL accounts for the gap (%.0f→%.0f%%); rows/claim rises with HOL off (%.1f→%.1f) — HOL's one-per-subscriber limit shrinks batches",
			onPct, offPct, foldOn.rowsPerClaim(), foldOff.rowsPerClaim())
	case !holHelped && batchGap:
		t.Logf("finding: HOL off does not close the gap (%.0f→%.0f%%), but fold rows/claim (%.1f) << naive (%.1f) — partition-scoped batching, not the HOL predicate",
			onPct, offPct, foldOn.rowsPerClaim(), naiveRes.rowsPerClaim())
	case holHelped:
		t.Logf("finding: HOL off helps (%.0f→%.0f%%) but residual remains; not a clean single cause", onPct, offPct)
	default:
		t.Logf("finding: neither HOL nor batch-size gap explains it cleanly (HOL %.0f→%.0f%%, rows/claim fold=%.1f naive=%.1f) — look elsewhere",
			onPct, offPct, foldOn.rowsPerClaim(), naiveRes.rowsPerClaim())
	}
}

type attributionRow struct {
	name string
	res  runResult
}

func formatAttributionTable(rows []attributionRow) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-12s %8s %7s %7s %8s %10s %8s %10s\n",
		"variant", "deliv/s", "%naive", "inv", "claims", "nonempty", "rows/cl", "p50")
	var naiveThr float64
	for _, r := range rows {
		if r.name == "naive" {
			naiveThr = r.res.Throughput
		}
	}
	for _, r := range rows {
		pct := 0.0
		if naiveThr > 0 {
			pct = 100 * r.res.Throughput / naiveThr
		}
		fmt.Fprintf(&b, "%-12s %8.1f %6.0f%% %7d %8d %10d %8.1f %10s\n",
			r.name, r.res.Throughput, pct, r.res.Inversions,
			r.res.ClaimCalls, r.res.ClaimNonEmpty, r.res.rowsPerClaim(),
			r.res.LatencyP50.Round(time.Millisecond),
		)
	}
	return b.String()
}

// TestUniformLoadBalance checks whether the residual uniform-latency gap is
// pinned-ownership imbalance: per-worker delivery counts and idle time under
// uniform-1ms for fold vs naive.
func TestUniformLoadBalance(t *testing.T) {
	if testing.Short() {
		t.Skip("load-balance check is long-running")
	}

	profile := latencyProfile{
		Name:      "uniform-1ms",
		FastDelay: time.Millisecond,
		SlowDelay: time.Millisecond,
		SlowEvery: 0,
	}

	type balRow struct {
		name  string
		res   runResult
		stats []workerBalStat
	}

	run := func(name string, useNaive bool) balRow {
		t.Helper()
		var fd *fold.Dispatcher
		var nd *naive.Dispatcher
		build := func(transport fold.Transport, workers int) (dispatcher, error) {
			if useNaive {
				d, err := naive.New(naive.Config{
					Transport:   transport,
					Workers:     workers,
					MaxAttempts: 5,
					BaseBackoff: 2 * time.Millisecond,
					MaxBackoff:  40 * time.Millisecond,
				})
				if err != nil {
					return nil, err
				}
				nd = d
				return d, nil
			}
			d, err := fold.New(fold.Config{
				Store:       memory.New(),
				Transport:   transport,
				Workers:     workers,
				Partitions:  256,
				MaxAttempts: 5,
				BaseBackoff: 2 * time.Millisecond,
				MaxBackoff:  40 * time.Millisecond,
			})
			if err != nil {
				return nil, err
			}
			fd = d
			return d, nil
		}
		res := runWorkload(t, name, profile, build)
		var stats []workerBalStat
		switch {
		case useNaive && nd != nil:
			for _, s := range nd.WorkerStats() {
				stats = append(stats, workerBalStat{Owner: s.Owner, Deliveries: s.Deliveries, Idle: s.Idle})
			}
		case fd != nil:
			for _, s := range fd.WorkerStats() {
				stats = append(stats, workerBalStat{Owner: s.Owner, Deliveries: s.Deliveries, Idle: s.Idle})
			}
		}
		return balRow{name: name, res: res, stats: stats}
	}

	foldRow := run("fold", false)
	naiveRow := run("naive", true)

	t.Logf("--- uniform load balance ---")
	t.Log("\nfold workers:\n" + formatWorkerBalance(foldRow.stats))
	t.Log("\nnaive workers:\n" + formatWorkerBalance(naiveRow.stats))

	foldSum := summarizeBalance(foldRow.stats)
	naiveSum := summarizeBalance(naiveRow.stats)
	t.Logf("fold:  deliveries min=%d max=%d mean=%.1f cv=%.3f max/min=%.2f | idle min=%s max=%s mean=%s cv=%.3f",
		foldSum.delivMin, foldSum.delivMax, foldSum.delivMean, foldSum.delivCV, foldSum.delivRatio,
		foldSum.idleMin.Round(time.Millisecond), foldSum.idleMax.Round(time.Millisecond),
		foldSum.idleMean.Round(time.Millisecond), foldSum.idleCV)
	t.Logf("naive: deliveries min=%d max=%d mean=%.1f cv=%.3f max/min=%.2f | idle min=%s max=%s mean=%s cv=%.3f",
		naiveSum.delivMin, naiveSum.delivMax, naiveSum.delivMean, naiveSum.delivCV, naiveSum.delivRatio,
		naiveSum.idleMin.Round(time.Millisecond), naiveSum.idleMax.Round(time.Millisecond),
		naiveSum.idleMean.Round(time.Millisecond), naiveSum.idleCV)

	// Confirmed only if fold is meaningfully more skewed than naive.
	foldMoreUneven := foldSum.delivCV > naiveSum.delivCV+0.05 && foldSum.delivRatio > naiveSum.delivRatio+0.2
	comparable := foldSum.delivCV <= naiveSum.delivCV+0.05

	switch {
	case foldMoreUneven:
		t.Logf("finding: confirmed — fold workers more uneven than naive (fold cv=%.3f max/min=%.2f; naive cv=%.3f max/min=%.2f); pinned-ownership imbalance can explain residual",
			foldSum.delivCV, foldSum.delivRatio, naiveSum.delivCV, naiveSum.delivRatio)
	case comparable:
		t.Logf("finding: rejected — fold delivery distribution is not more uneven than naive (fold cv=%.3f max/min=%.2f; naive cv=%.3f max/min=%.2f); residual is not load imbalance from pinned ownership",
			foldSum.delivCV, foldSum.delivRatio, naiveSum.delivCV, naiveSum.delivRatio)
	default:
		t.Logf("finding: inconclusive — fold cv=%.3f max/min=%.2f; naive cv=%.3f max/min=%.2f",
			foldSum.delivCV, foldSum.delivRatio, naiveSum.delivCV, naiveSum.delivRatio)
	}
}

type workerBalStat struct {
	Owner      string
	Deliveries int64
	Idle       time.Duration
}

type balanceSummary struct {
	delivMin, delivMax int64
	delivMean, delivCV, delivRatio float64
	idleMin, idleMax, idleMean     time.Duration
	idleCV                         float64
}

func summarizeBalance(stats []workerBalStat) balanceSummary {
	var s balanceSummary
	if len(stats) == 0 {
		return s
	}
	s.delivMin, s.delivMax = stats[0].Deliveries, stats[0].Deliveries
	s.idleMin, s.idleMax = stats[0].Idle, stats[0].Idle
	var delivSum float64
	var idleSum float64
	for _, w := range stats {
		if w.Deliveries < s.delivMin {
			s.delivMin = w.Deliveries
		}
		if w.Deliveries > s.delivMax {
			s.delivMax = w.Deliveries
		}
		if w.Idle < s.idleMin {
			s.idleMin = w.Idle
		}
		if w.Idle > s.idleMax {
			s.idleMax = w.Idle
		}
		delivSum += float64(w.Deliveries)
		idleSum += float64(w.Idle)
	}
	n := float64(len(stats))
	s.delivMean = delivSum / n
	s.idleMean = time.Duration(idleSum / n)
	if s.delivMin > 0 {
		s.delivRatio = float64(s.delivMax) / float64(s.delivMin)
	}
	var delivVar, idleVar float64
	for _, w := range stats {
		dd := float64(w.Deliveries) - s.delivMean
		delivVar += dd * dd
		di := float64(w.Idle) - idleSum/n
		idleVar += di * di
	}
	delivStd := math.Sqrt(delivVar / n)
	idleStd := math.Sqrt(idleVar / n)
	if s.delivMean > 0 {
		s.delivCV = delivStd / s.delivMean
	}
	if idleSum/n > 0 {
		s.idleCV = idleStd / (idleSum / n)
	}
	return s
}

func formatWorkerBalance(stats []workerBalStat) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-16s %10s %10s\n", "owner", "deliveries", "idle")
	for _, w := range stats {
		fmt.Fprintf(&b, "%-16s %10d %10s\n", w.Owner, w.Deliveries, w.Idle.Round(time.Millisecond))
	}
	return b.String()
}

func startFold(transport fold.Transport, workers int) (dispatcher, error) {
	return fold.New(fold.Config{
		Store:       memory.New(),
		Transport:   transport,
		Workers:     workers,
		Partitions:  256,
		MaxAttempts: 5,
		BaseBackoff: 2 * time.Millisecond,
		MaxBackoff:  40 * time.Millisecond,
	})
}

func startNaive(transport fold.Transport, workers int) (dispatcher, error) {
	return naive.New(naive.Config{
		Transport:   transport,
		Workers:     workers,
		MaxAttempts: 5,
		BaseBackoff: 2 * time.Millisecond,
		MaxBackoff:  40 * time.Millisecond,
	})
}

func startFIFO(transport fold.Transport, workers int) (dispatcher, error) {
	_ = workers // single-threaded by design
	return fifo.New(fifo.Config{
		Transport:   transport,
		MaxAttempts: 5,
		BaseBackoff: 2 * time.Millisecond,
		MaxBackoff:  40 * time.Millisecond,
	})
}

func runWorkload(t *testing.T, name string, profile latencyProfile, build func(fold.Transport, int) (dispatcher, error)) runResult {
	t.Helper()

	rec := &recorder{profile: profile}
	mux := http.NewServeMux()
	mux.HandleFunc("/hook/", rec.serve)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	transport := fold.NewHTTPTransport(5*time.Second, srv.Client())
	d, err := build(transport, benchWorkers)
	if err != nil {
		t.Fatalf("%s: New: %v", name, err)
	}

	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("%s: Start: %v", name, err)
	}

	subs := make([]fold.Subscriber, benchSubscribers)
	slow := profile.slowCount(benchSubscribers)
	for i := 0; i < benchSubscribers; i++ {
		id := fmt.Sprintf("sub-%d", i)
		subs[i] = fold.Subscriber{ID: id, URL: srv.URL + "/hook/" + id}
	}

	dispatchAt := make([]time.Time, benchEvents)
	start := time.Now()
	for e := 0; e < benchEvents; e++ {
		seq := int64(e + 1)
		payload, _ := json.Marshal(map[string]int64{"seq": seq})
		dispatchAt[e] = time.Now()
		if err := d.Dispatch(ctx, fold.Event{
			ID:      fmt.Sprintf("evt-%04d", e),
			Type:    "bench.event",
			Payload: payload,
		}, subs); err != nil {
			t.Fatalf("%s: Dispatch %d: %v", name, e, err)
		}
	}

	closeCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if err := d.Close(closeCtx); err != nil {
		t.Fatalf("%s: Close: %v", name, err)
	}
	elapsed := time.Since(start)

	rec.mu.Lock()
	receipts := append([]receipt(nil), rec.receipts...)
	rec.mu.Unlock()

	want := benchSubscribers * benchEvents
	if len(receipts) != want {
		t.Fatalf("%s: got %d receipts, want %d (retries may still be pending)", name, len(receipts), want)
	}

	inv := countInversions(receipts)
	latencies := make([]time.Duration, 0, len(receipts))
	for _, r := range receipts {
		idx := int(r.seq - 1)
		if idx < 0 || idx >= len(dispatchAt) {
			continue
		}
		lat := r.arrived.Sub(dispatchAt[idx])
		if lat < 0 {
			lat = 0
		}
		latencies = append(latencies, lat)
	}
	p50, p99 := percentiles(latencies)

	res := runResult{
		Name:       name,
		Profile:    profile.Name,
		Deliveries: len(receipts),
		Duration:   elapsed,
		Throughput: float64(len(receipts)) / elapsed.Seconds(),
		Inversions: inv,
		LatencyP50: p50,
		LatencyP99: p99,
		SlowSubs:   slow,
	}
	t.Log(res.String())
	return res
}

func countInversions(receipts []receipt) int {
	bySub := make(map[string][]int64)
	for _, r := range receipts {
		bySub[r.sub] = append(bySub[r.sub], r.seq)
	}
	inv := 0
	for _, seqs := range bySub {
		for i := 1; i < len(seqs); i++ {
			if seqs[i] < seqs[i-1] {
				inv++
			}
		}
	}
	return inv
}

func percentiles(ds []time.Duration) (p50, p99 time.Duration) {
	if len(ds) == 0 {
		return 0, 0
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	p50 = ds[len(ds)*50/100]
	idx99 := len(ds) * 99 / 100
	if idx99 >= len(ds) {
		idx99 = len(ds) - 1
	}
	return p50, ds[idx99]
}
