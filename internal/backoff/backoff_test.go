package backoff_test

import (
	"testing"
	"time"

	"github.com/sharathb5/sharathfold/internal/backoff"
)

func TestDelayExponentialCapped(t *testing.T) {
	base := time.Second
	max := 8 * time.Second

	// attempt 1 → ~1s, attempt 2 → ~2s, attempt 3 → ~4s, attempt 4+ → ~8s
	for attempt, wantMin := range map[int]time.Duration{
		1: time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
		4: 8 * time.Second,
		8: 8 * time.Second,
	} {
		d := backoff.Delay(attempt, base, max)
		// delay is base*2^(n-1) plus up to 10% jitter
		if d < wantMin || d > wantMin+wantMin/10+time.Millisecond {
			t.Errorf("attempt %d: delay=%v want in [%v, %v]", attempt, d, wantMin, wantMin+wantMin/10)
		}
	}
}

func TestDelayDefaults(t *testing.T) {
	d := backoff.Delay(1, 0, 0)
	if d < time.Second || d > time.Second+time.Second/10+time.Millisecond {
		t.Fatalf("default base delay=%v", d)
	}
}
