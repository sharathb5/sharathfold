package backoff

import (
	"math/rand"
	"time"
)

// Delay returns the wait before the next attempt after a failed try.
// attempt is 1-based (1 = first failure). Schedule:
//
//	min(max, base * 2^(attempt-1)) + up to 10% jitter
//
// base/max <= 0 fall back to 1s / 15m.
func Delay(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if base <= 0 {
		base = time.Second
	}
	if max <= 0 {
		max = 15 * time.Minute
	}

	delay := base
	for i := 1; i < attempt; i++ {
		if delay >= max {
			delay = max
			break
		}
		next := delay * 2
		if next > max || next < delay { // overflow
			delay = max
			break
		}
		delay = next
	}
	if delay > max {
		delay = max
	}

	jitter := time.Duration(rand.Int63n(int64(delay)/10 + 1))
	return delay + jitter
}
