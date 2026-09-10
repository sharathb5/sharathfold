// Package fold delivers webhooks with per-subscriber ordering under fan-out.
//
// Subscribers hash onto a fixed set of partitions; each worker owns a slice of
// those partitions. A given subscriber always lands on the same owner, so their
// events go out in sequence while other subscribers are delivered in parallel.
// Dispatch encodes the payload once and returns after enqueue; workers perform
// HTTP asynchronously with retries, head-of-line gating, and suspend/resume.
package fold
