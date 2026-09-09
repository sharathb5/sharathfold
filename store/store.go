package store

import (
	"context"
	"time"
)

// Status is the lifecycle state of a stored delivery.
type Status string

const (
	StatusPending      Status = "pending"
	StatusInFlight     Status = "in_flight"
	StatusDelivered    Status = "delivered"
	StatusDeadLettered Status = "dead_lettered"
	// StatusRetained holds a delivery for a suspended subscriber. Not claimable
	// until Resume moves it back to pending.
	StatusRetained Status = "retained"
)

// Delivery is one outbound webhook attempt row. Sequence is assigned by the
// Store on Enqueue and is monotonic per SubscriberID.
type Delivery struct {
	ID           string
	EventID      string
	SubscriberID string
	URL          string
	Partition    int
	Sequence     int64
	Payload      []byte
	EventType    string
	// Secret is the per-subscriber HMAC key copied at Enqueue. Empty means
	// unsigned delivery. Stores must copy if they retain the buffer.
	Secret      []byte
	Status      Status
	Attempt     int
	NextAttempt time.Time
	LastError   string
	Owner       string
	Generation  uint64
	ClaimedAt   time.Time
	CreatedAt   time.Time
}

// GapInfo reports whether retention overflow dropped events while a subscriber
// was suspended. Marker is opaque and empty when Occurred is false.
type GapInfo struct {
	Occurred bool
	Dropped  int
	Marker   string
}

// Store persists deliveries and hands claimable work to workers.
//
// Ownership identity (owner) is an opaque string — never a process-local
// integer worker index — so a later multi-node path can keep the same API.
type Store interface {
	// Enqueue inserts all deliveries atomically. Assigns Sequence per
	// subscriber (monotonic) and may set Partition. Payload may be shared
	// across rows; the Store must copy if it retains buffers past the call.
	// Rows for a suspended subscriber are retained (not pending) and subject
	// to retention bounds.
	Enqueue(ctx context.Context, ds []Delivery) error

	// Claim returns due, head-of-line-eligible rows in the given partitions,
	// marking them in_flight under owner+generation. A delivery is HOL-eligible
	// only when no earlier-sequence delivery for the same subscriber is still
	// pending or in_flight. Suspended subscribers are never claimed. Empty
	// partitions means claim nothing.
	Claim(ctx context.Context, owner string, generation uint64, partitions []int, limit int) ([]Delivery, error)

	MarkDelivered(ctx context.Context, id string, owner string, generation uint64) error
	MarkFailed(ctx context.Context, id string, owner string, generation uint64, attempt int, next time.Time, errMsg string) error
	MarkDeadLetter(ctx context.Context, id string, owner string, generation uint64, errMsg string) error

	// ExhaustAndSuspend dead-letters the in-flight delivery and suspends the
	// subscriber. Remaining pending rows for that subscriber become retained
	// (bounded). Claiming stops until Resume.
	ExhaustAndSuspend(ctx context.Context, id string, owner string, generation uint64, errMsg string) error

	// Resume clears suspension and returns retained rows to pending in
	// sequence order. Reports whether a retention gap occurred.
	Resume(ctx context.Context, subscriberID string) (GapInfo, error)

	// IsSuspended reports whether the subscriber is currently suspended.
	IsSuspended(ctx context.Context, subscriberID string) (bool, error)

	// RecoverStale turns in_flight rows older than age back to pending.
	RecoverStale(ctx context.Context, olderThan time.Duration) (int, error)
}
