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
	Status       Status
	Attempt      int
	NextAttempt  time.Time
	LastError    string
	Owner        string
	Generation   uint64
	ClaimedAt    time.Time
	CreatedAt    time.Time
}

// Store persists deliveries and hands claimable work to workers.
//
// Ownership identity (owner) is an opaque string — never a process-local
// integer worker index — so a later multi-node path can keep the same API.
type Store interface {
	// Enqueue inserts all deliveries atomically. Assigns Sequence per
	// subscriber (monotonic) and may set Partition. Payload may be shared
	// across rows; the Store must copy if it retains buffers past the call.
	Enqueue(ctx context.Context, ds []Delivery) error

	// Claim returns due, head-of-line-eligible rows in the given partitions,
	// marking them in_flight under owner+generation. A delivery is HOL-eligible
	// only when no earlier-sequence delivery for the same subscriber is still
	// pending or in_flight. Empty partitions means all partitions.
	Claim(ctx context.Context, owner string, generation uint64, partitions []int, limit int) ([]Delivery, error)

	MarkDelivered(ctx context.Context, id string, owner string, generation uint64) error
	MarkFailed(ctx context.Context, id string, owner string, generation uint64, attempt int, next time.Time, errMsg string) error
	MarkDeadLetter(ctx context.Context, id string, owner string, generation uint64, errMsg string) error

	// RecoverStale turns in_flight rows older than age back to pending.
	RecoverStale(ctx context.Context, olderThan time.Duration) (int, error)
}
