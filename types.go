package fold

import (
	"encoding/json"
	"fmt"
)

// Event is a single webhook event accepted by Dispatch.
type Event struct {
	ID      string          // required; host-facing identity
	Type    string          // e.g. "order.created"
	Payload json.RawMessage // encoded once in Dispatch; workers reuse the bytes
}

// Subscriber is a delivery destination. ID is the stable identity used for
// hashing onto partitions and per-subscriber sequencing.
type Subscriber struct {
	ID  string
	URL string
	// Secret is an optional per-subscriber HMAC key. Empty means the delivery
	// is sent unsigned. See Fold-Signature header on HTTPTransport.
	Secret []byte
}

// DeliveryStatus is the lifecycle state of a delivery attempt chain.
type DeliveryStatus string

const (
	StatusPending      DeliveryStatus = "pending"
	StatusInFlight     DeliveryStatus = "in_flight"
	StatusDelivered    DeliveryStatus = "delivered"
	StatusDeadLettered DeliveryStatus = "dead_lettered"
	StatusRetained     DeliveryStatus = "retained"
)

// DeliveryMutationError is a failed post-Deliver store transition. The HTTP
// attempt may already have succeeded or failed; the store row may be unchanged.
type DeliveryMutationError struct {
	Op           string // MarkDelivered, MarkFailed, ExhaustAndSuspend
	DeliveryID   string
	SubscriberID string
	EventID      string
	Err          error
}

func (e DeliveryMutationError) Error() string {
	return fmt.Sprintf("fold: %s %s: %v", e.Op, e.DeliveryID, e.Err)
}

func (e DeliveryMutationError) Unwrap() error { return e.Err }
