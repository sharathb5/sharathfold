package store

import "context"

// PartitionOwnership is the narrow Postgres capability for live cross-process
// partition handoff. It is not part of Store: memory does not implement it,
// and single-process Resize stays on Dispatcher.
type PartitionOwnership interface {
	// EnsureOwners seeds missing rows for partitions [0, partitions) under
	// ownerNode at generation 1, state active. Existing rows must already
	// belong to ownerNode (conflict otherwise).
	EnsureOwners(ctx context.Context, partitions int, ownerNode string) error

	// EnsureOwnerAssignments seeds missing rows for partitions [0, len(owners))
	// where owners[p] is the logical NodeID for partition p. Existing rows must
	// match. Enables DistributedOwnership.
	EnsureOwnerAssignments(ctx context.Context, owners []string) error

	// BeginHandoff moves partition fromNode → draining with next_owner toNode.
	// Generation is unchanged so in-flight Marks under the current stamp remain valid.
	BeginHandoff(ctx context.Context, partition int, fromNode, toNode string) error

	// CompleteHandoff transfers ownership to next_owner when in_flight for the
	// partition is zero, bumps generation, and returns to active.
	CompleteHandoff(ctx context.Context, partition int) error

	// ForceTakeover CAS-updates ownership to toNode (expectedGen must match),
	// bumps generation, clears draining, and resets in_flight rows to pending.
	// Fences durable store state only — an in-flight HTTP POST may still complete.
	ForceTakeover(ctx context.Context, partition int, toNode string, expectedGen uint64) error
}
