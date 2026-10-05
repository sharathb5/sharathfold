package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/sharathb5/sharathfold/store"
)

// EnsureOwners implements store.PartitionOwnership.
func (s *Store) EnsureOwners(ctx context.Context, partitions int, ownerNode string) error {
	if partitions <= 0 {
		return fmt.Errorf("postgres store: EnsureOwners requires partitions > 0")
	}
	if ownerNode == "" {
		return fmt.Errorf("postgres store: EnsureOwners requires ownerNode")
	}
	owners := make([]string, partitions)
	for i := range owners {
		owners[i] = ownerNode
	}
	return s.EnsureOwnerAssignments(ctx, owners)
}

// EnsureOwnerAssignments implements store.PartitionOwnership.
func (s *Store) EnsureOwnerAssignments(ctx context.Context, owners []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(owners) == 0 {
		return fmt.Errorf("postgres store: EnsureOwnerAssignments requires non-empty owners")
	}
	for p, ownerNode := range owners {
		if ownerNode == "" {
			return fmt.Errorf("postgres store: EnsureOwnerAssignments partition %d empty owner", p)
		}
		if strings.Contains(ownerNode, "/") {
			return fmt.Errorf("postgres store: EnsureOwnerAssignments owner %q must not contain '/'", ownerNode)
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, `LOCK TABLE fold_partition_owners IN EXCLUSIVE MODE`); err != nil {
		return fmt.Errorf("postgres store: EnsureOwnerAssignments lock: %w", err)
	}

	partitions := len(owners)
	rows, err := tx.Query(ctx, `
		SELECT partition, owner_node
		FROM fold_partition_owners
		WHERE partition >= 0 AND partition < $1
		ORDER BY partition
	`, partitions)
	if err != nil {
		return fmt.Errorf("postgres store: EnsureOwnerAssignments scan: %w", err)
	}
	existing := make(map[int]string)
	for rows.Next() {
		var p int
		var owner string
		if err := rows.Scan(&p, &owner); err != nil {
			rows.Close()
			return err
		}
		if owner != owners[p] {
			rows.Close()
			return fmt.Errorf("postgres store: EnsureOwnerAssignments conflict: partition %d owned by %q, not %q", p, owner, owners[p])
		}
		existing[p] = owner
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for p, ownerNode := range owners {
		if _, ok := existing[p]; ok {
			continue
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO fold_partition_owners (partition, owner_node, generation, state, next_owner)
			VALUES ($1, $2, 1, 'active', NULL)
		`, p, ownerNode)
		if err != nil {
			return fmt.Errorf("postgres store: EnsureOwnerAssignments insert partition %d: %w", p, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.distributedOwnership.Store(true)
	return nil
}

// BeginHandoff implements store.PartitionOwnership.
func (s *Store) BeginHandoff(ctx context.Context, partition int, fromNode, toNode string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.refreshDistributedOwnership(ctx); err != nil {
		return err
	}
	if !s.distributedOwnership.Load() {
		return fmt.Errorf("postgres store: BeginHandoff requires DistributedOwnership")
	}
	if fromNode == "" || toNode == "" {
		return fmt.Errorf("postgres store: BeginHandoff requires fromNode and toNode")
	}
	if strings.Contains(fromNode, "/") || strings.Contains(toNode, "/") {
		return fmt.Errorf("postgres store: BeginHandoff node IDs must not contain '/'")
	}
	if fromNode == toNode {
		return fmt.Errorf("postgres store: BeginHandoff fromNode and toNode must differ")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE fold_partition_owners
		SET state = 'draining',
		    next_owner = $3
		WHERE partition = $1
		  AND owner_node = $2
		  AND state = 'active'
		  AND next_owner IS NULL
	`, partition, fromNode, toNode)
	if err != nil {
		return fmt.Errorf("postgres store: BeginHandoff: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres store: BeginHandoff CAS miss on partition %d", partition)
	}
	return nil
}

// CompleteHandoff implements store.PartitionOwnership.
func (s *Store) CompleteHandoff(ctx context.Context, partition int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.refreshDistributedOwnership(ctx); err != nil {
		return err
	}
	if !s.distributedOwnership.Load() {
		return fmt.Errorf("postgres store: CompleteHandoff requires DistributedOwnership")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx, `
		UPDATE fold_partition_owners
		SET owner_node = next_owner,
		    state = 'active',
		    next_owner = NULL,
		    generation = generation + 1
		WHERE partition = $1
		  AND state = 'draining'
		  AND next_owner IS NOT NULL
		  AND NOT EXISTS (
			SELECT 1 FROM fold_deliveries
			WHERE partition = $1 AND status = 'in_flight'
		  )
	`, partition)
	if err != nil {
		return fmt.Errorf("postgres store: CompleteHandoff: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres store: CompleteHandoff CAS miss or in_flight remain on partition %d", partition)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.signal()
	return nil
}

// ForceTakeover implements store.PartitionOwnership.
//
// Semantics:
//   - owner == toNode, active, expectedGen or expectedGen+1: no-op (no bump, no reset)
//   - owner == toNode, draining, expectedGen: abort handoff back to active (no bump, no reset)
//   - generation == expectedGen and owner != toNode: steal, bump, reset in_flight
//   - otherwise: conflict
func (s *Store) ForceTakeover(ctx context.Context, partition int, toNode string, expectedGen uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.refreshDistributedOwnership(ctx); err != nil {
		return err
	}
	if !s.distributedOwnership.Load() {
		return fmt.Errorf("postgres store: ForceTakeover requires DistributedOwnership")
	}
	if toNode == "" {
		return fmt.Errorf("postgres store: ForceTakeover requires toNode")
	}
	if strings.Contains(toNode, "/") {
		return fmt.Errorf("postgres store: ForceTakeover toNode must not contain '/'")
	}
	if expectedGen == 0 {
		return fmt.Errorf("postgres store: ForceTakeover requires non-zero expectedGen")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var owner, state string
	var gen int64
	err = tx.QueryRow(ctx, `
		SELECT owner_node, state, generation
		FROM fold_partition_owners
		WHERE partition = $1
		FOR UPDATE
	`, partition).Scan(&owner, &state, &gen)
	if err == pgx.ErrNoRows {
		return fmt.Errorf("postgres store: ForceTakeover partition %d missing", partition)
	}
	if err != nil {
		return fmt.Errorf("postgres store: ForceTakeover lookup: %w", err)
	}

	if owner == toNode {
		switch {
		case state == "draining" && uint64(gen) == expectedGen:
			// Abort incomplete graceful handoff without fencing in-flight work.
			tag, err := tx.Exec(ctx, `
				UPDATE fold_partition_owners
				SET state = 'active',
				    next_owner = NULL
				WHERE partition = $1
				  AND generation = $2
				  AND state = 'draining'
				  AND owner_node = $3
			`, partition, int64(expectedGen), toNode)
			if err != nil {
				return fmt.Errorf("postgres store: ForceTakeover abort drain: %w", err)
			}
			if tag.RowsAffected() == 0 {
				return fmt.Errorf("postgres store: ForceTakeover abort drain CAS miss on partition %d", partition)
			}
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			s.signal()
			return nil
		case state == "active" && (uint64(gen) == expectedGen || uint64(gen) == expectedGen+1):
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			return nil
		default:
			return fmt.Errorf("postgres store: ForceTakeover conflict on partition %d (owner=%s state=%s gen=%d expected=%d)", partition, owner, state, gen, expectedGen)
		}
	}

	if uint64(gen) != expectedGen {
		return fmt.Errorf("postgres store: ForceTakeover conflict on partition %d (owner=%s state=%s gen=%d expected=%d)", partition, owner, state, gen, expectedGen)
	}

	tag, err := tx.Exec(ctx, `
		UPDATE fold_partition_owners
		SET owner_node = $2,
		    generation = generation + 1,
		    state = 'active',
		    next_owner = NULL
		WHERE partition = $1
		  AND generation = $3
	`, partition, toNode, int64(expectedGen))
	if err != nil {
		return fmt.Errorf("postgres store: ForceTakeover: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres store: ForceTakeover CAS miss on partition %d", partition)
	}

	if !s.SkipInFlightReset {
		_, err = tx.Exec(ctx, `
			UPDATE fold_deliveries
			SET status = 'pending',
			    owner = NULL,
			    generation = 0,
			    claimed_at = NULL
			WHERE partition = $1
			  AND status = 'in_flight'
		`, partition)
		if err != nil {
			return fmt.Errorf("postgres store: ForceTakeover reset in_flight: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.signal()
	return nil
}

// OwnerRow returns the durable ownership row for tests and diagnostics.
func (s *Store) OwnerRow(ctx context.Context, partition int) (ownerNode, state, nextOwner string, generation uint64, err error) {
	var next *string
	var gen int64
	err = s.pool.QueryRow(ctx, `
		SELECT owner_node, state, next_owner, generation
		FROM fold_partition_owners
		WHERE partition = $1
	`, partition).Scan(&ownerNode, &state, &next, &gen)
	if err != nil {
		return "", "", "", 0, err
	}
	if next != nil {
		nextOwner = *next
	}
	return ownerNode, state, nextOwner, uint64(gen), nil
}

// InFlightCount reports in_flight rows for a partition.
func (s *Store) InFlightCount(ctx context.Context, partition int) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM fold_deliveries
		WHERE partition = $1 AND status = 'in_flight'
	`, partition).Scan(&n)
	return n, err
}

// InFlightStamp returns id/owner/generation for the sole in_flight row on a
// partition. For handoff tests that need the pre-Force claim token.
func (s *Store) InFlightStamp(ctx context.Context, partition int) (id, owner string, generation uint64, err error) {
	var gen int64
	var ownerPtr *string
	err = s.pool.QueryRow(ctx, `
		SELECT id, owner, generation FROM fold_deliveries
		WHERE partition = $1 AND status = 'in_flight'
		ORDER BY sequence ASC
		LIMIT 1
	`, partition).Scan(&id, &ownerPtr, &gen)
	if err != nil {
		return "", "", 0, err
	}
	if ownerPtr != nil {
		owner = *ownerPtr
	}
	return id, owner, uint64(gen), nil
}

// DeliveryStatus returns status for a delivery id. For handoff tests.
func (s *Store) DeliveryStatus(ctx context.Context, id string) (string, error) {
	var status string
	err := s.pool.QueryRow(ctx, `SELECT status FROM fold_deliveries WHERE id = $1`, id).Scan(&status)
	return status, err
}

// Compile-time check that *Store implements store.PartitionOwnership.
var _ store.PartitionOwnership = (*Store)(nil)

// parseDistributedOwner requires NodeID/IncarnationID/worker-N with no extra
// slashes in NodeID or incarnation.
func parseDistributedOwner(owner string) (logicalNode string, ok bool) {
	parts := strings.Split(owner, "/")
	if len(parts) != 3 {
		return "", false
	}
	if parts[0] == "" || parts[1] == "" {
		return "", false
	}
	if !strings.HasPrefix(parts[2], "worker-") {
		return "", false
	}
	var n int
	if _, err := fmt.Sscanf(parts[2], "worker-%d", &n); err != nil {
		return "", false
	}
	if parts[2] != fmt.Sprintf("worker-%d", n) {
		return "", false
	}
	return parts[0], true
}
