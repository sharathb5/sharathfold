package postgres

import (
	"context"
	"embed"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sharathb5/sharathfold/store"
)

//go:embed schema.sql
var schemaFS embed.FS

const (
	// DefaultRetentionMaxCount caps retained deliveries per suspended subscriber.
	DefaultRetentionMaxCount = 1000
	// DefaultRetentionMaxAge drops retained deliveries older than this.
	DefaultRetentionMaxAge = 24 * time.Hour
)

// Store is a Postgres-backed store.Store using SELECT … FOR UPDATE SKIP LOCKED
// scoped to owned partitions with head-of-line gating.
type Store struct {
	pool   *pgxpool.Pool
	notify chan struct{}

	// RetentionMaxCount / RetentionMaxAge bound retained backlog per
	// suspended subscriber. Zero selects the defaults above.
	RetentionMaxCount int
	RetentionMaxAge   time.Duration

	// DisableHOL skips head-of-line gating. For invariant tests only.
	DisableHOL bool

	// SkipSuspendGate makes Claim ignore suspension and Enqueue always insert
	// pending rows. For invariant tests only — proves "nothing while suspended."
	SkipSuspendGate bool

	// BypassDrainingClaimGate drops only Claim's state='active' requirement
	// while keeping the owner_node match. For the graceful-handoff tooth only.
	BypassDrainingClaimGate bool

	// SkipInFlightReset makes ForceTakeover bump ownership without resetting
	// in_flight rows. For the forced-takeover tooth only.
	SkipInFlightReset bool

	// SkipMarkOwnerGenCheck makes Mark* ignore owner/generation match.
	// For the stale-completion tooth only.
	SkipMarkOwnerGenCheck bool

	// distributedOwnership, when true, requires NodeID-shaped claim tokens and
	// authorizes every Claim through fold_partition_owners. False preserves
	// legacy single-process Postgres Claim behavior. Atomic for worker Claim races.
	distributedOwnership atomic.Bool
}

// Open connects to Postgres and applies schema.sql.
func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres store: connect: %w", err)
	}
	s := &Store{
		pool:   pool,
		notify: make(chan struct{}, 1),
	}
	if err := s.Migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := s.refreshDistributedOwnership(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// refreshDistributedOwnership fail-closes: any durable ownership rows enable
// DistributedOwnership so a restarted process (or cleared in-memory flag) cannot
// fall back to legacy Claim against an owned database.
func (s *Store) refreshDistributedOwnership(ctx context.Context) error {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM fold_partition_owners`).Scan(&n); err != nil {
		return fmt.Errorf("postgres store: refresh DistributedOwnership: %w", err)
	}
	if n > 0 {
		s.distributedOwnership.Store(true)
	}
	return nil
}

// HasDistributedOwnership reports whether durable ownership enforcement is on.
func (s *Store) HasDistributedOwnership() bool {
	return s.distributedOwnership.Load()
}

// SetDistributedOwnership sets the in-memory enforcement flag. Tests use this to
// simulate misconfig; production callers should prefer EnsureOwners. Clearing the
// flag does not disable enforcement while fold_partition_owners rows exist.
func (s *Store) SetDistributedOwnership(v bool) {
	s.distributedOwnership.Store(v)
}

// Close closes the underlying pool.
func (s *Store) Close() {
	s.pool.Close()
}

// Migrate applies schema.sql.
func (s *Store) Migrate(ctx context.Context) error {
	ddl, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return fmt.Errorf("postgres store: read schema: %w", err)
	}
	if _, err := s.pool.Exec(ctx, string(ddl)); err != nil {
		return fmt.Errorf("postgres store: migrate: %w", err)
	}
	return nil
}

// Notify returns a channel that receives a signal after Enqueue and terminal
// marks. Workers may select on it to avoid busy-polling. Never closed.
func (s *Store) Notify() <-chan struct{} {
	return s.notify
}

func (s *Store) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *Store) retentionCount() int {
	if s.RetentionMaxCount > 0 {
		return s.RetentionMaxCount
	}
	return DefaultRetentionMaxCount
}

func (s *Store) retentionAge() time.Duration {
	if s.RetentionMaxAge > 0 {
		return s.RetentionMaxAge
	}
	return DefaultRetentionMaxAge
}

// Enqueue implements store.Store.
func (s *Store) Enqueue(ctx context.Context, ds []store.Delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ds) == 0 {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	now := time.Now().UTC()
	touched := map[string]struct{}{}

	for i := range ds {
		d := ds[i]
		if d.ID == "" {
			return fmt.Errorf("postgres store: delivery missing ID")
		}
		if d.SubscriberID == "" {
			return fmt.Errorf("postgres store: delivery %s missing SubscriberID", d.ID)
		}

		var seq int64
		err := tx.QueryRow(ctx, `
			INSERT INTO fold_subscriber_seq (subscriber_id, next_seq)
			VALUES ($1, 1)
			ON CONFLICT (subscriber_id) DO UPDATE
			SET next_seq = fold_subscriber_seq.next_seq + 1
			RETURNING next_seq
		`, d.SubscriberID).Scan(&seq)
		if err != nil {
			return fmt.Errorf("postgres store: allocate sequence: %w", err)
		}

		status := store.StatusPending
		if !s.SkipSuspendGate {
			var suspended bool
			err := tx.QueryRow(ctx, `
				SELECT suspended FROM fold_subscribers WHERE subscriber_id = $1
			`, d.SubscriberID).Scan(&suspended)
			if err == nil && suspended {
				status = store.StatusRetained
				touched[d.SubscriberID] = struct{}{}
			} else if err != nil && err != pgx.ErrNoRows {
				return fmt.Errorf("postgres store: lookup subscriber: %w", err)
			}
		}

		payload := d.Payload
		if payload == nil {
			payload = []byte{}
		}
		secret := d.Secret
		if secret == nil {
			secret = []byte{}
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO fold_deliveries (
				id, event_id, subscriber_id, url, partition, sequence,
				payload, event_type, status, attempt, next_attempt_at,
				last_error, owner, generation, claimed_at, created_at, secret
			) VALUES (
				$1, $2, $3, $4, $5, $6,
				$7, $8, $9, 0, $10,
				'', NULL, 0, NULL, $11, $12
			)
		`, d.ID, d.EventID, d.SubscriberID, d.URL, d.Partition, seq,
			payload, d.EventType, string(status), now, now, secret)
		if err != nil {
			return fmt.Errorf("postgres store: insert delivery: %w", err)
		}
	}

	for sub := range touched {
		if err := s.applyRetentionTx(ctx, tx, sub, now); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.signal()
	return nil
}

// Claim implements store.Store with partition scoping and head-of-line gating.
//
// When DistributedOwnership is false (legacy single-process), Claim uses the
// caller partition list and generation. When true, every Claim is authorized
// only through fold_partition_owners for a NodeID/incarnation/worker-N token;
// the partitions argument is ignored so a stale PartitionsClaim cannot strand
// work after handoff.
func (s *Store) Claim(ctx context.Context, owner string, generation uint64, partitions []int, limit int) ([]store.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, fmt.Errorf("postgres store: claim requires owner")
	}
	if generation == 0 {
		return nil, fmt.Errorf("postgres store: claim requires non-zero generation")
	}
	if limit <= 0 {
		return nil, nil
	}
	if err := s.refreshDistributedOwnership(ctx); err != nil {
		return nil, err
	}
	distributed := s.distributedOwnership.Load()
	if !distributed && len(partitions) == 0 {
		return nil, nil
	}

	// Build claim SQL. HOL and suspend gates are compile-time toggles for
	// invariant tests (DisableHOL / SkipSuspendGate); production leaves both on.
	holClause := `
		AND NOT EXISTS (
			SELECT 1 FROM fold_deliveries earlier
			WHERE earlier.subscriber_id = d.subscriber_id
			  AND earlier.sequence < d.sequence
			  AND earlier.status IN ('pending', 'in_flight')
		)`
	if s.DisableHOL {
		holClause = ""
	}
	suspendClause := `
		AND NOT EXISTS (
			SELECT 1 FROM fold_subscribers s
			WHERE s.subscriber_id = d.subscriber_id
			  AND s.suspended = TRUE
		)`
	if s.SkipSuspendGate {
		suspendClause = ""
	}

	var (
		query string
		args  []any
	)
	if distributed {
		logicalNode, ok := parseDistributedOwner(owner)
		if !ok {
			return nil, fmt.Errorf("postgres store: distributed Claim requires NodeID/incarnation/worker-N owner, got %q", owner)
		}
		if err := s.checkOwnershipMetadata(ctx); err != nil {
			return nil, err
		}
		stateClause := `AND po.state = 'active'`
		if s.BypassDrainingClaimGate {
			stateClause = ""
		}
		// Durable ownership is the source of truth for claimable partitions.
		query = fmt.Sprintf(`
			WITH candidates AS (
				SELECT d.id, po.generation AS own_gen
				FROM fold_deliveries d
				INNER JOIN fold_partition_owners po ON po.partition = d.partition
				WHERE d.status = 'pending'
				  AND d.next_attempt_at <= now()
				  AND po.owner_node = $1
				  %s
				  %s
				  %s
				ORDER BY d.partition, d.subscriber_id, d.sequence
				LIMIT $2
				FOR UPDATE OF d SKIP LOCKED
			)
			UPDATE fold_deliveries d
			SET status = 'in_flight',
			    owner = $3,
			    generation = c.own_gen,
			    claimed_at = now(),
			    attempt = d.attempt + 1
			FROM candidates c
			WHERE d.id = c.id
			RETURNING
				d.id, d.event_id, d.subscriber_id, d.url, d.partition, d.sequence,
				d.payload, d.event_type, d.status, d.attempt, d.next_attempt_at,
				d.last_error, d.owner, d.generation, d.claimed_at, d.created_at, d.secret
		`, stateClause, suspendClause, holClause)
		args = []any{logicalNode, limit, owner}
	} else {
		query = fmt.Sprintf(`
			WITH candidates AS (
				SELECT d.id
				FROM fold_deliveries d
				WHERE d.partition = ANY($1::int[])
				  AND d.status = 'pending'
				  AND d.next_attempt_at <= now()
				  %s
				  %s
				ORDER BY d.partition, d.subscriber_id, d.sequence
				LIMIT $2
				FOR UPDATE OF d SKIP LOCKED
			)
			UPDATE fold_deliveries d
			SET status = 'in_flight',
			    owner = $3,
			    generation = $4,
			    claimed_at = now(),
			    attempt = d.attempt + 1
			FROM candidates c
			WHERE d.id = c.id
			RETURNING
				d.id, d.event_id, d.subscriber_id, d.url, d.partition, d.sequence,
				d.payload, d.event_type, d.status, d.attempt, d.next_attempt_at,
				d.last_error, d.owner, d.generation, d.claimed_at, d.created_at, d.secret
		`, suspendClause, holClause)
		args = []any{partitions, limit, owner, int64(generation)}
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres store: claim: %w", err)
	}
	defer rows.Close()

	out := make([]store.Delivery, 0, limit)
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) checkOwnershipMetadata(ctx context.Context) error {
	var owners int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM fold_partition_owners`).Scan(&owners); err != nil {
		return fmt.Errorf("postgres store: ownership metadata lookup: %w", err)
	}
	if owners == 0 {
		return fmt.Errorf("postgres store: DistributedOwnership enabled but fold_partition_owners is empty")
	}
	var missing int
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM fold_deliveries d
		WHERE d.status IN ('pending', 'in_flight')
		  AND NOT EXISTS (
			SELECT 1 FROM fold_partition_owners po WHERE po.partition = d.partition
		  )
	`).Scan(&missing); err != nil {
		return fmt.Errorf("postgres store: ownership metadata check: %w", err)
	}
	if missing > 0 {
		return fmt.Errorf("postgres store: %d deliveries lack fold_partition_owners metadata", missing)
	}
	return nil
}

// MarkDelivered implements store.Store.
func (s *Store) MarkDelivered(ctx context.Context, id string, owner string, generation uint64) error {
	return s.markTerminal(ctx, id, owner, generation, store.StatusDelivered, "")
}

// MarkDeadLetter implements store.Store.
func (s *Store) MarkDeadLetter(ctx context.Context, id string, owner string, generation uint64, errMsg string) error {
	return s.markTerminal(ctx, id, owner, generation, store.StatusDeadLettered, errMsg)
}

// MarkFailed implements store.Store.
func (s *Store) MarkFailed(ctx context.Context, id string, owner string, generation uint64, attempt int, next time.Time, errMsg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if generation == 0 {
		return fmt.Errorf("postgres store: mark requires non-zero generation")
	}
	var (
		affected int64
		execErr  error
	)
	if s.SkipMarkOwnerGenCheck {
		tag, err := s.pool.Exec(ctx, `
			UPDATE fold_deliveries
			SET status = 'pending',
			    attempt = $2,
			    next_attempt_at = $3,
			    last_error = $4,
			    owner = NULL,
			    generation = 0,
			    claimed_at = NULL
			WHERE id = $1
			  AND status = 'in_flight'
		`, id, attempt, next.UTC(), errMsg)
		affected, execErr = tag.RowsAffected(), err
	} else {
		tag, err := s.pool.Exec(ctx, `
			UPDATE fold_deliveries
			SET status = 'pending',
			    attempt = $4,
			    next_attempt_at = $5,
			    last_error = $6,
			    owner = NULL,
			    generation = 0,
			    claimed_at = NULL
			WHERE id = $1
			  AND status = 'in_flight'
			  AND owner = $2
			  AND generation = $3
		`, id, owner, int64(generation), attempt, next.UTC(), errMsg)
		affected, execErr = tag.RowsAffected(), err
	}
	if execErr != nil {
		return fmt.Errorf("postgres store: mark failed: %w", execErr)
	}
	if affected == 0 {
		return fmt.Errorf("postgres store: stale claim on %s (owner/generation mismatch)", id)
	}
	s.signal()
	return nil
}

// ExhaustAndSuspend implements store.Store.
func (s *Store) ExhaustAndSuspend(ctx context.Context, id string, owner string, generation uint64, errMsg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if generation == 0 {
		return fmt.Errorf("postgres store: mark requires non-zero generation")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var subID string
	if s.SkipMarkOwnerGenCheck {
		err = tx.QueryRow(ctx, `
			UPDATE fold_deliveries
			SET status = 'dead_lettered',
			    last_error = $2,
			    owner = NULL,
			    generation = 0,
			    claimed_at = NULL
			WHERE id = $1
			  AND status = 'in_flight'
			RETURNING subscriber_id
		`, id, errMsg).Scan(&subID)
	} else {
		err = tx.QueryRow(ctx, `
			UPDATE fold_deliveries
			SET status = 'dead_lettered',
			    last_error = $4,
			    owner = NULL,
			    generation = 0,
			    claimed_at = NULL
			WHERE id = $1
			  AND status = 'in_flight'
			  AND owner = $2
			  AND generation = $3
			RETURNING subscriber_id
		`, id, owner, int64(generation), errMsg).Scan(&subID)
	}
	if err == pgx.ErrNoRows {
		return fmt.Errorf("postgres store: stale claim on %s (owner/generation mismatch)", id)
	}
	if err != nil {
		return fmt.Errorf("postgres store: exhaust: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO fold_subscribers (subscriber_id, suspended)
		VALUES ($1, TRUE)
		ON CONFLICT (subscriber_id) DO UPDATE
		SET suspended = TRUE
	`, subID)
	if err != nil {
		return fmt.Errorf("postgres store: suspend: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE fold_deliveries
		SET status = 'retained',
		    owner = NULL,
		    generation = 0,
		    claimed_at = NULL
		WHERE subscriber_id = $1
		  AND status = 'pending'
	`, subID)
	if err != nil {
		return fmt.Errorf("postgres store: retain pending: %w", err)
	}

	now := time.Now().UTC()
	if err := s.applyRetentionTx(ctx, tx, subID, now); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.signal()
	return nil
}

// Resume implements store.Store.
func (s *Store) Resume(ctx context.Context, subscriberID string) (store.GapInfo, error) {
	if err := ctx.Err(); err != nil {
		return store.GapInfo{}, err
	}
	if subscriberID == "" {
		return store.GapInfo{}, fmt.Errorf("postgres store: resume requires subscriberID")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return store.GapInfo{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	now := time.Now().UTC()
	if err := s.applyRetentionTx(ctx, tx, subscriberID, now); err != nil {
		return store.GapInfo{}, err
	}

	var gap store.GapInfo
	err = tx.QueryRow(ctx, `
		SELECT gap, dropped, marker
		FROM fold_subscribers
		WHERE subscriber_id = $1
	`, subscriberID).Scan(&gap.Occurred, &gap.Dropped, &gap.Marker)
	if err == pgx.ErrNoRows {
		// Never seen — nothing to resume.
		if err := tx.Commit(ctx); err != nil {
			return store.GapInfo{}, err
		}
		return store.GapInfo{}, nil
	}
	if err != nil {
		return store.GapInfo{}, fmt.Errorf("postgres store: resume lookup: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE fold_deliveries
		SET status = 'pending',
		    next_attempt_at = $2,
		    owner = NULL,
		    generation = 0,
		    claimed_at = NULL
		WHERE subscriber_id = $1
		  AND status = 'retained'
	`, subscriberID, now)
	if err != nil {
		return store.GapInfo{}, fmt.Errorf("postgres store: resume requeue: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE fold_subscribers
		SET suspended = FALSE,
		    gap = FALSE,
		    dropped = 0,
		    marker = ''
		WHERE subscriber_id = $1
	`, subscriberID)
	if err != nil {
		return store.GapInfo{}, fmt.Errorf("postgres store: clear suspension: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return store.GapInfo{}, err
	}
	s.signal()
	return gap, nil
}

// IsSuspended implements store.Store.
func (s *Store) IsSuspended(ctx context.Context, subscriberID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	var suspended bool
	err := s.pool.QueryRow(ctx, `
		SELECT suspended FROM fold_subscribers WHERE subscriber_id = $1
	`, subscriberID).Scan(&suspended)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return suspended, nil
}

// RecoverStale implements store.Store.
func (s *Store) RecoverStale(ctx context.Context, olderThan time.Duration) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	cutoff := time.Now().UTC().Add(-olderThan)
	tag, err := s.pool.Exec(ctx, `
		UPDATE fold_deliveries
		SET status = 'pending',
		    owner = NULL,
		    generation = 0,
		    claimed_at = NULL
		WHERE status = 'in_flight'
		  AND claimed_at IS NOT NULL
		  AND claimed_at < $1
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("postgres store: recover stale: %w", err)
	}
	n := int(tag.RowsAffected())
	if n > 0 {
		s.signal()
	}
	return n, nil
}

// BackdateClaimForTest sets claimed_at on an in_flight row. For crash-recovery tests.
func (s *Store) BackdateClaimForTest(ctx context.Context, id string, claimedAt time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE fold_deliveries
		SET claimed_at = $2
		WHERE id = $1 AND status = 'in_flight'
	`, id, claimedAt.UTC())
	if err != nil {
		return fmt.Errorf("postgres store: backdate claim: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres store: %s is not in_flight", id)
	}
	return nil
}

// PendingOrInFlight reports how many non-terminal, non-retained deliveries remain.
func (s *Store) PendingOrInFlight() int {
	var n int
	err := s.pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM fold_deliveries
		WHERE status IN ('pending', 'in_flight')
	`).Scan(&n)
	if err != nil {
		return 0
	}
	return n
}

// RetainedCount reports how many deliveries are held for a suspended subscriber.
func (s *Store) RetainedCount(ctx context.Context, subscriberID string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM fold_deliveries
		WHERE subscriber_id = $1 AND status = 'retained'
	`, subscriberID).Scan(&n)
	return n, err
}

func (s *Store) markTerminal(ctx context.Context, id, owner string, generation uint64, status store.Status, errMsg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if generation == 0 {
		return fmt.Errorf("postgres store: mark requires non-zero generation")
	}
	var (
		affected int64
		execErr  error
	)
	if s.SkipMarkOwnerGenCheck {
		// Tooth path: omit owner/generation so unused $params do not break pgx typing.
		tag, err := s.pool.Exec(ctx, `
			UPDATE fold_deliveries
			SET status = $2,
			    last_error = $3,
			    owner = NULL,
			    generation = 0,
			    claimed_at = NULL
			WHERE id = $1
			  AND status = 'in_flight'
		`, id, string(status), errMsg)
		affected, execErr = tag.RowsAffected(), err
	} else {
		tag, err := s.pool.Exec(ctx, `
			UPDATE fold_deliveries
			SET status = $4,
			    last_error = $5,
			    owner = NULL,
			    generation = 0,
			    claimed_at = NULL
			WHERE id = $1
			  AND status = 'in_flight'
			  AND owner = $2
			  AND generation = $3
		`, id, owner, int64(generation), string(status), errMsg)
		affected, execErr = tag.RowsAffected(), err
	}
	if execErr != nil {
		return fmt.Errorf("postgres store: mark terminal: %w", execErr)
	}
	if affected == 0 {
		return fmt.Errorf("postgres store: stale claim on %s (owner/generation mismatch)", id)
	}
	s.signal()
	return nil
}

func (s *Store) applyRetentionTx(ctx context.Context, tx pgx.Tx, subscriberID string, now time.Time) error {
	maxCount := s.retentionCount()
	maxAge := s.retentionAge()
	cutoff := now.Add(-maxAge)

	// Drop by age (oldest first for marker accuracy).
	rows, err := tx.Query(ctx, `
		SELECT id, sequence FROM fold_deliveries
		WHERE subscriber_id = $1
		  AND status = 'retained'
		  AND created_at < $2
		ORDER BY sequence ASC
	`, subscriberID, cutoff)
	if err != nil {
		return fmt.Errorf("postgres store: retention age scan: %w", err)
	}
	type drop struct {
		id  string
		seq int64
	}
	var aged []drop
	for rows.Next() {
		var d drop
		if err := rows.Scan(&d.id, &d.seq); err != nil {
			rows.Close()
			return err
		}
		aged = append(aged, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, d := range aged {
		if err := s.dropRetainedTx(ctx, tx, subscriberID, d.id, d.seq, "age"); err != nil {
			return err
		}
	}

	// Drop by count (keep newest sequences).
	rows, err = tx.Query(ctx, `
		SELECT id, sequence FROM fold_deliveries
		WHERE subscriber_id = $1 AND status = 'retained'
		ORDER BY sequence ASC
	`, subscriberID)
	if err != nil {
		return fmt.Errorf("postgres store: retention count scan: %w", err)
	}
	var retained []drop
	for rows.Next() {
		var d drop
		if err := rows.Scan(&d.id, &d.seq); err != nil {
			rows.Close()
			return err
		}
		retained = append(retained, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for len(retained) > maxCount {
		d := retained[0]
		retained = retained[1:]
		if err := s.dropRetainedTx(ctx, tx, subscriberID, d.id, d.seq, "count"); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) dropRetainedTx(ctx context.Context, tx pgx.Tx, subscriberID, id string, seq int64, reason string) error {
	_, err := tx.Exec(ctx, `DELETE FROM fold_deliveries WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("postgres store: drop retained: %w", err)
	}

	var dropped int
	err = tx.QueryRow(ctx, `
		INSERT INTO fold_subscribers (subscriber_id, suspended, gap, dropped, marker)
		VALUES ($1, TRUE, TRUE, 1, '')
		ON CONFLICT (subscriber_id) DO UPDATE
		SET gap = TRUE,
		    dropped = fold_subscribers.dropped + 1
		RETURNING dropped
	`, subscriberID).Scan(&dropped)
	if err != nil {
		return fmt.Errorf("postgres store: record gap: %w", err)
	}

	marker := fmt.Sprintf("%s:dropped=%d:through_seq=%d:%s", subscriberID, dropped, seq, reason)
	_, err = tx.Exec(ctx, `
		UPDATE fold_subscribers SET marker = $2 WHERE subscriber_id = $1
	`, subscriberID, marker)
	if err != nil {
		return fmt.Errorf("postgres store: update gap marker: %w", err)
	}
	return nil
}

type scannable interface {
	Scan(dest ...any) error
}

func scanDelivery(row scannable) (store.Delivery, error) {
	var (
		d          store.Delivery
		status     string
		owner      *string
		generation int64
		claimedAt  *time.Time
		lastError  string
		secret     []byte
	)
	err := row.Scan(
		&d.ID, &d.EventID, &d.SubscriberID, &d.URL, &d.Partition, &d.Sequence,
		&d.Payload, &d.EventType, &status, &d.Attempt, &d.NextAttempt,
		&lastError, &owner, &generation, &claimedAt, &d.CreatedAt, &secret,
	)
	if err != nil {
		return store.Delivery{}, fmt.Errorf("postgres store: scan delivery: %w", err)
	}
	d.Status = store.Status(status)
	d.LastError = lastError
	d.Generation = uint64(generation)
	d.Secret = secret
	if owner != nil {
		d.Owner = *owner
	}
	if claimedAt != nil {
		d.ClaimedAt = *claimedAt
	}
	return d, nil
}

// TruncateForTest wipes all fold tables. For tests only.
func (s *Store) TruncateForTest(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		TRUNCATE fold_deliveries, fold_subscribers, fold_subscriber_seq, fold_partition_owners
	`)
	if err != nil {
		return err
	}
	s.distributedOwnership.Store(false)
	return nil
}

// testAdvisoryLockKey serializes packages that share the fold_test database.
// go test ./... runs packages in parallel; without this, concurrent Truncate/
// Claim races flake.
const testAdvisoryLockKey = int64(0xF01D_7E57)

// AcquireTestDBLock takes a session-level advisory lock on a dedicated pool
// connection so only one Postgres test suite mutates the shared DB at a time.
// Call the returned release function from t.Cleanup.
func (s *Store) AcquireTestDBLock(ctx context.Context) (release func(), err error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres store: acquire test lock conn: %w", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, testAdvisoryLockKey); err != nil {
		conn.Release()
		return nil, fmt.Errorf("postgres store: advisory lock: %w", err)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, testAdvisoryLockKey)
			conn.Release()
		})
	}, nil
}
