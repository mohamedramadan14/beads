package issueops

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// CloseResult holds the result of a CloseIssueInTx call.
type CloseResult struct {
	IsWisp        bool
	AlreadyClosed bool
}

// CloseIssueInTx closes an issue within a transaction, setting status to closed
// and recording the close event. Routes to the correct table (issues/wisps)
// automatically. The caller is responsible for Dolt versioning if needed.
func CloseIssueInTx(ctx context.Context, tx DBTX, id string, reason, actor, session string) (*CloseResult, error) {
	return closeIssueInTx(ctx, tx, id, reason, actor, session, true)
}

func CloseIssueWithoutEventInTx(ctx context.Context, tx DBTX, id string, reason, actor, session string) (*CloseResult, error) {
	return closeIssueInTx(ctx, tx, id, reason, actor, session, false)
}

// CloseIssueCheckedInTx closes an issue within a transaction, refusing with
// storage.ErrCloseBlocked when it is still blocked (is_blocked=1) unless force
// is set. The guard (IsBlockedInTx) and the close (CloseIssueInTx) share the
// SAME transaction, so no blocker can clear between the check and the close.
//
// When expectedVersion is non-nil it adds an ORTHOGONAL optimistic-concurrency
// precondition: the row's current RowVersion (row_lock) must still equal
// *expectedVersion or the close refuses with storage.ErrVersionMismatch. This
// runs first — before the is_blocked guard and before force short-circuits it —
// so force bypasses only the is_blocked guard, never the version check. Because
// the read shares this transaction, a mismatch returns before any write and the
// transaction rolls back with the issue unchanged (a true compare-and-swap).
func CloseIssueCheckedInTx(ctx context.Context, tx DBTX, id, reason, actor, session string, force bool, expectedVersion *int64) (*CloseResult, error) {
	if expectedVersion != nil {
		if err := CheckVersionInTx(ctx, tx, id, *expectedVersion); err != nil {
			return nil, err
		}
	}
	if !force {
		blocked, blockers, err := IsBlockedInTx(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if blocked {
			// A purely transitive block (e.g. an active parent-child chain)
			// sets is_blocked=1 without any direct blocks/waits-for/
			// conditional-blocks edge, so blockers is empty — omit the "blocked
			// by" clause rather than render "blocked by []".
			if len(blockers) > 0 {
				return nil, fmt.Errorf("%w: %s is blocked by %v", storage.ErrCloseBlocked, id, blockers)
			}
			return nil, fmt.Errorf("%w: %s", storage.ErrCloseBlocked, id)
		}
	}
	return CloseIssueInTx(ctx, tx, id, reason, actor, session)
}

//nolint:gosec // G201: table names come from WispTableRouting (hardcoded constants)
func closeIssueInTx(ctx context.Context, tx DBTX, id string, reason, actor, session string, recordEvent bool) (*CloseResult, error) {
	isWisp := IsActiveWispInTx(ctx, tx, id)
	issueTable, _, eventTable, _ := WispTableRouting(isWisp)

	var affectedIssues, affectedWisps []string
	var aerr error
	if isWisp {
		affectedIssues, affectedWisps, aerr = AffectedByStatusChangeForWispInTx(ctx, tx, id)
	} else {
		affectedIssues, affectedWisps, aerr = AffectedByStatusChangeInTx(ctx, tx, id)
	}
	if aerr != nil {
		return nil, fmt.Errorf("affected by close for %s: %w", id, aerr)
	}

	now := time.Now().UTC()

	// row_lock is rewritten on close so a concurrent reclaim (which also rewrites
	// row_lock) collides on this cell and is forced to conflict-and-retry rather
	// than silently cell-merging a revert-to-ready over a completed close (see
	// lease.go). The lease row is deleted below: a closed issue holds no lease.
	result, err := tx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s SET status = ?, closed_at = ?, updated_at = ?, close_reason = ?, closed_by_session = ?,
			row_lock = ?
		WHERE id = ? AND status != ?
	`, issueTable), types.StatusClosed, now, now, reason, session, freshRowLock(), id, types.StatusClosed)
	if err != nil {
		return nil, fmt.Errorf("failed to close issue: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		var status string
		qerr := tx.QueryRowContext(ctx,
			fmt.Sprintf(`SELECT status FROM %s WHERE id = ?`, issueTable), id,
		).Scan(&status)
		if qerr == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: issue %s", storage.ErrNotFound, id)
		}
		if qerr != nil {
			return nil, fmt.Errorf("failed to check issue existence: %w", qerr)
		}
		if types.Status(status) == types.StatusClosed {
			return &CloseResult{IsWisp: isWisp, AlreadyClosed: true}, nil
		}
		return nil, fmt.Errorf("failed to close issue: %s", id)
	}

	// A closed issue holds no lease (no-op for wisps, which are never leased).
	if err := DeleteLeaseInTx(ctx, tx, id); err != nil {
		return nil, err
	}

	if recordEvent {
		if err := RecordEventInTable(ctx, tx, eventTable, id, types.EventClosed, actor, reason); err != nil {
			return nil, fmt.Errorf("failed to record event: %w", err)
		}
	}

	if err := RecomputeIsBlockedInTx(ctx, tx, affectedIssues, affectedWisps); err != nil {
		return nil, fmt.Errorf("recompute is_blocked after close for %s: %w", id, err)
	}

	return &CloseResult{IsWisp: isWisp}, nil
}
