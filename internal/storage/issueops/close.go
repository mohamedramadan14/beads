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

	leaseColumnsPresent, err := hasLeaseCloseColumnsInTx(ctx, tx, issueTable)
	if err != nil {
		return nil, fmt.Errorf("check lease columns for %s: %w", id, err)
	}

	// An ownership guard (--if-assignee/--if-fence) folds into the WHERE so
	// the check-and-close is one atomic compare-and-swap (see guard.go).
	guard, hasGuard := GuardFrom(ctx)
	guardClause, guardArgs := guard.whereClause()

	// row_lock is rewritten on close so a concurrent reclaim (which also rewrites
	// row_lock) collides on this cell and is forced to conflict-and-retry rather
	// than silently cell-merging a revert-to-ready over a completed close (see
	// lease.go). lease_expires_at/heartbeat_at are cleared: a closed issue holds
	// no lease. Older upgraded workspaces may still be missing the lease columns,
	// so make the lease update conditional rather than failing the close.
	setClause := "status = ?, closed_at = ?, updated_at = ?, close_reason = ?, closed_by_session = ?"
	args := []interface{}{types.StatusClosed, now, now, reason, session}
	if leaseColumnsPresent {
		setClause += ", lease_expires_at = NULL, heartbeat_at = NULL, row_lock = ?"
		args = append(args, freshRowLock())
	}
	args = append(args, id, types.StatusClosed)
	args = append(args, guardArgs...)
	result, err := tx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s SET %s
		WHERE id = ? AND status != ?%s
	`, issueTable, setClause, guardClause), args...)
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
			if hasGuard {
				// Idempotency must not bypass the guard: a zombie's stale
				// snapshot on an already-closed row is a conflict, not a
				// false completion signal. A legitimate same-caller retry
				// still matches (close neither clears assignee nor bumps the
				// fence) and keeps the AlreadyClosed success.
				matched, merr := GuardMatchesCurrentRow(ctx, tx, issueTable, id, guard)
				if merr != nil {
					return nil, merr
				}
				if !matched {
					return nil, GuardPreconditionError(ctx, tx, issueTable, id, guard)
				}
			}
			return &CloseResult{IsWisp: isWisp, AlreadyClosed: true}, nil
		}
		if hasGuard {
			// The row exists and is not closed — the guard is what failed.
			return nil, GuardPreconditionError(ctx, tx, issueTable, id, guard)
		}
		return nil, fmt.Errorf("failed to close issue: %s", id)
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

func hasLeaseCloseColumnsInTx(ctx context.Context, tx DBTX, table string) (bool, error) {
	for _, column := range []string{"lease_expires_at", "heartbeat_at", "row_lock"} {
		var count int
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE()
			  AND TABLE_NAME = ?
			  AND COLUMN_NAME = ?
		`, table, column).Scan(&count); err != nil {
			return false, err
		}
		if count == 0 {
			return false, nil
		}
	}
	return true, nil
}
