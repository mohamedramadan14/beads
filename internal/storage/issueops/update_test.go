package issueops

import (
	"context"
	"testing"

	"github.com/steveyegge/beads/internal/types"
)

// TestManageLeaseOnUpdate pins the clear-only contract (bd-9hpgf, GH#4716):
// generic updates never arm a lease — arming belongs to claim/heartbeat. The
// helper reports that the lease row must be dropped (DeleteLeaseInTx) whenever
// the update leaves the claimed state or changes who holds the claim, and
// leaves the lease untouched when the same claim stays in place.
func TestManageLeaseOnUpdate(t *testing.T) {
	tests := []struct {
		name      string
		oldStatus types.Status
		oldOwner  string
		updates   map[string]interface{}
		wantClear bool // false = lease columns untouched
	}{
		{
			name:      "bare hand-dole claim does not arm",
			oldStatus: types.StatusOpen,
			updates:   map[string]interface{}{"status": "in_progress", "assignee": "crow"},
			wantClear: true,
		},
		{
			name:      "status-only transition into in_progress with existing assignee does not arm",
			oldStatus: types.StatusOpen,
			oldOwner:  "crow",
			updates:   map[string]interface{}{"status": "in_progress"},
			wantClear: true,
		},
		{
			name:      "assignee transfer clears the old owner's lease",
			oldStatus: types.StatusInProgress,
			oldOwner:  "alice",
			updates:   map[string]interface{}{"assignee": "bob"},
			wantClear: true,
		},
		{
			name:      "reopen clears the lease",
			oldStatus: types.StatusInProgress,
			oldOwner:  "alice",
			updates:   map[string]interface{}{"status": "open"},
			wantClear: true,
		},
		{
			name:      "unassign clears the lease",
			oldStatus: types.StatusInProgress,
			oldOwner:  "alice",
			updates:   map[string]interface{}{"assignee": nil},
			wantClear: true,
		},
		{
			name:      "same-claim status re-assert leaves lease alone",
			oldStatus: types.StatusInProgress,
			oldOwner:  "alice",
			updates:   map[string]interface{}{"status": "in_progress"},
			wantClear: false,
		},
		{
			name:      "same-claim assignee re-assert leaves lease alone",
			oldStatus: types.StatusInProgress,
			oldOwner:  "alice",
			updates:   map[string]interface{}{"status": types.StatusInProgress, "assignee": "alice"},
			wantClear: false,
		},
		{
			name:      "no status or assignee in updates is a no-op",
			oldStatus: types.StatusInProgress,
			oldOwner:  "alice",
			updates:   map[string]interface{}{"priority": 1},
			wantClear: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldIssue := &types.Issue{Status: tt.oldStatus, Assignee: tt.oldOwner}

			setClauses, _ := ManageLeaseOnUpdate(oldIssue, tt.updates, nil, nil, context.Background())
			gotClear := len(setClauses) > 0
			if gotClear != tt.wantClear {
				t.Fatalf("clear = %v, want %v", gotClear, tt.wantClear)
			}
			if gotClear {
				if len(setClauses) != 2 {
					t.Fatalf("clear clauses = %v, want exactly 2 lease-clearing clauses", setClauses)
				}
				if setClauses[0] != "lease_expires_at = NULL" || setClauses[1] != "heartbeat_at = NULL" {
					t.Fatalf("clear clauses = %v, want lease column clears", setClauses)
				}
			}
		})
	}
}
