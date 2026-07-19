package schema

import (
	"context"
	"fmt"
)

// Pre-migration repairs run immediately before a specific pending migration
// file is applied. Shipped migration files are frozen (see
// scripts/check-migration-hygiene.sh): editing one forks fresh clones from
// upgraded clones via the recorded content hash, and a bug that makes a
// migration FAIL on drifted databases cannot be fixed forward with a new
// migration either — the failing file aborts the pass before any later
// version runs. Repairing the drift in code, keyed to the pending version,
// is the only path that heals affected databases without touching shipped
// SQL. Precedent: ensureContentHashColumn, the aux row-id backfill.

// preMigrationRepair dispatches any repair registered for (source, version).
func (m migrationSource) preMigrationRepair(ctx context.Context, db DBConn, version int) error {
	if m.cursorTable == "schema_migrations" && version == 53 {
		if err := ensureIssuesRigColumns(ctx, db); err != nil {
			return err
		}
		if err := ensureDependenciesSplitTargets(ctx, db); err != nil {
			return err
		}
		return ensureWispDependenciesSplitTargets(ctx, db)
	}
	if m.cursorTable == "schema_migrations" && version == 54 {
		return ensureLeaseColumns(ctx, db)
	}
	return nil
}

// ensureIssuesRigColumns repairs #4502: the rig/agent columns were only ever
// added to the squashed bootstrap 0001_create_issues, so a database
// bootstrapped before they existed reaches schema v52 without them, and
// migration 0053 — which copies exactly these columns from wisps into
// issues — fails with "Unknown column" even with zero rig wisps to repair.
// Databases in the wild may have some but not all six, so each is checked
// individually. Definitions mirror the current bootstrap schema.
func ensureIssuesRigColumns(ctx context.Context, db DBConn) error {
	columns := []struct{ name, definition string }{
		{"hook_bead", "VARCHAR(255) DEFAULT ''"},
		{"role_bead", "VARCHAR(255) DEFAULT ''"},
		{"agent_state", "VARCHAR(32) DEFAULT ''"},
		{"last_activity", "DATETIME"},
		{"role_type", "VARCHAR(32) DEFAULT ''"},
		{"rig", "VARCHAR(255) DEFAULT ''"},
	}
	for _, col := range columns {
		present, err := schemaColumnExists(ctx, db, "issues", col.name)
		if err != nil {
			return fmt.Errorf("checking issues.%s: %w", col.name, err)
		}
		if present {
			continue
		}
		if _, err := db.ExecContext(ctx, "ALTER TABLE issues ADD COLUMN "+col.name+" "+col.definition); err != nil {
			return fmt.Errorf("adding issues.%s for migration 0053: %w", col.name, err)
		}
	}
	return nil
}

// ensureWispDependenciesSplitTargets repairs #4555: a mixed-vintage local
// wisp_dependencies table can have the post-0005 id column while still lacking
// one or more split target columns. Migration 0053 reads those columns when it
// repairs rig wisps, so add the missing columns and backfill them from the
// legacy depends_on_id column when that source column is still available.
func ensureDependenciesSplitTargets(ctx context.Context, db DBConn) error {
	table, err := schemaTableExists(ctx, db, "dependencies")
	if err != nil {
		return fmt.Errorf("checking dependencies table: %w", err)
	}
	if !table {
		return nil
	}

	columns := dependenciesSplitTargetColumns()
	missing := make([]struct{ name, definition string }, 0, len(columns))
	for _, col := range columns {
		present, err := schemaColumnExists(ctx, db, "dependencies", col.name)
		if err != nil {
			return fmt.Errorf("checking dependencies.%s: %w", col.name, err)
		}
		if !present {
			missing = append(missing, col)
		}
	}
	if len(missing) == 0 {
		return nil
	}

	for _, col := range missing {
		if _, err := db.ExecContext(ctx, "ALTER TABLE dependencies ADD COLUMN "+col.name+" "+col.definition); err != nil {
			return fmt.Errorf("adding dependencies.%s for migration 0053: %w", col.name, err)
		}
	}

	legacyTarget, err := schemaColumnExists(ctx, db, "dependencies", "depends_on_id")
	if err != nil {
		return fmt.Errorf("checking dependencies.depends_on_id: %w", err)
	}
	if !legacyTarget {
		return nil
	}

	for _, repair := range dependenciesSplitTargetBackfillSQL() {
		if _, err := db.ExecContext(ctx, repair); err != nil {
			return fmt.Errorf("backfilling dependencies split targets for migration 0053: %w", err)
		}
	}
	return nil
}

// ensureWispDependenciesSplitTargets repairs #4555: a mixed-vintage local
// wisp_dependencies table can have the post-0005 id column while still lacking
// one or more split target columns. Migration 0053 reads those columns when it
// repairs rig wisps, so add the missing columns and backfill them from the
// legacy depends_on_id column when that source column is still available.
func ensureWispDependenciesSplitTargets(ctx context.Context, db DBConn) error {
	table, err := schemaTableExists(ctx, db, "wisp_dependencies")
	if err != nil {
		return fmt.Errorf("checking wisp_dependencies table: %w", err)
	}
	if !table {
		return nil
	}

	columns := wispDependenciesSplitTargetColumns()
	missing := make([]struct{ name, definition string }, 0, len(columns))
	for _, col := range columns {
		present, err := schemaColumnExists(ctx, db, "wisp_dependencies", col.name)
		if err != nil {
			return fmt.Errorf("checking wisp_dependencies.%s: %w", col.name, err)
		}
		if !present {
			missing = append(missing, col)
		}
	}
	if len(missing) == 0 {
		return nil
	}

	for _, col := range missing {
		if _, err := db.ExecContext(ctx, "ALTER TABLE wisp_dependencies ADD COLUMN "+col.name+" "+col.definition); err != nil {
			return fmt.Errorf("adding wisp_dependencies.%s for migration 0053: %w", col.name, err)
		}
	}

	legacyTarget, err := schemaColumnExists(ctx, db, "wisp_dependencies", "depends_on_id")
	if err != nil {
		return fmt.Errorf("checking wisp_dependencies.depends_on_id: %w", err)
	}
	if !legacyTarget {
		return nil
	}

	for _, repair := range wispDependenciesSplitTargetBackfillSQL() {
		if _, err := db.ExecContext(ctx, repair); err != nil {
			return fmt.Errorf("backfilling wisp_dependencies split targets for migration 0053: %w", err)
		}
	}
	return nil
}

// ensureLeaseColumns repairs older workspaces created before 0054. They may
// still be missing the lease/row-lock columns that close/claim paths expect.
// Add the columns idempotently so upgraded databases can use the current
// lease-aware SQL even if the initial migration pass skipped or partially
// applied 0054.
func ensureLeaseColumns(ctx context.Context, db DBConn) error {
	if err := ensureLeaseColumnsOnTable(ctx, db, "issues", true); err != nil {
		return err
	}
	if err := ensureLeaseColumnsOnTable(ctx, db, "wisps", false); err != nil {
		return err
	}
	return nil
}

func ensureLeaseColumnsOnTable(ctx context.Context, db DBConn, table string, addIndex bool) error {
	exists, err := schemaTableExists(ctx, db, table)
	if err != nil {
		return fmt.Errorf("checking %s table: %w", table, err)
	}
	if !exists {
		return nil
	}

	columns := []struct{ name, definition string }{
		{"lease_expires_at", "DATETIME"},
		{"heartbeat_at", "DATETIME"},
		{"row_lock", "BIGINT NOT NULL DEFAULT 0"},
	}
	for _, col := range columns {
		present, err := schemaColumnExists(ctx, db, table, col.name)
		if err != nil {
			return fmt.Errorf("checking %s.%s: %w", table, col.name, err)
		}
		if present {
			continue
		}
		if _, err := db.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN "+col.name+" "+col.definition); err != nil {
			return fmt.Errorf("adding %s.%s for migration 0054: %w", table, col.name, err)
		}
	}

	if !addIndex {
		return nil
	}
	if _, err := db.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS idx_issues_lease ON issues (status, lease_expires_at)
	`); err != nil {
		// Dolt/MySQL compatibility: some engines reject IF NOT EXISTS on CREATE
		// INDEX, so fall back to a direct existence probe before giving up.
		exists, probeErr := schemaIndexExists(ctx, db, "issues", "idx_issues_lease")
		if probeErr != nil {
			return fmt.Errorf("creating idx_issues_lease: %w", err)
		}
		if exists {
			return nil
		}
		if _, secondErr := db.ExecContext(ctx, "CREATE INDEX idx_issues_lease ON issues (status, lease_expires_at)"); secondErr != nil {
			return fmt.Errorf("creating idx_issues_lease: %w", secondErr)
		}
	}
	return nil
}

func wispDependenciesSplitTargetColumns() []struct{ name, definition string } {
	return []struct{ name, definition string }{
		{"depends_on_issue_id", "VARCHAR(255) NULL"},
		{"depends_on_wisp_id", "VARCHAR(255) NULL"},
		{"depends_on_external", "VARCHAR(255) NULL"},
	}
}

func dependenciesSplitTargetColumns() []struct{ name, definition string } {
	return []struct{ name, definition string }{
		{"depends_on_issue_id", "VARCHAR(255) NULL"},
		{"depends_on_wisp_id", "VARCHAR(255) NULL"},
		{"depends_on_external", "VARCHAR(255) NULL"},
	}
}

func dependenciesSplitTargetBackfillSQL() []string {
	return []string{
		"UPDATE dependencies SET depends_on_external = depends_on_id WHERE depends_on_external IS NULL AND depends_on_id LIKE 'external:%'",
		"UPDATE dependencies d JOIN wisps w ON w.id = d.depends_on_id SET d.depends_on_wisp_id = d.depends_on_id WHERE d.depends_on_wisp_id IS NULL AND d.depends_on_external IS NULL",
		"UPDATE dependencies d JOIN issues i ON i.id = d.depends_on_id SET d.depends_on_issue_id = d.depends_on_id WHERE d.depends_on_issue_id IS NULL AND d.depends_on_external IS NULL AND d.depends_on_wisp_id IS NULL",
		"UPDATE dependencies SET depends_on_external = depends_on_id WHERE depends_on_external IS NULL AND depends_on_wisp_id IS NULL AND depends_on_issue_id IS NULL",
	}
}

func wispDependenciesSplitTargetBackfillSQL() []string {
	return []string{
		"UPDATE wisp_dependencies SET depends_on_external = depends_on_id WHERE depends_on_external IS NULL AND depends_on_id LIKE 'external:%'",
		"UPDATE wisp_dependencies wd JOIN wisps w ON w.id = wd.depends_on_id SET wd.depends_on_wisp_id = wd.depends_on_id WHERE wd.depends_on_wisp_id IS NULL AND wd.depends_on_external IS NULL",
		"UPDATE wisp_dependencies wd JOIN issues i ON i.id = wd.depends_on_id SET wd.depends_on_issue_id = wd.depends_on_id WHERE wd.depends_on_issue_id IS NULL AND wd.depends_on_external IS NULL AND wd.depends_on_wisp_id IS NULL",
		"UPDATE wisp_dependencies SET depends_on_external = depends_on_id WHERE depends_on_external IS NULL AND depends_on_wisp_id IS NULL AND depends_on_issue_id IS NULL",
	}
}

func schemaTableExists(ctx context.Context, db DBConn, table string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
	`, table).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func schemaColumnExists(ctx context.Context, db DBConn, table, column string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?
	`, table, column).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func schemaIndexExists(ctx context.Context, db DBConn, table, index string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?
	`, table, index).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}
