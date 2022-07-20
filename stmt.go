package dbresolver

import (
	"context"
	"database/sql"
)

// Stmt is an aggregate prepared statement.
// It holds a prepared statement for each underlying physical db.
type Stmt interface {
	Close() error
	Exec(...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error)
	Query(...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error)
	QueryRow(args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row
}

type stmt struct {
	db *DBImpl

	primaryStmt  *sql.Stmt
	replicaStmts []*sql.Stmt
}

// Close closes the statement by concurrently closing all underlying
// statements concurrently, returning the first non nil error.
func (s *stmt) Close() error {
	return doParallely(s.db.totalConnection, func(i int) error {
		if i == 0 {
			return s.primaryStmt.Close()
		}

		return s.replicaStmts[i-1].Close()
	})
}

// Exec executes a prepared statement with the given arguments
// and returns a Result summarizing the effect of the statement.
// Exec uses the master as the underlying physical db.
func (s *stmt) Exec(args ...interface{}) (sql.Result, error) {
	// return s.stmts[0].Exec(args...)
	return s.RWStmt().Exec(args...)
}

// ExecContext executes a prepared statement with the given arguments
// and returns a Result summarizing the effect of the statement.
// Exec uses the master as the underlying physical db.
func (s *stmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	// return s.stmts[0].Exec(args...)
	return s.RWStmt().ExecContext(ctx, args...)
}

// Query executes a prepared query statement with the given
// arguments and returns the query results as a *sql.Rows.
// Query uses the read only DB as the underlying physical db.
func (s *stmt) Query(args ...interface{}) (*sql.Rows, error) {
	return s.ROStmt().Query(args...)
}

// QueryContext executes a prepared query statement with the given
// arguments and returns the query results as a *sql.Rows.
// Query uses the read only DB as the underlying physical db.
func (s *stmt) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	return s.ROStmt().QueryContext(ctx, args...)
}

// QueryRow executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error
// will be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *sql.Row's Scan scans the first selected row and discards the rest.
// QueryRow uses the read only DB as the underlying physical db.
func (s *stmt) QueryRow(args ...interface{}) *sql.Row {
	return s.ROStmt().QueryRow(args...)
}

// QueryRowContext executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error
// will be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *sql.Row's Scan scans the first selected row and discards the rest.
// QueryRowContext uses the read only DB as the underlying physical db.
func (s *stmt) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {
	return s.ROStmt().QueryRowContext(ctx, args...)
}

// ROStmt return the replica statment
func (s *stmt) ROStmt() *sql.Stmt {
	if len(s.replicaStmts) == 0 {
		return s.primaryStmt
	}
	return s.replicaStmts[s.db.rounRobin(len(s.replicaStmts))]
}

// RWStmt return the primary statement
func (s *stmt) RWStmt() *sql.Stmt {
	return s.primaryStmt
}
