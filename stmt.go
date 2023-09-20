package dbresolver

import (
	"context"
	"database/sql"

	"go.uber.org/multierr"
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
	loadBalancer StmtLoadBalancer
	primaryStmts []*sql.Stmt
	replicaStmts []*sql.Stmt
	writeFlag    bool
	dbStmt       map[*sql.DB]*sql.Stmt
}

// Close closes the statement by concurrently closing all underlying
// statements concurrently, returning the first non nil error.
func (s *stmt) Close() error {
	errPrimaries := doParallely(len(s.primaryStmts), func(i int) error {
		return s.primaryStmts[i].Close()
	})
	errReplicas := doParallely(len(s.replicaStmts), func(i int) error {
		return s.replicaStmts[i].Close()
	})

	return multierr.Combine(errPrimaries, errReplicas)
}

// Exec executes a prepared statement with the given arguments
// and returns a Result summarizing the effect of the statement.
// Exec uses the master as the underlying physical db.
func (s *stmt) Exec(args ...interface{}) (sql.Result, error) {
	return s.ExecContext(context.Background(), args...)
}

// ExecContext executes a prepared statement with the given arguments
// and returns a Result summarizing the effect of the statement.
// Exec uses the master as the underlying physical db.
func (s *stmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	return s.RWStmt().ExecContext(ctx, args...)
}

// Query executes a prepared query statement with the given
// arguments and returns the query results as a *sql.Rows.
// Query uses the read only DB as the underlying physical db.
func (s *stmt) Query(args ...interface{}) (*sql.Rows, error) {
	return s.QueryContext(context.Background(), args...)
}

// QueryContext executes a prepared query statement with the given
// arguments and returns the query results as a *sql.Rows.
// Query uses the read only DB as the underlying physical db.
func (s *stmt) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	var curStmt *sql.Stmt
	if s.writeFlag {
		curStmt = s.RWStmt()
	} else {
		curStmt = s.ROStmt()
	}

	rows, err := curStmt.QueryContext(ctx, args...)
	if isDBConnectionError(err) && !s.writeFlag {
		rows, err = s.RWStmt().QueryContext(ctx, args...)
	}
	return rows, err
}

// QueryRow executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error
// will be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *sql.Row's Scan scans the first selected row and discards the rest.
// QueryRow uses the read only DB as the underlying physical db.
func (s *stmt) QueryRow(args ...interface{}) *sql.Row {
	return s.QueryRowContext(context.Background(), args...)
}

// QueryRowContext executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error
// will be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *sql.Row's Scan scans the first selected row and discards the rest.
// QueryRowContext uses the read only DB as the underlying physical db.
func (s *stmt) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {
	var curStmt *sql.Stmt
	if s.writeFlag {
		curStmt = s.RWStmt()
	} else {
		curStmt = s.ROStmt()
	}

	row := curStmt.QueryRowContext(ctx, args...)
	if isDBConnectionError(row.Err()) && !s.writeFlag {
		row = s.RWStmt().QueryRowContext(ctx, args...)
	}
	return row
}

// ROStmt return the replica statement
func (s *stmt) ROStmt() *sql.Stmt {
	totalStmtsConn := len(s.replicaStmts) + len(s.primaryStmts)
	if totalStmtsConn == len(s.primaryStmts) {
		return s.loadBalancer.Resolve(s.primaryStmts)
	}
	return s.loadBalancer.Resolve(s.replicaStmts)
}

// RWStmt return the primary statement
func (s *stmt) RWStmt() *sql.Stmt {
	return s.loadBalancer.Resolve(s.primaryStmts)
}

// stmtForDB returns the corresponding *sql.Stmt instance for the given *sql.DB.
// Ihis is needed because sql.Tx.Stmt() requires that the passed *sql.Stmt be from the same database
// as the transaction.
func (s *stmt) stmtForDB(db *sql.DB) *sql.Stmt {
	xsm, ok := s.dbStmt[db]
	if ok {
		return xsm
	}

	// return any statement so errors can be detected by Tx.Stmt()
	return s.RWStmt()
}

// newSingleDBStmt creates a new stmt for a single DB connection.
// This is used by statements return by transaction and connections.
func newSingleDBStmt(sourceDB *sql.DB, st *sql.Stmt, writeFlag bool) *stmt {
	return &stmt{
		loadBalancer: &RoundRobinLoadBalancer[*sql.Stmt]{},
		primaryStmts: []*sql.Stmt{st},
		dbStmt: map[*sql.DB]*sql.Stmt{
			sourceDB: st,
		},
		writeFlag: writeFlag,
	}
}
