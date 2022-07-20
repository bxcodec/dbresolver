package dbresolver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync/atomic"
	"time"
)

// DB is a logical database with multiple underlying physical databases
// forming a single ReadWrite with multiple ReadOnly database.
// Reads and writes are automatically directed to the correct physical db.
type DB struct {
	rwdb            *sql.DB
	rodbs           []*sql.DB
	totalConnection int
	roCount         uint64 // Monotonically incrementing counter on each query
}

// Open concurrently opens each underlying physical db.
// dataSourceNames must be a semi-comma separated list of DSNs with the first
// one being used as the RW-database and the rest as RO databases.
func Open(driverName, dataSourceNames string) (db *DB, err error) {
	conns := strings.Split(dataSourceNames, ";")
	db = &DB{
		rodbs: make([]*sql.DB, len(conns)-1),
	}

	db.totalConnection = len(conns)
	err = doParallely(db.totalConnection, func(i int) (err error) {
		if i == 0 {
			db.rwdb, err = sql.Open(driverName, conns[i])
			return err
		}
		var roDB *sql.DB
		roDB, err = sql.Open(driverName, conns[i])
		if err != nil {
			return
		}
		db.rodbs[i-1] = roDB
		return err
	})

	return db, err
}

// Close closes all physical databases concurrently, releasing any open resources.
func (db *DB) Close() error {
	return doParallely(db.totalConnection, func(i int) (err error) {
		if i == 0 {
			return db.rwdb.Close()
		}
		return db.rodbs[i-1].Close()
	})

}

// Driver returns the physical database's underlying driver.
func (db *DB) Driver() driver.Driver {
	return db.ReadWrite().Driver()
}

// Begin starts a transaction on the RW-database. The isolation level is dependent on the driver.
func (db *DB) Begin() (*sql.Tx, error) {
	return db.ReadWrite().Begin()
}

// BeginTx starts a transaction with the provided context on the RW-database.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return db.ReadWrite().BeginTx(ctx, opts)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the RW-database as the underlying physical db.
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.ReadWrite().Exec(query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the RW-database as the underlying physical db.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.ReadWrite().ExecContext(ctx, query, args...)
}

// Ping verifies if a connection to each physical database is still alive,
// establishing a connection if necessary.
func (db *DB) Ping() error {
	return doParallely(db.totalConnection, func(i int) error {
		if i == 0 {
			return db.rwdb.Ping()
		}
		return db.rodbs[i-1].Ping()
	})
}

// PingContext verifies if a connection to each physical database is still
// alive, establishing a connection if necessary.
func (db *DB) PingContext(ctx context.Context) error {
	return doParallely(db.totalConnection, func(i int) error {
		if i == 0 {
			return db.rwdb.PingContext(ctx)
		}
		return db.rodbs[i-1].Ping()
	})
}

// Prepare creates a prepared statement for later queries or executions
// on each physical database, concurrently.
func (db *DB) Prepare(query string) (Stmt, error) {
	stmt := &stmt{
		db: db,
	}
	roStmts := make([]*sql.Stmt, len(db.rodbs))
	err := doParallely(db.totalConnection, func(i int) (err error) {
		if i == 0 {
			stmt.rwstmt, err = db.rwdb.Prepare(query)
			return err
		}

		return doParallely(len(db.rodbs), func(i int) (err error) {
			roStmts[i], err = db.rodbs[i].Prepare(query)
			return err
		})
	})

	if err != nil {
		return nil, err
	}
	stmt.rostmts = roStmts

	return stmt, nil
}

// PrepareContext creates a prepared statement for later queries or executions
// on each physical database, concurrently.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func (db *DB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	stmt := &stmt{
		db: db,
	}
	roStmts := make([]*sql.Stmt, len(db.rodbs))
	err := doParallely(db.totalConnection, func(i int) (err error) {
		if i == 0 {
			stmt.rwstmt, err = db.rwdb.PrepareContext(ctx, query)
			return err
		}

		return doParallely(len(db.rodbs), func(i int) (err error) {
			roStmts[i], err = db.rodbs[i].PrepareContext(ctx, query)
			return err
		})
	})

	if err != nil {
		return nil, err
	}

	stmt.rostmts = roStmts
	return stmt, nil
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// Query uses a slave as the physical db.
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.ReadOnly().Query(query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// QueryContext uses a slave as the physical db.
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return db.ReadOnly().QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRow uses a slave as the physical db.
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.ReadOnly().QueryRow(query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRowContext uses a slave as the physical db.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return db.ReadOnly().QueryRowContext(ctx, query, args...)
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool for each underlying physical db.
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns then the
// new MaxIdleConns will be reduced to match the MaxOpenConns limit
// If n <= 0, no idle connections are retained.
func (db *DB) SetMaxIdleConns(n int) {
	db.rwdb.SetMaxIdleConns(n)
	for i := range db.rodbs {
		db.rodbs[i].SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// to each physical database.
// If MaxIdleConns is greater than 0 and the new MaxOpenConns
// is less than MaxIdleConns, then MaxIdleConns will be reduced to match
// the new MaxOpenConns limit. If n <= 0, then there is no limit on the number
// of open connections. The default is 0 (unlimited).
func (db *DB) SetMaxOpenConns(n int) {
	db.rwdb.SetMaxOpenConns(n)
	for i := range db.rodbs {
		db.rodbs[i].SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are reused forever.
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	db.rwdb.SetConnMaxLifetime(d)
	for i := range db.rodbs {
		db.rodbs[i].SetConnMaxLifetime(d)
	}
}

// ReadOnly returns the ReadOnly database
func (db *DB) ReadOnly() *sql.DB {
	if db.totalConnection == 1 {
		return db.rwdb
	}
	return db.rodbs[db.rounRobin(len(db.rodbs))]
}

// ReadWrite returns the main writer physical database
func (db *DB) ReadWrite() *sql.DB {
	return db.rwdb
}

func (db *DB) rounRobin(n int) int {
	return int(1 + (atomic.AddUint64(&db.roCount, 1) % uint64(n)))
}
