package dbresolver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync/atomic"
	"time"
)

type DB interface {
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
	// Conn(ctx context.Context) (*sql.Conn, error) // not relevant for multi connection DB
	Driver() driver.Driver
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Ping() error
	PingContext(ctx context.Context) error
	Prepare(query string) (Stmt, error)
	PrepareContext(ctx context.Context, query string) (Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	SetConnMaxIdleTime(d time.Duration)
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	// Stats() sql.DBStats // not relevant for multi connection DB
}

// DB is a logical database with multiple underlying physical databases
// forming a single ReadWrite with multiple ReadOnly database.
// Reads and writes are automatically directed to the correct physical dbImpl.
type DBImpl struct {
	rwdb            *sql.DB
	rodbs           []*sql.DB
	totalConnection int
	roCount         uint64 // Monotonically incrementing counter on each query
}

// Open concurrently opens each underlying physical dbImpl.
// dataSourceNames must be a semi-comma separated list of DSNs with the first
// one being used as the RW-database and the rest as RO databases.
func Open(driverName, dataSourceNames string) (db DB, err error) {
	conns := strings.Split(dataSourceNames, ";")
	dbImpl := &DBImpl{
		rodbs: make([]*sql.DB, len(conns)-1),
	}

	dbImpl.totalConnection = len(conns)
	err = doParallely(dbImpl.totalConnection, func(i int) (err error) {
		if i == 0 {
			dbImpl.rwdb, err = sql.Open(driverName, conns[i])
			return err
		}
		var roDB *sql.DB
		roDB, err = sql.Open(driverName, conns[i])
		if err != nil {
			return
		}
		dbImpl.rodbs[i-1] = roDB
		return err
	})

	return dbImpl, err
}

// Close closes all physical databases concurrently, releasing any open resources.
func (dbImpl *DBImpl) Close() error {
	return doParallely(dbImpl.totalConnection, func(i int) (err error) {
		if i == 0 {
			return dbImpl.rwdb.Close()
		}
		return dbImpl.rodbs[i-1].Close()
	})

}

// Driver returns the physical database's underlying driver.
func (dbImpl *DBImpl) Driver() driver.Driver {
	return dbImpl.ReadWrite().Driver()
}

// Begin starts a transaction on the RW-database. The isolation level is dependent on the driver.
func (dbImpl *DBImpl) Begin() (*sql.Tx, error) {
	return dbImpl.ReadWrite().Begin()
}

// BeginTx starts a transaction with the provided context on the RW-database.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (dbImpl *DBImpl) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return dbImpl.ReadWrite().BeginTx(ctx, opts)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the RW-database as the underlying physical dbImpl.
func (dbImpl *DBImpl) Exec(query string, args ...interface{}) (sql.Result, error) {
	return dbImpl.ReadWrite().Exec(query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the RW-database as the underlying physical dbImpl.
func (dbImpl *DBImpl) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return dbImpl.ReadWrite().ExecContext(ctx, query, args...)
}

// Ping verifies if a connection to each physical database is still alive,
// establishing a connection if necessary.
func (dbImpl *DBImpl) Ping() error {
	return doParallely(dbImpl.totalConnection, func(i int) error {
		if i == 0 {
			return dbImpl.rwdb.Ping()
		}
		return dbImpl.rodbs[i-1].Ping()
	})
}

// PingContext verifies if a connection to each physical database is still
// alive, establishing a connection if necessary.
func (dbImpl *DBImpl) PingContext(ctx context.Context) error {
	return doParallely(dbImpl.totalConnection, func(i int) error {
		if i == 0 {
			return dbImpl.rwdb.PingContext(ctx)
		}
		return dbImpl.rodbs[i-1].Ping()
	})
}

// Prepare creates a prepared statement for later queries or executions
// on each physical database, concurrently.
func (dbImpl *DBImpl) Prepare(query string) (Stmt, error) {
	stmt := &stmt{
		db: dbImpl,
	}
	roStmts := make([]*sql.Stmt, len(dbImpl.rodbs))
	err := doParallely(dbImpl.totalConnection, func(i int) (err error) {
		if i == 0 {
			stmt.rwstmt, err = dbImpl.rwdb.Prepare(query)
			return err
		}

		return doParallely(len(dbImpl.rodbs), func(i int) (err error) {
			roStmts[i], err = dbImpl.rodbs[i].Prepare(query)
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
func (dbImpl *DBImpl) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	stmt := &stmt{
		db: dbImpl,
	}
	roStmts := make([]*sql.Stmt, len(dbImpl.rodbs))
	err := doParallely(dbImpl.totalConnection, func(i int) (err error) {
		if i == 0 {
			stmt.rwstmt, err = dbImpl.rwdb.PrepareContext(ctx, query)
			return err
		}

		return doParallely(len(dbImpl.rodbs), func(i int) (err error) {
			roStmts[i], err = dbImpl.rodbs[i].PrepareContext(ctx, query)
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
// Query uses a radonly db as the physical db.
func (dbImpl *DBImpl) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return dbImpl.ReadOnly().Query(query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// QueryContext uses a radonly db as the physical db.
func (dbImpl *DBImpl) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return dbImpl.ReadOnly().QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRow uses a radonly db as the physical db.
func (dbImpl *DBImpl) QueryRow(query string, args ...interface{}) *sql.Row {
	return dbImpl.ReadOnly().QueryRow(query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRowContext uses a radonly db as the physical db.
func (dbImpl *DBImpl) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return dbImpl.ReadOnly().QueryRowContext(ctx, query, args...)
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool for each underlying physical dbImpl.
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns then the
// new MaxIdleConns will be reduced to match the MaxOpenConns limit
// If n <= 0, no idle connections are retained.
func (dbImpl *DBImpl) SetMaxIdleConns(n int) {
	dbImpl.rwdb.SetMaxIdleConns(n)
	for i := range dbImpl.rodbs {
		dbImpl.rodbs[i].SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// to each physical database.
// If MaxIdleConns is greater than 0 and the new MaxOpenConns
// is less than MaxIdleConns, then MaxIdleConns will be reduced to match
// the new MaxOpenConns limit. If n <= 0, then there is no limit on the number
// of open connections. The default is 0 (unlimited).
func (dbImpl *DBImpl) SetMaxOpenConns(n int) {
	dbImpl.rwdb.SetMaxOpenConns(n)
	for i := range dbImpl.rodbs {
		dbImpl.rodbs[i].SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are reused forever.
func (dbImpl *DBImpl) SetConnMaxLifetime(d time.Duration) {
	dbImpl.rwdb.SetConnMaxLifetime(d)
	for i := range dbImpl.rodbs {
		dbImpl.rodbs[i].SetConnMaxLifetime(d)
	}
}

//SetConnMaxIdleTime sets the maximum amount of time a connection may be idle.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are not closed due to a connection's idle time.
func (dbImpl *DBImpl) SetConnMaxIdleTime(d time.Duration) {
	dbImpl.rwdb.SetConnMaxIdleTime(d)
	for i := range dbImpl.rodbs {
		dbImpl.rodbs[i].SetConnMaxIdleTime(d)
	}
}

// ReadOnly returns the ReadOnly database
func (dbImpl *DBImpl) ReadOnly() *sql.DB {
	if dbImpl.totalConnection == 1 {
		return dbImpl.rwdb
	}
	return dbImpl.rodbs[dbImpl.rounRobin(len(dbImpl.rodbs))]
}

// ReadWrite returns the main writer physical database
func (dbImpl *DBImpl) ReadWrite() *sql.DB {
	return dbImpl.rwdb
}

func (dbImpl *DBImpl) rounRobin(n int) int {
	return int(1 + (atomic.AddUint64(&dbImpl.roCount, 1) % uint64(n)))
}
