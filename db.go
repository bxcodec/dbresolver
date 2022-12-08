package dbresolver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync/atomic"
	"time"
)

// DB interface is a contract that supported by this library.
// All offered function of this library defined here.
// This supposed to be aligned with sql.DB, but since some of the functions is not relevant
// with multi dbs connection, we decided to not support it
type DB interface {
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
	Conn(ctx context.Context) (*sql.Conn, error) // db stats for only primary db
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
	Stats() sql.DBStats // db stats for only primary db
}

// DBImpl is a logical database with multiple underlying physical databases
// forming a single ReadWrite (primary) with multiple ReadOnly(replicas) database.
// Reads and writes are automatically directed to the correct db connection
type DBImpl struct {
	primarydb       *sql.DB
	replicas        []*sql.DB
	totalConnection int
	replicasCount   uint64 // Monotonically incrementing counter on each query
}

// Open concurrently opens each underlying db connection
// dataSourceNames must be a semi-comma separated list of DSNs with the first
// one being used as the RW-database(primary) and the rest as RO databases (replicas).
func Open(driverName, dataSourceNames string) (db *DBImpl, err error) {
	conns := strings.Split(dataSourceNames, ";")
	dbImpl := &DBImpl{
		replicas: make([]*sql.DB, len(conns)-1),
	}

	dbImpl.totalConnection = len(conns)
	err = doParallely(dbImpl.totalConnection, func(i int) (err error) {
		if i == 0 {
			dbImpl.primarydb, err = sql.Open(driverName, conns[i])
			return err
		}
		var roDB *sql.DB
		roDB, err = sql.Open(driverName, conns[i])
		if err != nil {
			return
		}
		dbImpl.replicas[i-1] = roDB
		return err
	})

	return dbImpl, err
}

// Close closes all physical databases concurrently, releasing any open resources.
func (dbImpl *DBImpl) Close() error {
	return doParallely(dbImpl.totalConnection, func(i int) (err error) {
		if i == 0 {
			return dbImpl.primarydb.Close()
		}
		return dbImpl.replicas[i-1].Close()
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
// Exec uses the RW-database as the underlying db connection
func (dbImpl *DBImpl) Exec(query string, args ...interface{}) (sql.Result, error) {
	return dbImpl.ReadWrite().Exec(query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the RW-database as the underlying db connection
func (dbImpl *DBImpl) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return dbImpl.ReadWrite().ExecContext(ctx, query, args...)
}

// Ping verifies if a connection to each physical database is still alive,
// establishing a connection if necessary.
func (dbImpl *DBImpl) Ping() error {
	return doParallely(dbImpl.totalConnection, func(i int) error {
		if i == 0 {
			return dbImpl.primarydb.Ping()
		}
		return dbImpl.replicas[i-1].Ping()
	})
}

// PingContext verifies if a connection to each physical database is still
// alive, establishing a connection if necessary.
func (dbImpl *DBImpl) PingContext(ctx context.Context) error {
	return doParallely(dbImpl.totalConnection, func(i int) error {
		if i == 0 {
			return dbImpl.primarydb.PingContext(ctx)
		}
		return dbImpl.replicas[i-1].Ping()
	})
}

// Prepare creates a prepared statement for later queries or executions
// on each physical database, concurrently.
func (dbImpl *DBImpl) Prepare(query string) (Stmt, error) {
	stmt := &stmt{
		db: dbImpl,
	}
	roStmts := make([]*sql.Stmt, len(dbImpl.replicas))
	err := doParallely(dbImpl.totalConnection, func(i int) (err error) {
		if i == 0 {
			stmt.primaryStmt, err = dbImpl.primarydb.Prepare(query)
			return err
		}

		return doParallely(len(dbImpl.replicas), func(i int) (err error) {
			roStmts[i], err = dbImpl.replicas[i].Prepare(query)
			return err
		})
	})

	if err != nil {
		return nil, err
	}
	stmt.replicaStmts = roStmts

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
	roStmts := make([]*sql.Stmt, len(dbImpl.replicas))
	err := doParallely(dbImpl.totalConnection, func(i int) (err error) {
		if i == 0 {
			stmt.primaryStmt, err = dbImpl.primarydb.PrepareContext(ctx, query)
			return err
		}

		return doParallely(len(dbImpl.replicas), func(i int) (err error) {
			roStmts[i], err = dbImpl.replicas[i].PrepareContext(ctx, query)
			return err
		})
	})

	if err != nil {
		return nil, err
	}

	stmt.replicaStmts = roStmts
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
// connection pool for each underlying db connection
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns then the
// new MaxIdleConns will be reduced to match the MaxOpenConns limit
// If n <= 0, no idle connections are retained.
func (dbImpl *DBImpl) SetMaxIdleConns(n int) {
	dbImpl.primarydb.SetMaxIdleConns(n)
	for i := range dbImpl.replicas {
		dbImpl.replicas[i].SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// to each physical database.
// If MaxIdleConns is greater than 0 and the new MaxOpenConns
// is less than MaxIdleConns, then MaxIdleConns will be reduced to match
// the new MaxOpenConns limit. If n <= 0, then there is no limit on the number
// of open connections. The default is 0 (unlimited).
func (dbImpl *DBImpl) SetMaxOpenConns(n int) {
	dbImpl.primarydb.SetMaxOpenConns(n)
	for i := range dbImpl.replicas {
		dbImpl.replicas[i].SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are reused forever.
func (dbImpl *DBImpl) SetConnMaxLifetime(d time.Duration) {
	dbImpl.primarydb.SetConnMaxLifetime(d)
	for i := range dbImpl.replicas {
		dbImpl.replicas[i].SetConnMaxLifetime(d)
	}
}

// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are not closed due to a connection's idle time.
func (dbImpl *DBImpl) SetConnMaxIdleTime(d time.Duration) {
	dbImpl.primarydb.SetConnMaxIdleTime(d)
	for i := range dbImpl.replicas {
		dbImpl.replicas[i].SetConnMaxIdleTime(d)
	}
}

// ReadOnly returns the replica database
func (dbImpl *DBImpl) ReadOnly() *sql.DB {
	if dbImpl.totalConnection == 1 {
		return dbImpl.primarydb
	}
	return dbImpl.replicas[dbImpl.rounRobin(len(dbImpl.replicas))]
}

// ReadWrite returns the primary database
func (dbImpl *DBImpl) ReadWrite() *sql.DB {
	return dbImpl.primarydb
}

func (dbImpl *DBImpl) rounRobin(n int) int {
	if n <= 1 {
		return 0
	}
	return int((atomic.AddUint64(&dbImpl.replicasCount, 1) % uint64(n)))
}

// Conn returns a single connection by either opening a new connection or returning an existing connection from the
// connection pool of primary db.
func (dbImpl *DBImpl) Conn(ctx context.Context) (*sql.Conn, error) {
	return dbImpl.primarydb.Conn(ctx)
}

// Stats returns database statistics for the primary db
func (dbImpl *DBImpl) Stats() sql.DBStats {
	return dbImpl.primarydb.Stats()
}
