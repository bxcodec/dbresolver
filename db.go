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
	Conn(ctx context.Context) (*sql.Conn, error) // db stats for only one of the primary db
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
	PrimaryDBs() []*sql.DB
	ReplicaDBs() []*sql.DB
	Stats() sql.DBStats // db stats for only one of the primary db
}

// sqlDB is a logical database with multiple underlying physical databases
// forming a single ReadWrite (primary) with multiple ReadOnly(replicas) database.
// Reads and writes are automatically directed to the correct db connection
type sqlDB struct {
	primarydbs      []*sql.DB
	replicas        []*sql.DB
	totalConnection int
	replicasCount   uint64 // Monotonically incrementing counter on each query
	primarydbsCount uint64 // Monotonically incrementing counter on each query
}

// Open concurrently opens each underlying db connection
// dataSourceNames must be a semi-comma separated list of DSNs with the first
// one being used as the RW-database(primary) and the rest as RO databases (replicas).
func Open(driverName, dataSourceNames string) (db DB, err error) {
	conns := strings.Split(dataSourceNames, ";")
	dbResolver := &sqlDB{
		replicas:   make([]*sql.DB, len(conns)-1),
		primarydbs: make([]*sql.DB, 1),
	}

	dbResolver.totalConnection = len(conns)
	err = doParallely(dbResolver.totalConnection, func(i int) (err error) {
		if i == 0 {
			dbResolver.primarydbs[0], err = sql.Open(driverName, conns[i])
			return err
		}
		var roDB *sql.DB
		roDB, err = sql.Open(driverName, conns[i])
		if err != nil {
			return
		}
		dbResolver.replicas[i-1] = roDB
		return err
	})

	return dbResolver, err
}

// OpenMultiPrimary concurrently opens each underlying db connection
// both primaryDataSourceNames and readOnlyDataSourceNames must be a semi-comma separated list of DSNs
// primaryDataSourceNames will be used as the RW-database(primary)
// and readOnlyDataSourceNames as RO databases (replicas).
func OpenMultiPrimary(driverName, primaryDataSourceNames, readOnlyDataSourceNames string) (db DB, err error) {
	primaryConns := strings.Split(primaryDataSourceNames, ";")
	readOnlyConns := strings.Split(readOnlyDataSourceNames, ";")
	dbResolver := &sqlDB{
		replicas:   make([]*sql.DB, len(readOnlyConns)),
		primarydbs: make([]*sql.DB, len(primaryConns)),
	}

	dbResolver.totalConnection = len(primaryConns) + len(readOnlyConns)
	err = doParallely(dbResolver.totalConnection, func(i int) (err error) {
		if i < len(primaryConns) {
			dbResolver.primarydbs[0], err = sql.Open(driverName, primaryConns[i])
			return err
		}
		roIndex := i - len(primaryConns)
		dbResolver.replicas[roIndex], err = sql.Open(driverName, readOnlyConns[roIndex])
		return err
	})

	return dbResolver, err
}

// PrimaryDBs return all the active primary DB
func (dbResolver *sqlDB) PrimaryDBs() []*sql.DB {
	return dbResolver.primarydbs
}

// PrimaryDBs return all the active replica DB
func (dbResolver *sqlDB) ReplicaDBs() []*sql.DB {
	return dbResolver.replicas
}

// Close closes all physical databases concurrently, releasing any open resources.
func (dbResolver *sqlDB) Close() error {
	return doParallely(dbResolver.totalConnection, func(i int) (err error) {
		if i < len(dbResolver.primarydbs) {
			return dbResolver.primarydbs[i].Close()
		}

		roIndex := i - len(dbResolver.primarydbs)
		return dbResolver.replicas[roIndex].Close()
	})
}

// Driver returns the physical database's underlying driver.
func (dbResolver *sqlDB) Driver() driver.Driver {
	return dbResolver.ReadWrite().Driver()
}

// Begin starts a transaction on the RW-database. The isolation level is dependent on the driver.
func (dbResolver *sqlDB) Begin() (*sql.Tx, error) {
	return dbResolver.ReadWrite().Begin()
}

// BeginTx starts a transaction with the provided context on the RW-database.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (dbResolver *sqlDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return dbResolver.ReadWrite().BeginTx(ctx, opts)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the RW-database as the underlying db connection
func (dbResolver *sqlDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return dbResolver.ReadWrite().Exec(query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the RW-database as the underlying db connection
func (dbResolver *sqlDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return dbResolver.ReadWrite().ExecContext(ctx, query, args...)
}

// Ping verifies if a connection to each physical database is still alive,
// establishing a connection if necessary.
func (dbResolver *sqlDB) Ping() error {
	return doParallely(dbResolver.totalConnection, func(i int) error {
		if i < len(dbResolver.primarydbs) {
			return dbResolver.primarydbs[i].Ping()
		}

		roIndex := i - len(dbResolver.primarydbs)
		return dbResolver.replicas[roIndex].Ping()
	})
}

// PingContext verifies if a connection to each physical database is still
// alive, establishing a connection if necessary.
func (dbResolver *sqlDB) PingContext(ctx context.Context) error {
	return doParallely(dbResolver.totalConnection, func(i int) error {
		if i < len(dbResolver.primarydbs) {
			return dbResolver.primarydbs[i].PingContext(ctx)
		}
		roIndex := i - len(dbResolver.primarydbs)
		return dbResolver.replicas[roIndex].PingContext(ctx)
	})
}

// Prepare creates a prepared statement for later queries or executions
// on each physical database, concurrently.
func (dbResolver *sqlDB) Prepare(query string) (Stmt, error) {
	stmt := &stmt{
		db: dbResolver,
	}
	roStmts := make([]*sql.Stmt, len(dbResolver.replicas))
	primaryStmts := make([]*sql.Stmt, len(dbResolver.primarydbs))
	err := doParallely(dbResolver.totalConnection, func(i int) (err error) {
		if i < len(dbResolver.primarydbs) {
			primaryStmts[i], err = dbResolver.primarydbs[i].Prepare(query)
			return err
		}

		roIndex := i - len(dbResolver.primarydbs)
		roStmts[roIndex], err = dbResolver.replicas[roIndex].Prepare(query)
		return err
	})

	if err != nil {
		return nil, err
	}

	stmt.replicaStmts = roStmts
	stmt.primaryStmts = primaryStmts
	return stmt, nil
}

// PrepareContext creates a prepared statement for later queries or executions
// on each physical database, concurrently.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func (dbResolver *sqlDB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	stmt := &stmt{
		db: dbResolver,
	}
	roStmts := make([]*sql.Stmt, len(dbResolver.replicas))
	primaryStmts := make([]*sql.Stmt, len(dbResolver.primarydbs))
	err := doParallely(dbResolver.totalConnection, func(i int) (err error) {
		if i < len(dbResolver.primarydbs) {
			primaryStmts[i], err = dbResolver.primarydbs[i].PrepareContext(ctx, query)
			return err
		}

		roIndex := i - len(dbResolver.primarydbs)
		roStmts[roIndex], err = dbResolver.replicas[roIndex].PrepareContext(ctx, query)
		return err
	})

	if err != nil {
		return nil, err
	}

	stmt.replicaStmts = roStmts
	stmt.primaryStmts = primaryStmts
	return stmt, nil
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// Query uses a radonly db as the physical db.
func (dbResolver *sqlDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return dbResolver.ReadOnly().Query(query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// QueryContext uses a radonly db as the physical db.
func (dbResolver *sqlDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return dbResolver.ReadOnly().QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRow uses a radonly db as the physical db.
func (dbResolver *sqlDB) QueryRow(query string, args ...interface{}) *sql.Row {
	return dbResolver.ReadOnly().QueryRow(query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRowContext uses a radonly db as the physical db.
func (dbResolver *sqlDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return dbResolver.ReadOnly().QueryRowContext(ctx, query, args...)
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool for each underlying db connection
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns then the
// new MaxIdleConns will be reduced to match the MaxOpenConns limit
// If n <= 0, no idle connections are retained.
func (dbResolver *sqlDB) SetMaxIdleConns(n int) {
	for i := range dbResolver.primarydbs {
		dbResolver.primarydbs[i].SetMaxIdleConns(n)
	}

	for i := range dbResolver.replicas {
		dbResolver.replicas[i].SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// to each physical database.
// If MaxIdleConns is greater than 0 and the new MaxOpenConns
// is less than MaxIdleConns, then MaxIdleConns will be reduced to match
// the new MaxOpenConns limit. If n <= 0, then there is no limit on the number
// of open connections. The default is 0 (unlimited).
func (dbResolver *sqlDB) SetMaxOpenConns(n int) {
	for i := range dbResolver.primarydbs {
		dbResolver.primarydbs[i].SetMaxOpenConns(n)
	}
	for i := range dbResolver.replicas {
		dbResolver.replicas[i].SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are reused forever.
func (dbResolver *sqlDB) SetConnMaxLifetime(d time.Duration) {
	for i := range dbResolver.primarydbs {
		dbResolver.primarydbs[i].SetConnMaxLifetime(d)
	}
	for i := range dbResolver.replicas {
		dbResolver.replicas[i].SetConnMaxLifetime(d)
	}
}

// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are not closed due to a connection's idle time.
func (dbResolver *sqlDB) SetConnMaxIdleTime(d time.Duration) {
	for i := range dbResolver.primarydbs {
		dbResolver.primarydbs[i].SetConnMaxIdleTime(d)
	}

	for i := range dbResolver.replicas {
		dbResolver.replicas[i].SetConnMaxIdleTime(d)
	}
}

// ReadOnly returns the readonly database
func (dbResolver *sqlDB) ReadOnly() *sql.DB {
	if dbResolver.totalConnection == len(dbResolver.primarydbs) {
		return dbResolver.primarydbs[dbResolver.rounRobinRW(len(dbResolver.primarydbs))]
	}

	return dbResolver.replicas[dbResolver.rounRobinRO(len(dbResolver.replicas))]
}

// ReadWrite returns the primary database
func (dbResolver *sqlDB) ReadWrite() *sql.DB {
	return dbResolver.primarydbs[dbResolver.rounRobinRW(len(dbResolver.primarydbs))]
}

func (dbResolver *sqlDB) rounRobinRO(n int) int {
	if n <= 1 {
		return 0
	}
	return int((atomic.AddUint64(&dbResolver.replicasCount, 1) % uint64(n)))
}

func (dbResolver *sqlDB) rounRobinRW(n int) int {
	if n <= 1 {
		return 0
	}
	return int((atomic.AddUint64(&dbResolver.primarydbsCount, 1) % uint64(n)))
}

// Conn returns a single connection by either opening a new connection or returning an existing connection from the
// connection pool of the first primary db.
func (dbResolver *sqlDB) Conn(ctx context.Context) (*sql.Conn, error) {
	return dbResolver.primarydbs[0].Conn(ctx)
}

// Stats returns database statistics for the first primary db
func (dbResolver *sqlDB) Stats() sql.DBStats {
	return dbResolver.primarydbs[0].Stats()
}
