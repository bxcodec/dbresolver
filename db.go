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
	primaries        []*sql.DB
	replicas         []*sql.DB
	totalConnections int
	replicasCount    uint64 // Monotonically incrementing counter on each query
	primariesCount   uint64 // Monotonically incrementing counter on each query
}

// Open concurrently opens each underlying db connection
// dataSourceNames must be a semi-comma separated list of DSNs with the first
// one being used as the RW-database(primary) and the rest as RO databases (replicas).
func Open(driverName, dataSourceNames string) (db DB, err error) {
	conns := strings.Split(dataSourceNames, ";")
	database := &sqlDB{
		replicas:  make([]*sql.DB, len(conns)-1),
		primaries: make([]*sql.DB, 1),
	}

	database.totalConnections = len(conns)
	err = doParallely(database.totalConnections, func(i int) (err error) {
		if i == 0 {
			database.primaries[0], err = sql.Open(driverName, conns[i])
			return err
		}
		var roDB *sql.DB
		roDB, err = sql.Open(driverName, conns[i])
		if err != nil {
			return
		}
		database.replicas[i-1] = roDB
		return err
	})

	return database, err
}

// OpenMultiPrimary concurrently opens each underlying db connection
// both primaryDataSourceNames and readOnlyDataSourceNames must be a semi-comma separated list of DSNs
// primaryDataSourceNames will be used as the RW-database(primary)
// and readOnlyDataSourceNames as RO databases (replicas).
func OpenMultiPrimary(driverName, primaryDataSourceNames, readOnlyDataSourceNames string) (db DB, err error) {
	primaryConns := strings.Split(primaryDataSourceNames, ";")
	readOnlyConns := strings.Split(readOnlyDataSourceNames, ";")
	database := &sqlDB{
		replicas:  make([]*sql.DB, len(readOnlyConns)),
		primaries: make([]*sql.DB, len(primaryConns)),
	}

	database.totalConnections = len(primaryConns) + len(readOnlyConns)
	err = doParallely(database.totalConnections, func(i int) (err error) {
		if i < len(primaryConns) {
			database.primaries[0], err = sql.Open(driverName, primaryConns[i])
			return err
		}
		roIndex := i - len(primaryConns)
		database.replicas[roIndex], err = sql.Open(driverName, readOnlyConns[roIndex])
		return err
	})

	return database, err
}

// PrimaryDBs return all the active primary DB
func (database *sqlDB) PrimaryDBs() []*sql.DB {
	return database.primaries
}

// PrimaryDBs return all the active replica DB
func (database *sqlDB) ReplicaDBs() []*sql.DB {
	return database.replicas
}

// Close closes all physical databases concurrently, releasing any open resources.
func (database *sqlDB) Close() error {
	return doParallely(database.totalConnections, func(i int) (err error) {
		if i < len(database.primaries) {
			return database.primaries[i].Close()
		}

		roIndex := i - len(database.primaries)
		return database.replicas[roIndex].Close()
	})
}

// Driver returns the physical database's underlying driver.
func (database *sqlDB) Driver() driver.Driver {
	return database.ReadWrite().Driver()
}

// Begin starts a transaction on the RW-database. The isolation level is dependent on the driver.
func (database *sqlDB) Begin() (*sql.Tx, error) {
	return database.ReadWrite().Begin()
}

// BeginTx starts a transaction with the provided context on the RW-database.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (database *sqlDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return database.ReadWrite().BeginTx(ctx, opts)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the RW-database as the underlying db connection
func (database *sqlDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return database.ReadWrite().Exec(query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the RW-database as the underlying db connection
func (database *sqlDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return database.ReadWrite().ExecContext(ctx, query, args...)
}

// Ping verifies if a connection to each physical database is still alive,
// establishing a connection if necessary.
func (database *sqlDB) Ping() error {
	return doParallely(database.totalConnections, func(i int) error {
		if i < len(database.primaries) {
			return database.primaries[i].Ping()
		}

		roIndex := i - len(database.primaries)
		return database.replicas[roIndex].Ping()
	})
}

// PingContext verifies if a connection to each physical database is still
// alive, establishing a connection if necessary.
func (database *sqlDB) PingContext(ctx context.Context) error {
	return doParallely(database.totalConnections, func(i int) error {
		if i < len(database.primaries) {
			return database.primaries[i].PingContext(ctx)
		}
		roIndex := i - len(database.primaries)
		return database.replicas[roIndex].PingContext(ctx)
	})
}

// Prepare creates a prepared statement for later queries or executions
// on each physical database, concurrently.
func (database *sqlDB) Prepare(query string) (Stmt, error) {
	stmt := &stmt{
		db: database,
	}
	roStmts := make([]*sql.Stmt, len(database.replicas))
	primaryStmts := make([]*sql.Stmt, len(database.primaries))
	err := doParallely(database.totalConnections, func(i int) (err error) {
		if i < len(database.primaries) {
			primaryStmts[i], err = database.primaries[i].Prepare(query)
			return err
		}

		roIndex := i - len(database.primaries)
		roStmts[roIndex], err = database.replicas[roIndex].Prepare(query)
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
func (database *sqlDB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	stmt := &stmt{
		db: database,
	}
	roStmts := make([]*sql.Stmt, len(database.replicas))
	primaryStmts := make([]*sql.Stmt, len(database.primaries))
	err := doParallely(database.totalConnections, func(i int) (err error) {
		if i < len(database.primaries) {
			primaryStmts[i], err = database.primaries[i].PrepareContext(ctx, query)
			return err
		}

		roIndex := i - len(database.primaries)
		roStmts[roIndex], err = database.replicas[roIndex].PrepareContext(ctx, query)
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
func (database *sqlDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return database.ReadOnly().Query(query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// QueryContext uses a radonly db as the physical db.
func (database *sqlDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return database.ReadOnly().QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRow uses a radonly db as the physical db.
func (database *sqlDB) QueryRow(query string, args ...interface{}) *sql.Row {
	return database.ReadOnly().QueryRow(query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRowContext uses a radonly db as the physical db.
func (database *sqlDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return database.ReadOnly().QueryRowContext(ctx, query, args...)
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool for each underlying db connection
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns then the
// new MaxIdleConns will be reduced to match the MaxOpenConns limit
// If n <= 0, no idle connections are retained.
func (database *sqlDB) SetMaxIdleConns(n int) {
	for i := range database.primaries {
		database.primaries[i].SetMaxIdleConns(n)
	}

	for i := range database.replicas {
		database.replicas[i].SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// to each physical database.
// If MaxIdleConns is greater than 0 and the new MaxOpenConns
// is less than MaxIdleConns, then MaxIdleConns will be reduced to match
// the new MaxOpenConns limit. If n <= 0, then there is no limit on the number
// of open connections. The default is 0 (unlimited).
func (database *sqlDB) SetMaxOpenConns(n int) {
	for i := range database.primaries {
		database.primaries[i].SetMaxOpenConns(n)
	}
	for i := range database.replicas {
		database.replicas[i].SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are reused forever.
func (database *sqlDB) SetConnMaxLifetime(d time.Duration) {
	for i := range database.primaries {
		database.primaries[i].SetConnMaxLifetime(d)
	}
	for i := range database.replicas {
		database.replicas[i].SetConnMaxLifetime(d)
	}
}

// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are not closed due to a connection's idle time.
func (database *sqlDB) SetConnMaxIdleTime(d time.Duration) {
	for i := range database.primaries {
		database.primaries[i].SetConnMaxIdleTime(d)
	}

	for i := range database.replicas {
		database.replicas[i].SetConnMaxIdleTime(d)
	}
}

// ReadOnly returns the readonly database
func (database *sqlDB) ReadOnly() *sql.DB {
	if database.totalConnections == len(database.primaries) {
		return database.primaries[database.rounRobinRW(len(database.primaries))]
	}

	return database.replicas[database.rounRobinRO(len(database.replicas))]
}

// ReadWrite returns the primary database
func (database *sqlDB) ReadWrite() *sql.DB {
	return database.primaries[database.rounRobinRW(len(database.primaries))]
}

func (database *sqlDB) rounRobinRO(n int) int {
	if n <= 1 {
		return 0
	}
	return int(atomic.AddUint64(&database.replicasCount, 1) % uint64(n))
}

func (database *sqlDB) rounRobinRW(n int) int {
	if n <= 1 {
		return 0
	}
	return int(atomic.AddUint64(&database.primariesCount, 1) % uint64(n))
}

// Conn returns a single connection by either opening a new connection or returning an existing connection from the
// connection pool of the first primary db.
func (database *sqlDB) Conn(ctx context.Context) (*sql.Conn, error) {
	return database.primaries[0].Conn(ctx)
}

// Stats returns database statistics for the first primary db
func (database *sqlDB) Stats() sql.DBStats {
	return database.primaries[0].Stats()
}
