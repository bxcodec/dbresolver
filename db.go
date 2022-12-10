package dbresolver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"time"
)

// DB interface is a contract that supported by this library.
// All offered function of this library defined here.
// This supposed to be aligned with sql.DB, but since some of the functions is not relevant
// with multi dbs connection, we decided to forward all single connection DB related function to the first primary DB
// For example, function like, `Conn()“, or `Stats()` only available for the primary DB, or the first primary DB (if using multi-primary)
type DB interface {
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
	// Conn only available for the primary db or the first primary db (if using multi-primary)
	Conn(ctx context.Context) (*sql.Conn, error)
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
	// Stats only available for the primary db or the first primary db (if using multi-primary)
	Stats() sql.DBStats
}

// DBLoadBalancer is loadbalancer for physical DBs
type DBLoadBalancer LoadBalancer[*sql.DB]

// StmtLoadBalancer is loadbalancer for query prepared statements
type StmtLoadBalancer LoadBalancer[*sql.Stmt]

// sqlDB is a logical database with multiple underlying physical databases
// forming a single ReadWrite (primary) with multiple ReadOnly(replicas) db.
// Reads and writes are automatically directed to the correct db connection
type sqlDB struct {
	primaries        []*sql.DB
	replicas         []*sql.DB
	totalConnection  int
	loadBalancer     DBLoadBalancer
	stmtLoadBalancer StmtLoadBalancer
}

// Open concurrently opens each underlying db connection
// dataSourceNames must be a semi-comma separated list of DSNs with the first
// one being used as the RW-database(primary) and the rest as RO databases (replicas).
func Open(driverName, dataSourceNames string) (res DB, err error) {
	conns := strings.Split(dataSourceNames, ";")
	if len(conns) == 0 {
		return nil, errors.New("invalid data source name")
	}
	opt := defaultOption()
	db := &sqlDB{
		replicas:         make([]*sql.DB, len(conns)-1),
		primaries:        make([]*sql.DB, 1),
		loadBalancer:     opt.DBLB,
		stmtLoadBalancer: opt.StmtLB,
	}

	db.totalConnection = len(conns)
	err = doParallely(db.totalConnection, func(i int) (err error) {
		if i == 0 {
			db.primaries[0], err = sql.Open(driverName, conns[i])
			return err
		}
		var roDB *sql.DB
		roDB, err = sql.Open(driverName, conns[i])
		if err != nil {
			return
		}
		db.replicas[i-1] = roDB
		return err
	})

	return db, err
}

// OpenMultiPrimary concurrently opens each underlying db connection
// both primaryDataSourceNames and readOnlyDataSourceNames must be a semi-comma separated list of DSNs
// primaryDataSourceNames will be used as the RW-database(primary)
// and readOnlyDataSourceNames as RO databases (replicas).
func OpenMultiPrimary(driverName, primaryDataSourceNames, readOnlyDataSourceNames string) (res DB, err error) {
	primaryConns := strings.Split(primaryDataSourceNames, ";")
	readOnlyConns := strings.Split(readOnlyDataSourceNames, ";")

	if len(primaryConns) == 0 {
		return nil, errors.New("require primary data source name")
	}

	opt := defaultOption()
	db := &sqlDB{
		replicas:         make([]*sql.DB, len(readOnlyConns)),
		primaries:        make([]*sql.DB, len(primaryConns)),
		loadBalancer:     opt.DBLB,
		stmtLoadBalancer: opt.StmtLB,
	}

	db.totalConnection = len(primaryConns) + len(readOnlyConns)
	err = doParallely(db.totalConnection, func(i int) (err error) {
		if i < len(primaryConns) {
			db.primaries[0], err = sql.Open(driverName, primaryConns[i])
			return err
		}
		roIndex := i - len(primaryConns)
		db.replicas[roIndex], err = sql.Open(driverName, readOnlyConns[roIndex])
		return err
	})

	return db, err
}

// PrimaryDBs return all the active primary DB
func (db *sqlDB) PrimaryDBs() []*sql.DB {
	return db.primaries
}

// PrimaryDBs return all the active replica DB
func (db *sqlDB) ReplicaDBs() []*sql.DB {
	return db.replicas
}

// Close closes all physical databases concurrently, releasing any open resources.
func (db *sqlDB) Close() error {
	return doParallely(db.totalConnection, func(i int) (err error) {
		if i < len(db.primaries) {
			return db.primaries[i].Close()
		}

		roIndex := i - len(db.primaries)
		return db.replicas[roIndex].Close()
	})
}

// Driver returns the physical database's underlying driver.
func (db *sqlDB) Driver() driver.Driver {
	return db.ReadWrite().Driver()
}

// Begin starts a transaction on the RW-db. The isolation level is dependent on the driver.
func (db *sqlDB) Begin() (*sql.Tx, error) {
	return db.ReadWrite().Begin()
}

// BeginTx starts a transaction with the provided context on the RW-db.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (db *sqlDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return db.ReadWrite().BeginTx(ctx, opts)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the RW-database as the underlying db connection
func (db *sqlDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.ReadWrite().Exec(query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the RW-database as the underlying db connection
func (db *sqlDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.ReadWrite().ExecContext(ctx, query, args...)
}

// Ping verifies if a connection to each physical database is still alive,
// establishing a connection if necessary.
func (db *sqlDB) Ping() error {
	return doParallely(db.totalConnection, func(i int) error {
		if i < len(db.primaries) {
			return db.primaries[i].Ping()
		}

		roIndex := i - len(db.primaries)
		return db.replicas[roIndex].Ping()
	})
}

// PingContext verifies if a connection to each physical database is still
// alive, establishing a connection if necessary.
func (db *sqlDB) PingContext(ctx context.Context) error {
	return doParallely(db.totalConnection, func(i int) error {
		if i < len(db.primaries) {
			return db.primaries[i].PingContext(ctx)
		}
		roIndex := i - len(db.primaries)
		return db.replicas[roIndex].PingContext(ctx)
	})
}

// Prepare creates a prepared statement for later queries or executions
// on each physical database, concurrently.
func (db *sqlDB) Prepare(query string) (Stmt, error) {
	stmt := &stmt{
		db:           db,
		loadBalancer: db.stmtLoadBalancer,
	}
	roStmts := make([]*sql.Stmt, len(db.replicas))
	primaryStmts := make([]*sql.Stmt, len(db.primaries))
	err := doParallely(db.totalConnection, func(i int) (err error) {
		if i < len(db.primaries) {
			primaryStmts[i], err = db.primaries[i].Prepare(query)
			return err
		}

		roIndex := i - len(db.primaries)
		roStmts[roIndex], err = db.replicas[roIndex].Prepare(query)
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
func (db *sqlDB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	stmt := &stmt{
		db:           db,
		loadBalancer: db.stmtLoadBalancer,
	}
	roStmts := make([]*sql.Stmt, len(db.replicas))
	primaryStmts := make([]*sql.Stmt, len(db.primaries))
	err := doParallely(db.totalConnection, func(i int) (err error) {
		if i < len(db.primaries) {
			primaryStmts[i], err = db.primaries[i].PrepareContext(ctx, query)
			return err
		}

		roIndex := i - len(db.primaries)
		roStmts[roIndex], err = db.replicas[roIndex].PrepareContext(ctx, query)
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
func (db *sqlDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.ReadOnly().Query(query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// QueryContext uses a radonly db as the physical db.
func (db *sqlDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return db.ReadOnly().QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRow uses a radonly db as the physical db.
func (db *sqlDB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.ReadOnly().QueryRow(query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRowContext uses a radonly db as the physical db.
func (db *sqlDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return db.ReadOnly().QueryRowContext(ctx, query, args...)
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool for each underlying db connection
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns then the
// new MaxIdleConns will be reduced to match the MaxOpenConns limit
// If n <= 0, no idle connections are retained.
func (db *sqlDB) SetMaxIdleConns(n int) {
	for i := range db.primaries {
		db.primaries[i].SetMaxIdleConns(n)
	}

	for i := range db.replicas {
		db.replicas[i].SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// to each physical db.
// If MaxIdleConns is greater than 0 and the new MaxOpenConns
// is less than MaxIdleConns, then MaxIdleConns will be reduced to match
// the new MaxOpenConns limit. If n <= 0, then there is no limit on the number
// of open connections. The default is 0 (unlimited).
func (db *sqlDB) SetMaxOpenConns(n int) {
	for i := range db.primaries {
		db.primaries[i].SetMaxOpenConns(n)
	}
	for i := range db.replicas {
		db.replicas[i].SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are reused forever.
func (db *sqlDB) SetConnMaxLifetime(d time.Duration) {
	for i := range db.primaries {
		db.primaries[i].SetConnMaxLifetime(d)
	}
	for i := range db.replicas {
		db.replicas[i].SetConnMaxLifetime(d)
	}
}

// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are not closed due to a connection's idle time.
func (db *sqlDB) SetConnMaxIdleTime(d time.Duration) {
	for i := range db.primaries {
		db.primaries[i].SetConnMaxIdleTime(d)
	}

	for i := range db.replicas {
		db.replicas[i].SetConnMaxIdleTime(d)
	}
}

// ReadOnly returns the readonly database
func (db *sqlDB) ReadOnly() *sql.DB {
	if db.totalConnection == len(db.primaries) {
		return db.loadBalancer.Resolve(db.primaries)
	}
	return db.loadBalancer.Resolve(db.replicas)
}

// ReadWrite returns the primary database
func (db *sqlDB) ReadWrite() *sql.DB {
	return db.loadBalancer.Resolve(db.primaries)
}

// Conn returns a single connection by either opening a new connection or returning an existing connection from the
// connection pool of the first primary db.
func (db *sqlDB) Conn(ctx context.Context) (*sql.Conn, error) {
	return db.primaries[0].Conn(ctx)
}

// Stats returns database statistics for the first primary db
func (db *sqlDB) Stats() sql.DBStats {
	return db.primaries[0].Stats()
}
