package dbresolver

import "database/sql"

// WrapDBs will wrap all DB connection
// first DB connection is the primary-writer connection (RW),
// the rest connection will be used for RO connection
func WrapDBs(dbs ...*sql.DB) DB {
	if len(dbs) == 0 {
		panic("required primary connection")
	}
	return &DBImpl{
		primarydb:       dbs[0],
		replicas:        dbs[1:],
		totalConnection: len(dbs),
	}
}
