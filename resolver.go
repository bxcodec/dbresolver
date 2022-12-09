package dbresolver

import "database/sql"

// WrapDBs will wrap all DB connection
// first DB connection is the primary-writer connection (RW),
// the rest connection will be used for RO connection
func WrapDBs(dbs ...*sql.DB) DB {
	if len(dbs) == 0 {
		panic("required primary connection")
	}
	return &sqlDB{
		primaries:        dbs[:1],
		replicas:         dbs[1:],
		totalConnections: len(dbs),
	}
}

// WrapDBsMultiPrimary will wrap all DB connection
// first DB array connection is the primary-writer connection (RW),
// the second DB array will be used for RO connection
func WrapDBsMultiPrimary(primaryDBs, roDBs []*sql.DB) DB {

	totalConnections := len(primaryDBs) + len(roDBs)

	if totalConnections == 0 {
		panic("required primary connection")
	}

	return &sqlDB{
		primaries:        primaryDBs,
		replicas:         roDBs,
		totalConnections: totalConnections,
	}
}
