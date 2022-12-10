package dbresolver

// Resolver will resolve all the passed connection
// first DB connection is the primary-writer connection (RW),
// the rest connection will be used for RO connection
func Resolver(opts ...OptionFunc) DB {
	opt := defaultOption()
	for _, optFunc := range opts {
		optFunc(opt)
	}

	if len(opt.PrimaryDBs) == 0 {
		panic("required primary db connection, set the primary db " +
			"connection with dbresolver.Resolver(dbresolver.WithPrimaryDBs(primaryDB))")
	}
	return &sqlDB{
		primaries:        opt.PrimaryDBs,
		replicas:         opt.ReplicaDBs,
		loadBalancer:     opt.DBLB,
		stmtLoadBalancer: opt.StmtLB,
	}
}
