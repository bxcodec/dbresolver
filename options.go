package dbresolver

import "database/sql"

type LoadBalancerPolicy string

const (
	RoundRobinLB LoadBalancerPolicy = "ROUND_ROBIN"
	RandomLB     LoadBalancerPolicy = "RANDOM"
)

type Option struct {
	PrimaryDBs []*sql.DB
	ReplicaDBs []*sql.DB
	StmtLB     StmtLoadBalancer
	DBLB       DBLoadBalancer
}

type OptionFunc func(opt *Option)

func WithPrimaryDBs(primaryDBs ...*sql.DB) OptionFunc {
	return func(opt *Option) {
		opt.PrimaryDBs = primaryDBs
	}
}

func WithReplicaDBs(replicaDBs ...*sql.DB) OptionFunc {
	return func(opt *Option) {
		opt.ReplicaDBs = replicaDBs
	}
}

func WithLoadBalancer(lb LoadBalancerPolicy) OptionFunc {
	return func(opt *Option) {
		switch lb {
		case RoundRobinLB:
			opt.DBLB = &RoundRobinLoadBalancer[*sql.DB]{}
			opt.StmtLB = &RoundRobinLoadBalancer[*sql.Stmt]{}
		case RandomLB:
			opt.DBLB = &RandomLoadBalancer[*sql.DB]{}
			opt.StmtLB = &RandomLoadBalancer[*sql.Stmt]{}
		}
	}
}

func defaultOption() *Option {
	return &Option{
		DBLB:   &RoundRobinLoadBalancer[*sql.DB]{},
		StmtLB: &RoundRobinLoadBalancer[*sql.Stmt]{},
	}
}
