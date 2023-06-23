package dbresolver

import (
	"database/sql"
	"fmt"
)

// LoadBalancerPolicy define the loadbalancer policy data type
type LoadBalancerPolicy string

// Supported Loadbalancer policy
const (
	RoundRobinLB LoadBalancerPolicy = "ROUND_ROBIN"
	RandomLB     LoadBalancerPolicy = "RANDOM"
)

// Option define the option property
type Option struct {
	PrimaryDBs       []*sql.DB
	ReplicaDBs       []*sql.DB
	StmtLB           StmtLoadBalancer
	DBLB             DBLoadBalancer
	QueryTypeChecker QueryTypeChecker
}

// OptionFunc used for option chaining
type OptionFunc func(opt *Option)

// WithPrimaryDBs add primaryDBs to the resolver
func WithPrimaryDBs(primaryDBs ...*sql.DB) OptionFunc {
	return func(opt *Option) {
		opt.PrimaryDBs = primaryDBs
	}
}

// WithReplicaDBs add replica DBs to the resolver
func WithReplicaDBs(replicaDBs ...*sql.DB) OptionFunc {
	return func(opt *Option) {
		opt.ReplicaDBs = replicaDBs
	}
}

// WithQueryTypeChecker sets the query type checker instance.
// The default one just checks for the presence of the string "RETURNING" in the uppercase query.
func WithQueryTypeChecker(checker QueryTypeChecker) OptionFunc {
	return func(opt *Option) {
		opt.QueryTypeChecker = checker
	}
}

// WithLoadBalancer configure the loadbalancer for the resolver
func WithLoadBalancer(lb LoadBalancerPolicy) OptionFunc {
	return func(opt *Option) {
		switch lb {
		case RoundRobinLB:
			opt.DBLB = &RoundRobinLoadBalancer[*sql.DB]{}
			opt.StmtLB = &RoundRobinLoadBalancer[*sql.Stmt]{}
		case RandomLB:
			opt.DBLB = &RandomLoadBalancer[*sql.DB]{
				randInt: make(chan int, 1),
			}
			opt.StmtLB = &RandomLoadBalancer[*sql.Stmt]{
				randInt: make(chan int, 1),
			}
		default:
			panic(fmt.Sprintf("LoadBalancer: %s is not supported", lb))
		}
	}
}

func defaultOption() *Option {
	return &Option{
		DBLB:             &RoundRobinLoadBalancer[*sql.DB]{},
		StmtLB:           &RoundRobinLoadBalancer[*sql.Stmt]{},
		QueryTypeChecker: &DefaultQueryTypeChecker{},
	}
}
