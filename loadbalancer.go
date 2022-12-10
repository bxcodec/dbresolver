package dbresolver

import (
	"database/sql"
	"math/rand"
	"sync/atomic"
	"time"
)

// DBOpsType is the generic type for DB and Stmt operation
type DBOpsType interface {
	*sql.DB | *sql.Stmt
}

// LoadBalancer define the load balancer contract
type LoadBalancer[T DBOpsType] interface {
	Resolve([]T) T
	Name() LoadBalancerPolicy
}

// RandomLoadBalancer represent for Random LB policy
type RandomLoadBalancer[T DBOpsType] struct {
}

// RandomLoadBalancer return the LB policy name
func (lb RandomLoadBalancer[T]) Name() LoadBalancerPolicy {
	return RandomLB
}

// Resolve return the resolved option for Random LB
func (lb RandomLoadBalancer[T]) Resolve(dbs []T) T {
	rand.Seed(time.Now().UnixNano())
	max := len(dbs) - 1
	min := 0
	idx := rand.Intn(max-min+1) + min
	return dbs[idx]
}

// RoundRobinLoadBalancer represent for RoundRobin LB policy
type RoundRobinLoadBalancer[T DBOpsType] struct {
	counter uint64 // Monotonically incrementing counter on every call
}

// RandomLoadBalancer return the LB policy name
func (lb RoundRobinLoadBalancer[T]) Name() LoadBalancerPolicy {
	return RoundRobinLB
}

// Resolve return the resolved option for RoundRobin LB
func (lb *RoundRobinLoadBalancer[T]) Resolve(dbs []T) T {
	n := len(dbs)
	if n == 1 {
		return dbs[0]
	}
	idx := int((atomic.AddUint64(&lb.counter, 1) % uint64(n)))
	return dbs[idx]
}
