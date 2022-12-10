package dbresolver

import (
	"database/sql"
	"math/rand"
	"sync/atomic"
	"time"
)

type DBConn interface {
	*sql.DB | *sql.Stmt
}

type LoadBalancer[T DBConn] interface {
	Resolve([]T) T
	Name() LoadBalancerPolicy
}

type RandomLoadBalancer[T DBConn] struct {
}

func (lb RandomLoadBalancer[T]) Name() LoadBalancerPolicy {
	return RandomLB
}

func (lb RandomLoadBalancer[T]) Resolve(dbs []T) T {
	rand.Seed(time.Now().UnixNano())
	max := len(dbs) - 1
	min := 0
	idx := rand.Intn(max-min+1) + min
	return dbs[idx]
}

type RoundRobinLoadBalancer[T DBConn] struct {
	counter uint64 // Monotonically incrementing counter on every call
}

func (lb RoundRobinLoadBalancer[T]) Name() LoadBalancerPolicy {
	return RoundRobinLB
}

func (lb *RoundRobinLoadBalancer[T]) Resolve(dbs []T) T {
	n := len(dbs)
	if n == 1 {
		return dbs[0]
	}
	idx := int((atomic.AddUint64(&lb.counter, 1) % uint64(n)))
	return dbs[idx]
}
