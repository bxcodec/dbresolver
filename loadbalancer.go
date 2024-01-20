package dbresolver

import (
	"database/sql"
	"math/rand"
	"sync/atomic"
	"time"
)

// DBConnection is the generic type for DB and Stmt operation
type DBConnection interface {
	*sql.DB | *sql.Stmt
}

// LoadBalancer define the load balancer contract
type LoadBalancer[T DBConnection] interface {
	Resolve([]T) T
	Name() LoadBalancerPolicy
	predict(n int) int
}

// RandomLoadBalancer represent for Random LB policy
type RandomLoadBalancer[T DBConnection] struct {
	randInt chan int
}

// RandomLoadBalancer return the LB policy name
func (lb RandomLoadBalancer[T]) Name() LoadBalancerPolicy {
	return RandomLB
}

// Resolve return the resolved option for Random LB.
// Marked with go:nosplit to prevent preemption.
//
//go:nosplit
func (lb RandomLoadBalancer[T]) Resolve(dbs []T) T {
	if len(lb.randInt) == 0 {
		lb.predict(len(dbs))
	}
	randomInt := <-lb.randInt
	return dbs[randomInt]
}

func (lb RandomLoadBalancer[T]) predict(n int) int {
	max := n - 1
	min := 0
	idx := rand.Intn(max-min+1) + min
	lb.randInt <- idx
	return idx
}

// RoundRobinLoadBalancer represent for RoundRobin LB policy
type RoundRobinLoadBalancer[T DBConnection] struct {
	counter uint64 // Monotonically incrementing counter on every call
}

// Name return the LB policy name
func (lb RoundRobinLoadBalancer[T]) Name() LoadBalancerPolicy {
	return RoundRobinLB
}

// Resolve return the resolved option for RoundRobin LB
func (lb *RoundRobinLoadBalancer[T]) Resolve(dbs []T) T {
	idx := lb.roundRobin(len(dbs))
	return dbs[idx]
}

func (lb *RoundRobinLoadBalancer[T]) roundRobin(n int) int {
	if n <= 1 {
		return 0
	}
	return int(atomic.AddUint64(&lb.counter, 1) % uint64(n))
}

func (lb *RoundRobinLoadBalancer[T]) predict(n int) int {
	if n <= 1 {
		return 0
	}
	counter := lb.counter
	return int(atomic.AddUint64(&counter, 1) % uint64(n))
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
