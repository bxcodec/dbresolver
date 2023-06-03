//go:build gofuzz

//go:generate go install github.com/dvyukov/go-fuzz/go-fuzz@latest
//go:generate go install github.com/dvyukov/go-fuzz/go-fuzz-build@latest

//go:generate go-fuzz-build
//go:generate go-fuzz

package dbresolver

import (
	"database/sql"
	fuzz "github.com/google/gofuzz"
)

var LoadBalancerPolicies = []LoadBalancerPolicy{
	RandomLB,
	RoundRobinLB,
}

func Fuzz(data []byte) int {

	var rdbCount, wdbCount, lbPolicyID uint8

	fuzz.NewFromGoFuzz(data).Fuzz(&rdbCount)
	fuzz.NewFromGoFuzz(data).Fuzz(&wdbCount)
	fuzz.NewFromGoFuzz(data).Fuzz(&lbPolicyID)

	if lbPolicyID > 2 { //release: update the no of lb policies
		return 0
	}

	primaries := make([]*sql.DB, wdbCount)
	replicas := make([]*sql.DB, rdbCount)

	lbPolicy := LoadBalancerPolicies[lbPolicyID]

	resolver := New(WithPrimaryDBs(primaries...), WithReplicaDBs(replicas...), WithLoadBalancer(lbPolicy)).(*sqlDB)

	if len(resolver.replicas) != int(rdbCount) {
		panic("no of replicas have changed")
	}

	return 1
}
