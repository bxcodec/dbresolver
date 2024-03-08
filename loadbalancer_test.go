package dbresolver

import (
	"database/sql"
	"testing"
	"testing/quick"
)

func TestReplicaRoundRobin(t *testing.T) {
	db := &RoundRobinLoadBalancer[*sql.DB]{}
	last := -1

	err := quick.Check(func(n int) bool {
		index := db.predict(n)
		if n <= 1 {
			return index == 0
		}

		result := index > 0 && index < n && index != last
		last = index

		return result
	}, nil)

	if err != nil {
		t.Error(err)
	}
}
