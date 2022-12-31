package dbresolver_test

import (
	"database/sql"
	"testing"

	"github.com/bxcodec/dbresolver/v2"
)

func TestOptionWithPrimaryDBs(t *testing.T) {
	dbPrimary := &sql.DB{}
	optFunc := dbresolver.WithPrimaryDBs(dbPrimary)
	opt := &dbresolver.Option{}
	optFunc(opt)

	if len(opt.PrimaryDBs) != 1 {
		t.Errorf("want %v, got %v", 1, len(opt.PrimaryDBs))
	}
}

func TestOptionWithReplicaDBs(t *testing.T) {
	dbReplica := &sql.DB{}
	optFunc := dbresolver.WithReplicaDBs(dbReplica)
	opt := &dbresolver.Option{}
	optFunc(opt)

	if len(opt.ReplicaDBs) != 1 {
		t.Errorf("want %v, got %v", 1, len(opt.PrimaryDBs))
	}
}

func TestOptionWithLoadBalancer(t *testing.T) {
	optFunc := dbresolver.WithLoadBalancer(dbresolver.RoundRobinLB)
	opt := &dbresolver.Option{}
	optFunc(opt)

	if opt.DBLB.Name() != dbresolver.RoundRobinLB {
		t.Errorf("want %v, got %v", dbresolver.RoundRobinLB, opt.DBLB.Name())
	}
}

func TestOptionWithLoadBalancerNonExist(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Should throw panic, but it does not")
		}
	}()

	optFunc := dbresolver.WithLoadBalancer(dbresolver.LoadBalancerPolicy("NON_EXIST"))
	opt := &dbresolver.Option{}
	optFunc(opt)
}
