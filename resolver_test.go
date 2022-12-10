package dbresolver_test

import (
	"database/sql"
	"testing"

	"github.com/bxcodec/dbresolver/v2"
)

func TestWrapDBWithMultiDBs(t *testing.T) {
	db1 := &sql.DB{}
	db2 := &sql.DB{}
	db3 := &sql.DB{}

	db := dbresolver.New(dbresolver.WithPrimaryDBs(db1), dbresolver.WithReplicaDBs(db2, db3))

	if db == nil {
		t.Errorf("expected %v, got %v", "not nil", db)
	}
}

func TestWrapDBWithOneDB(t *testing.T) {
	db1 := &sql.DB{}

	db := dbresolver.New(dbresolver.WithPrimaryDBs(db1))

	if db == nil {
		t.Errorf("expected %v, got %v", "not nil", db)
	}
}
