package dbresolver_test

import (
	"database/sql"
	"testing"

	"github.com/bxcodec/dbresolver"
)

func TestWrapDBWithMultiDBs(t *testing.T) {
	db1 := &sql.DB{}
	db2 := &sql.DB{}
	db3 := &sql.DB{}

	db := dbresolver.WrapDBs(db1, db2, db3)

	if db == nil {
		t.Errorf("expected %v, got %v", "not nil", db)
	}
}

func TestWrapDBWith1DB(t *testing.T) {
	db1 := &sql.DB{}

	db := dbresolver.WrapDBs(db1)

	if db == nil {
		t.Errorf("expected %v, got %v", "not nil", db)
	}
}
