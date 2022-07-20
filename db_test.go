package dbresolver

import (
	"testing"
	"testing/quick"

	_ "github.com/mattn/go-sqlite3"
)

func TestOpen(t *testing.T) {
	// https://www.sqlite.org/inmemorydb.html
	db, err := Open("sqlite3", ":memory:;:memory:;:memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		t.Error(err)
	}

	if expected := (db.primarydb); expected == nil {
		t.Error("PrimaryDB is nil")
	}

	if want, got := 2, len(db.replicas); want != got {
		t.Errorf("Unexpected number of replicas dbs. Got: %d, Want: %d", got, want)
	}
}

func TestClose(t *testing.T) {
	db, err := Open("sqlite3", ":memory:;:memory:;:memory:")
	if err != nil {
		t.Fatal(err)
	}

	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	if err = db.Ping(); err.Error() != "sql: database is closed" {
		t.Errorf("All dbs were not closed correctly. Got: %s", err)
	}
}

func TestReplicaRoundRobin(t *testing.T) {
	db := &DBImpl{}
	last := -1

	err := quick.Check(func(n int) bool {
		index := db.rounRobin(n)
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
