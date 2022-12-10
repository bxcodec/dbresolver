package dbresolver

import (
	"testing"

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

	if expected := len(db.PrimaryDBs()); expected == 0 {
		t.Error("No Primary DB is ready")
	}

	if want, got := 2, len(db.ReplicaDBs()); want != got {
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
