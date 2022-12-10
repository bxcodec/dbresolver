package dbresolver_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/bxcodec/dbresolver/v2"
	_ "github.com/lib/pq"
)

func ExampleNew() {
	var (
		host1     = "localhost"
		port1     = 5432
		user1     = "postgresrw"
		password1 = "<password>"
		host2     = "localhost"
		port2     = 5433
		user2     = "postgresro"
		password2 = "<password>"
		dbname    = "<dbname>"
	)
	// connection string
	rwPrimary := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host1, port1, user1, password1, dbname)
	readOnlyReplica := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host2, port2, user2, password2, dbname)

	// open database for primary
	dbPrimary, err := sql.Open("postgres", rwPrimary)
	if err != nil {
		log.Print("go error when connecting to the DB")
	}
	// configure the DBs for other setup eg, tracing, etc
	// eg, tracing.Postgres(dbPrimary)

	// open database for replica
	dbReadOnlyReplica, err := sql.Open("postgres", readOnlyReplica)
	if err != nil {
		log.Print("go error when connecting to the DB")
	}
	// configure the DBs for other setup eg, tracing, etc
	// eg, tracing.Postgres(dbReadOnlyReplica)

	connectionDB := dbresolver.New(
		dbresolver.WithPrimaryDBs(dbPrimary),
		dbresolver.WithReplicaDBs(dbReadOnlyReplica),
		dbresolver.WithLoadBalancer(dbresolver.RoundRobinLB))

	defer connectionDB.Close()
	// now you can use the connection for all DB operation
	_, err = connectionDB.ExecContext(context.Background(), "DELETE FROM book WHERE id=$1") // will use primaryDB
	if err != nil {
		log.Print("go error when executing the query to the DB", err)
	}
	_ = connectionDB.QueryRowContext(context.Background(), "SELECT * FROM book WHERE id=$1") // will use replicaReadOnlyDB

	// Output:
	//
}
