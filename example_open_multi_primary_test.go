package dbresolver_test

import (
	"context"
	"fmt"
	"log"

	"github.com/bxcodec/dbresolver/v2"
	_ "github.com/lib/pq"
)

func ExampleOpenMultiPrimary() {
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
	rwPrimary1 := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host1, port1, user1, password1, dbname)
	rwPrimary2 := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host2, port2, user2, password2, dbname)
	readOnlyReplica1 := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host1, port1, user1, password1, dbname)
	readOnlyReplica2 := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host2, port2, user2, password2, dbname)

	rwPrimary := fmt.Sprintf("%s;%s", rwPrimary1, rwPrimary2)
	readOnlyReplica := fmt.Sprintf("%s;%s", readOnlyReplica1, readOnlyReplica2)
	connectionDB, err := dbresolver.Open("postgres", fmt.Sprintf("%s;%s", rwPrimary, readOnlyReplica))
	if err != nil {
		log.Print("go error when connecting to the DB", err)
	}

	// now you can use the connection for all DB operation
	_, err = connectionDB.ExecContext(context.Background(), "DELETE FROM book WHERE id=$1") // will use primaryDB
	if err != nil {
		log.Print("go error when connecting to the DB", err)
	}
	_ = connectionDB.QueryRowContext(context.Background(), "SELECT * FROM book WHERE id=$1") // will use replicaReadOnlyDB

	// Output:
	//
}
