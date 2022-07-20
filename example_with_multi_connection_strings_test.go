package dbresolver_test

import (
	"context"
	"fmt"

	"github.com/bxcodec/dbresolver"
	_ "github.com/lib/pq"
)

func Example_WithMultiConnectionString() {
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
	connectionDB, err := dbresolver.Open("postgres", fmt.Sprintf("%s;%s", rwPrimary, readOnlyReplica))
	if err != nil {
		panic(err)
	}

	//now you can use the connection for all DB operation
	connectionDB.ExecContext(context.Background(), "DELETE FROM book WHERE id=$1")       // will use primaryDB
	connectionDB.QueryRowContext(context.Background(), "SELECT * FROM book WHERE id=$1") // will use replicaReadOnlyDB

	//Output:
	//
}
