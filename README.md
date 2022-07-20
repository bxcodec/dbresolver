# dbresolver
DB Connection Resolver for Cross Region Databases, Separated Readonly and ReadWrite Databases

## Idea and Inspiration

This DBResolver library will split your connections to correct defined DBs. Eg, all Read query will routed to ReadOnly replica db, and all write operation(Insert, Update, Delete) will routed to Write/Master DB. 

### Usecase 1: Separated RW and RO Database connection
<details open>

<summary>Click to Expand</summary>

- You have your application deployed
- Your application is heavy on read operations
- Your DBs replicated to multiple replicas for faster queries
- You separate the connections for optimized query 
- ![image](https://user-images.githubusercontent.com/11002383/180010864-c9e2a0b6-520d-48d6-bf0d-490eb070e75d.png) 

</details>

### Usecases 2: Cross Region Database
<details>

<summary>Click to Expand</summary>

- Your application deployed to multi regions.
- You have your Databases configured globally.
- ![image](https://user-images.githubusercontent.com/11002383/179894026-7206cbb8-35d7-4fd9-9ce9-4e62bf1ec156.png)

</details>

## Support

You can file an [Issue](https://github.com/bxcodec/dbresolver/issues/new).
See documentation in [Go.Dev](https://pkg.go.dev/github.com/bxcodec/dbresolver?tab=doc)

## Getting Started

#### Download

```shell
go get -u github.com/bxcodec/dbresolver
```

# Example
---

### With Multi *sql.DB
<details open>

<summary>Click to Expand</summary>

```go
package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bxcodec/dbresolver"
	_ "github.com/lib/pq"
)

func main() {
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
		panic(err)
	}
	//configure the DBs for other setup eg, tracing, etc
	// eg, tracing.Postgres(dbPrimary)

	// open database for replica
	dbReadOnlyReplica, err := sql.Open("postgres", readOnlyReplica)
	if err != nil {
		panic(err)
	}
	//configure the DBs for other setup eg, tracing, etc
	// eg, tracing.Postgres(dbReadOnlyReplica)

	connectionDB := dbresolver.WrapDBs(dbPrimary, dbReadOnlyReplica)

	//now you can use the connection for all DB operation
	connectionDB.ExecContext(context.Background(), "DELETE FROM book WHERE id=$1")       // will use primaryDB
	connectionDB.QueryRowContext(context.Background(), "SELECT * FROM book WHERE id=$1") // will use replicaReadOnlyDB
}

```

</details>


### With Multi Connection String
<details open>

<summary>Click to Expand</summary>

```go
package main

import (
	"context"
	"fmt"

	"github.com/bxcodec/dbresolver"
	_ "github.com/lib/pq"
)

func main() {
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

}

```

</details>

## Contribution
---

To contrib to this project, you can open a PR or an issue.
