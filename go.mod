module github.com/bxcodec/dbresolver/v2

go 1.18

require (
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/google/gofuzz v1.2.0
	github.com/lib/pq v1.10.9
	go.uber.org/multierr v1.8.0
)

require (
	github.com/stretchr/testify v1.8.1 // indirect
	go.uber.org/atomic v1.7.0 // indirect
)

retract (
	// below versions doesn't support Update,Insert queries with "RETURNING CLAUSE"
	//	v1.0.0
	//    v1.0.0-beta
	//    v1.0.1
	//    v1.0.2
	//    v1.1.0
	v2.0.0
	v2.0.0-beta.2
	v2.0.0-beta
	v2.0.0-alpha.5
	v2.0.0-alpha.4
	v2.0.0-alpha.3
	v2.0.0-alpha.2
	v2.0.0-alpha
)
