module github.com/bxcodec/dbresolver/v2

go 1.18

require (
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Masterminds/squirrel v1.5.4
	github.com/google/gofuzz v1.2.0
	github.com/labstack/echo/v4 v4.10.2
	github.com/lib/pq v1.10.7
	go.uber.org/multierr v1.8.0
)

require (
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/labstack/gommon v0.4.0 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.2.2 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	golang.org/x/crypto v0.6.0 // indirect
	golang.org/x/net v0.10.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
	golang.org/x/text v0.9.0 // indirect
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
