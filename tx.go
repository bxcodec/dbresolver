package dbresolver

import (
	"context"
	"database/sql"
)

type Tx interface {
	Commit() error
	Rollback() error
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (Stmt, error)
	PrepareContext(ctx context.Context, query string) (Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	Stmt(stmt Stmt) Stmt
	StmtContext(ctx context.Context, stmt Stmt) Stmt
}

type tx struct {
	db       *sqlDB
	sourceDB *sql.DB
	tx       *sql.Tx
}

func (t *tx) Commit() error {
	return t.tx.Commit()
}

func (t *tx) Rollback() error {
	return t.tx.Rollback()
}

func (t *tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return t.ExecContext(context.Background(), query, args...)
}

func (t *tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t *tx) Prepare(query string) (Stmt, error) {
	return t.PrepareContext(context.Background(), query)
}

func (t *tx) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	txstmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return &stmt{
		db:           t.db,
		loadBalancer: &RoundRobinLoadBalancer[*sql.Stmt]{},
		primaryStmts: []*sql.Stmt{txstmt},
		dbStmt: map[*sql.DB]*sql.Stmt{
			t.sourceDB: txstmt,
		},
	}, nil
}

func (t *tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return t.QueryContext(context.Background(), query, args...)
}

func (t *tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

func (t *tx) QueryRow(query string, args ...interface{}) *sql.Row {
	return t.QueryRowContext(context.Background(), query, args...)
}

func (t *tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

func (t *tx) Stmt(s Stmt) Stmt {
	return t.StmtContext(context.Background(), s)
}

func (t *tx) StmtContext(ctx context.Context, s Stmt) Stmt {
	if rstmt, ok := s.(*stmt); ok {
		tstmt := t.tx.StmtContext(ctx, rstmt.stmtForDB(t.sourceDB))
		return &stmt{
			db:           t.db,
			loadBalancer: &RoundRobinLoadBalancer[*sql.Stmt]{},
			primaryStmts: []*sql.Stmt{tstmt},
			dbStmt: map[*sql.DB]*sql.Stmt{
				t.sourceDB: tstmt,
			},
		}
	}
	return s
}
