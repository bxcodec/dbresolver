package dbresolver

import (
	"context"
	"database/sql"
)

// Tx is a *sql.Tx wrapper.
// Its main purpose is to be able to return the internal Stmt interface.
type Tx interface {
	Commit() error
	Rollback() error
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Prepare(query string) (Stmt, error)
	PrepareContext(ctx context.Context, query string) (Stmt, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	Stmt(stmt Stmt) Stmt
	StmtContext(ctx context.Context, stmt Stmt) Stmt
}

type tx struct {
	sourceDB *sql.DB
	tx       *sql.Tx
}

func (t *tx) Commit() error {
	return t.tx.Commit()
}

func (t *tx) Rollback() error {
	return t.tx.Rollback()
}

func (t *tx) Exec(query string, args ...any) (sql.Result, error) {
	return t.ExecContext(context.Background(), query, args...)
}

func (t *tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
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

	return newSingleDBStmt(t.sourceDB, txstmt, true), nil
}

func (t *tx) Query(query string, args ...any) (*sql.Rows, error) {
	return t.QueryContext(context.Background(), query, args...)
}

func (t *tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

func (t *tx) QueryRow(query string, args ...any) *sql.Row {
	return t.QueryRowContext(context.Background(), query, args...)
}

func (t *tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

func (t *tx) Stmt(s Stmt) Stmt {
	return t.StmtContext(context.Background(), s)
}

func (t *tx) StmtContext(ctx context.Context, s Stmt) Stmt {
	if rstmt, ok := s.(*stmt); ok {
		return newSingleDBStmt(t.sourceDB, t.tx.StmtContext(ctx, rstmt.stmtForDB(t.sourceDB)), true)
	}
	return s
}
