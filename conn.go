package dbresolver

import (
	"context"
	"database/sql"
	"strings"
)

// Conn is a *sql.Conn wrapper.
// Its main purpose is to be able to return the internal Tx and Stmt interfaces.
type Conn interface {
	Close() error
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PingContext(ctx context.Context) error
	PrepareContext(ctx context.Context, query string) (Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	Raw(f func(driverConn any) error) (err error)
}

type conn struct {
	sourceDB *sql.DB
	conn     *sql.Conn
}

func (c *conn) Close() error {
	return c.conn.Close()
}

func (c *conn) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	stx, err := c.conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &tx{
		sourceDB: c.sourceDB,
		tx:       stx,
	}, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.conn.ExecContext(ctx, query, args...)
}

func (c *conn) PingContext(ctx context.Context) error {
	return c.conn.PingContext(ctx)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	pstmt, err := c.conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	_query := strings.ToUpper(query)
	writeFlag := strings.Contains(_query, "RETURNING")

	return newSingleDBStmt(c.sourceDB, pstmt, writeFlag), nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.conn.QueryContext(ctx, query, args...)
}

func (c *conn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return c.conn.QueryRowContext(ctx, query, args...)
}

func (c *conn) Raw(f func(driverConn any) error) (err error) {
	return c.conn.Raw(f)
}
