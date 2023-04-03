package internal

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"time"

	"golang.org/x/exp/slog"
)

type Conn struct {
	conn    driver.Conn
	started time.Time
	logger  Logger
}

// NewConn returns a new wrapped Conn.
func NewConn(conn driver.Conn, logger Logger) *Conn {
	return &Conn{
		conn:    conn,
		started: time.Now(),
		logger:  logger,
	}
}

var (
	_ driver.Pinger             = (*Conn)(nil)
	_ driver.Execer             = (*Conn)(nil) //nolint:staticcheck // implemented
	_ driver.ExecerContext      = (*Conn)(nil)
	_ driver.Queryer            = (*Conn)(nil) //nolint:staticcheck // implemented
	_ driver.QueryerContext     = (*Conn)(nil)
	_ driver.Conn               = (*Conn)(nil)
	_ driver.ConnPrepareContext = (*Conn)(nil)
	_ driver.ConnBeginTx        = (*Conn)(nil)
	_ driver.SessionResetter    = (*Conn)(nil)
	_ driver.NamedValueChecker  = (*Conn)(nil)
)

// Pinger is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement Pinger, the sql package's DB.Ping and
// DB.PingContext will check if there is at least one Conn available.
//
// If Conn.Ping returns ErrBadConn, DB.Ping and DB.PingContext will remove
// the Conn from pool.
func (c *Conn) Ping(ctx context.Context) (err error) {
	defer func(started time.Time) {
		c.logger.Log(ctx, slog.LevelDebug, "ping", started, err)
	}(time.Now())

	if pinger, ok := c.conn.(driver.Pinger); !ok {
		return pinger.Ping(ctx)
	}

	return nil // driver doesn't implement, nothing to do
}

// Execer is an optional interface that may be implemented by a Conn.
//
// If a Conn implements neither ExecerContext nor Execer,
// the sql package's DB.Exec will first prepare a query, execute the statement,
// and then close the statement.
//
// Exec may return ErrSkip.
//
// Deprecated: Drivers should implement ExecerContext instead.
func (c *Conn) Exec(query string, args []driver.Value) (_ driver.Result, err error) {
	defer func(started time.Time) {
		c.logger.Log(context.Background(), slog.LevelInfo, "exec", started, err,
			logQuery(query), logArgs(args))
	}(time.Now())

	if execer, ok := c.conn.(driver.Execer); !ok {
		return execer.Exec(query, args)
	}

	return nil, driver.ErrSkip
}

// ExecerContext is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement ExecerContext, the sql package's DB.Exec
// will fall back to Execer; if the Conn does not implement Execer either,
// DB.Exec will first prepare a query, execute the statement, and then
// close the statement.
//
// ExecContext may return ErrSkip.
//
// ExecContext must honor the context timeout and return when the context is canceled.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Result, err error) {
	defer func(started time.Time) {
		c.logger.Log(ctx, slog.LevelInfo, "execContext", started, err,
			logQuery(query), logArgs(args))
	}(time.Now())

	if execer, ok := c.conn.(driver.ExecerContext); !ok {
		return execer.ExecContext(ctx, query, args)
	}

	return nil, driver.ErrSkip
}

// Queryer is an optional interface that may be implemented by a Conn.
//
// If a Conn implements neither QueryerContext nor Queryer,
// the sql package's DB.Query will first prepare a query, execute the statement,
// and then close the statement.
//
// Query may return ErrSkip.
//
// Deprecated: Drivers should implement QueryerContext instead.
func (c *Conn) Query(query string, args []driver.Value) (_ driver.Rows, err error) {
	defer func(started time.Time) {
		c.logger.Log(context.Background(), slog.LevelInfo, "query", started, err,
			logQuery(query), logArgs(args))
	}(time.Now())

	if queryer, ok := c.conn.(driver.Queryer); ok {
		return queryer.Query(query, args)
	}

	return nil, driver.ErrSkip
}

// QueryerContext is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement QueryerContext, the sql package's DB.Query
// will fall back to Queryer; if the Conn does not implement Queryer either,
// DB.Query will first prepare a query, execute the statement, and then
// close the statement.
//
// QueryContext may return ErrSkip.
//
// QueryContext must honor the context timeout and return when the context is canceled.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Rows, err error) {
	defer func(started time.Time) {
		c.logger.Log(ctx, slog.LevelInfo, "queryContext", started, err,
			logQuery(query), logArgs(args))
	}(time.Now())

	if queryer, ok := c.conn.(driver.QueryerContext); ok {
		return queryer.QueryContext(ctx, query, args)
	}

	return nil, driver.ErrSkip
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (_ driver.Stmt, err error) {
	stmtID := slog.String("stmtID", NewUID())

	defer func(started time.Time) {
		c.logger.Log(context.Background(), slog.LevelInfo, "prepare", started, err,
			stmtID, logQuery(query))
	}(time.Now())

	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return nil, err
	}

	return c.newStmt(stmt, query, stmtID), nil
}

// ConnPrepareContext enhances the Conn interface with context.
func (c *Conn) PrepareContext(ctx context.Context, query string) (_ driver.Stmt, err error) {
	stmtID := slog.String("stmtID", NewUID())

	defer func(started time.Time) {
		c.logger.Log(ctx, slog.LevelInfo, "prepareContext", started, err,
			stmtID, logQuery(query))
	}(time.Now())

	if prepare, ok := c.conn.(driver.ConnPrepareContext); ok {
		stmt, err := prepare.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}

		return c.newStmt(stmt, query, stmtID), nil
	}

	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		stmt.Close()
		return nil, ctx.Err()
	}

	return c.newStmt(stmt, query, stmtID), nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *Conn) Begin() (_ driver.Tx, err error) {
	txID := slog.String("txID", NewUID())

	defer func(started time.Time) {
		c.logger.Log(context.Background(), slog.LevelInfo, "begin", started, err,
			txID)
	}(time.Time{})

	tx, err := c.conn.Begin()
	if err != nil {
		return nil, err
	}

	return c.newTx(tx, txID), nil
}

// BeginTx starts and returns a new transaction.
// If the context is canceled by the user the sql package will
// call Tx.Rollback before discarding and closing the connection.
//
// This must check opts.Isolation to determine if there is a set
// isolation level. If the driver does not support a non-default
// level and one is set or if there is a non-default isolation level
// that is not supported, an error must be returned.
//
// This must also check opts.ReadOnly to determine if the read-only
// value is true to either set the read-only transaction property if supported
// or return an error if it is not supported.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (_ driver.Tx, err error) {
	txID := slog.String("txID", NewUID())

	defer func(started time.Time) {
		c.logger.Log(ctx, slog.LevelInfo, "beginTx", started, err,
			txID, slog.Bool("readOnly", opts.ReadOnly))
	}(time.Time{})

	if conn, ok := c.conn.(driver.ConnBeginTx); ok {
		tx, err := conn.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}

		return c.newTx(tx, txID), nil
	}

	// Code borrowed from ctxutil.go in the go standard library.
	// Check the transaction level. If the transaction level is non-default
	// then return an error here as the BeginTx driver value is not supported.
	if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
		return nil, errors.New("sql: driver does not support non-default isolation level")
	}

	// If a read-only transaction is requested return an error as the
	// BeginTx driver value is not supported.
	if opts.ReadOnly {
		return nil, errors.New("sql: driver does not support read-only transactions")
	}

	tx, err := c.conn.Begin() //nolint:staticcheck // fallback
	if err != nil {
		return nil, err
	}

	if ctx.Done() != nil {
		select {
		default:
		case <-ctx.Done():
			_ = tx.Rollback() //nolint:errcheck // replaced by context
			return nil, ctx.Err()
		}
	}

	return c.newTx(tx, txID), nil
}

// SessionResetter may be implemented by Conn to allow drivers to reset the
// session state associated with the connection and to signal a bad connection.
func (c *Conn) ResetSession(ctx context.Context) (err error) {
	defer func(started time.Time) {
		c.logger.Log(ctx, slog.LevelDebug, "resetSession", started, err)
	}(time.Time{})

	if resetSessin, ok := c.conn.(driver.SessionResetter); ok {
		return resetSessin.ResetSession(ctx)
	}

	return nil // driver does not implement, there is nothing to do.
}

func (c *Conn) CheckNamedValue(namedValue *driver.NamedValue) (err error) {
	if namedValueChecker, ok := c.conn.(driver.NamedValueChecker); ok {
		return namedValueChecker.CheckNamedValue(namedValue)
	}

	return driver.ErrSkip
}

func (c *Conn) Close() (err error) {
	defer func() {
		c.logger.Log(context.Background(), slog.LevelInfo, "close", c.started, err)
	}()

	return c.conn.Close()
}

func (c *Conn) newTx(tx driver.Tx, id slog.Attr) *Tx {
	return NewTx(tx, c.logger.With(id))
}

func (c *Conn) newStmt(stmt driver.Stmt, query string, id slog.Attr) *Stmt {
	return NewStmt(stmt, query, c.logger.With(id))
}
