package internal

import (
	"context"
	"database/sql/driver"
	"time"

	"golang.org/x/exp/slog"
)

type Stmt struct {
	stmt   driver.Stmt
	query  string
	logger Logger
}

func NewStmt(stmt driver.Stmt, query string, logger Logger) *Stmt {
	return &Stmt{
		stmt:   stmt,
		query:  query,
		logger: logger,
	}
}

var (
	_ driver.Stmt              = (*Stmt)(nil)
	_ driver.StmtExecContext   = (*Stmt)(nil)
	_ driver.StmtQueryContext  = (*Stmt)(nil)
	_ driver.NamedValueChecker = (*Stmt)(nil)
)

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
//
// Drivers must ensure all network calls made by Close
// do not block indefinitely (e.g. apply a timeout).
func (s *Stmt) Close() (err error) {
	defer func(started time.Time) {
		s.logger.Log(context.Background(), slog.LevelInfo, s.logger.StmtPrefix+"close", started, err)
	}(time.Time{})

	return s.stmt.Close()
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s *Stmt) NumInput() int {
	return s.stmt.NumInput()
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *Stmt) Exec(args []driver.Value) (_ driver.Result, err error) {
	defer func(started time.Time) {
		s.logger.Log(context.Background(), slog.LevelInfo, s.logger.StmtPrefix+"exec", started, err, logArgs(args))
	}(time.Now())

	return s.stmt.Exec(args)
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (_ driver.Result, err error) {
	defer func(started time.Time) {
		s.logger.Log(ctx, slog.LevelInfo, s.logger.StmtPrefix+"execContext", started, err, logArgs(args))
	}(time.Now())

	if execer, ok := s.stmt.(driver.StmtExecContext); ok {
		return execer.ExecContext(ctx, args)
	}

	// StmtExecContext.ExecContext is not permitted to return ErrSkip. fall back to Exec.
	var dargs []driver.Value
	if dargs, err = namedValueToValue(args); err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return s.stmt.Exec(dargs) //nolint:staticcheck // fallback
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *Stmt) Query(args []driver.Value) (_ driver.Rows, err error) {
	defer func(started time.Time) {
		s.logger.Log(context.Background(), slog.LevelInfo, s.logger.StmtPrefix+"query", started, err, logArgs(args))
	}(time.Now())

	return s.stmt.Query(args)
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext must honor the context timeout and return when it is canceled.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (_ driver.Rows, err error) {
	defer func(started time.Time) {
		s.logger.Log(ctx, slog.LevelInfo, s.logger.StmtPrefix+"queryContext", started, err, logArgs(args))
	}(time.Now())

	if query, ok := s.stmt.(driver.StmtQueryContext); ok {
		return query.QueryContext(ctx, args)
	}

	// StmtQueryContext.QueryContext is not permitted to return ErrSkip. fall back to Query.
	var dargs []driver.Value
	if dargs, err = namedValueToValue(args); err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return s.stmt.Query(dargs) //nolint:staticcheck // fallback
}

// CheckNamedValue is called before passing arguments to the driver
// and is called in place of any ColumnConverter. CheckNamedValue must do type
// validation and conversion as appropriate for the driver.
func (s *Stmt) CheckNamedValue(namedValue *driver.NamedValue) (err error) {
	namedValueChecker, ok := s.stmt.(driver.NamedValueChecker)
	if !ok {
		return driver.ErrSkip
	}

	return namedValueChecker.CheckNamedValue(namedValue)
}
