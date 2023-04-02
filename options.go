package sqlog

import (
	"golang.org/x/exp/slog"

	"github.com/mdigger/sqlog/internal"
)

// Options is a function that can be applied to a Logger.
type Options interface {
	apply(cfg *internal.Logger)
}

// WithLogger set the logger.
func WithLogger(log *slog.Logger) Options {
	return option{func(cfg *internal.Logger) {
		cfg.Logger = log
	}}
}

// WithBaseLevel set the base level.
func WithBaseLevel(level slog.Level) Options {
	return option{func(cfg *internal.Logger) {
		cfg.BaseLevel = level
	}}
}

// WithPrefix set the prefix.
func WithPrefix(prefix string) Options {
	return option{func(cfg *internal.Logger) {
		cfg.BasePrefix = prefix
	}}
}

// WithStmtPrefix set the statement prefix.
func WithStmtPrefix(prefix string) Options {
	return option{func(cfg *internal.Logger) {
		cfg.StmtPrefix = prefix
	}}
}

// WithTxPrefix set the transaction prefix.
func WithTxPrefix(prefix string) Options {
	return option{func(cfg *internal.Logger) {
		cfg.TxPrefix = prefix
	}}
}

// WithoutDuration disable log duration output.
func WithoutDuration() Options {
	return option{func(cfg *internal.Logger) {
		cfg.WithDuration = false
	}}
}

// WithWarnErrSkip log driver.ErrSkip error as warning.
func WithWarnErrSkip() Options {
	return option{func(cfg *internal.Logger) {
		cfg.WarnErrSkip = true
	}}
}

func newDefaultLogger(opt ...Options) internal.Logger {
	logger := internal.Logger{
		Logger:       slog.Default(),
		BasePrefix:   "sql:",
		StmtPrefix:   "stmt:",
		TxPrefix:     "tx:",
		WithDuration: true,
		WarnErrSkip:  false,
	}

	for _, o := range opt {
		o.apply(&logger)
	}

	return logger
}

type option struct {
	f func(cfg *internal.Logger)
}

func (o option) apply(cfg *internal.Logger) {
	o.f(cfg)
}
