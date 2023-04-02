package internal

import (
	"context"
	"time"

	"golang.org/x/exp/slog"
)

type Logger struct {
	*slog.Logger
	BaseLevel    slog.Level
	BasePrefix   string
	StmtPrefix   string
	TxPrefix     string
	WithDuration bool
}

func (l Logger) Log(ctx context.Context, level slog.Level, msg string, started time.Time, err error, attrs ...slog.Attr) {
	if l.Logger == nil {
		return
	}

	if l.WithDuration && !started.IsZero() {
		attrs = append(attrs, slog.Duration("duration", time.Since(started)))
	}

	level = l.BaseLevel + level

	if err != nil {
		level = slog.LevelError
		attrs = append(attrs, slog.String("error", err.Error()))
	}

	l.Logger.LogAttrs(ctx, level, l.BasePrefix+msg, attrs...)
}

func (l Logger) With(attrs ...any) Logger {
	l.Logger = l.Logger.With(attrs...)
	return l
}
