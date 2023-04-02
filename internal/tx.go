package internal

import (
	"context"
	"database/sql/driver"
	"time"

	"golang.org/x/exp/slog"
)

var _ driver.Tx = (*Tx)(nil)

// Tx is a transaction.
type Tx struct {
	tx      driver.Tx
	started time.Time
	logger  Logger
}

func NewTx(tx driver.Tx, logger Logger) *Tx {
	return &Tx{
		tx:      tx,
		started: time.Now(),
		logger:  logger,
	}
}

func (t *Tx) Commit() (err error) {
	defer func() {
		t.logger.Log(context.Background(), slog.LevelInfo, t.logger.TxPrefix+"commit", t.started, err)
	}()

	return t.tx.Commit()
}

func (t *Tx) Rollback() (err error) {
	defer func() {
		t.logger.Log(context.Background(), slog.LevelInfo, t.logger.TxPrefix+"rollback", t.started, err)
	}()
	return t.tx.Rollback()
}
