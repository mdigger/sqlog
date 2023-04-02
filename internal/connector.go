package internal

import (
	"context"
	"database/sql/driver"
	"time"

	"golang.org/x/exp/slog"
)

// Connector represents a driver in a fixed configuration.
type Connector struct {
	dsn    string
	driver driver.Driver
	logger Logger
}

func NewConnector(dsn string, d driver.Driver, logger Logger) *Connector {
	return &Connector{
		dsn:    dsn,
		driver: d,
		logger: logger,
	}
}

var _ driver.Connector = (*Connector)(nil)

// Connect returns a connection to the database.
// Connect may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The provided context.Context is for dialing purposes only
// (see net.DialContext) and should not be stored or used for
// other purposes. A default timeout should still be used
// when dialing as a connection pool may call Connect
// asynchronously to any query.
//
// The returned connection is only used by one goroutine at a
// time.
func (c *Connector) Connect(ctx context.Context) (_ driver.Conn, err error) {
	connID := slog.String("connID", NewUID())

	defer func(started time.Time) {
		c.logger.Log(ctx, slog.LevelInfo, "connect", started, err, connID)
	}(time.Now())

	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}

	return NewConn(conn, c.logger.With(connID)), nil
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (c *Connector) Driver() driver.Driver { return c.driver }
