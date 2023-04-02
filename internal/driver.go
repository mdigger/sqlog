package internal

import (
	"database/sql/driver"

	"golang.org/x/exp/slog"
)

// Driver is the interface that must be implemented by a database.
type Driver struct {
	driver driver.Driver
	logger Logger
}

// NewDriver returns a new wrapped driver.
func NewDriver(d driver.Driver, logger Logger) driver.Driver {
	dr := &Driver{
		driver: d,
		logger: logger,
	}

	if _, ok := d.(driver.DriverContext); ok {
		return dr
	}

	return struct{ driver.Driver }{dr} // only implements driver.Driver
}

var (
	_ driver.Driver        = (*Driver)(nil)
	_ driver.DriverContext = (*Driver)(nil)
)

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
//
// Open may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The returned connection is only used by one goroutine at a
// time.
func (d *Driver) Open(name string) (driver.Conn, error) {
	conn, err := d.driver.Open(name)
	if err != nil {
		return nil, err
	}

	connID := slog.String("connID", NewUID())
	return NewConn(conn, d.logger.With(connID)), nil
}

// If a Driver implements DriverContext, then sql.DB will call OpenConnector
// to obtain a Connector and then invoke that Connector's Connect method to
// obtain each needed connection, instead of invoking the Driver's Open method
// for each connection. The two-step sequence allows drivers to parse the name
// just once and also provides access to per-Conn contexts.
//
// OpenConnector must parse the name in the same format that Driver.Open
// parses the name parameter.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return d.driver.(driver.DriverContext).OpenConnector(name) // used only if supported
}
