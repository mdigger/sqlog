package sqlog

import (
	"database/sql"

	"github.com/mdigger/sqlog/internal"
)

// Open opens a database specified by its database driver name and
// a driver-specific data source name with logging support.
func Open(driverName, dsn string, opt ...Options) (*sql.DB, error) {
	// Retrieve the driver implementation we need to wrap with instrumentation
	db, err := sql.Open(driverName, "")
	if err != nil {
		return nil, err
	}

	d := db.Driver()

	if err := db.Close(); err != nil {
		return nil, err
	}

	opt = append([]Options{WithPrefix(driverName + ":")}, opt...)
	logger := newDefaultLogger(opt...)
	connector := internal.NewConnector(dsn, d, logger)

	return sql.OpenDB(connector), nil
}
