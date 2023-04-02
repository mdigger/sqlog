package internal

import (
	"database/sql/driver"
	"errors"

	"golang.org/x/exp/slog"
)

// Copied from stdlib database/sql package: src/database/sql/ctxutil.go.
func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("sql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}

func logQuery(query string) slog.Attr {
	return slog.String("query", query)
}

func logArgs(args any) slog.Attr {
	var dargs []driver.Value
	switch args := args.(type) {
	case nil:
	case []driver.NamedValue:
		dargs = make([]driver.Value, len(args))
		for n, param := range args {
			dargs[n] = param.Value
		}
	case []driver.Value:
		dargs = args
	}

	return slog.Any("args", dargs)
}
