package sql

import (
	"context"
	"database/sql"
)

type Database interface {
	Conn() (*sql.DB, error)
	DSN() string
	Lock()
	Unlock()
	Close() error
	IndexFeature(context.Context, []Table, []byte, ...interface{}) error
}
