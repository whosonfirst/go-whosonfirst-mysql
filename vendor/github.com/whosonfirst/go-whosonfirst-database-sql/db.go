package sql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"sync"
)

type SQLDatabase struct {
	Database
	conn *sql.DB
	dsn  string
	mu   *sync.Mutex
}

func NewSQLDB(ctx context.Context, uri string) (Database, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	driver := u.Scheme

	q := u.Query()
	dsn := q.Get("dsn")

	conn, err := sql.Open(driver, dsn)

	if err != nil {
		return nil, fmt.Errorf("Failed to open database, %w", err)
	}

	mu := new(sync.Mutex)

	db := SQLDatabase{
		conn: conn,
		dsn:  dsn,
		mu:   mu,
	}

	return &db, err
}

func (db *SQLDatabase) Lock() {
	db.mu.Lock()
}

func (db *SQLDatabase) Unlock() {
	db.mu.Unlock()
}

func (db *SQLDatabase) Conn() (*sql.DB, error) {
	return db.conn, nil
}

func (db *SQLDatabase) DSN() string {
	return db.dsn
}

func (db *SQLDatabase) Close() error {
	return db.conn.Close()
}

func (db *SQLDatabase) IndexFeature(ctx context.Context, tables []Table, body []byte, args ...interface{}) error {

	conn, err := db.Conn()

	if err != nil {
		return fmt.Errorf("Failed to establish database connection, %w", err)
	}

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})

	if err != nil {
		return fmt.Errorf("Failed to create transaction, %w", err)
	}

	for _, t := range tables {

		err := t.IndexFeature(ctx, tx, body, args...)

		if err != nil {
			tx.Rollback()
			return fmt.Errorf("Failed to index %s table, %w", t.Name(), err)
		}
	}

	err = tx.Commit()

	if err != nil {
		return fmt.Errorf("Failed to commit transaction, %w", err)
	}

	return nil
}
