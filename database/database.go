package database

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"net/url"
	"sync"
)

type MySQLDatabase struct {
	conn *sql.DB
	dsn  string
	mu   *sync.Mutex
}

func NewDB(ctx context.Context, uri string) (*MySQLDatabase, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	q := u.Query()
	dsn := q.Get("dsn")

	// if u.Path read config...

	conn, err := sql.Open("mysql", dsn)

	if err != nil {
		return nil, err
	}

	mu := new(sync.Mutex)

	db := MySQLDatabase{
		conn: conn,
		dsn:  dsn,
		mu:   mu,
	}

	return &db, err
}

func (db *MySQLDatabase) Lock() {
	db.mu.Lock()
}

func (db *MySQLDatabase) Unlock() {
	db.mu.Unlock()
}

func (db *MySQLDatabase) Conn() (*sql.DB, error) {
	return db.conn, nil
}

func (db *MySQLDatabase) DSN() string {
	return db.dsn
}

func (db *MySQLDatabase) Close() error {
	return db.conn.Close()
}
