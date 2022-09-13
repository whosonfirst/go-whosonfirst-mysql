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

	return NewDBWithDSN(ctx, dsn)
}

func NewDBWithDSN(ctx context.Context, dsn string) (*MySQLDatabase, error) {

	conn, err := sql.Open("mysql", dsn)

	if err != nil {
		return nil, fmt.Errorf("Failed to open database, %w", err)
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

func (db *MySQLDatabase) IndexFeature(ctx context.Context, tables []mysql.Table, body []byte, args ...interface{}) error {

	conn, err := db.Conn()

	if err != nil {
		return fmt.Errorf("Failed to establish database connection, %w", err)
	}

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	
	if err != nil {
		return fmt.Errorf("Failed to create transaction, %w", err)
	}

	for _, t := range table {


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
