package tables

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	wof_sql "github.com/whosonfirst/go-whosonfirst-database-sql"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"
	"github.com/whosonfirst/go-whosonfirst-spelunker/document"	
	wof_tables "github.com/whosonfirst/go-whosonfirst-sql/tables"
	"github.com/whosonfirst/go-whosonfirst-uri"
)

type SpelunkerTable struct {
	wof_sql.Table
}

func NewSpelunkerTableWithDatabase(ctx context.Context, db wof_sql.Database) (wof_sql.Table, error) {

	t, err := NewSpelunkerTable(ctx)

	if err != nil {
		return nil, fmt.Errorf("Failed to create Spelunker table, %w", err)
	}

	err = t.InitializeTable(ctx, db)

	if err != nil {
		return nil, fmt.Errorf("Failed to initialize Spelunker table, %w", err)
	}

	return t, nil
}

func NewSpelunkerTable(ctx context.Context) (wof_sql.Table, error) {
	t := SpelunkerTable{}
	return &t, nil
}

func (t *SpelunkerTable) Name() string {
	return wof_tables.SPELUNKER_TABLE_NAME
}

func (t *SpelunkerTable) Schema() string {
	s, _ := wof_tables.LoadSchema("mysql", wof_tables.SPELUNKER_TABLE_NAME)
	return s
}

func (t *SpelunkerTable) InitializeTable(ctx context.Context, db wof_sql.Database) error {
	return wof_sql.CreateTableIfNecessary(ctx, db, t)
}

func (t *SpelunkerTable) IndexRecord(ctx context.Context, db wof_sql.Database, i interface{}, custom ...interface{}) error {

	conn, err := db.Conn()

	if err != nil {
		return fmt.Errorf("Failed to establish database connection, %w", err)
	}

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})

	if err != nil {
		return fmt.Errorf("Failed to create transaction, %w", err)
	}

	err = t.IndexFeature(ctx, tx, i.([]byte), custom...)

	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Failed to index %s table, %w", t.Name(), err)
	}

	err = tx.Commit()

	if err != nil {
		return fmt.Errorf("Failed to commit transaction, %w", err)
	}

	return nil
}

func (t *SpelunkerTable) IndexFeature(ctx context.Context, tx *sql.Tx, body []byte, custom ...interface{}) error {

	id, err := properties.Id(body)

	if err != nil {
		return fmt.Errorf("Failed to derive ID, %w", err)
	}

	var alt *uri.AltGeom

	if len(custom) >= 1 {
		alt = custom[0].(*uri.AltGeom)
	}

	lastmod := properties.LastModified(body)

	str_alt := ""

	if alt != nil {

		str_alt, err = alt.String()

		if err != nil {
			return fmt.Errorf("Failed to stringify alt, %w", err)
		}
	}

	doc, err := document.PrepareSpelunkerV2Document(ctx, body)

	if err != nil {
		return fmt.Errorf("Failed to prepare spelunker document, %w", err)
	}

	enc_doc, err := json.Marshal(doc)

	if err != nil {
		return fmt.Errorf("Failed to marshal spelunker document, %w", err)
	}
	
	q := fmt.Sprintf(`REPLACE INTO %s (
		id, alt, body, lastmodified
	) VALUES (
		?, ?, ?, ?
	)`, wof_tables.SPELUNKER_TABLE_NAME)

	_, err = tx.ExecContext(ctx, q, id, str_alt, string(enc_doc), lastmod)

	if err != nil {
		return fmt.Errorf("Failed to update %s table, %w", wof_tables.SPELUNKER_TABLE_NAME, err)
	}

	return nil
}
