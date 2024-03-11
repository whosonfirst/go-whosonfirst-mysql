package tables

import (
	"context"
	"database/sql"
	"fmt"

	wof_sql "github.com/whosonfirst/go-whosonfirst-database-sql"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"
	foo_tables "github.com/whosonfirst/go-whosonfirst-sql/tables"
	"github.com/whosonfirst/go-whosonfirst-uri"
)

type GeoJSONTable struct {
	wof_sql.Table
}

func NewGeoJSONTableWithDatabase(ctx context.Context, db wof_sql.Database) (wof_sql.Table, error) {

	t, err := NewGeoJSONTable(ctx)

	if err != nil {
		return nil, fmt.Errorf("Failed to create GeoJSON table, %w", err)
	}

	err = t.InitializeTable(ctx, db)

	if err != nil {
		return nil, fmt.Errorf("Failed to initialize GeoJSON table, %w", err)
	}

	return t, nil
}

func NewGeoJSONTable(ctx context.Context) (wof_sql.Table, error) {
	t := GeoJSONTable{}
	return &t, nil
}

func (t *GeoJSONTable) Name() string {
	return foo_tables.GEOJSON_TABLE_NAME
}

func (t *GeoJSONTable) Schema() string {
	s, _ := foo_tables.LoadSchema("mysql", foo_tables.GEOJSON_TABLE_NAME)
	return s
}

func (t *GeoJSONTable) InitializeTable(ctx context.Context, db wof_sql.Database) error {
	return wof_sql.CreateTableIfNecessary(ctx, db, t)
}

func (t *GeoJSONTable) IndexRecord(ctx context.Context, db wof_sql.Database, i interface{}, custom ...interface{}) error {

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

func (t *GeoJSONTable) IndexFeature(ctx context.Context, tx *sql.Tx, body []byte, custom ...interface{}) error {

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

	q := fmt.Sprintf(`REPLACE INTO %s (
		id, alt, body, lastmodified
	) VALUES (
		?, ?, ?, ?
	)`, foo_tables.GEOJSON_TABLE_NAME)

	_, err = tx.ExecContext(ctx, q, id, str_alt, string(body), lastmod)

	if err != nil {
		return fmt.Errorf("Failed to update geojson table, %w", err)
	}

	return nil
}
