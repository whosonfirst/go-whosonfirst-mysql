package tables

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"
	"github.com/whosonfirst/go-whosonfirst-mysql"
	"github.com/whosonfirst/go-whosonfirst-uri"
)

//go:embed geojson.schema
var geojson_schema string

const GEOJSON_TABLE string = "geojson"

type GeoJSONTable struct {
	mysql.Table
}

func NewGeoJSONTableWithDatabase(ctx context.Context, db mysql.Database) (mysql.Table, error) {

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

func NewGeoJSONTable(ctx context.Context) (mysql.Table, error) {
	t := GeoJSONTable{}
	return &t, nil
}

func (t *GeoJSONTable) Name() string {
	return GEOJSON_TABLE
}

func (t *GeoJSONTable) Schema() string {
	return geojson_schema
}

func (t *GeoJSONTable) InitializeTable(ctx context.Context, db mysql.Database) error {

	return mysql.CreateTableIfNecessary(ctx, db, t)
}

func (t *GeoJSONTable) IndexRecord(ctx context.Context, db mysql.Database, i interface{}, custom ...interface{}) error {
	return t.IndexFeature(ctx, db, i.([]byte), custom...)
}

func (t *GeoJSONTable) IndexFeature(ctx context.Context, db mysql.Database, body []byte, custom ...interface{}) error {

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

	conn, err := db.Conn()

	if err != nil {
		return fmt.Errorf("Failed to create database connection, %w", err)
	}

	sql := fmt.Sprintf(`REPLACE INTO %s (
		id, alt, body, lastmodified
	) VALUES (
		?, ?, ?, ?
	)`, GEOJSON_TABLE)

	_, err = conn.ExecContext(ctx, sql, id, str_alt, string(body), lastmod)

	if err != nil {
		return fmt.Errorf("Failed to update geojson table, %w", err)
	}

	return nil
}
