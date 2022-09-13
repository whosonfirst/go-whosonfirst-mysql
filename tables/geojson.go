package tables

import (
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
	name string
}

func NewGeoJSONTableWithDatabase(db mysql.Database) (mysql.Table, error) {

	t, err := NewGeoJSONTable()

	if err != nil {
		return nil, fmt.Errorf("Failed to create GeoJSON table, %w", err)
	}

	err = t.InitializeTable(db)

	if err != nil {
		return nil, fmt.Errorf("Failed to initialize GeoJSON table, %w", err)
	}

	return t, nil
}

func NewGeoJSONTable() (mysql.Table, error) {
	t := GeoJSONTable{}
	return &t, nil
}

func (t *GeoJSONTable) Name() string {
	return GEOJSON_TABLE
}

func (t *GeoJSONTable) Schema() string {
	return geojson_schema
}

func (t *GeoJSONTable) InitializeTable(db mysql.Database) error {

	return mysql.CreateTableIfNecessary(db, t)
}

func (t *GeoJSONTable) IndexRecord(db mysql.Database, i interface{}, custom ...interface{}) error {
	return t.IndexFeature(db, i.([]byte), custom...)
}

func (t *GeoJSONTable) IndexFeature(db mysql.Database, body []byte, custom ...interface{}) error {

	id, err := properties.Id(body)

	if err != nil {
		return fmt.Errorf("Failed to derive ID, %w", err)
	}

	var alt *uri.AltGeom

	if len(custom) >= 1 {
		alt = custom[0].(*uri.AltGeom)
	}

	conn, err := db.Conn()

	if err != nil {
		return fmt.Errorf("Failed to create database connection, %w", err)
	}

	tx, err := conn.Begin()

	if err != nil {
		return fmt.Errorf("Failed to start transaction, %w", err)
	}

	sql := fmt.Sprintf(`REPLACE INTO %s (
		id, alt, body, lastmodified
	) VALUES (
		?, ?, ?, ?
	)`, GEOJSON_TABLE)

	stmt, err := tx.Prepare(sql)

	if err != nil {
		return fmt.Errorf("Failed to create statement, %w", err)
	}

	defer stmt.Close()

	lastmod := properties.LastModified(body)

	str_alt := ""

	if alt != nil {

		str_alt, err = alt.String()

		if err != nil {
			return fmt.Errorf("Failed to stringify alt, %w", err)
		}
	}

	_, err = stmt.Exec(id, str_alt, string(body), lastmod)

	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
