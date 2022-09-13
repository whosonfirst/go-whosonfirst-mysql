package tables

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/paulmach/orb/encoding/wkt"
	"github.com/tidwall/gjson"
	wof_sql "github.com/whosonfirst/go-whosonfirst-database-sql"
	"github.com/whosonfirst/go-whosonfirst-feature/geometry"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"
	"github.com/whosonfirst/go-whosonfirst-uri"
)

//go:embed whosonfirst.schema
var whosonfirst_schema string

const WHOSONFIRST_TABLE string = "whosonfirst"

type WhosonfirstTable struct {
	wof_sql.Table
}

func NewWhosonfirstTableWithDatabase(ctx context.Context, db wof_sql.Database) (wof_sql.Table, error) {

	t, err := NewWhosonfirstTable(ctx)

	if err != nil {
		return nil, fmt.Errorf("Failed to create new whosonfirst table, %w", err)
	}

	err = t.InitializeTable(ctx, db)

	if err != nil {
		return nil, fmt.Errorf("Failed to initialize whosonfirst table, %w", err)
	}

	return t, nil
}

func NewWhosonfirstTable(ctx context.Context) (wof_sql.Table, error) {
	t := WhosonfirstTable{}
	return &t, nil
}

func (t *WhosonfirstTable) Name() string {
	return WHOSONFIRST_TABLE
}

// https://dev.sql.com/doc/refman/8.0/en/json-functions.html
// https://www.percona.com/blog/2016/03/07/json-document-fast-lookup-with-mysql-5-7/
// https://archive.fosdem.org/2016/schedule/event/mysql57_json/attachments/slides/1291/export/events/attachments/mysql57_json/slides/1291/MySQL_57_JSON.pdf

func (t *WhosonfirstTable) Schema() string {
	return whosonfirst_schema
}

func (t *WhosonfirstTable) InitializeTable(ctx context.Context, db wof_sql.Database) error {
	return wof_sql.CreateTableIfNecessary(ctx, db, t)
}

func (t *WhosonfirstTable) IndexRecord(ctx context.Context, db wof_sql.Database, i interface{}, custom ...interface{}) error {

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

func (t *WhosonfirstTable) IndexFeature(ctx context.Context, tx *sql.Tx, body []byte, custom ...interface{}) error {

	id, err := properties.Id(body)

	if err != nil {
		return fmt.Errorf("Failed to derive ID, %w", err)
	}

	var alt *uri.AltGeom

	if len(custom) >= 1 {
		alt = custom[0].(*uri.AltGeom)
	}

	if alt != nil {
		return nil
	}

	geojson_geom, err := geometry.Geometry(body)

	if err != nil {
		return fmt.Errorf("Failed to derive geometry, %w", err)
	}

	orb_geom := geojson_geom.Geometry()
	wkt_geom := wkt.MarshalString(orb_geom)

	centroid, _, err := properties.Centroid(body)

	if err != nil {
		return fmt.Errorf("Failed to derive centroid, %w", err)
	}

	// See the *centroid stuff? That's important because
	// the code in paulmach/orb/encoding/wkt/wkt.go is type-checking
	// on not-a-references

	wkt_centroid := wkt.MarshalString(*centroid)

	props := gjson.GetBytes(body, "properties")
	props_json, err := json.Marshal(props.Value())

	if err != nil {
		return fmt.Errorf("Failed to encode properties, %w", err)
	}

	lastmod := properties.LastModified(body)

	q := fmt.Sprintf(`REPLACE INTO %s (
		geometry, centroid, id, properties, lastmodified
	) VALUES (
		ST_GeomFromText('%s'), ST_GeomFromText('%s'), ?, ?, ?
	)`, WHOSONFIRST_TABLE, wkt_geom, wkt_centroid)

	_, err = tx.ExecContext(ctx, q, id, string(props_json), lastmod)

	if err != nil {
		return fmt.Errorf("Failed to update table, %w", err)
	}

	return nil
}
