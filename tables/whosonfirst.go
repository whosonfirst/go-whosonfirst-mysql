package tables

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/paulmach/orb/encoding/wkt"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-whosonfirst-feature/geometry"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"
	"github.com/whosonfirst/go-whosonfirst-mysql"
	"github.com/whosonfirst/go-whosonfirst-uri"
)

//go:embed whosonfirst.schema
var whosonfirst_schema string

const WHOSONFIRST_TABLE string = "whosonfirst"

type WhosonfirstTable struct {
	mysql.Table
}

func NewWhosonfirstTableWithDatabase(ctx context.Context, db mysql.Database) (mysql.Table, error) {

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

func NewWhosonfirstTable(ctx context.Context) (mysql.Table, error) {
	t := WhosonfirstTable{}
	return &t, nil
}

func (t *WhosonfirstTable) Name() string {
	return WHOSONFIRST_TABLE
}

// https://dev.mysql.com/doc/refman/8.0/en/json-functions.html
// https://www.percona.com/blog/2016/03/07/json-document-fast-lookup-with-mysql-5-7/
// https://archive.fosdem.org/2016/schedule/event/mysql57_json/attachments/slides/1291/export/events/attachments/mysql57_json/slides/1291/MySQL_57_JSON.pdf

func (t *WhosonfirstTable) Schema() string {
	return whosonfirst_schema
}

func (t *WhosonfirstTable) InitializeTable(ctx context.Context, db mysql.Database) error {

	return mysql.CreateTableIfNecessary(ctx, db, t)
}

func (t *WhosonfirstTable) IndexRecord(ctx context.Context, db mysql.Database, i interface{}, custom ...interface{}) error {
	return t.IndexFeature(ctx, db, i.([]byte), custom...)
}

func (t *WhosonfirstTable) IndexFeature(ctx context.Context, db mysql.Database, body []byte, custom ...interface{}) error {

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

	wkt_centroid := wkt.MarshalString(centroid)

	props := gjson.GetBytes(body, "properties")
	props_json, err := json.Marshal(props.Value())

	if err != nil {
		return fmt.Errorf("Failed to encode properties, %w", err)
	}

	lastmod := properties.LastModified(body)

	conn, err := db.Conn()

	if err != nil {
		return fmt.Errorf("Failed to establish database connection,%w", err)
	}

	sql := fmt.Sprintf(`REPLACE INTO %s (
		geometry, centroid, id, properties, lastmodified
	) VALUES (
		ST_GeomFromText('%s'), ST_GeomFromText('%s'), ?, ?, ?
	)`, WHOSONFIRST_TABLE, wkt_geom, wkt_centroid)

	_, err = conn.ExecContext(ctx, sql, id, string(props_json), lastmod)

	if err != nil {
		return fmt.Errorf("Failed to update table, %w", err)
	}

	return nil
}
