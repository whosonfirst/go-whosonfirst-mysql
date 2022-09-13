package tables

import (
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

func NewWhosonfirstTableWithDatabase(db mysql.Database) (mysql.Table, error) {

	t, err := NewWhosonfirstTable()

	if err != nil {
		return nil, err
	}

	err = t.InitializeTable(db)

	if err != nil {
		return nil, err
	}

	return t, nil
}

func NewWhosonfirstTable() (mysql.Table, error) {
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

func (t *WhosonfirstTable) InitializeTable(db mysql.Database) error {

	return mysql.CreateTableIfNecessary(db, t)
}

func (t *WhosonfirstTable) IndexRecord(db mysql.Database, i interface{}, custom ...interface{}) error {
	return t.IndexFeature(db, i.([]byte), custom...)
}

func (t *WhosonfirstTable) IndexFeature(db mysql.Database, body []byte, custom ...interface{}) error {

	id, err := properties.Id(body)

	if err != nil {
		return err
	}

	var alt *uri.AltGeom

	if len(custom) >= 1 {
		alt = custom[0].(*uri.AltGeom)
	}

	if alt != nil {
		return nil
	}

	conn, err := db.Conn()

	if err != nil {
		return err
	}

	tx, err := conn.Begin()

	if err != nil {
		return err
	}

	geojson_geom, err := geometry.Geometry(body)

	if err != nil {
		return err
	}

	orb_geom := geojson_geom.Geometry()
	wkt_geom := wkt.MarshalString(orb_geom)

	centroid, _, err := properties.Centroid(body)

	if err != nil {
		return err
	}

	wkt_centroid := wkt.MarshalString(centroid)

	sql := fmt.Sprintf(`REPLACE INTO %s (
		geometry, centroid, id, properties, lastmodified
	) VALUES (
		ST_GeomFromText('%s'), ST_GeomFromText('%s'), ?, ?, ?
	)`, WHOSONFIRST_TABLE, wkt_geom, wkt_centroid)

	stmt, err := tx.Prepare(sql)

	if err != nil {
		return err
	}

	defer stmt.Close()

	props := gjson.GetBytes(body, "properties")
	props_json, err := json.Marshal(props.Value())

	if err != nil {
		return err
	}

	lastmod := properties.LastModified(body)

	_, err = stmt.Exec(id, string(props_json), lastmod)

	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
