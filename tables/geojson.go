package tables

import (
       "encoding/json"
	"fmt"
	"github.com/tidwall/gjson"
	"github.com/twpayne/go-geom"
	gogeom_geojson "github.com/twpayne/go-geom/encoding/geojson"
	"github.com/twpayne/go-geom/encoding/wkt"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/geometry"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/whosonfirst"
	"github.com/whosonfirst/go-whosonfirst-mysql"
	"github.com/whosonfirst/go-whosonfirst-mysql/utils"
	// "log"
)

type GeoJSONTable struct {
	mysql.Table
	name string
}

func NewGeoJSONTableWithDatabase(db mysql.Database) (mysql.Table, error) {

	t, err := NewGeoJSONTable()

	if err != nil {
		return nil, err
	}

	err = t.InitializeTable(db)

	if err != nil {
		return nil, err
	}

	return t, nil
}

func NewGeoJSONTable() (mysql.Table, error) {

	t := GeoJSONTable{
		name: "geojson",
	}

	return &t, nil
}

func (t *GeoJSONTable) Name() string {
	return t.name
}

func (t *GeoJSONTable) Schema() string {

	sql := `CREATE TABLE IF NOT EXISTS %s (
		      id BIGINT UNSIGNED PRIMARY KEY,
		      properties JSON NOT NULL,
		      geometry GEOMETRY NOT NULL,
		      lastmodified INT NOT NULL,
		      SPATIAL KEY %s_geometry (geometry),
	      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	return fmt.Sprintf(sql, t.Name(), t.Name(), t.Name())
}

func (t *GeoJSONTable) InitializeTable(db mysql.Database) error {

	return utils.CreateTableIfNecessary(db, t)
}

func (t *GeoJSONTable) IndexRecord(db mysql.Database, i interface{}) error {
	return t.IndexFeature(db, i.(geojson.Feature))
}

func (t *GeoJSONTable) IndexFeature(db mysql.Database, f geojson.Feature) error {

	conn, err := db.Conn()

	if err != nil {
		return err
	}

	tx, err := conn.Begin()

	if err != nil {
		return err
	}

	str_geom, err := geometry.ToString(f)

	if err != nil {
		return err
	}

	var g geom.T
	err = gogeom_geojson.Unmarshal([]byte(str_geom), &g)

	if err != nil {
		return err
	}

	str_wkt, err := wkt.Marshal(g)

	sql := fmt.Sprintf(`REPLACE INTO %s (
		id, properies, geometry, lastmodified
	) VALUES (
		?, ?, ST_GeomFromText('%s'), ?
	)`, t.Name(), str_wkt)

	stmt, err := tx.Prepare(sql)

	if err != nil {
		return err
	}

	defer stmt.Close()

	props := gjson.GetBytes(f.Bytes(), "properties")
	props_json, err := json.Marshal(props.Value())

	if err != nil {
		return err
	}
	
	lastmod := whosonfirst.LastModified(f)

	_, err = stmt.Exec(f.Id(), string(props_json), lastmod)

	if err != nil {
		return err
	}

	return tx.Commit()
}
