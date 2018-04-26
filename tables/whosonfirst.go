package tables

// https://mariadb.com/kb/en/library/geographic-geometric-features/

import (
	"fmt"
	"github.com/twpayne/go-geom"
	gogeom_geojson "github.com/twpayne/go-geom/encoding/geojson"
	"github.com/twpayne/go-geom/encoding/wkt"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/geometry"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/whosonfirst"
	"github.com/whosonfirst/go-whosonfirst-mysql"
	"github.com/whosonfirst/go-whosonfirst-mysql/utils"
	// "log"
	"strconv"
)

type WhosonfirstTable struct {
	mysql.Table
	name string
}

type WhosonfirstRow struct {
	Id           int64
	Body         string
	LastModified int64
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

	t := WhosonfirstTable{
		name: "whosonfirst",
	}

	return &t, nil
}

func (t *WhosonfirstTable) Name() string {
	return t.name
}

func (t *WhosonfirstTable) Schema() string {

	// `properties` JSON NOT NULL,
	// `hierarchies` JSON NOT NULL,

	sql := `CREATE TABLE IF NOT EXISTS %s (
		      id BIGINT UNSIGNED PRIMARY KEY,
		      name varchar(100) DEFAULT NULL,
		      country char(2) NOT NULL,
		      placetype VARCHAR(24) NOT NULL,
		      parent_id BIGINT UNSIGNED NOT NULL,
		      is_current TINYINT NOT NULL,
		      is_deprecated TINYINT NOT NULL,
		      is_ceased TINYINT NOT NULL,
		      geometry GEOMETRY NOT NULL,
		      centroid POINT NOT NULL,
		      lastmodified INT NOT NULL,
		      SPATIAL KEY %s_geometry (geometry),
		      SPATIAL KEY %s_centroid (centroid),
		      FULLTEXT (name)
		      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	return fmt.Sprintf(sql, t.Name(), t.Name(), t.Name())
}

func (t *WhosonfirstTable) InitializeTable(db mysql.Database) error {

	return utils.CreateTableIfNecessary(db, t)
}

func (t *WhosonfirstTable) IndexRecord(db mysql.Database, i interface{}) error {
	return t.IndexFeature(db, i.(geojson.Feature))
}

func (t *WhosonfirstTable) IndexFeature(db mysql.Database, f geojson.Feature) error {

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

	sql := fmt.Sprintf(`INSERT INTO %s (
		id, name, country, placetype, parent_id,
		is_current, is_deprecated, is_ceased,
		geometry, centroid,
		lastmodified
	) VALUES (
		?, ?, ?, ?, ?,
		?, ?, ?,
		ST_GeomFromText('%s'), ST_Centroid(ST_GeomFromText('%s')),
		? 
	)`, t.Name(), str_wkt, str_wkt)

	stmt, err := tx.Prepare(sql)

	if err != nil {
		return err
	}

	defer stmt.Close()

	is_current, err := whosonfirst.IsCurrent(f)

	if err != nil {
		return err
	}

	is_deprecated, err := whosonfirst.IsDeprecated(f)

	if err != nil {
		return err
	}

	is_ceased, err := whosonfirst.IsCeased(f)

	if err != nil {
		return err
	}

	country := whosonfirst.Country(f)
	lastmod := whosonfirst.LastModified(f)

	parent_id := strconv.FormatInt(whosonfirst.ParentId(f), 10)

	_, err = stmt.Exec(f.Id(), f.Name(), country, f.Placetype(), parent_id, is_current.StringFlag(), is_deprecated.StringFlag(), is_ceased.StringFlag(), lastmod)

	if err != nil {
		return err
	}

	return tx.Commit()
}
