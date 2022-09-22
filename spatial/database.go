package spatial

import (
	"context"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	gocache "github.com/patrickmn/go-cache"
	"github.com/paulmach/orb"
	"github.com/whosonfirst/go-ioutil"
	wof_sql "github.com/whosonfirst/go-whosonfirst-database-sql"
	"github.com/whosonfirst/go-whosonfirst-mysql/tables"
	wof_reader "github.com/whosonfirst/go-whosonfirst-reader"
	"github.com/whosonfirst/go-whosonfirst-spatial"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/timer"
	"github.com/whosonfirst/go-whosonfirst-spr/v2"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"io"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

func init() {
	ctx := context.Background()
	database.RegisterSpatialDatabase(ctx, "mysql", NewMysqlSpatialDatabase)
}

// MysqlSpatialDatabase is a struct that implements the `database.SpatialDatabase` for performing
// spatial queries on data stored in a Mysql databases from tables defined by the `whosonfirst/go-whosonfirst-mysql/tables`
// package.
type MysqlSpatialDatabase struct {
	database.SpatialDatabase
	Logger        *log.Logger
	Timer         *timer.Timer
	mu            *sync.RWMutex
	db            wof_sql.Database
	spatial_table wof_sql.Table
	geojson_table wof_sql.Table
	gocache       *gocache.Cache
	dsn           string
}

// MysqlResults is a struct that implements the `whosonfirst/go-whosonfirst-spr.StandardPlacesResults`
// interface for rows matching a spatial query.
type MysqlResults struct {
	spr.StandardPlacesResults `json:",omitempty"`
	// Places is the list of `whosonfirst/go-whosonfirst-spr.StandardPlacesResult` instances returned for a spatial query.
	Places []spr.StandardPlacesResult `json:"places"`
}

// Results returns a `whosonfirst/go-whosonfirst-spr.StandardPlacesResults` instance for rows matching a spatial query.
func (r *MysqlResults) Results() []spr.StandardPlacesResult {
	return r.Places
}

// NewMysqlSpatialDatabase returns a new `whosonfirst/go-whosonfirst-spatial/database.database.SpatialDatabase`
// instance for performing spatial operations derived from 'uri'.
func NewMysqlSpatialDatabase(ctx context.Context, uri string) (database.SpatialDatabase, error) {

	mysql_db, err := wof_sql.NewSQLDB(ctx, uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to create new database, %w", err)
	}

	return NewMysqlSpatialDatabaseWithDatabase(ctx, uri, mysql_db)
}

// NewMysqlSpatialDatabaseWithDatabase returns a new `whosonfirst/go-whosonfirst-spatial/database.database.SpatialDatabase`
// instance for performing spatial operations derived from 'uri' and an existing `aaronland/go-mysql/database.MysqlDatabase`
// instance defined by 'mysql_db'.
func NewMysqlSpatialDatabaseWithDatabase(ctx context.Context, uri string, mysql_db wof_sql.Database) (database.SpatialDatabase, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	q := u.Query()

	dsn := q.Get("dsn")

	spatial_table, err := tables.NewWhosonfirstTableWithDatabase(ctx, mysql_db)

	if err != nil {
		return nil, fmt.Errorf("Failed to create rtree table, %w", err)
	}

	// This is so we can satisfy the reader.Reader requirement
	// in the spatial.SpatialDatabase interface

	geojson_table, err := tables.NewGeoJSONTableWithDatabase(ctx, mysql_db)

	if err != nil {
		return nil, fmt.Errorf("Failed to create geojson table, %w", err)
	}

	logger := log.Default()

	expires := 5 * time.Minute
	cleanup := 30 * time.Minute

	gc := gocache.New(expires, cleanup)

	mu := new(sync.RWMutex)

	t := timer.NewTimer()

	spatial_db := &MysqlSpatialDatabase{
		Logger:        logger,
		Timer:         t,
		db:            mysql_db,
		spatial_table: spatial_table,
		geojson_table: geojson_table,
		gocache:       gc,
		dsn:           dsn,
		mu:            mu,
	}

	return spatial_db, nil
}

// Disconnect will close the underlying database connection.
func (r *MysqlSpatialDatabase) Disconnect(ctx context.Context) error {
	return r.db.Close()
}

// IndexFeature will index a Who's On First GeoJSON Feature record, defined in 'body', in the spatial database.
func (r *MysqlSpatialDatabase) IndexFeature(ctx context.Context, body []byte) error {

	to_index := []wof_sql.Table{
		r.spatial_table,
		r.geojson_table,
	}

	return r.db.IndexFeature(ctx, to_index, body)
}

// RemoveFeature will remove the database record with ID 'id' from the database.
func (r *MysqlSpatialDatabase) RemoveFeature(ctx context.Context, str_id string) error {

	id, err := strconv.ParseInt(str_id, 10, 64)

	if err != nil {
		return fmt.Errorf("Failed to parse string ID '%s', %w", str_id, err)
	}

	conn, err := r.db.Conn()

	if err != nil {
		return fmt.Errorf("Failed to establish database connection, %w", err)
	}

	tx, err := conn.Begin()

	if err != nil {
		return fmt.Errorf("Failed to create transaction, %w", err)
	}

	// defer tx.Rollback()

	to_remove := []wof_sql.Table{
		r.spatial_table,
		r.geojson_table,
	}

	for _, t := range to_remove {

		var q string

		switch t.Name() {
		case "rtree":
			q = fmt.Sprintf("DELETE FROM %s WHERE wof_id = ?", t.Name())
		default:
			q = fmt.Sprintf("DELETE FROM %s WHERE id = ?", t.Name())
		}

		stmt, err := tx.Prepare(q)

		if err != nil {
			return fmt.Errorf("Failed to create query statement for %s, %w", t.Name(), err)
		}

		_, err = stmt.ExecContext(ctx, id)

		if err != nil {
			return fmt.Errorf("Failed execute query statement for %s, %w", t.Name(), err)
		}
	}

	err = tx.Commit()

	if err != nil {
		return fmt.Errorf("Failed to commit transaction, %w", err)
	}

	return nil
}

// PointInPolygon will perform a point in polygon query against the database for records that contain 'coord' and
// that are inclusive of any filters defined by 'filters'.
func (r *MysqlSpatialDatabase) PointInPolygon(ctx context.Context, coord *orb.Point, filters ...spatial.Filter) (spr.StandardPlacesResults, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rsp_ch := make(chan spr.StandardPlacesResult)
	err_ch := make(chan error)
	done_ch := make(chan bool)

	results := make([]spr.StandardPlacesResult, 0)
	working := true

	go r.PointInPolygonWithChannels(ctx, rsp_ch, err_ch, done_ch, coord, filters...)

	for {
		select {
		case <-ctx.Done():
			return nil, nil
		case <-done_ch:
			working = false
		case rsp := <-rsp_ch:
			results = append(results, rsp)
		case err := <-err_ch:
			return nil, fmt.Errorf("Point in polygon request failed, %w", err)
		default:
			// pass
		}

		if !working {
			break
		}
	}

	spr_results := &MysqlResults{
		Places: results,
	}

	return spr_results, nil
}

// PointInPolygonWithChannels will perform a point in polygon query against the database for records that contain 'coord' and
// that are inclusive of any filters defined by 'filters' emitting results to 'rsp_ch' (for matches), 'err_ch' (for errors) and 'done_ch'
// (when the query is completed).
func (r *MysqlSpatialDatabase) PointInPolygonWithChannels(ctx context.Context, rsp_ch chan spr.StandardPlacesResult, err_ch chan error, done_ch chan bool, coord *orb.Point, filters ...spatial.Filter) {

	defer func() {
		done_ch <- true
	}()

	conn, err := r.db.Conn()

	if err != nil {
		err_ch <- fmt.Errorf("Failed to establish database connection, %w", err)
		return
	}

	lat := coord.Lat()
	lon := coord.Lon()

	q := fmt.Sprintf("SELECT id FROM %s WHERE ST_Contains(geometry, GeometryFromText('POINT(? ?)'))", r.spatial_table.Name())

	rows, err := conn.QueryContext(ctx, q, lon, lat)

	if err != nil {
		err_ch <- fmt.Errorf("Failed to issue query, %w", err)
		return
	}

	defer rows.Close()

	wg := new(sync.WaitGroup)

	for rows.Next() {

		var id int64

		err := rows.Scan(&id)

		if err != nil {
			err_ch <- fmt.Errorf("Failed to scan row, %w", err)
			return
		}

		wg.Add(1)

		go func(id int64) {
			defer wg.Done()
			r.inflateSpatialIndexWithChannels(ctx, rsp_ch, err_ch, id, filters...)
		}(id)
	}

	err = rows.Close()

	if err != nil {
		err_ch <- fmt.Errorf("Failed to close rows, %w", err)
		return
	}

	err = rows.Err()

	if err != nil {
		err_ch <- fmt.Errorf("There was a problem iterating query rows, %w", err)
		return
	}

	wg.Wait()
	return
}

// PointAndPolygonCandidates will perform a point in polygon query against the database for records that contain 'coord' and
// that are inclusive of any filters defined by 'filters' returning the list of `spatial.PointInPolygonCandidate` candidate bounding
// boxes that match an initial RTree-based spatial query.
func (r *MysqlSpatialDatabase) PointInPolygonCandidates(ctx context.Context, coord *orb.Point, filters ...spatial.Filter) ([]*spatial.PointInPolygonCandidate, error) {

	// return r.PointInPolygon(ctx, coord, filters...)

	return nil, fmt.Errorf("Not implemented")
}

// PointAndPolygonCandidatesWithChannels will perform a point in polygon query against the database for records that contain 'coord' and
// that are inclusive of any filters defined by 'filters' returning the list of `spatial.PointInPolygonCandidate` candidate bounding
// boxes that match an initial RTree-based spatial query emitting results to 'rsp_ch' (for matches), 'err_ch' (for errors) and 'done_ch'
// (when the query is completed).
func (r *MysqlSpatialDatabase) PointInPolygonCandidatesWithChannels(ctx context.Context, rsp_ch chan *spatial.PointInPolygonCandidate, err_ch chan error, done_ch chan bool, coord *orb.Point, filters ...spatial.Filter) {

	// return r.PointInPolygonWithChannels(ctx, rsp_ch, err_ch, done_ch, coord, filters...)
}

// inflateSpatialIndexWithChannels creates `spr.StandardPlacesResult` instance for 'sp' applying any filters defined in 'filters'
// emitting results to 'rsp_ch' (on succcess) and 'err_ch' (if there was an error). If a given record is already found in 'seen' it
// will be skipped; if not it will be added (to 'seen') once the spatial index has been successfully inflated.
func (r *MysqlSpatialDatabase) inflateSpatialIndexWithChannels(ctx context.Context, rsp_ch chan spr.StandardPlacesResult, err_ch chan error, id int64, filters ...spatial.Filter) {

	select {
	case <-ctx.Done():
		return
	default:
		// pass
	}

	str_id := strconv.FormatInt(id, 10)

	t4 := time.Now()

	body, err := wof_reader.LoadBytes(ctx, r, id)

	if err != nil {
		err_ch <- fmt.Errorf("Failed to read body for %d, %w", id, err)
		return
	}

	s, err := spr.WhosOnFirstSPR(body)

	if err != nil {
		err_ch <- fmt.Errorf("Failed to create SPR for %d, %w", id, err)
		return
	}

	r.Timer.Add(ctx, str_id, "time to retrieve SPR", time.Since(t4))

	t5 := time.Now()

	for _, f := range filters {

		err = filter.FilterSPR(f, s)

		if err != nil {
			// r.Logger.Printf("SKIP %s because filter error %s", sp_id, err)
			return
		}
	}

	r.Timer.Add(ctx, str_id, "time to filter SPR", time.Since(t5))

	rsp_ch <- s
}

// Read implements the whosonfirst/go-reader interface so that the database itself can be used as a
// reader.Reader instance (reading features from the `geojson` table.
func (r *MysqlSpatialDatabase) Read(ctx context.Context, str_uri string) (io.ReadSeekCloser, error) {

	id, _, err := uri.ParseURI(str_uri)

	if err != nil {
		return nil, err
	}

	conn, err := r.db.Conn()

	if err != nil {
		return nil, err
	}

	// TO DO : ALT STUFF HERE

	q := fmt.Sprintf("SELECT body FROM %s WHERE id = ?", r.geojson_table.Name())

	row := conn.QueryRowContext(ctx, q, id)

	var body string

	err = row.Scan(&body)

	if err != nil {
		return nil, err
	}

	sr := strings.NewReader(body)
	fh, err := ioutil.NewReadSeekCloser(sr)

	if err != nil {
		return nil, err
	}

	return fh, nil
}

// ReadURI implements the whosonfirst/go-reader interface so that the database itself can be used as a
// reader.Reader instance
func (r *MysqlSpatialDatabase) ReaderURI(ctx context.Context, str_uri string) string {
	return str_uri
}

// Write implements the whosonfirst/go-writer interface so that the database itself can be used as a
// writer.Writer instance (by invoking the `IndexFeature` method).
func (r *MysqlSpatialDatabase) Write(ctx context.Context, key string, fh io.ReadSeeker) (int64, error) {

	body, err := io.ReadAll(fh)

	if err != nil {
		return 0, err
	}

	err = r.IndexFeature(ctx, body)

	if err != nil {
		return 0, err
	}

	return int64(len(body)), nil
}

// WriterURI implements the whosonfirst/go-writer interface so that the database itself can be used as a
// writer.Writer instance
func (r *MysqlSpatialDatabase) WriterURI(ctx context.Context, str_uri string) string {
	return str_uri
}

// Flush implements the whosonfirst/go-writer interface so that the database itself can be used as a
// writer.Writer instance. This method is a no-op and simply returns `nil`.
func (r *MysqlSpatialDatabase) Flush(ctx context.Context) error {
	return nil
}

// Close implements the whosonfirst/go-writer interface so that the database itself can be used as a
// writer.Writer instance. This method is a no-op and simply returns `nil`.
func (r *MysqlSpatialDatabase) Close(ctx context.Context) error {
	return nil
}

// SetLogger implements the whosonfirst/go-writer interface so that the database itself can be used as a
// writer.Writer instance. This method is a no-op and simply returns `nil`.
func (r *MysqlSpatialDatabase) SetLogger(ctx context.Context, logger *log.Logger) error {
	return nil
}
