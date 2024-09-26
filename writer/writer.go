package writer

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/url"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	
	wof_sql "github.com/whosonfirst/go-whosonfirst-database-sql"
	"github.com/whosonfirst/go-whosonfirst-mysql/tables"
	wof_writer "github.com/whosonfirst/go-writer/v3"	
)

func init() {
	ctx := context.Background()
	wof_writer.RegisterWriter(ctx, "mysql", NewMySQLWriter)
}

type MySQLWriter struct {
	wof_writer.Writer
	db     wof_sql.Database
	tables []wof_sql.Table
}

func NewMySQLWriter(ctx context.Context, uri string) (wof_writer.Writer, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	q := u.Query()

	dsn := q.Get("dsn")
	enc_dsn := url.QueryEscape(dsn)

	db_uri := fmt.Sprintf("mysql://?dsn=%s", enc_dsn)

	db, err := wof_sql.NewSQLDB(ctx, db_uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to create database, %w", err)
	}

	index_geojson := true
	index_whosonfirst := true

	if q.Get("geojson") != "" {

		index, err := strconv.ParseBool(q.Get("geojson"))

		if err != nil {
			return nil, fmt.Errorf("Failed to parse ?geojson= parameter, %w", err)
		}

		index_geojson = index
	}

	if q.Get("whosonfirst") != "" {

		index, err := strconv.ParseBool(q.Get("whosonfirst"))

		if err != nil {
			return nil, fmt.Errorf("Failed to parse ?whosonfirst= parameter, %w", err)
		}

		index_whosonfirst = index
	}

	to_index := make([]wof_sql.Table, 0)

	if index_geojson {

		t, err := tables.NewGeoJSONTableWithDatabase(ctx, db)

		if err != nil {
			return nil, fmt.Errorf("Failed to create GeoJSON table, %w", err)
		}

		to_index = append(to_index, t)
	}

	if index_whosonfirst {

		t, err := tables.NewWhosonfirstTableWithDatabase(ctx, db)

		if err != nil {
			return nil, fmt.Errorf("Failed to create Whosonfirst table, %w", err)
		}

		to_index = append(to_index, t)
	}

	wr := &MySQLWriter{
		db:     db,
		tables: to_index,
	}

	return wr, nil
}

func (wr *MySQLWriter) Write(ctx context.Context, path string, r io.ReadSeeker) (int64, error) {

	body, err := io.ReadAll(r)

	if err != nil {
		return 0, fmt.Errorf("Failed to read document, %w", err)
	}

	err = wr.db.IndexFeature(ctx, wr.tables, body)

	if err != nil {
		return 0, fmt.Errorf("Failed to index %s, %w", path, err)
	}

	return 0, nil
}

func (wr *MySQLWriter) WriterURI(ctx context.Context, uri string) string {
	return uri
}

func (wr *MySQLWriter) Flush(ctx context.Context) error {
	return nil
}

func (wr *MySQLWriter) Close(ctx context.Context) error {
	return nil
}

func (wr *MySQLWriter) SetLogger(ctx context.Context, logger *log.Logger) error {
	slog.Warn("MySQLWriter no longer supports SetLogger. Please use log/slog methods instead.")
	return nil
}
