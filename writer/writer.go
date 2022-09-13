package writer

import (
	"context"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-mysql"
	"github.com/whosonfirst/go-whosonfirst-mysql/database"
	"github.com/whosonfirst/go-whosonfirst-mysql/tables"
	wof_writer "github.com/whosonfirst/go-writer/v2"
	"io"
	"log"
	"net/url"
)

func init() {
	ctx := context.Background()
	wof_writer.RegisterWriter(ctx, "mysql", NewMySQLWriter)
}

type MySQLWriter struct {
	wof_writer.Writer
	db     *database.MySQLDatabase
	tables []mysql.Table
	logger *log.Logger
}

func NewMySQLWriter(ctx context.Context, uri string) (wof_writer.Writer, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	q := u.Query()
	dsn := q.Get("dsn")

	db, err := database.NewDBWithDSN(ctx, dsn)

	if err != nil {
		return nil, fmt.Errorf("Failed to create database, %w", err)
	}

	to_index := make([]mysql.Table, 0)

	if q.Get("geojson") != "" {

		t, err := tables.NewGeoJSONTableWithDatabase(db)

		if err != nil {
			return nil, fmt.Errorf("Failed to create GeoJSON table, %w", err)
		}

		to_index = append(to_index, t)
	}

	if q.Get("geojson") != "" {

		t, err := tables.NewWhosonfirstTableWithDatabase(db)

		if err != nil {
			return nil, fmt.Errorf("Failed to create Whosonfirst table, %w", err)
		}

		to_index = append(to_index, t)
	}

	logger := log.Default()

	wr := &MySQLWriter{
		db:     db,
		tables: to_index,
		logger: logger,
	}

	return wr, nil
}

func (wr *MySQLWriter) Write(ctx context.Context, path string, r io.ReadSeeker) (int64, error) {

	body, err := io.ReadAll(r)

	if err != nil {
		return 0, fmt.Errorf("Failed to read document, %w", err)
	}

	for _, t := range wr.tables {

		err := t.IndexFeature(wr.db, body)

		if err != nil {
			return 0, fmt.Errorf("Failed to index %s table, %w", t.Name(), err)
		}
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
	wr.logger = logger
	return nil
}
