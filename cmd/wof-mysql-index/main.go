package main

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/whosonfirst/go-whosonfirst-iterate-git/v2"
)

import (
	"context"
	"fmt"
	"github.com/sfomuseum/go-flags/flagset"
	"github.com/sfomuseum/go-timings"
	"github.com/whosonfirst/go-whosonfirst-database-sql"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/iterator"
	"github.com/whosonfirst/go-whosonfirst-mysql/tables"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"io"
	"log"
	"os"
)

func main() {

	fs := flagset.NewFlagSet("index")

	database_uri := fs.String("database-uri", "", "")

	iterator_uri := fs.String("iterator-uri", "repo://", "A valid whosonfirst/go-whosonfirst-iterate/v2 URI")

	index_geojson := fs.Bool("geojson", false, "Index the 'geojson' tables")
	index_whosonfirst := fs.Bool("whosonfirst", false, "Index the 'whosonfirst' tables")
	index_all := fs.Bool("all", false, "Index all the tables")

	flagset.Parse(fs)

	ctx := context.Background()
	logger := log.Default()

	err := flagset.SetFlagsFromEnvVars(fs, "WOF")

	if err != nil {
		logger.Fatalf("Failed to set flags from environment variables, %v", err)
	}

	monitor, err := timings.NewMonitor(ctx, "counter://PT60S")

	if err != nil {
		logger.Fatalf("Failed to create timings monitor, %w", err)
	}

	db, err := sql.NewSQLDB(ctx, *database_uri)

	if err != nil {
		logger.Fatalf("unable to create database because %v", err)
	}

	defer db.Close()

	to_index := make([]sql.Table, 0)

	if *index_geojson || *index_all {

		tbl, err := tables.NewGeoJSONTableWithDatabase(ctx, db)

		if err != nil {
			logger.Fatalf("failed to create 'geojson' table because %s", err)
		}

		to_index = append(to_index, tbl)
	}

	if *index_whosonfirst || *index_all {

		tbl, err := tables.NewWhosonfirstTableWithDatabase(ctx, db)

		if err != nil {
			logger.Fatalf("failed to create 'whosonfirst' table because %s", err)
		}

		to_index = append(to_index, tbl)
	}

	if len(to_index) == 0 {
		logger.Fatalf("You forgot to specify which (any) tables to index")
	}

	iter_cb := func(ctx context.Context, path string, fh io.ReadSeeker, args ...interface{}) error {

		_, uri_args, err := uri.ParseURI(path)

		if err != nil {
			return err
		}

		body, err := io.ReadAll(fh)

		if err != nil {
			return err
		}

		db.Lock()
		defer db.Unlock()

		var alt *uri.AltGeom

		if uri_args.IsAlternate {
			alt = uri_args.AltGeom
		}
		
		err = db.IndexFeature(ctx, to_index, body, alt)

		if err != nil {
			return fmt.Errorf("Failed to index %s, %w", path, err)
		}

		go monitor.Signal(ctx)
		return nil
	}

	iter, err := iterator.NewIterator(ctx, *iterator_uri, iter_cb)

	if err != nil {
		logger.Fatalf("Failed to create new iterator because: %s", err)
	}

	monitor.Start(ctx, os.Stdout)
	defer monitor.Stop(ctx)

	to_iterate := fs.Args()
	err = iter.IterateURIs(ctx, to_iterate...)

	if err != nil {
		logger.Fatalf("Failed to index paths in %s mode because: %s", *iterator_uri, err)
	}

	os.Exit(0)
}
