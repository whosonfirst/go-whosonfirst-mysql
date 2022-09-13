package main

import (
	_ "github.com/go-sql-driver/mysql"
)

import (
	"context"
	"github.com/sfomuseum/go-flags/flagset"
	"github.com/whosonfirst/go-whosonfirst-database-sql"
	"github.com/whosonfirst/go-whosonfirst-database-sql/prune"
	"github.com/whosonfirst/go-whosonfirst-mysql/tables"
	"log"
	"os"
)

func main() {

	fs := flagset.NewFlagSet("purge")

	database_uri := fs.String("database-uri", "", "")

	iterator_uri := fs.String("iterator-uri", "", "A valid whosonfirst/go-whosonfirst-iterate/v2 URI to determine records to purge.")
	iterator_source := fs.String("iterator-source", "", "A valid URI to iterator records with")

	purge_geojson := fs.Bool("geojson", false, "Purge the 'geojson' tables")
	purge_whosonfirst := fs.Bool("whosonfirst", false, "Purge the 'whosonfirst' tables")
	purge_all := fs.Bool("all", false, "Purge all the tables")

	flagset.Parse(fs)

	ctx := context.Background()
	logger := log.Default()

	err := flagset.SetFlagsFromEnvVars(fs, "WOF")

	if err != nil {
		logger.Fatalf("Failed to set flags from environment variables, %v", err)
	}

	db, err := sql.NewSQLDB(ctx, *database_uri)

	if err != nil {
		logger.Fatalf("unable to create database because %v", err)
	}

	defer db.Close()

	to_purge := make([]sql.Table, 0)

	if *purge_geojson || *purge_all {

		tbl, err := tables.NewGeoJSONTable(ctx)

		if err != nil {
			logger.Fatalf("failed to create 'geojson' table because %s", err)
		}

		to_purge = append(to_purge, tbl)
	}

	if *purge_whosonfirst || *purge_all {

		tbl, err := tables.NewWhosonfirstTable(ctx)

		if err != nil {
			logger.Fatalf("failed to create 'whosonfirst' table because %s", err)
		}

		to_purge = append(to_purge, tbl)
	}

	if len(to_purge) == 0 {
		logger.Fatalf("You forgot to specify which (any) tables to purge")
	}

	if *iterator_uri != "" {

		err := prune.PruneTablesWithIterator(ctx, *iterator_uri, *iterator_source, db, to_purge...)

		if err != nil {
			logger.Fatalf("Failed to prune tables with iterator, %v", err)
		}

	} else {

		err := prune.PruneTables(ctx, db, to_purge...)

		if err != nil {
			logger.Fatalf("Failed to prune tables, %v", err)
		}

	}
	os.Exit(0)
}
