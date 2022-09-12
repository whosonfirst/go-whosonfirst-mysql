package main

import (
	"context"
	"flag"
	"github.com/whosonfirst/go-whosonfirst-cli/flags"
	"github.com/whosonfirst/go-whosonfirst-mysql"
	"github.com/whosonfirst/go-whosonfirst-mysql/database"
	"github.com/whosonfirst/go-whosonfirst-mysql/prune"
	"github.com/whosonfirst/go-whosonfirst-mysql/tables"
	"log"
	"os"
)

func main() {

	config := flag.String("config", "", "Read some or all flags from an ini-style config file. Values in the config file take precedence over command line flags.")
	section := flag.String("section", "wof-mysql", "A valid ini-style config file section.")

	dsn := flag.String("dsn", "", "A valid go-sql-driver DSN string, for example '{USER}:{PASSWORD}@/{DATABASE}'")

	iterator_uri := flag.String("iterator-uri", "", "A valid whosonfirst/go-whosonfirst-iterate/v2 URI to determine records to purge.")
	iterator_source := flag.String("iterator-source", "", "A valid URI to iterator records with")

	purge_geojson := flag.Bool("geojson", false, "Purge the 'geojson' tables")
	purge_whosonfirst := flag.Bool("whosonfirst", false, "Purge the 'whosonfirst' tables")
	purge_all := flag.Bool("all", false, "Purge all the tables")

	flag.Parse()

	ctx := context.Background()
	logger := log.Default()

	if *config != "" {

		err := flags.SetFlagsFromConfig(*config, *section)

		if err != nil {
			logger.Fatalf("Unable to set flags from config file because %s", err)
		}

	} else {

		err := flags.SetFlagsFromEnvVars("WOF_MYSQL")

		if err != nil {
			logger.Fatalf("Unable to set flags from environment variables because %s", err)
		}
	}

	db, err := database.NewDB(*dsn)

	if err != nil {
		logger.Fatalf("unable to create database (%s) because %s", *dsn, err)
	}

	defer db.Close()

	to_purge := make([]mysql.Table, 0)

	if *purge_geojson || *purge_all {

		tbl, err := tables.NewGeoJSONTable()

		if err != nil {
			logger.Fatalf("failed to create 'geojson' table because %s", err)
		}

		to_purge = append(to_purge, tbl)
	}

	if *purge_whosonfirst || *purge_all {

		tbl, err := tables.NewWhosonfirstTable()

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
