package main

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/whosonfirst/go-whosonfirst-iterate-git/v2"
)

import (
	"context"
	"github.com/sfomuseum/go-flags/flagset"
	"github.com/whosonfirst/go-whosonfirst-database-sql"
	"github.com/whosonfirst/go-whosonfirst-database-sql/index"	
	"github.com/whosonfirst/go-whosonfirst-mysql/tables"
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

	timings := fs.Bool("timings", false, "Enable timings")
	
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

	index_opts := &index.IndexTablesOptions{
		Database: db,
		Tables: to_index,
		Logger: logger,
		Timings: *timings,
	}

	to_iterate := fs.Args()
	
	err = index.IndexTables(ctx, index_opts, *iterator_uri, to_iterate...)
	
	if err != nil {
		logger.Fatalf("Failed to index paths in %s mode because: %s", *iterator_uri, err)
	}

	os.Exit(0)
}
