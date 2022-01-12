package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-cli/flags"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/iterator"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-mysql"
	"github.com/whosonfirst/go-whosonfirst-mysql/database"
	"github.com/whosonfirst/go-whosonfirst-mysql/tables"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"io"
	"os"
)

func main() {

	config := flag.String("config", "", "Read some or all flags from an ini-style config file. Values in the config file take precedence over command line flags.")
	section := flag.String("section", "wof-mysql", "A valid ini-style config file section.")

	dsn := flag.String("dsn", "", "A valid go-sql-driver DSN string, for example '{USER}:{PASSWORD}@/{DATABASE}'")

	iterator_uri := flag.String("iterator-uri", "", "A valid whosonfirst/go-whosonfirst-iterate/v2 URI to determine records to purge.")

	purge_geojson := flag.Bool("geojson", false, "Purge the 'geojson' tables")
	purge_whosonfirst := flag.Bool("whosonfirst", false, "Purge the 'whosonfirst' tables")
	purge_all := flag.Bool("all", false, "Purge all the tables")

	flag.Parse()

	logger := log.SimpleWOFLogger()

	stdout := io.Writer(os.Stdout)
	logger.AddLogger(stdout, "status")

	if *config != "" {

		err := flags.SetFlagsFromConfig(*config, *section)

		if err != nil {
			logger.Fatal("Unable to set flags from config file because %s", err)
		}

	} else {

		err := flags.SetFlagsFromEnvVars("WOF_MYSQL")

		if err != nil {
			logger.Fatal("Unable to set flags from environment variables because %s", err)
		}
	}

	db, err := database.NewDB(*dsn)

	if err != nil {
		logger.Fatal("unable to create database (%s) because %s", *dsn, err)
	}

	defer db.Close()

	to_purge := make([]mysql.Table, 0)

	if *purge_geojson || *purge_all {

		tbl, err := tables.NewGeoJSONTable()

		if err != nil {
			logger.Fatal("failed to create 'geojson' table because %s", err)
		}

		to_purge = append(to_purge, tbl)
	}

	if *purge_whosonfirst || *purge_all {

		tbl, err := tables.NewWhosonfirstTable()

		if err != nil {
			logger.Fatal("failed to create 'whosonfirst' table because %s", err)
		}

		to_purge = append(to_purge, tbl)
	}

	if len(to_purge) == 0 {
		logger.Fatal("You forgot to specify which (any) tables to purge")
	}

	ctx := context.Background()

	conn, err := db.Conn()

	if err != nil {
		logger.Fatal("Failed to create DB conn, because %s", err)
	}

	if *iterator_uri != "" {

		uris := flag.Args()

		iter_cb := func(ctx context.Context, path string, r io.ReadSeeker, args ...interface{}) error {

			id, _, err := uri.ParseURI(path)

			if err != nil {
				return fmt.Errorf("Failed to parse %s, %w", path, err)
			}

			tx, err := conn.Begin()

			if err != nil {
				return fmt.Errorf("Failed create transaction, because %w", err)
			}

			for _, t := range to_purge {

				sql := fmt.Sprintf("DELETE FROM %s WHERE id = ?", t.Name())
				stmt, err := tx.Prepare(sql)

				if err != nil {
					return fmt.Errorf("Failed to prepare statement (%s), because %w", sql, err)
				}

				_, err = stmt.Exec(id)

				if err != nil {
					return fmt.Errorf("Failed to execute statement (%s, %d), because %w", sql, id, err)
				}
			}

			err = tx.Commit()

			if err != nil {
				fmt.Errorf("Failed to commit transaction to purge %d, because %w", id, err)
			}

			return nil
		}

		iter, err := iterator.NewIterator(ctx, *iterator_uri, iter_cb)

		if err != nil {
			logger.Fatal("Failed to create iterator, %v", err)
		}

		err = iter.IterateURIs(ctx, uris...)

		if err != nil {
			logger.Fatal("Failed to iterate URIs, %v", err)
		}

	} else {

		//

		tx, err := conn.Begin()

		if err != nil {
			logger.Fatal("Failed create transaction, because %s", err)
		}

		for _, t := range to_purge {

			sql := fmt.Sprintf("DELETE FROM %s", t.Name())
			stmt, err := tx.Prepare(sql)

			if err != nil {
				logger.Fatal("Failed to prepare statement (%s), because %s", sql, err)
			}

			_, err = stmt.Exec()

			if err != nil {
				logger.Fatal("Failed to execute statement (%s), because %s", sql, err)
			}
		}

		err = tx.Commit()

		if err != nil {
			logger.Fatal("Failed to commit transaction, because %s", err)
		}

	}

	os.Exit(0)
}
