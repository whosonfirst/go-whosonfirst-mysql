package main

import (
	"flag"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-cli/flags"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-mysql"
	"github.com/whosonfirst/go-whosonfirst-mysql/database"
	"github.com/whosonfirst/go-whosonfirst-mysql/tables"
	"io"
	"os"
)

func main() {

	config := flag.String("config", "", "Read some or all flags from an ini-style config file. Values in the config file take precedence over command line flags.")
	section := flag.String("section", "wof-mysql", "A valid ini-style config file section.")

	dsn := flag.String("dsn", "", "A valid go-sql-driver DSN string, for example '{USER}:{PASSWORD}@/{DATABASE}'")

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

	conn, err := db.Conn()

	if err != nil {
		logger.Fatal("Failed to create DB conn, because %s", err)
	}

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

	os.Exit(0)
}
