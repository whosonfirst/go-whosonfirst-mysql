package main

import (
	_ "github.com/whosonfirst/go-whosonfirst-iterate-git"	
)

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-cli/flags"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/feature"
	"github.com/whosonfirst/go-whosonfirst-iterate/iterator"
	"github.com/whosonfirst/go-whosonfirst-iterate/emitter"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-mysql"
	"github.com/whosonfirst/go-whosonfirst-mysql/database"
	"github.com/whosonfirst/go-whosonfirst-mysql/tables"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"github.com/whosonfirst/warning"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {

	config := flag.String("config", "", "Read some or all flags from an ini-style config file. Values in the config file take precedence over command line flags.")
	section := flag.String("section", "wof-mysql", "A valid ini-style config file section.")

	dsn := flag.String("dsn", "", "A valid go-sql-driver DSN string, for example '{USER}:{PASSWORD}@/{DATABASE}'")
	mode := flag.String("mode", "repo://", "A valid whosonfirst/go-whosonfirst-iterate URI" )

	index_geojson := flag.Bool("geojson", false, "Index the 'geojson' tables")
	index_whosonfirst := flag.Bool("whosonfirst", false, "Index the 'whosonfirst' tables")
	index_all := flag.Bool("all", false, "Index all the tables")

	liberal := flag.Bool("liberal", false, "Parse records with (go-geojson-v2) feature.LoadGeoJSONFeatureFromReader rather than feature.LoadWOFFeatureFromReader")
	timings := flag.Bool("timings", false, "Display timings during and after indexing")

	flag.Parse()

	ctx := context.Background()
	
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

	to_index := make([]mysql.Table, 0)

	if *index_geojson || *index_all {

		tbl, err := tables.NewGeoJSONTableWithDatabase(db)

		if err != nil {
			logger.Fatal("failed to create 'geojson' table because %s", err)
		}

		to_index = append(to_index, tbl)
	}

	if *index_whosonfirst || *index_all {

		tbl, err := tables.NewWhosonfirstTableWithDatabase(db)

		if err != nil {
			logger.Fatal("failed to create 'whosonfirst' table because %s", err)
		}

		to_index = append(to_index, tbl)
	}

	if len(to_index) == 0 {
		logger.Fatal("You forgot to specify which (any) tables to index")
	}

	table_timings := make(map[string]time.Duration)
	mu := new(sync.RWMutex)

	iter_cb := func(ctx context.Context, fh io.ReadSeeker, args ...interface{}) error {

		path, err := emitter.PathForContext(ctx)

		if err != nil {
			return err
		}

		_, uri_args, err := uri.ParseURI(path)

		if err != nil {
			return err
		}
		

		var f geojson.Feature
		var alt *uri.AltGeom

		if !uri_args.IsAlternate {

			if *liberal {
				f, err = feature.LoadGeoJSONFeatureFromReader(fh)
			} else {
				f, err = feature.LoadWOFFeatureFromReader(fh)
			}

		} else {

			f, err = feature.LoadGeoJSONFeatureFromReader(fh)

			if err == nil {
				alt, err = uri.AltGeomFromPath(path)
			}
		}

		if err != nil {

			if err != nil && !warning.IsWarning(err) {
				msg := fmt.Sprintf("Unable to load %s, because %s", path, err)
				return errors.New(msg)
			}
		}

		db.Lock()
		defer db.Unlock()

		for _, t := range to_index {

			t1 := time.Now()

			err = t.IndexFeature(db, f, alt)

			if err != nil {
				logger.Warning("failed to index feature (%s) in '%s' table because %s", path, t.Name(), err)
				return nil
			}

			t2 := time.Since(t1)

			n := t.Name()

			mu.Lock()

			_, ok := table_timings[n]

			if ok {
				table_timings[n] += t2
			} else {
				table_timings[n] = t2
			}

			mu.Unlock()
		}

		return nil
	}

	iter, err := iterator.NewIterator(ctx, *mode, iter_cb)

	if err != nil {
		logger.Fatal("Failed to create new iterator because: %s", err)
	}

	done_ch := make(chan bool)
	t1 := time.Now()

	show_timings := func() {

		t2 := time.Since(t1)

		i := atomic.LoadInt64(&iter.Seen) // please just make this part of go-whosonfirst-index

		mu.RLock()
		defer mu.RUnlock()

		for t, d := range table_timings {
			logger.Status("time to index %s (%d) : %v", t, i, d)
		}

		logger.Status("time to index all (%d) : %v", i, t2)
	}

	if *timings {

		go func() {

			for {

				select {
				case <-done_ch:
					return
				case <-time.After(1 * time.Minute):
					show_timings()
				}
			}
		}()

		defer func() {
			done_ch <- true
		}()
	}

	to_iterate := flag.Args()
	err = iter.IterateURIs(ctx, to_iterate...)

	if err != nil {
		logger.Fatal("Failed to index paths in %s mode because: %s", *mode, err)
	}

	os.Exit(0)
}
