package main

import (
	_ "github.com/whosonfirst/go-whosonfirst-iterate-git/v2"
)

import (
	"context"
	"github.com/sfomuseum/go-flags/flagset"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/iterator"
	"github.com/whosonfirst/go-whosonfirst-mysql"
	"github.com/whosonfirst/go-whosonfirst-mysql/database"
	"github.com/whosonfirst/go-whosonfirst-mysql/tables"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {

	fs := flagset.NewFlagSet("index")

	database_uri := fs.String("database-uri", "", "")

	iterator_uri := fs.String("iterator-uri", "repo://", "A valid whosonfirst/go-whosonfirst-iterate/v2 URI")

	index_geojson := fs.Bool("geojson", false, "Index the 'geojson' tables")
	index_whosonfirst := fs.Bool("whosonfirst", false, "Index the 'whosonfirst' tables")
	index_all := fs.Bool("all", false, "Index all the tables")

	timings := fs.Bool("timings", false, "Display timings during and after indexing")

	flagset.Parse(fs)

	ctx := context.Background()
	logger := log.Default()

	err := flagset.SetFlagsFromEnvVars(fs, "WOF")

	if err != nil {
		logger.Fatalf("Failed to set flags from environment variables, %v", err)
	}

	db, err := database.NewDB(ctx, *database_uri)

	if err != nil {
		logger.Fatalf("unable to create database because %v", err)
	}

	defer db.Close()

	to_index := make([]mysql.Table, 0)

	if *index_geojson || *index_all {

		tbl, err := tables.NewGeoJSONTableWithDatabase(db)

		if err != nil {
			logger.Fatalf("failed to create 'geojson' table because %s", err)
		}

		to_index = append(to_index, tbl)
	}

	if *index_whosonfirst || *index_all {

		tbl, err := tables.NewWhosonfirstTableWithDatabase(db)

		if err != nil {
			logger.Fatalf("failed to create 'whosonfirst' table because %s", err)
		}

		to_index = append(to_index, tbl)
	}

	if len(to_index) == 0 {
		logger.Fatalf("You forgot to specify which (any) tables to index")
	}

	table_timings := make(map[string]time.Duration)
	mu := new(sync.RWMutex)

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

		for _, t := range to_index {

			t1 := time.Now()

			err = t.IndexFeature(db, body, uri_args.IsAlternate)

			if err != nil {
				logger.Printf("Failed to index feature (%s) in '%s' table because %s", path, t.Name(), err)
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

	iter, err := iterator.NewIterator(ctx, *iterator_uri, iter_cb)

	if err != nil {
		logger.Fatalf("Failed to create new iterator because: %s", err)
	}

	done_ch := make(chan bool)
	t1 := time.Now()

	show_timings := func() {

		t2 := time.Since(t1)

		i := atomic.LoadInt64(&iter.Seen) // please just make this part of go-whosonfirst-index

		mu.RLock()
		defer mu.RUnlock()

		for t, d := range table_timings {
			logger.Printf("Time to index %s (%d) : %v", t, i, d)
		}

		logger.Printf("Time to index all (%d) : %v", i, t2)
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

	to_iterate := fs.Args()
	err = iter.IterateURIs(ctx, to_iterate...)

	if err != nil {
		logger.Fatalf("Failed to index paths in %s mode because: %s", *iterator_uri, err)
	}

	os.Exit(0)
}
