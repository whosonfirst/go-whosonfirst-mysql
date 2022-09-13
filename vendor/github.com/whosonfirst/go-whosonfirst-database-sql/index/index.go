package index

import (
	"context"
	"fmt"
	"github.com/sfomuseum/go-timings"
	"github.com/whosonfirst/go-whosonfirst-database-sql"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/iterator"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"io"
	"log"
	"os"
)

type IndexTablesOptions struct {
	Database sql.Database
	Tables   []sql.Table
	Logger   *log.Logger
	Timings bool
}

func IndexTables(ctx context.Context, opts *IndexTablesOptions, iterator_uri string, to_iterate ...string) error {

	var monitor timings.Monitor
	
	iter_cb := func(ctx context.Context, path string, r io.ReadSeeker, args ...interface{}) error {

		_, uri_args, err := uri.ParseURI(path)

		if err != nil {
			return fmt.Errorf("Failed to parse URI for %s, %w", path, err)
		}

		body, err := io.ReadAll(r)

		if err != nil {
			return fmt.Errorf("Failed to read body for %s, %w", path, err)
		}

		opts.Database.Lock()
		defer opts.Database.Unlock()

		var alt *uri.AltGeom

		if uri_args.IsAlternate {
			alt = uri_args.AltGeom
		}

		err = opts.Database.IndexFeature(ctx, opts.Tables, body, alt)

		if err != nil {
			return fmt.Errorf("Failed to index %s, %w", path, err)
		}

		if opts.Timings {
			go monitor.Signal(ctx)
		}
		
		return nil
	}
	
	iter, err := iterator.NewIterator(ctx, iterator_uri, iter_cb)

	if err != nil {
		return fmt.Errorf("Failed to create new iterator because: %s", err)
	}

	if opts.Timings {
		
		m, err := timings.NewMonitor(ctx, "counter://PT60S")
		
		if err != nil {
			return fmt.Errorf("Failed to create timings monitor, %w", err)
		}

		monitor = m
		
		monitor.Start(ctx, os.Stdout)
		defer monitor.Stop(ctx)
	}
	
	err = iter.IterateURIs(ctx, to_iterate...)

	if err != nil {
		return fmt.Errorf("Failed to index paths in %s mode because: %s", iterator_uri, err)
	}

	return nil
}
