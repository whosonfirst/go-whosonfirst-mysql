package iterwriter

import (
	"context"
	"flag"
	"fmt"
	"github.com/sfomuseum/go-flags/flagset"
	"github.com/sfomuseum/go-flags/multi"
	"github.com/whosonfirst/go-whosonfirst-iterwriter"
	"github.com/whosonfirst/go-writer/v2"
	"log"
)

var writer_uris multi.MultiCSVString
var iterator_uri string

func DefaultFlagSet() *flag.FlagSet {

	fs := flagset.NewFlagSet("es")

	fs.Var(&writer_uris, "writer-uri", "")
	fs.StringVar(&iterator_uri, "iterator-uri", "repo://", "")

	return fs
}

func Run(ctx context.Context, logger *log.Logger) error {
	fs := DefaultFlagSet()
	return RunWithFlagSet(ctx, fs, logger)
}

func RunWithFlagSet(ctx context.Context, fs *flag.FlagSet, logger *log.Logger) error {

	flagset.Parse(fs)

	iterator_paths := fs.Args()

	writers := make([]writer.Writer, len(writer_uris))

	for idx, wr_uri := range writer_uris {

		wr, err := writer.NewWriter(ctx, wr_uri)

		if err != nil {

			return fmt.Errorf("Failed to create new writer for %s, %w", wr_uri, err)
		}

		writers[idx] = wr
	}

	mw := writer.NewMultiWriter(writers...)

	err := iterwriter.IterateWithWriter(ctx, mw, iterator_uri, iterator_paths...)

	if err != nil {
		return fmt.Errorf("Failed to iterate with writer, %w", err)
	}

	return nil
}
