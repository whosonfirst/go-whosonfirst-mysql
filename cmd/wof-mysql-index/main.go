package main

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/whosonfirst/go-whosonfirst-iterate-git/v2"
	_ "github.com/whosonfirst/go-whosonfirst-mysql/writer"
)

import (
	"context"
	"log"
	"log/slog"
	
	"github.com/whosonfirst/go-whosonfirst-iterwriter/app/iterwriter"
)

func main() {

	ctx := context.Background()
	logger := slog.Default()

	err := iterwriter.Run(ctx, logger)

	if err != nil {
		log.Fatalf("Failed to iterate, %v", err)
	}

}
