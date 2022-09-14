package main

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/whosonfirst/go-whosonfirst-iterate-git/v2"
	_ "github.com/whosonfirst/go-whosonfirst-mysql/writer"
)

import (
	"context"
	"github.com/whosonfirst/go-whosonfirst-iterwriter/application/iterwriter"
	"log"
)

func main() {

	ctx := context.Background()
	logger := log.Default()

	err := iterwriter.Run(ctx, logger)

	if err != nil {
		logger.Fatalf("Failed to iterate, %v", err)
	}

}
