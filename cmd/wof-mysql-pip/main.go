package main

import (
	_ "github.com/whosonfirst/go-whosonfirst-mysql/spatial"
)

import (
	"context"
	"github.com/whosonfirst/go-whosonfirst-spatial/application/pointinpolygon"
	"log"
)

func main() {

	ctx := context.Background()
	logger := log.Default()

	err := pointinpolygon.Run(ctx, logger)

	if err != nil {
		logger.Fatalf("Failed to run point in polygon application, %w", err)
	}
}
