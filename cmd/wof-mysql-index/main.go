package main

import (
	"context"
	"log"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/whosonfirst/go-whosonfirst-iterate-git/v3"
	_ "github.com/whosonfirst/go-whosonfirst-mysql/writer"
	
	"github.com/whosonfirst/go-whosonfirst-iterwriter/v4/app/iterwriter"
)

func main() {

	ctx := context.Background()
	err := iterwriter.Run(ctx)

	if err != nil {
		log.Fatalf("Failed to iterate, %v", err)
	}

}
