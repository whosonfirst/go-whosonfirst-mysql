# go-whosonfirst-mysql

Go package for working with Who's On First documents and MySQL databases and implementing the `whosonfirst/go-whosonfirst-database-sql` interfaces.

## Documentation

[![Go Reference](https://pkg.go.dev/badge/github.com/whosonfirst/go-whosonfirst-mysql.svg)](https://pkg.go.dev/github.com/whosonfirst/go-whosonfirst-mysql)

Documentation is incomplete at this time.

## Tools

To build binary versions of these tools run the `cli` Makefile target. For example:

```
$> make cli
go build -mod vendor -o bin/wof-mysql-index cmd/wof-mysql-index/main.go
```

### wof-mysql-index

```
$> ./bin/wof-mysql-index -h
  -iterator-uri string
    	A valid whosonfirst/go-whosonfirst-iterate/v2 URI. (default "repo://")
  -monitor-uri string
    	A valid sfomuseum/go-timings URI. (default "counter://PT60S")
  -writer-uri value
    	One or more valid whosonfirst/go-writer/v2 URIs, each encoded as a gocloud.dev/runtimevar URI.
```

For example, assuming a `whosonfirst/go-writer/v2` URI of "mysql://?dsn={USER}:{PASS}@/{DATABASE}":

```
$> bin/wof-mysql-index \
   	-writer-uri 'constant://?val=mysql%3A%2F%2F%3Fdsn%3D%7BUSER%7D%3A%7BPASS%7D%40%2F%7BDATABASE%7D'
	/usr/local/data/whosonfirst-data-admin-ca
```

This command will publish (or write) all the records in `whosonfirst-data-admin-ca` to the `{DATABASE}` MySQL database.

The `es-whosonfirst-index` tool is a thin wrapper around the `iterwriter` tool which is provided by the [whosonfirst/go-whosonfirst-iterwriter](https://github.com/whosonfirst/go-whosonfirst-iterwriter) package.

The need to URL-encode `whosonfirst/go-writer/v2` URIs is unfortunate but is tolerated since the use to `gocloud.dev/runtimevar` URIs provides a means to keep secrets and other sensitive values out of command-line arguments (and by extension process lists).

By default both the `geojson` and `whosonfirst` tables (described below) are indexed. To disable this behaviour include the `?geojson=false` or `?whosonfirst-false` parameters in the `whosonfirst/go-writer/v2` URI.

If you are indexing large WOF records (like countries) you should make sure to append the `?maxAllowedPacket=0` query string to your DSN. Per [the documentation](https://github.com/go-sql-driver/mysql#maxallowedpacket) this will "automatically fetch the max_allowed_packet variable from server on every connection". Or you could pass it a value larger than the default (in `go-mysql`) 4MB. You may also need to set the `max_allowed_packets` setting your MySQL daemon config file. Check [the documentation](https://dev.mysql.com/doc/refman/8.0/en/packet-too-large.html) for details.

### Environment variables

You can set (or override) command line flags with environment variables. Environment variable are expected to:

* Be upper-cased
* Replace all instances of `-` with `_`
* Be prefixed with `WOF_MYSQL`

For example the `-database-uri` flag would be overridden by the `WOF_DATABASE_URI` environment variable.

## Writers

The `writer` package implements the [whosonfirst/go-writer/v2](https://github.com/whosonfirst/go-writer) interfaces. For example:

```
import (
       "github.com/whosonfirst/go-writer/v2"
       _ "github.com/whosonfirst/go-whosonfirst-mysql/writer"
)

ctx := context.Background()

wr_uri := "mysql:///?dsn={USER}:{PASSWORD}@/{DATABASE}&geojson=true&whosonfirst=true"
wr, _ := writer.NewWriter(ctx, wr_uri)
```

## Tables

### geojson

The `geojson` table is used to index the body of a Who's On First feature keyed by its unique ID. The complete schema for the table is here:

* [tables/geojson.schema](tables/geojson.schema)

### whosonfirst

The `whosonfirst` table is used to index the body of a Who's On First feature keyed by its unique ID with additional columns and indexes for performing SPR and spatial queries. The complete schema for the table is here:

* [tables/whosonfirst.schema](tables/whosonfirst.schema)

There are a few important things to note about the `whosonfirst` table:

1. It is technically possible to add VIRTUAL centroid along the lines of `centroid POINT GENERATED ALWAYS AS (ST_Centroid(geometry)) VIRTUAL` we don't because MySQL will return the math centroid and well we all know what that means for places like San Francisco (SF) - if you don't it means the [math centroid will be in the Pacific Ocean](https://spelunker.whosonfirst.org/id/85922583/) because technically the Farralon Islands are part of SF - so instead we we compute the centroid in the code (using the go-whosonfirst-geojson-v2 Centroid interface)
2. It's almost certainly going to be moved in to a different package (once this code base is reconciled with the `go-whosonfirst-sqlite` packages)
3. It is now a _third_ way to "spatially" store WOF records, along with the [go-whosonfirst-sqlite-features `geometries`](https://github.com/whosonfirst/go-whosonfirst-sqlite-features#geometries) and the [go-whosonfirst-spatialite-geojson geojson](https://github.com/whosonfirst/go-whosonfirst-spatialite-geojson#geojson) tables. It is entirely possible that this is "just how it is" and there is no value in a single unified table schema but, equally, it seems like it's something to have a think about.

## Custom tables

Sure. You just need to write a per-table package that implements the `Table` interface, described above.

## See also:

* https://github.com/whosonfirst/go-whosonfirst-database-sql
* https://github.com/whosonfirst/go-whosonfirst-iterate
* https://github.com/whosonfirst/go-whosonfirst-writer
* https://github.com/whosonfirst/go-whosonfirst-iterwriter

* https://github.com/go-sql-driver/mysql#dsn-data-source-name
* https://dev.mysql.com/doc/refman/5.7/en/spatial-analysis-functions.html
* https://dev.mysql.com/doc/refman/8.0/en/json-functions.html
* https://www.percona.com/blog/2016/03/07/json-document-fast-lookup-with-mysql-5-7/
* https://archive.fosdem.org/2016/schedule/event/mysql57_json/attachments/slides/1291/export/events/attachments/mysql57_json/slides/1291/MySQL_57_JSON.pdf
