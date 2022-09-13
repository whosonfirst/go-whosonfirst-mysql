# go-whosonfirst-mysql

Go package for working with Who's On First documents and MySQL databases and implementing the `whosonfirst/go-whosonfirst-database-sql` interfaces.

## Documentation

Documentation is incomplete at this time.

## Tables

### geojson

```
CREATE TABLE IF NOT EXISTS geojson (
      id BIGINT UNSIGNED,
      alt VARCHAR(255) NOT NULL,
      body LONGBLOB NOT NULL,
      lastmodified INT NOT NULL,
      UNIQUE KEY id_alt (id, alt),
      KEY lastmodified (lastmodified)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
```

### whosonfirst

```
CREATE TABLE IF NOT EXISTS %s (
      id BIGINT UNSIGNED PRIMARY KEY,
      properties JSON NOT NULL,
      geometry GEOMETRY NOT NULL,
      centroid POINT NOT NULL COMMENT 'This is not necessary a math centroid',
      lastmodified INT NOT NULL,
      parent_id BIGINT       GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(properties,'$."wof:parent_id"'))) VIRTUAL,
      placetype VARCHAR(64)  GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(properties,'$."wof:placetype"'))) VIRTUAL,
      is_current TINYINT     GENERATED ALWAYS AS (JSON_CONTAINS_PATH(properties, 'one', '$."mz:is_current"') AND JSON_UNQUOTE(JSON_EXTRACT(properties,'$."mz:is_current"'))) VIRTUAL,
      is_nullisland TINYINT  GENERATED ALWAYS AS (JSON_CONTAINS_PATH(properties, 'one', '$."mz:is_nullisland"') AND JSON_LENGTH(JSON_EXTRACT(properties, '$."mz:is_nullisland"'))) VIRTUAL,
      is_approximate TINYINT GENERATED ALWAYS AS (JSON_CONTAINS_PATH(properties, 'one', '$."mz:is_approximate"') AND JSON_LENGTH(JSON_EXTRACT(properties, '$."mz:is_approximate"'))) VIRTUAL,
      is_ceased TINYINT      GENERATED ALWAYS AS (JSON_CONTAINS_PATH(properties, 'one', '$."edtf:cessation"') AND JSON_UNQUOTE(JSON_EXTRACT(properties,'$."edtf:cessation"')) != "" AND JSON_UNQUOTE(JSON_EXTRACT(properties,'$."edtf:cessation"')) != "open" AND json_unquote(json_extract(properties,'$."edtf:cessation"')) != "uuuu") VIRTUAL,
      is_deprecated TINYINT  GENERATED ALWAYS AS (JSON_CONTAINS_PATH(properties, 'one', '$."edtf:deprecated"') AND JSON_UNQUOTE(JSON_EXTRACT(properties,'$."edtf:deprecated"')) != "" AND json_unquote(json_extract(properties,'$."edtf:deprecated"')) != "uuuu") VIRTUAL,
      is_superseded TINYINT  GENERATED ALWAYS AS (JSON_LENGTH(JSON_EXTRACT(properties, '$."wof:superseded_by"')) > 0) VIRTUAL,
      is_superseding TINYINT GENERATED ALWAYS AS (JSON_LENGTH(JSON_EXTRACT(properties, '$."wof:supersedes"')) > 0) VIRTUAL,
      date_upper DATE	     GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(properties, '$."date:cessation_upper"'))) VIRTUAL,
      date_lower DATE	     GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(properties, '$."date:inception_lower"'))) VIRTUAL,
      KEY parent_id (parent_id),
      KEY placetype (placetype),
      KEY is_current (is_current),
      KEY is_nullisland (is_nullisland),
      KEY is_approximate (is_approximate),
      KEY is_deprecated (is_deprecated),
      KEY is_superseded (is_superseded),
      KEY is_superseding (is_superseding),
      KEY date_upper (date_upper),
      KEY date_lower (date_lower),
      SPATIAL KEY idx_geometry (geometry),
      SPATIAL KEY idx_centroid (centroid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
```

There are a few important things to note about the `whosonfirst` table:

1. It is technically possible to add VIRTUAL centroid along the lines of `centroid POINT GENERATED ALWAYS AS (ST_Centroid(geometry)) VIRTUAL` we don't because MySQL will return the math centroid and well we all know what that means for places like San Francisco (SF) - if you don't it means the [math centroid will be in the Pacific Ocean](https://spelunker.whosonfirst.org/id/85922583/) because technically the Farralon Islands are part of SF - so instead we we compute the centroid in the code (using the go-whosonfirst-geojson-v2 Centroid interface)
2. It's almost certainly going to be moved in to a different package (once this code base is reconciled with the `go-whosonfirst-sqlite` packages)
3. It is now a _third_ way to "spatially" store WOF records, along with the [go-whosonfirst-sqlite-features `geometries`](https://github.com/whosonfirst/go-whosonfirst-sqlite-features#geometries) and the [go-whosonfirst-spatialite-geojson geojson](https://github.com/whosonfirst/go-whosonfirst-spatialite-geojson#geojson) tables. It is entirely possible that this is "just how it is" and there is no value in a single unified table schema but, equally, it seems like it's something to have a think about.

## Custom tables

Sure. You just need to write a per-table package that implements the `Table` interface, described above.

## Tools

### wof-mysql-index 

```
$> ./bin/wof-mysql-index -h
  -all
    	Index all the tables
  -database-uri string
    	A URI in the form of 'mysql://?dsn={DSN}'
  -geojson
    	Index the 'geojson' table
  -iterator-uri string
    	A valid whosonfirst/go-whosonfirst-iterate/v2 URI (default "repo://")
  -timings
    	Enable timings during indexing
  -whosonfirst
    	Index the 'whosonfirst' table
```

For example:

```
./bin/wof-mysql-index \
	-all \
	-database-uri 'mysql://?dsn={USER}:{PASSWORD}@/{DATABASE}' \
	/usr/local/data/whosonfirst-data/
```

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

## Docker

* https://dev.mysql.com/doc/mysql-installation-excerpt/8.0/en/docker-mysql-getting-started.html

## See also:

* https://github.com/whosonfirst/go-whosonfirst-database-sql
* https://github.com/go-sql-driver/mysql#dsn-data-source-name

* https://dev.mysql.com/doc/refman/5.7/en/spatial-analysis-functions.html
* https://dev.mysql.com/doc/refman/8.0/en/json-functions.html
* https://www.percona.com/blog/2016/03/07/json-document-fast-lookup-with-mysql-5-7/
* https://archive.fosdem.org/2016/schedule/event/mysql57_json/attachments/slides/1291/export/events/attachments/mysql57_json/slides/1291/MySQL_57_JSON.pdf
