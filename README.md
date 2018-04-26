# go-whosonfirst-mysql

Go package for working with Who's On First documents and MySQL databases.

## Install

You will need to have both `Go` (specifically a version of Go more recent than 1.6 so let's just assume you need [Go 1.8](https://golang.org/dl/) or higher) and the `make` programs installed on your computer. Assuming you do just type:

```
make bin
```

All of this package's dependencies are bundled with the code in the `vendor` directory.

## A few things before we get started

1. This package assumes you are running a version of [MySQL](https://dev.mysql.com/doc/refman/5.7/en/spatial-analysis-functions.html) (or [MariaDB](https://mariadb.com/kb/en/library/geographic-geometric-features/)) with spatial extensions, so version 5.7 or higher.
2. This package assumes Who's On First documents and is not yet able to index arbitrary GeoJSON documents.
3. This package shares the same basic model as the [go-whosonfirst-sqlite-*](https://github.com/whosonfirst?utf8=%E2%9C%93&q=go-whosonfirst-sqlite&type=&language=) packages. They should be reconciled. Today, they are not.
4. This is not an abstract package for working with databases and tables that aren't Who's On First specific, the way [go-whosonfirst-sqlite](https://github.com/whosonfirst/go-whosonfirst-sqlite) is. It probably _should_ be but that seems like something that will happen as a result of doing #3 (above). 

## Interfaces

### Database

```
type Database interface {
     Conn() (*sql.DB, error)
     DSN() string
     Close() error
}
```

### Table

```
type Table interface {
     Name() string
     Schema() string
     InitializeTable(Database) error
     IndexRecord(Database, interface{}) error
}
```

It is left up to people implementing the `Table` interface to figure out what to do with the second value passed to the `IndexRecord` method. For example:

```
func (t *WhosonfirstTable) IndexRecord(db mysql.Database, i interface{}) error {
	return t.IndexFeature(db, i.(geojson.Feature))
}

func (t *WhosonfirstTable) IndexFeature(db mysql.Database, f geojson.Feature) error {
	// code to index geojson.Feature here
}
```

## Tables

### whosonfirst

```
CREATE TABLE IF NOT EXISTS %s (
      id BIGINT UNSIGNED PRIMARY KEY,
      name VARCHAR(255) DEFAULT NULL,
      country CHAR(2) NOT NULL,
      placetype VARCHAR(24) NOT NULL,
      parent_id BIGINT NOT NULL COMMENT 'this can not be unsigned because you know -1, -2 and so on...',
      is_current TINYINT NOT NULL COMMENT 'also not unsigned because -1',
      is_deprecated TINYINT NOT NULL COMMENT 'also not unsigned because -1',
      is_ceased TINYINT NOT NULL COMMENT 'also not unsigned because -1',
      geometry GEOMETRY NOT NULL,
      centroid POINT NOT NULL,
      lastmodified INT NOT NULL,
      SPATIAL KEY %s_geometry (geometry),
      SPATIAL KEY %s_centroid (centroid),
      FULLTEXT (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
```

There are a few important things to note about the `whosonfirst` table:

1. It's almost certainly going to be moved in to a different package (once this code base is reconciled with the `go-whosonfirst-sqlite` packages)
2. It does _not_ store all the necessary fields to return a [standard places response](https://github.com/whosonfirst/go-whosonfirst-spr) (SPR) yet
3. It is now a _third_ way to "spatially" store WOF records, along with the [go-whosonfirst-sqlite-features `geometries`](https://github.com/whosonfirst/go-whosonfirst-sqlite-features#geometries) and the [go-whosonfirst-spatialite-geojson geojson](https://github.com/whosonfirst/go-whosonfirst-spatialite-geojson#geojson) tables. It is entirely possible that this is "just how it is" and there is no value in a single unified table schema but, equally, it seems like it's something to have a think about.

## Custom tables

Sure. You just need to write a per-table package that implements the `Table` interface, described above.

## Tools

### wof-mysql-index 

```
./bin/wof-mysql-index -h
Usage of ./bin/wof-mysql-index:
  -dsn string
       A valid go-sql-driver DSN string, for example '{USER}:{PASSWORD}@/{DATABASE}'
  -mode string
    	The mode to use importing data. Valid modes are: directory,feature,feature-collection,files,geojson-ls,meta,path,repo,sqlite. (default "repo")
  -timings
	Display timings during and after indexing
```

For example:

```
./bin/wof-mysql-index -dsn '{USER}:{PASSWORD}@/{DATABASE}' /usr/local/data/whosonfirst-data/
```

## See also:

* https://dev.mysql.com/doc/refman/5.7/en/spatial-analysis-functions.html
* https://mariadb.com/kb/en/library/geographic-geometric-features/
* https://github.com/whosonfirst/go-whosonfirst-sqlite

