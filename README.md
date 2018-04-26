# go-whosonfirst-mysql

Go package for working with Who's On First documents and MySQL databases.

## Install

You will need to have both `Go` (specifically a version of Go more recent than 1.6 so let's just assume you need [Go 1.8](https://golang.org/dl/) or higher) and the `make` programs installed on your computer. Assuming you do just type:

```
make bin
```

All of this package's dependencies are bundled with the code in the `vendor` directory.

## Tools

### wof-mysql-index 

_Please write me_

```
./bin/wof-mysql-index -dsn '{USER}:{PASSWORD}@/{DATABASE}' /usr/local/data/whosonfirst-data/
```

## To do

This package shares the same basic model as the [go-whosonfirst-sqlite-*](https://github.com/whosonfirst?utf8=%E2%9C%93&q=go-whosonfirst-sqlite&type=&language=) packages. They should be reconciled. Today, they are not.