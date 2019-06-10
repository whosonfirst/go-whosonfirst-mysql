fmt:
	go fmt cmd/wof-mysql-index/main.go
	go fmt cmd/wof-mysql-purge/main.go
	go fmt database/*.go
	go fmt tables/*.go
	go fmt utils/*.go

tools:
	go build -mod vendor -o bin/wof-mysql-index cmd/wof-mysql-index/main.go
	go build -mod vendor -o bin/wof-mysql-purge cmd/wof-mysql-purge/main.go
