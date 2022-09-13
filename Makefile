vuln:
	govulncheck ./...

cli:
	go build -mod vendor -o bin/wof-mysql-index cmd/wof-mysql-index/main.go
