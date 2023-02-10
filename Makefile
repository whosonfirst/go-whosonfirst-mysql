vuln:
	govulncheck ./...

cli:
	go build -mod vendor -ldflags="-s -w" -o bin/wof-mysql-index cmd/wof-mysql-index/main.go
