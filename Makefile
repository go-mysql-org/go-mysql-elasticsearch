all: build

build: build-elasticsearch build-dump

build-elasticsearch:
	godep go build -o bin/go-mysql-elasticsearch ./cmd/go-mysql-elasticsearch

build-dump:
	godep go build -o bin/go-dump ./cmd/go-mysql-elastic-dump

test:
	godep go test ./...

clean:
	godep go clean -i ./...
	@rm -rf bin
