all: build

build: build-elasticsearch

build-elasticsearch:
	godep go build -o bin/go-mysql-elasticsearch ./cmd/go-mysql-elasticsearch

test:
	godep go test --race ./...

clean:
	godep go clean -i ./...
	@rm -rf bin
