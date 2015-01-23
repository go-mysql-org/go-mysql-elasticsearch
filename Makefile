all: build

build:
	godep go install ./...

test:
	godep go test ./...

clean:
	godep go clean -i ./...