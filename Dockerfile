FROM golang:alpine

MAINTAINER siddontang

COPY . /go/src/github.com/siddontang/go-mysql-elasticsearch

RUN cd /go/src/github.com/siddontang/go-mysql-elasticsearch/ && \
    go build -o bin/go-mysql-elasticsearch ./cmd/go-mysql-elasticsearch && \
    cp -f ./bin/go-mysql-elasticsearch /go/bin/go-mysql-elasticsearch

ENTRYPOINT ["go-mysql-elasticsearch"]

