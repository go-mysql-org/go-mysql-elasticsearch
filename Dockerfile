FROM golang:alpine

MAINTAINER siddontang

ADD . /go/src/github.com/siddontang/go-mysql-elasticsearch

RUN cd /go/src/github.com/siddontang/go-mysql-elasticsearch/ && \
    go build -o bin/go-mysql-elasticsearch ./cmd/go-mysql-elasticsearch && \
    cp -f ./bin/go-mysql-elasticsearch /go/bin/go-mysql-elasticsearch

RUN apk add --no-cache tini mariadb-client

ENTRYPOINT ["/sbin/tini","--","go-mysql-elasticsearch"]
