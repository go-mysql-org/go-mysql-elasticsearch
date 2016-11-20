FROM golang:1.6
MAINTAINER Eagle Chen <chygr1234@gmail.com>

ENV MARIADB_MAJOR 10.0
ENV MARIADB_VERSION 10.0.28+maria-1~jessie

RUN echo "deb http://ftp.osuosl.org/pub/mariadb/repo/$MARIADB_MAJOR/debian jessie main" > /etc/apt/sources.list.d/mariadb.list \
    && { \
            echo 'Package: *'; \
            echo 'Pin: release o=MariaDB'; \
            echo 'Pin-Priority: 999'; \
    } > /etc/apt/preferences.d/mariadb


RUN apt-get update && \
  apt-get install -y --force-yes mariadb-client=$MARIADB_VERSION && \
  go get github.com/tools/godep && \
  (go get github.com/siddontang/go-mysql-elasticsearch || true ) && \
  cd /go/src/github.com/siddontang/go-mysql-elasticsearch && \
  make

COPY run.sh /run_go_mysql_es
ENTRYPOINT ["/run_go_mysql_es"]
