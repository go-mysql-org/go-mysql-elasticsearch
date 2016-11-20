#!/bin/bash

GO_PATH=/go
CONFIG_FILE=${CONFIG:-/go_mysql_river.toml}

${GO_PATH}/src/github.com/siddontang/go-mysql-elasticsearch/bin/go-mysql-elasticsearch -config=$CONFIG_FILE
