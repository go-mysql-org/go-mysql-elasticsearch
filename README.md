go-mysql-elasticsearch is a service syncing your MySQL data into Elasticsearch automatically.

It uses `mysqldump` to fetch the origin data at first, then syncs data incrementally with binlog.

## Install

+ Install Go and set your [GOPATH](https://golang.org/doc/code.html#GOPATH)
+ Install godep `go get github.com/tools/godep`
+ `go get github.com/siddontang/go-mysql-elasticsearch`, it will print some messages in console, skip it. :-)
+ cd `$GOPATH/src/github.com/siddontang/go-mysql-elasticsearch`
+ `make`

## How to use?

+ Create table in MySQL.
+ Create the associated Elasticsearch index, document type and mappings if possible, if not, Elasticsearch will create these automatically.
+ Config base, see the example config [river.toml](./etc/river.toml).
+ Set MySQL source in config file, see [Source](#source) below.
+ Customize MySQL and Elasticsearch mapping rule in config file, see [Rule](#rule) below.
+ Start `./bin/go-mysql-elasticsearch -config=./etc/river.toml` and enjoy it.

## Notice

+ binlog format must be **row**.
+ binlog row image must be **full** for MySQL, you may lost some field data if you update PK data in MySQL with minimal or noblob binlog row image. MariaDB only supports full row image.
+ Can not alter table format at runtime.
+ MySQL table which will be synced must have a PK(primary key), multi columns PK is allowed now, e,g, if the PKs is (a, b), we will use "a:b" as the key. The PK data will be used as "id" in Elasticsearch.
+ You should create the associated mappings in Elasticsearch first, I don't think using the default mapping is a wise decision, you must know how to search accurately.
+ `mysqldump` must exist in the same node with go-mysql-elasticsearch, if not, go-mysql-elasticsearch will try to sync binlog only.
+ Don't change too many rows at same time in one SQL.

## Source

In go-mysql-elasticsearch, you must decide which tables you want to sync into elasticsearch in the source config.

The format in config file is below:

```
[[source]]
schema = "test"
tables = ["t1", t2]

[[source]]
schema = "test_1"
tables = ["t3", t4]
```

`schema` is the database name, and `tables` includes the table need to be synced.

## Rule

By default, go-mysql-elasticsearch will use MySQL table name as the Elasticserach's index and type name, use MySQL table field name as the Elasticserach's field name.
e.g, if a table named blog, the default index and type in Elasticserach are both named blog, if the table field named title,
the default field name is also named title.

In addition, one-to-many join ( [parent-child relationship](https://www.elastic.co/guide/en/elasticsearch/guide/current/parent-child.html) in Elasticsearch ) is supported. Simply specify the field name for `parent` property.

Rule can let you change this name mapping. Rule format in config file is below:

```
[[rule]]
schema = "test"
table = "t1"
index = "t"
type = "t"
parent = "parent_id"

    [[rule.fields]]
    mysql = "title"
    elastic = "my_title"
```

In the example above, we will use a new index and type both named "t" instead of default "t1", and use "my_title" instead of field name "title".

## Rule field types

In order to map a mysql column on different elasticsearch types you can define the field type as follows:

```
[[rule]]
schema = "test"
table = "t1"
index = "t"
type = "t"
parent = "parent_id"

    [rule.field]
    // This will map column title to elastic search my_title
    title="my_title"

    // This will map column title to elastic search my_title and use array type
    title="my_title,list"

    // This will map column title to elastic search title and use array type
    title=",list"
```

Modifier "list" will translates a mysql string field like "a,b,c" on an elastic array type '{"a", "b", "c"}' this is specially useful if you need to use those fields on filtering on elasticsearch.

## Wildcard table

go-mysql-elasticsearch only allows you determind which table to be synced, but sometimes, if you split a big table into multi sub tables, like 1024, table_0000, table_0001, ... table_1023, it is very hard to write rules for every table.

go-mysql-elasticserach supports using wildcard table, e.g:

```
[[source]]
schema = "test"
tables = ["test_river_[0-9]{4}"]

[[rule]]
schema = "test"
table = "test_river_[0-9]{4}"
index = "river"
type = "river"
```

"test_river_[0-9]{4}" is a wildcard table definition, which represents "test_river_0000" to "test_river_9999", at the same time, the table in the rule must be same as it.

At the above example, if you have 1024 sub tables, all tables will be synced into Elasticsearch with index "river" and type "river".

## Why not other rivers?

Although there are some other MySQL rivers for Elasticsearch, like [elasticsearch-river-jdbc](https://github.com/jprante/elasticsearch-river-jdbc), [elasticsearch-river-mysql](https://github.com/scharron/elasticsearch-river-mysql), I still want to build a new one with Go, why?

+ Customization, I want to decide which table to be synced, the associated index and type name, or even the field name in Elasticsearch.
+ Incremental update with binlog, and can resume from the last sync position when the service starts again.
+ A common sync framework not only for Elasticsearch but also for others, like memcached, redis, etc...
+ Wildcard tables support, we have many sub tables like table_0000 - table_1023, but want use a unique Elasticsearch index and type.

## Todo

+ Filtering table field support, only fields in filter config will be synced.
+ Statistic.
+ Docker

## Feedback

go-mysql-elasticsearch is still in development, and we will try to use it in production later. Any feedback is very welcome.

Email: siddontang@gmail.com
