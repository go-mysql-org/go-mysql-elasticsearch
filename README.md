go-mysql-elasticsearch is a service to sync your MySQL data into Elasticsearch automatically. 

It uses `mysqldump` to fetch the origin data at first, then syncs data incrementally with binlog.

## How to use?

+ Create table in MySQL.
+ Create the associated Elasticsearch index, document type and mappings if possible, if not, Elasticsearch will create these automatically.
+ Config base, see the example config [river.toml](./etc/river.toml).
+ Set MySQL source in config file, see [Source](#source) below.
+ Customize MySQL and Elasticsearch mapping rule in config file, see [Rule](#rule) below.
+ Start `go-mysql-elasticsearch` and enjoy it.

## Notice

+ binlog format must be **row**.
+ binlog row image may be **full** for MySQL. (MariaDB only supports full row image).
+ Can not alter table format at runtime.
+ MySQL table which will be synced must have a PK(primary key), multi columns PK is not allowed now. The PK data will be used as "id" in Elasticsearch.  
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

By default, go-mysql-elasticsearch will use MySQL table name as the Elasticserach's index and type name, 
use MySQL table field name as the Elasticserach's field name. 
e.g, if a table named blog, the default index and type in Elasticserach are both named blog, if the table field named title, 
the default field name is also named title.

Rule can let you change this name mapping. Rule format in config file is below:

```
[[rule]]
schema = "test"
table = "t1"
index = "t"
type = "t"

    [rule.field]
    title = "my_title"
```

In the example above, we will use a new index and type both named "t" instead of default "t1", and use "my_title" instead of field name "title".

## Why not other rivers?

Although there are some other MySQL rivers for Elasticsearch, like [elasticsearch-river-jdbc](https://github.com/jprante/elasticsearch-river-jdbc), [elasticsearch-river-mysql](https://github.com/scharron/elasticsearch-river-mysql), I still want to build a new one with Go, why?

+ Customization, I want to decide which table to be synced, the associated index and type name, or even the field name in Elasticsearch.
+ Incremental replication with binlog, and can resume from the last sync position when the service starts. 
+ A common sync framework not only for Elasticsearch but also for others, like memcached, redis, etc...

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

## Todo

+ Filtering table field support, only fields in filter config will be synced.
+ Statistic.

## Feedback

go-mysql-elasticsearch is still in development, and we will try to use it in production later. Any feedback is very welcome.

Email: siddontang@gmail.com
