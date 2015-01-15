go-mysql-elasticsearch is a service to sync your MySQL data into elastissearch automatically. 

It uses mysqldump to fetch data at first, then syncs data incrementally with binlog.

## Use

## Notice

+ binlog format must be **row**.
+ binlog row image may be **full** for MySQL. (MariaDB only supports full row image).
+ MySQL table should have a primary key.
+ Can not alter table format at runtime.