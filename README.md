# Implementation of a Versioned Time-series Database

## Integration to existing relational databases

Our repo provide the interface to 2 kinds of typical databases,
for other DB, One can adopt similar ways of integration.

### PostgreSQL:

1. U need first download the newest jar for Postgres connection.

An example jar is provided in this repo: postgresql-42.5.0.jar
Add the jar to this repo.

2. Launch an instance in docker: postgres

Execute SQL: create database db1;

Connect it with:
jdbc:postgresql://localhost:49153/db1
when the db port is 49153

3. See postgres.java
It provides the interface to 
(1) connect the docker instance
(2) import the data
(3) benchmark the database through SQLs.

4. dataset: download 

<https://cloud.tsinghua.edu.cn/f/7e6ab80cac6845b8b6c0/?dl=1>



### TimescaleDB

1. To launch TimescaleDB docker, See <https://docs.timescale.com/install/latest/installation-docker/>

2. use import functions in postgres.java to import data from csv file.

3. test performance by postgres.java benchmark


## Versioned TSDB:

We also use a prototype and optimization of Apache IoTDB, there are two alternative sources to look into.

1. In open source repo (a branch) <https://github.com/marisuki/iotdb/tree/version-and-opt>

we show there a ./warehouse package for the series reader, operator, executor, plan, optimizer as a demo.
This branch contains series reader similar to the original code repo <https://github.com/apache/iotdb>, and we use streaming for transforming the data.

2. in this repo, we also give simple implementation of the prototype and optimization.
See VersionSeriesReader.java for implementation of series reader and simple query execution.
See VersionBlock.java for database and data storage.
