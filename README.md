# DataFusion: Big Data Platform for Rust

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/crates/v/datafusion.svg)](https://crates.io/crates/datafusion)
[![Docs](https://docs.rs/datafusion/badge.svg)](https://docs.rs/datafusion)
[![Build Status](https://travis-ci.org/andygrove/datafusion-rs.svg?branch=master)](https://travis-ci.org/andygrove/datafusion-rs)
[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/datafusion-rs)

DataFusion is a distributed data processing platform implemented in Rust. It is very much inspired by Apache Spark and has a similar programming style through the use of DataFrames and SQL.

DataFusion can also be used as a crate dependency in your project if you want the ability to perform SQL queries and DataFrame style data manipulation in-process.

# Project Home Page

The project home page is now at [https://datafusion.rs](https://datafusion.rs)

# Current Status

It is currently possible to use DataFusion as a crate dependency to execute SQL and DataFrame operations against data in-process and it is also possible to deploy DataFusion as a distributed data processing platform (but only with a single worker so far).

## Standalone

- [SQL Example](https://github.com/andygrove/distributed-query-rs/blob/master/examples/sql_query.rs)
- [DataFrame Example](https://github.com/andygrove/distributed-query-rs/blob/master/examples/dataframe.rs)

Both of these examples run a trivial query against a trivial CSV file using a single thread.

## Distributed

It is possible to start a cluster of worker nodes and use a SQL console to execute queries against the cluster. There must be a running etcd cluster for the workers to register with. 

### Run Worker

```bash
cargo run --bin worker -- --bind 0.0.0.0:8080 --etcd http://127.0.0.1:2379
```

The worker should produce output similar to the following:

```
Worker 8987a3f3-71e1-5cca-aadf-bc165f528fac listening on 0.0.0.0:8080 and serving content from ./src/bin/worker/

```

### Run Console

```bash
cargo run --bin console -- --etcd http://127.0.0.1:2379
```

```
DataFusion Console
$ CREATE EXTERNAL TABLE uk_cities (name VARCHAR(100) NOT NULL, lat DOUBLE NOT NULL, lng DOUBLE NOT NULL)
Executing: CREATE EXTERNAL TABLE uk_cities (name VARCHAR(100) NOT NULL, lat DOUBLE NOT NULL, lng DOUBLE NOT NULL)

$ SELECT name, lat, lng FROM uk_cities WHERE lat < 51
Executing: SELECT name, lat, lng FROM uk_cities WHERE lat < 51
Eastbourne, East Sussex, UK,50.768036,0.290472
Weymouth, Dorset, UK,50.614429,-2.457621
Bournemouth, UK,50.720806,-1.904755
Hastings, East Sussex, UK,50.854259,0.573453
Uckfield, East Sussex, UK,50.967941,0.085831
Worthing, West Sussex, UK,50.825024,-0.383835
Plymouth, UK,50.376289,-4.143841

Query executed in 0.005250537 seconds
```

# Roadmap

I've started defining [milestones](https://github.com/andygrove/datafusion-rs/milestones) and [issues](https://github.com/andygrove/datafusion-rs/issues) in github issues, but the current priorities are.

- Implement basic partitioning so that a query can run in parallel on multiple worker nodes
- Implement shuffle so that more advanced distributed jobs can be executed
- Implement in-memory SORT, JOIN, GROUP BY so more categories of query can be executed
- Add support for Hadoop data sources such as HDFS, Parquet, and Kudu

# Contributing

Contributers are welcome! Please see [CONTRIBUTING.md](/CONTRIBUTING.md) for details.



 
