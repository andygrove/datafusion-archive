# Distributed Query Processing implemented in Rust

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This project is intended to be a proof-of-concept of a distributed data processing platform in Rust with features similar to Apache Spark.

# Current Status

So far, very little works. It is possible to run a trivial SQL query against a CSV file. Just a few data types are implemented. See [examples/sql_query.rs](https://github.com/andygrove/distributed-query-rs/blob/master/examples/sql_query.rs) for an example.

# Roadmap / TODO list

In no particular order...

- Implement error handling consistently and remove all calls to panic, unwrap, etc
- Partitioning
- Basic set of data types
- Design deployment model (most likely kubernetes and etcd for service discovery)
- UI for worker node
- UI for co-ordinator node
- Set up Docker container for worker nodes
- Read from HDFS
- Read from RDBMS 
- Type coercion
- UDTs
- UDFs
- UDAFs
- ORDER BY 
- GROUP BY with basic aggregation functions (MIN, MAX, SUM, AVG)
- JOIN
- DataFrame-style API to build query plans
- Query optimizer (starting with simple optimizations like predicate push-down)

 