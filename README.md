# Distributed Query Processing implemented in Rust

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This is a proof-of-concept of a distributed data processing platform in Rust with features similar to Apache Spark.

# Current Status

So far, very little works. It is possible to run a trivial SQL query against a CSV file. Just a few data types are implemented. See (examples/sql_query.rs)[examples/sql_query.rs] for an example.

# Roadmap / TODO list

In no particular order...

- Implement error handling consistently and remove all calls to panic, unwrap, etc
- Set up docker container for worker nodes
- Design deployment model (most likely kubernetes and etcd for service discovery)
- UI for worker node
- UI for co-ordinator node
- partitioning
- basic set of data types
- HDFS support (read CSVs from HDFS to start with)
- Other hadoop file types (parquet etc)
- RDBMS support 
- ORDER BY 
- GROUP BY with MIN, MAX, SUM, AVG
- some subset of JOIN
- UDFs
- UDAFs
- DataFrame-style API 

 