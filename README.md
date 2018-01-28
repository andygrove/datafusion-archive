# Distributed Query Processing in Rust

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.org/andygrove/distributed-query-rs.svg?branch=master)](https://travis-ci.org/andygrove/distributed-query-rs)

This project is a proof-of-concept of a distributed data processing platform in Rust with features somewhat similar to Apache Spark but it is not intended to be a clone of Apache Spark.

# Why am I building this?

Primarily, this is a just a fun side-project for me to use to become a better Rust developer since it involves solving some non-trivial problems. I'm also generally interested in researching distributed systems and query optimizers since I've been working with these concepts professionally for quite a few years now.

Apart from using this is a way to learn, I do think that it could result in a useful product.

I have a hypothesis that even a naive implementation in Rust will have performance that is roughly comparable to that of Apache Spark for simple use cases, but more importantly the performance will be _predictable_ and _reliable_ because there is no garbage collector involved.

# What will be similar to Apache Spark?

- There will be a DataFrame API as well as SQL support and both approaches ultimately are just different ways of defining a query plan
- A master node will create a distributed execution plan and co-ordinate the execution of the plan across the worker nodes
- There will be a query optimizer, with some basic optimizations 
- HDFS will be supported

# What will be different to Apache Spark?

Due to the statically compiled nature of Rust, this platform will be less interactive:

- No support is planned for allowing idiomatic Rust lambda functions to be applied to a DataFrame but instead pre-registered UDFs can be used to perform transformations on DataFrames
- UDTs and UDFs will need to be statically compiled into the worker nodes, at least initially (it would be possible to do some dynamic loading eventually)
- No interactive REPL is planned although a SQL console would be possible

# Current Status

There are two working examples:

- [DataFrame Example](https://github.com/andygrove/distributed-query-rs/blob/master/examples/dataframe.rs)
- [SQL Example](https://github.com/andygrove/distributed-query-rs/blob/master/examples/sql_query.rs)

Both of these examples run a trivial query against a trivial CSV file using a single thread.

# Roadmap

## Phase 1 - Benchmark simple use case against Apache Spark

I'd like to be able to run a job that reads a partitioned CSV file from HDFS and performs some computationally intensive processing on that data on a cluster and see how the performance compares to Apache Spark.

Features needed:

- Partitioning
- Shuffle
- Scalar functions
- HDFS support
- Worker nodes (dockerized)

## Phase 2 - Usability and Stability

- Deployment model (most likely kubernetes and etcd for service discovery)
- UI
- Documentation
- Automated integration tests
- Automated performance tests
- Complete data type support
- Make SQL / DataFrame functionality more complete
 
## Phase 3 - Make it usable for real-world problems

- ORDER BY 
- GROUP BY with basic aggregation functions (MIN, MAX, SUM, AVG)
- JOIN
- UDTs
- UDFs
- UDAFs
- Query optimizer (starting with simple optimizations like predicate push-down, push predicate through join etc)
- Integrations with other data sources



 