# Distributed Query Processing implemented in Rust

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This project is intended to be a proof-of-concept of a distributed data processing platform in Rust with features somewhat similar to Apache Spark but it is not intended to be a clone of Apache Spark.

I have a hypothesis that even a naive implementation in Rust will have performance that is roughly comparable to that of Apache Spark, but more importantly the performance will be _predictable_ and _reliable_ because there is no garbage collector involved.

# What will be similar to Apache Spark?

- There will be a DataFrame API as well as SQL support and both approaches ultimately are just different ways of defining a query plan
- A master node will create a distributed execution plan and co-ordinate the execution of the plan across the worker nodes
- There will be a query optimizer, with some basic optimizations 
- HDFS will be supported

# What will be different to Apache Spark?

Due to the statically compiled nature of Rust, this platform will be less interactive:

- No support is planned for allowing idiomatic functional code to be applied to a DataFrame and have the lambda functions be serialized and sent to the worker nodes
- UDTs and UDFs will need to be statically compiled into the worker nodes, at least initially (it would be possible to do some dynamic loading eventually)
- No interactive REPL is planned although a SQL console would be possible

# Current Status

This project is in a very early stage of development. So far, very little works. It is possible to run a trivial SQL query against a CSV file locally. See [examples/sql_query.rs](https://github.com/andygrove/distributed-query-rs/blob/master/examples/sql_query.rs) for an example.

There is also an [example of using a DataFrame API](https://github.com/andygrove/distributed-query-rs/blob/master/examples/dataframe.rs) to define a job but this isn't working yet.

# Why am I building this?

Primarily, this is a just a fun side-project for me to use to become a better Rust developer since it involves solving some non-trivial problems. I'm also generally interested in researching distributed systems and query optimizers.

# Roadmap

## Phase 1 - Benchmark simple use case against Apache Spark

I'd like to be able to run a job that reads a partitioned CSV file from HDFS and performs some computationally intensive processing on that data on a cluster and see how the performance compares to Apache Spark.

Features needed:

- Worker nodes
- Partitioning
- Shuffle
- Scalar functions
- Deployment model (most likely kubernetes and etcd for service discovery)
- Docker container for worker nodes
- HDFS support

## Phase 2 - Usability

- Complete data type support
- UI
- Make SQL / DataFrame functionality more complete
- Documentation
 
## Phase 3 - Make it usable for real-world problems

- ORDER BY 
- GROUP BY with basic aggregation functions (MIN, MAX, SUM, AVG)
- JOIN
- UDTs
- UDFs
- UDAFs
- Query optimizer (starting with simple optimizations like predicate push-down)
- Integrations with other data sources



 