# DataFusion: Distributed Query Processing in Rust

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/crates/v/datafusion.svg)](https://crates.io/crates/datafusion)
[![Docs](https://docs.rs/datafusion/badge.svg)](https://docs.rs/datafusion)
[![Build Status](https://travis-ci.org/andygrove/datafusion-rs.svg?branch=master)](https://travis-ci.org/andygrove/datafusion-rs)

This project is a proof-of-concept of a distributed data processing platform in Rust with features somewhat similar to Apache Spark but it is not intended to be a clone of Apache Spark.

DataFusion can also be used as a crate dependency in your project if you want the ability to perform SQL queries and DataFrame style data manipulation in-process.

# Why am I building this?

Primarily, this is a just a fun side-project for me to use to become a better Rust developer since it involves solving some non-trivial problems. I'm also generally interested in researching distributed systems and query optimizers since I've been working with these concepts professionally for quite a few years now.

Apart from using this as a way to learn, I do think that it could result in a useful product.

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

- [SQL Example](https://github.com/andygrove/distributed-query-rs/blob/master/examples/sql_query.rs)
- [DataFrame Example](https://github.com/andygrove/distributed-query-rs/blob/master/examples/dataframe.rs)

Both of these examples run a trivial query against a trivial CSV file using a single thread.

# Roadmap

I've started defining [milestones](https://github.com/andygrove/datafusion-rs/milestones) and [issues](https://github.com/andygrove/datafusion-rs/issues) in github issues, but here's a high level summary of the plan with some rough guesses of timescale.

## POC (Q1 2018)

For the POC, I want to be able to run a single worker process (preferably dockerized) and be able to send it a query (via JSON) and have it execute that query. This will be sufficient to run some representative (but trivial) workloads to compare with Apache Spark.

The workloads will read and write CSV files from HDFS.

## MVP (Q2 2018)

MVP should be fully deployable, have a good UX, have good documentation etc. It could still be lacking major features though such as JOIN, GROUP BY, user-defined functions etc.

## 1.0 (Q4 2018)

The 1.0 release should be able to support real-world workloads with performance, scalability, and reliability that generally exceed those of Apache Spark.

# Contributing

Contributers are welcome! Please see [CONTRIBUTING.md](/CONTRIBUTING.md) for details.



 
