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



 
