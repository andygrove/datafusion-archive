# DataFusion: SQL Query Execution in Rust

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/crates/v/datafusion.svg)](https://crates.io/crates/datafusion)
[![Build Status](https://travis-ci.org/datafusion-rs/datafusion.svg?branch=master)](https://travis-ci.org/datafusion-rs/datafusion)
[![Coverage Status](https://coveralls.io/repos/github/datafusion-rs/datafusion/badge.svg?branch=master)](https://coveralls.io/github/datafusion-rs/datafusion?branch=master)
[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/datafusion-rs)

DataFusion is an attempt at building a modern distributed compute platform in Rust, using [Apache Arrow](https://arrow.apache.org/) as the memory model.

See my article [How To Build a Modern Distributed Compute Platform](https://andygrove.io/how_to_build_a_modern_distributed_compute_platform/) to learn about the design and my motivation for building this. The TL;DR is that this project is a great way to learn about building distributed systems but there are plenty of better choices if you need something mature and supported.

The following features are currently supported:

- SQL Parser, Planner and Optimizer
- DataFrame API
- Columnar processing using [Apache Arrow](https://arrow.apache.org/)
- Support for local CSV and [Apache Parquet](https://parquet.apache.org/) files
- Single-threaded execution of SQL queries, supporting:
  - Projection
  - Selection
  - Scalar Functions
  - Aggregates (Min, Max, Count)
  - Grouping
- User-defined Scalar Functions (UDFs)

DataFusion can be used as a crate dependency in your project to add SQL support for custom data sources.

A [Docker image](https://datafusion.rs/guides/getting-started-docker/) is also available if you just want to run SQL queries against your CSV and Parquet files.

# Project Home Page

The project home page is now at [https://datafusion.rs](https://datafusion.rs) and contains the [roadmap](https://datafusion.rs/roadmap) as well as documentation for using this crate. I am using [GitHub issues](https://github.com/datafusion-rs/datafusion-rs/issues) to track development tasks and feedback.

# Prerequisites

- Rust nightly (required by `parquet-rs` crate)

# Building DataFusion

See [BUILDING.md](/BUILDING.md).

# Gitter

There is a [Gitter channel](https://gitter.im/datafusion-rs/Lobby) where you can ask questions about the project or make feature suggestions too.

# Contributing

Contributors are welcome! Please see [CONTRIBUTING.md](/CONTRIBUTING.md) for details.


 
