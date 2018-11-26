# DataFusion: SQL Query Execution in Rust

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/crates/v/datafusion.svg)](https://crates.io/crates/datafusion)
[![Build Status](https://travis-ci.org/datafusion-rs/datafusion.svg?branch=master)](https://travis-ci.org/datafusion-rs/datafusion)
[![Coverage Status](https://coveralls.io/repos/github/datafusion-rs/datafusion/badge.svg?branch=master)](https://coveralls.io/github/datafusion-rs/datafusion?branch=master)
[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/datafusion-rs)

DataFusion is an attempt at building a modern distributed compute platform in Rust, using [Apache Arrow](https://arrow.apache.org/) as the memory model.

See my article [How To Build a Modern Distributed Compute Platform](https://andygrove.io/how_to_build_a_modern_distributed_compute_platform/) to learn about the design and my motivation for building this. The TL;DR is that this project is a great way to learn about building distributed systems but there are plenty of better choices if you need something mature and supported.

# Status

The original POC no longer works due to changes in Rust nightly since 11/3/18 and since then I have been contributing more code to the Apache Arrow project and decided to start implementing DataFusion from scratch based on that latest Arrow code and incorporating lessons learned from the first attempt. The original POC code is is now on the [original_poc branch](https://github.com/andygrove/datafusion/tree/original_poc) and supports single threaded SQL execution against Parquet and CSV files using Apache Arrow as the memory model.

The current task list:

- [x] Delete existing code and update the README with the new plan
- [ ] Implement serializable logical query plan
- [ ] Implement data source for CSV
- [ ] Implement data source for Parquet
- [ ] Implement query execution: Projection
- [ ] Implement query execution: Selection
- [ ] Implement query execution: Sort
- [ ] Implement query execution: Aggregate
- [ ] Implement query execution: Scalar Functions
- [ ] Implement parallel query execution (multithreaded, single process)
- [ ] Generate query plan from SQL
- [ ] Implement worker node that can receive a query plan, execute the query, and return a result in Arrow IPC format
- [ ] Implement distributed query execution using Kubernetes

# Prerequisites

- Rust nightly (required by `parquet-rs` crate)

# Building DataFusion

See [BUILDING.md](/BUILDING.md).

# Gitter

There is a [Gitter channel](https://gitter.im/datafusion-rs/Lobby) where you can ask questions about the project or make feature suggestions too.

# Contributing

Contributors are welcome! Please see [CONTRIBUTING.md](/CONTRIBUTING.md) for details.


 
