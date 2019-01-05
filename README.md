
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/crates/v/datafusion.svg)](https://crates.io/crates/datafusion)
[![Build Status](https://travis-ci.org/andygrove/datafusion.svg?branch=master)](https://travis-ci.org/andygrove/datafusion)
[![Coverage Status](https://coveralls.io/repos/github/andygrove/datafusion/badge.svg?branch=master)](https://coveralls.io/github/andygrove/datafusion?branch=master)
[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/datafusion-rs)

# DataFusion: Modern Distributed Compute Platform implemented in Rust

<img src="img/datafusion-logo.png" width="256" />

DataFusion is an attempt at building a modern distributed compute platform in Rust, leveraging [Apache Arrow](https://arrow.apache.org/) as the memory model and execution engine.

See my article [How To Build a Modern Distributed Compute Platform](https://andygrove.io/how_to_build_a_modern_distributed_compute_platform/) to learn about the design and my motivation for building this. The TL;DR is that this project is a great way to learn about building distributed systems but there are plenty of better choices if you need something mature and supported.

# Status

The current code supports single-threaded execution of limited SQL queries (projection, selection, and aggregates) against CSV files. Parquet files will be supported shortly.

To use DataFusion as a crate dependency, add the following to your Cargo.toml:

```toml
[dependencies]
version = "0.5.2"
```

Here is a brief example for running a SQL query against a CSV file. See the [examples](examples) directory for working examples.

```rust
// create local execution context
let mut ctx = ExecutionContext::new();

// define schema for data source (csv file)
let schema = Arc::new(Schema::new(vec![
    Field::new("city", DataType::Utf8, false),
    Field::new("lat", DataType::Float64, false),
    Field::new("lng", DataType::Float64, false),
]));

// register csv file with the execution context
let csv_datasource = CsvDataSource::new("test/data/uk_cities.csv", schema.clone(), 1024);
ctx.register_datasource("cities", Rc::new(RefCell::new(csv_datasource)));

// simple projection and selection
let sql = "SELECT city, lat, lng FROM cities WHERE lat > 51.0 AND lat < 53";

// execute the query
let results = ctx.sql(&sql).unwrap();
```

# Roadmap

See [ROADMAP.md](ROADMAP.md) for the full roadmap.

# Prerequisites

- Rust nightly (required by `parquet-rs` crate)

# Building DataFusion

See [BUILDING.md](/BUILDING.md).

# Gitter

There is a [Gitter channel](https://gitter.im/datafusion-rs/Lobby) where you can ask questions about the project or make feature suggestions too.

# Contributing

Contributors are welcome! Please see [CONTRIBUTING.md](/CONTRIBUTING.md) for details.


 
