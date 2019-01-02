
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/crates/v/datafusion.svg)](https://crates.io/crates/datafusion)
[![Build Status](https://travis-ci.org/andygrove/datafusion.svg?branch=master)](https://travis-ci.org/andygrove/datafusion)
[![Coverage Status](https://coveralls.io/repos/github/andygrove/datafusion/badge.svg?branch=master)](https://coveralls.io/github/andygrove/datafusion?branch=master)
[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/datafusion-rs)

# DataFusion: Modern Distributed Compute Platform implemented in Rust

<img src="img/datafusion-logo.png" width="256" />

DataFusion is an attempt at building a modern distributed compute platform in Rust, leveraging [Apache Arrow](https://arrow.apache.org/) as the memory model and execution engine.

See my article [How To Build a Modern Distributed Compute Platform](https://andygrove.io/how_to_build_a_modern_distributed_compute_platform/) to learn about the design and my motivation for building this. The TL;DR is that this project is a great way to learn about building distributed systems but there are plenty of better choices if you need something mature and supported.

# Status / Roadmap

The original POC no longer works due to changes in Rust nightly since 11/3/18 and since then I have been contributing more code to the Apache Arrow project and decided to start implementing DataFusion from scratch based on that latest Arrow code and incorporating lessons learned from the first attempt. The original POC code is is now on the [original_poc branch](https://github.com/andygrove/datafusion/tree/original_poc) and supports single threaded SQL execution against Parquet and CSV files using Apache Arrow as the memory model.

The current code supports single-threaded execution of limited SQL queries (projection and selection) against CSV files.

See [ROADMAP.md](ROADMAP.md) for the full roadmap.

# Prerequisites

- Rust nightly (required by `parquet-rs` crate)

# Building DataFusion

See [BUILDING.md](/BUILDING.md).

# Gitter

There is a [Gitter channel](https://gitter.im/datafusion-rs/Lobby) where you can ask questions about the project or make feature suggestions too.

# Contributing

Contributors are welcome! Please see [CONTRIBUTING.md](/CONTRIBUTING.md) for details.


 
