#!/usr/bin/env bash
#
# Builds examples

set -e

cargo build --examples
cargo run --example sql_query
cargo run --example dataframe
