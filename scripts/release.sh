#!/usr/bin/env bash

set -e

DATAFUSION_VERSION=`grep version Cargo.toml | head -1 | awk -F' ' '{ print $3 }' | sed 's/\"//g'`

echo Version: $DATAFUSION_VERSION

# Make sure there are no uncommitted changes
git diff-index --quiet HEAD --

# Run tests
cargo test

# Run examples
cargo run --example csv_sql
cargo run --example csv_dataframe
cargo run --example ndjson_sql
cargo run --example parquet_sql
cargo run --example parquet_dataframe

# Build Docker image
./scripts/docker/console/build.sh

# Publish crate
cargo publish

# Push Docker image
docker push datafusionrs/console:$DATAFUSION_VERSION
docker push datafusionrs/console:latest

# Tag release
git tag $DATAFUSION_VERSION
git push origin :$DATAFUSION_VERSION
