#!/usr/bin/env bash

set -e

DATAFUSION_VERSION=`grep version Cargo.toml | head -1 | awk -F' ' '{ print $3 }' | sed 's/\"//g'`

echo Version: $DATAFUSION_VERSION

cargo test

cargo run --example csv_sql
cargo run --example csv_dataframe
cargo run --example parquet_sql
cargo run --example parquet_dataframe

./scripts/docker/console/build.sh

cargo publish
docker push datafusionrs/console:$DATAFUSION_VERSION
docker push datafusionrs/console:latest
git tag $DATAFUSION_VERSION
git push origin :$DATAFUSION_VERSION
