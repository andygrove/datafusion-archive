#!/usr/bin/env bash
cargo clean
cargo build --target=x86_64-unknown-linux-musl --release
docker build -t datafusionrs/datafusion .

