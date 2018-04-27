FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y curl vim pkg-config libssl-dev
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
RUN ~/.cargo/bin/rustup default nightly
