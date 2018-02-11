FROM ubuntu

RUN mkdir -p /opt/datafusion/bin

COPY target/x86_64-unknown-linux-musl/release/console /opt/datafusion/bin
COPY target/x86_64-unknown-linux-musl/release/worker /opt/datafusion/bin
