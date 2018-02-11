FROM ubuntu

# Prepare directories for copy of the DataFusion binaries, and logging directory
RUN mkdir -p /opt/datafusion/bin && \
    mkdir -p /var/log/datafusion

COPY target/x86_64-unknown-linux-musl/release/console /opt/datafusion/bin
COPY target/x86_64-unknown-linux-musl/release/worker /opt/datafusion/bin

# Expose port 8080 for the worker, if the worker is run
EXPOSE 8080

# Export /opt/datafusion for any logs
VOLUME ["/opt/datafusion", "/var/log/datafusion"]
