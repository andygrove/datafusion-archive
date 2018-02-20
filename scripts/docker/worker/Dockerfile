FROM datafusionrs/base:latest

# Copy the source files into the image
RUN mkdir -p /tmp/datafusion_source
RUN mkdir -p /tmp/datafusion_source/src
RUN mkdir -p /tmp/datafusion_source/examples
RUN mkdir -p /tmp/datafusion_source/benches

ADD src /tmp/datafusion_source/src/
ADD examples /tmp/datafusion_source/examples/
ADD benches /tmp/datafusion_source/benches/
ADD Cargo.toml /tmp/datafusion_source/

# Build the release
RUN cd /tmp/datafusion_source ; . ~/.cargo/env ; cargo build --release

## Prepare directories for copy of the DataFusion binaries, and logging directory
RUN mkdir -p /opt/datafusion/bin && \
    mkdir -p /opt/datafusion/www && \
    mkdir -p /opt/datafusion/www/css && \
    mkdir -p /var/log/datafusion

RUN cp /tmp/datafusion_source/target/release/worker /opt/datafusion/bin/
RUN cp /tmp/datafusion_source/src/bin/worker/*.html /opt/datafusion/www/
RUN cp /tmp/datafusion_source/src/bin/worker/css/* /opt/datafusion/www/css/

RUN rm -rf /tmp/datafusion_source

# Expose port 8080 for the worker, if the worker is run
EXPOSE 8080

# Export /var/datafusion for data and logs
VOLUME [ "/var/datafusion/data", "/var/datafusion/logs" ]

ENTRYPOINT [ "/opt/datafusion/bin/worker" ]

CMD [ "--help" ]


