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
    mkdir -p /var/log/datafusion

RUN cp /tmp/datafusion_source/target/release/console /opt/datafusion/bin/

RUN rm -rf /tmp/datafusion_source

ENTRYPOINT [ "/opt/datafusion/bin/console" ]

CMD [ "--help" ]


