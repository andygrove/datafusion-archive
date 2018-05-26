FROM datafusionrs/base:latest

# Copy the source files into the image
RUN mkdir -p /tmp/datafusion_source
RUN mkdir -p /tmp/datafusion_source/src
RUN mkdir -p /tmp/datafusion_source/examples

ADD src /tmp/datafusion_source/src/
ADD examples /tmp/datafusion_source/examples/
ADD Cargo.toml /tmp/datafusion_source/

# Build the release
RUN cd /tmp/datafusion_source ; . ~/.cargo/env ; cargo build --release

## Prepare directories for copy of the DataFusion binaries, and logging directory
RUN mkdir -p /opt/datafusion/bin && \
    mkdir -p /opt/datafusion/data && \
    mkdir -p /var/log/datafusion

RUN cp /tmp/datafusion_source/target/release/console /opt/datafusion/bin/

# Add some sample files
ADD scripts/docker/console/all_types_flat.csv /opt/datafusion/data/
ADD scripts/docker/console/all_types_flat.parquet /opt/datafusion/data/
ADD scripts/docker/console/uk_cities.csv /opt/datafusion/data/

# Delete sources
RUN rm -rf /tmp/datafusion_source

ADD scripts/docker/console/console.sh /usr/bin/datafusion-console.sh

ENTRYPOINT [ "/usr/bin/datafusion-console.sh" ]


