#!/usr/bin/env bash
set -e

# get the product version from Cargo.toml
DATAFUSION_VERSION=`grep version Cargo.toml | head -1 | awk -F' ' '{ print $3 }' | sed 's/\"//g'`

# build the image
echo "Building docker image for DataFusion Worker ${DATAFUSION_VERSION} ..."
sleep 0.2
docker build -f scripts/docker/worker/Dockerfile -t "datafusionrs/worker:${DATAFUSION_VERSION}" .

# tag as latest
docker tag "datafusionrs/worker:${DATAFUSION_VERSION}" "datafusionrs/worker:latest"
