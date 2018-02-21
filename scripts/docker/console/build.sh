#!/usr/bin/env bash
set -e

# get the product version from Cargo.toml
DATAFUSION_VERSION=`grep version Cargo.toml | head -1 | awk -F' ' '{ print $3 }' | sed 's/\"//g'`

# build the image
echo "Building docker image for DataFusion Console ${DATAFUSION_VERSION} ..."
sleep 0.2
docker build -f scripts/docker/console/Dockerfile -t "datafusionrs/console:${DATAFUSION_VERSION}" .

# tag as latest
docker tag "datafusionrs/console:${DATAFUSION_VERSION}" "datafusionrs/console:latest"
