#!/usr/bin/env bash
#
# Build file for the Dockerfile

set -e

# get the product version from Cargo.toml
DATAFUSION_VERSION=`grep version Cargo.toml | head -1 | awk -F' ' '{ print $3 }' | sed 's/\"//g'`

# DOCKER_REPOSITORY is the master repository where the docker image will be referenced and stored.  Defaults to "datafusionrs"
DOCKER_REPOSITORY="${DOCKER_REPOSITORY:-}"

# Note: Setting DOCKER_PUSH to "true" outside of this script will cause the docker container to push to a repository.
DOCKER_PUSH="${DOCKER_PUSH:-false}"

# DOCKER_TAG is the combined tag from the docker repository and version.  Defaults to (repository)/datafusion:(version)
DOCKER_TAG="${DOCKER_TAG:-datafusionrs/console:${DATAFUSION_VERSION}}"

echo "Building docker: ${DOCKER_TAG}"
docker build -f scripts/docker/console/Dockerfile -t "${DOCKER_TAG}" .

#docker push "${DOCKER_TAG}"
