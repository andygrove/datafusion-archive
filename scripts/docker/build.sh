#!/usr/bin/env bash
#
# Build file for the Dockerfile

set -e 

# DOCKER_REPOSITORY is the master repository where the docker image will be referenced and stored.  Defaults to "datafusionrs"
DOCKER_REPOSITORY="${DOCKER_REPOSITORY:datafusionrs}"

# DOCKER_VERSION is the current version of the docker image.  Defaults to "0.1.4"
DOCKER_VERSION="${DOCKER_VERSION:0.1.4}"

# DOCKER_TAG is the combined tag from the docker repository and version.  Defaults to (repository)/datafusion:(version)
DOCKER_TAG="${DOCKER_TAG:${DOCKER_REPOSITORY}/datafusion:${DOCKER_VERSION}}"

# Note: Setting DOCKER_PUSH to "true" outside of this script will cause the docker container to push to a repository.
DOCKER_PUSH="${DOCKER_PUSH:false}"

# BUILD_RELEASE set to true will perform a "cargo build --release"; otherwise this step will be skipped.  Defaults to "false"
BUILD_RELEASE="${BUILD_RELEASE:false}"

# Build the final release candidate, create a docker container from it.
if [ "${BUILD_RELEASE}" == true ]; then
  sudo apt-get install musl-tools
  cargo build --target=x86_64-unknown-linux-musl --release --verbose
fi

if [ -f "target/x86_64-unknown-linux-musl/release/console" ]; then 
  echo "Building docker: ${TAG}"
  echo

  docker build -f scripts/docker/Dockerfile -t "${TAG}" .
  if [ "${DOCKER_PUSH}" == true ]; then
    docker push "${TAG}"
  fi
else
  echo "Skipping Docker release."
fi
