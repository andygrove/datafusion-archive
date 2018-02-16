#!/usr/bin/env bash
#
# Build file for the Dockerfile

set -e

# DOCKER_REPOSITORY is the master repository where the docker image will be referenced and stored.  Defaults to "datafusionrs"
DOCKER_REPOSITORY="${DOCKER_REPOSITORY:-datafusionrs}"

# Note: Setting DOCKER_PUSH to "true" outside of this script will cause the docker container to push to a repository.
DOCKER_PUSH="${DOCKER_PUSH:-false}"

# BUILD_RELEASE set to true will perform a "cargo build --release"; otherwise this step will be skipped.  Defaults to "false"
BUILD_RELEASE="${BUILD_RELEASE:-false}"

# Build the final release candidate, create a docker container from it.  Only builds for x86_6_64-unknown-linux-musl target.
#if [ "${TARGET}" == "x86_64-unknown-linux-musl" ]; then
  if [ "${BUILD_RELEASE}" == true ]; then
    cargo build --release
  fi
  
  if [ -f "target/release/console" ]; then 
    echo "Building docker: ${DOCKER_TAG}"
    echo
  
    # DATAFUSION_VERSION is the current version of the docker image.
    DATAFUSION_VERSION=`./target/debug/console --version | awk -F' ' '{ print $3 }'`

    # DOCKER_TAG is the combined tag from the docker repository and version.  Defaults to (repository)/datafusion:(version)
    DOCKER_TAG="${DOCKER_TAG:-${DOCKER_REPOSITORY}/datafusion:${DATAFUSION_VERSION}-SNAPSHOT}"

    docker build -f scripts/docker/Dockerfile -t "${DOCKER_TAG}" .

    if [ "${DOCKER_PUSH}" == true ]; then
      docker push "${DOCKER_TAG}"
    fi
  else
    echo "Skipping Docker release."
  fi
#else
#  echo "Skipping build - expecting x86_64-unknown-linux-musl as target."
#fi
