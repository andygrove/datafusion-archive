#!/usr/bin/env bash
#
# Build file for the Dockerfile

DOCKER_REPOSITORY="${DOCKER_REPOSITORY:datafusionrs}"
DOCKER_VERSION="${DOCKER_VERSION:0.1.4}"
DOCKER_TAG="${DOCKER_TAG:${DOCKER_REPOSITORY}/datafusion:${DOCKER_VERSION}}"

DOCKER_PUSH="${DOCKER_PUSH:false}"

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
