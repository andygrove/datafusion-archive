#!/usr/bin/env bash
#
# Installs all of the tools required for installing Docker on a box with APT tools

set -e

apt install -y sudo
apt install -y apt-transport-https ca-certificates curl gnupg2 software-properties-common
curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg | apt-key add -
add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
   $(lsb_release -cs) \
   stable"
apt update
apt install -y docker-ce

docker --version
