#!/usr/bin/env bash
source /root/.cargo/env
export LD_LIBRARY_PATH=$(rustc --print sysroot)/lib
cd /opt/datafusion
./bin/console "$@"