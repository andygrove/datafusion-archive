#!/usr/bin/env bash

test_dir="target/smoketest"
expected_file="test/data/smoketest-expected.txt"
output_file="${test_dir}/smoketest_output.txt"

mkdir -p $test_dir

function cleanup {
    echo "CLEANUP: Removing ${test_dir}"
    rm -rf $test_dir
}
trap cleanup EXIT

# run tests
cargo fmt
cargo test

#TODO: check output from examples
cargo run --example csv_sql
cargo run --example csv_dataframe
cargo run --example parquet_sql
cargo run --example parquet_dataframe

# run benchmarks
cargo bench

## NOTE that distributed queries are broken since moving to Arrow ... will be be fixed later

#./scripts/docker/worker/build.sh
./scripts/docker/console/build.sh
#
## stop etcd
#docker kill etcd
#docker rm etcd
#
## stop datafusion worker
#docker kill datafusion
#docker rm datafusion
#
## run etcd
#docker run -d -v /usr/share/ca-certificates/:/etc/ssl/certs -p 4001:4001 -p 2380:2380 -p 2379:2379 \
# --name etcd quay.io/coreos/etcd:v2.3.8 \
# -name etcd0 \
# -advertise-client-urls http://${HostIP}:2379,http://${HostIP}:4001 \
# -listen-client-urls http://0.0.0.0:2379,http://0.0.0.0:4001 \
# -initial-advertise-peer-urls http://${HostIP}:2380 \
# -listen-peer-urls http://0.0.0.0:2380 \
# -initial-cluster-token etcd-cluster-1 \
# -initial-cluster etcd0=http://${HostIP}:2380 \
# -initial-cluster-state new
#
## give etcd a chance to start up
#sleep 2
#
## run worker
#docker run --network=host -d -p 8088:8088 \
# -v`pwd`/test/data:/var/datafusion/data \
# --name datafusion datafusionrs/worker:latest \
# --etcd http://127.0.0.1:2379 \
# --bind 127.0.0.1:8088 \
# --data_dir /var/datafusion/data \
# --webroot /opt/datafusion/www
#
## give the worker a chance to start up
#sleep 2
#
# run the console in interactive mode and run a test script
docker run \
  --network=host \
  -v`pwd`/test/data:/test/data \
  -it datafusionrs/console:latest \
 --script /test/data/smoketest.sql \
 > $output_file


echo "###### Verifying smoke test results"

file_diff="$(diff -bBZ -I seconds $output_file $expected_file)"
if [ -n "$file_diff" ]
then
   echo "${file_diff}"
   echo "ERROR: smoke test output differs from expected output"
   exit 1
fi
echo "SUCCESS: smoke test successfully executed"

