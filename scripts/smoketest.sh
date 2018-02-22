#!/usr/bin/env bash
cargo test
cargo run --example sql_query
cargo run --example dataframe
./scripts/docker/worker/build.sh
./scripts/docker/console/build.sh

# stop etcd
docker kill etcd
docker rm etcd

# stop datafusion worker
docker kill datafusion
docker rm datafusion

# run etcd
docker run -d -v /usr/share/ca-certificates/:/etc/ssl/certs -p 4001:4001 -p 2380:2380 -p 2379:2379 \
 --name etcd quay.io/coreos/etcd:v2.3.8 \
 -name etcd0 \
 -advertise-client-urls http://${HostIP}:2379,http://${HostIP}:4001 \
 -listen-client-urls http://0.0.0.0:2379,http://0.0.0.0:4001 \
 -initial-advertise-peer-urls http://${HostIP}:2380 \
 -listen-peer-urls http://0.0.0.0:2380 \
 -initial-cluster-token etcd-cluster-1 \
 -initial-cluster etcd0=http://${HostIP}:2380 \
 -initial-cluster-state new

# give etcd a chance to start up
sleep 2

# run worker
docker run --network=host -d -p 8088:8088 \
 -v`pwd`/test/data:/var/datafusion/data \
 --name datafusion datafusionrs/worker:latest \
 --etcd http://127.0.0.1:2379 \
 --bind 127.0.0.1:8088 \
 --data_dir /var/datafusion/data \
 --webroot /opt/datafusion/www

# give the worker a chance to start up
sleep 2

# run the console in interactive mode and run a test script
docker run \
  --network=host \
  -v`pwd`/test/data:/test/data \
  -it datafusionrs/console:latest \
 --etcd http://127.0.0.1:2379 \
 --script /test/data/smoketest.sql \
 > _smoketest.txt


# did we get the expected results?
grep -v seconds _smoketest.txt > _a.txt
grep -v seconds test/data/smoketest-expected.txt > _b.txt
diff -b _a.txt _b.txt

# clean up
rm -f _*.txt 2>/dev/null
rm -f _*.csv 2>/dev/null
