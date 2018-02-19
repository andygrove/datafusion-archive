#!/usr/bin/env bash
echo Starting DataFusion worker node ...
#TODO --bind 0.0.0.0 --register 127.0.0.1
/opt/datafusion/bin/worker --etcd http://127.0.0.1:2379 --webroot /opt/datafusion/www --data_dir /tmp