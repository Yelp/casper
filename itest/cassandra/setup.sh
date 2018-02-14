#!/bin/bash

set -e

# Wait for cassandra to start
delay=1
timeout=60
echo "Checking for cassandra receiving connections..."

#  Checking `nc -w 1 127.0.0.1 9042` is not sufficient as on startup cassandra
#  immediately bounds to the port (and netcat succeeds) but accepts client
#  connections only after it has been completely setup.
while ! (nodetool info 2>&1 | grep "Thrift active" | grep true > /dev/null); do
    timeout=$(expr $timeout - $delay)

    if [ $timeout -eq 0 ]; then
        echo "Timeout error occurred waiting on Cassandra."
        exit 1
    fi
    sleep $delay
done

echo 'create keyspace spectre_db'
cqlsh -f /etc/cassandra/setup.cql

echo 'create table cache_db'
cqlsh -k spectre_db -f /etc/cassandra/spectre_db.cache_store.cql

echo 'write in-memory data to disk'
nodetool flush spectre_db
