#!/bin/bash

SRV_CONFIGS_PATH=${SRV_CONFIGS_PATH:-/nail/srv/configs/spectre}
SERVICES_YAML_PATH=${SERVICES_YAML_PATH:-/nail/etc/services/services.yaml}
CASSANDRA_CLUSTER_CONFIG=${CASSANDRA_CLUSTER_CONFIG:-/var/run/synapse/services/cassandra_spectre.main.json}
SYSLOG_HOST=${SYSLOG_HOST:-169.254.255.254}
METEORITE_WORKER_PORT=${METEORITE_WORKER_PORT:-$(cat /nail/etc/services/statsite/port)}
# We run 4 workers in production. See yelpsoa-configs repo.
WORKER_PROCESSES=${WORKER_PROCESSES:-4}

if [ $ITEST ]; then
    SYSLOG_HOST=$(getent hosts syslog2scribe | awk '{ print $1 }')
    # Replace the error_log directive with a path to a file so we can
    # test syslog output in itests. /var/log/nginx is created in Dockerfile
    sed "s@error_log syslog.*@error_log /var/log/nginx/error.log warn;@" -i config/nginx.conf
    # Lower http timeout to test that we return a 504 on timeouts
    HTTP_TIMEOUT_MS=1000
elif [ $ACCEPTANCE ]; then
    # Cassandra ip is automatically generated
    host=$(grep 'cassandra_spectre.main:' /nail/etc/services/services.yaml | cut -d' ' -f3 | cut -d',' -f1)
    echo '[{"name": "cassandra-spectre-itest","host": "'$host'","port": 9042,"id": 1,"weight": 10}]' > $CASSANDRA_CLUSTER_CONFIG
fi

SRV_CONFIGS_PATH=$SRV_CONFIGS_PATH \
    SERVICES_YAML_PATH=$SERVICES_YAML_PATH \
    CASSANDRA_CLUSTER_CONFIG=$CASSANDRA_CLUSTER_CONFIG \
    METEORITE_WORKER_PORT=$METEORITE_WORKER_PORT \
    SYSLOG_HOST=$SYSLOG_HOST \
    HTTP_TIMEOUT_MS=$HTTP_TIMEOUT_MS \
    /usr/local/openresty/nginx/sbin/nginx \
        -c /code/config/nginx.conf \
        -g "worker_processes $WORKER_PROCESSES;"
