#!/usr/bin/env bash

SRV_CONFIGS_PATH=${SRV_CONFIGS_PATH:-/nail/srv/configs/spectre}
SERVICES_YAML_PATH=${SERVICES_YAML_PATH:-/nail/etc/services/services.yaml}
CASSANDRA_CLUSTER_CONFIG=${CASSANDRA_CLUSTER_CONFIG:-/var/run/synapse/services/cassandra_spectre.main.json}
SYSLOG_HOST=${SYSLOG_HOST:-169.254.255.254}
SYSLOG_PORT=${SYSLOG_PORT:-1514}
METEORITE_WORKER_PORT=${METEORITE_WORKER_PORT:-$(cat /nail/etc/services/statsite/port)}
# We run 1 worker per container in production
WORKER_PROCESSES=${WORKER_PROCESSES:-1}

if [ $ACCEPTANCE ]; then
    # Cassandra ip is automatically generated
    host=$(grep 'cassandra_spectre.main:' /nail/etc/services/services.yaml | cut -d' ' -f3 | cut -d',' -f1)
    echo '[{"name": "cassandra-spectre-itest","host": "'$host'","port": 9042,"id": 1,"weight": 10}]' > $CASSANDRA_CLUSTER_CONFIG
fi

SRV_CONFIGS_PATH=$SRV_CONFIGS_PATH \
    SERVICES_YAML_PATH=$SERVICES_YAML_PATH \
    CASSANDRA_CLUSTER_CONFIG=$CASSANDRA_CLUSTER_CONFIG \
    METEORITE_WORKER_PORT=$METEORITE_WORKER_PORT \
    SYSLOG_HOST=$SYSLOG_HOST \
    SYSLOG_PORT=$SYSLOG_PORT \
    HTTP_TIMEOUT_MS=$HTTP_TIMEOUT_MS \
    /usr/local/openresty/nginx/sbin/nginx \
        -c /code/config/nginx.conf \
        -g "worker_processes $WORKER_PROCESSES;"
