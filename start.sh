#!/usr/bin/env bash

SRV_CONFIGS_PATH=${SRV_CONFIGS_PATH:-/nail/srv/configs/spectre}
SERVICES_YAML_PATH=${SERVICES_YAML_PATH:-/nail/etc/services/services.yaml}
CASSANDRA_CLUSTER_CONFIG=${CASSANDRA_CLUSTER_CONFIG:-/var/run/synapse/services/cassandra_spectre.main.json}
# We run 1 worker per container in production
WORKER_PROCESSES=${WORKER_PROCESSES:-1}
NGINX_CONF=config/nginx.conf

if [ $ACCEPTANCE ]; then
    # Cassandra ip is automatically generated
    host=$(grep 'cassandra_spectre.main:' /nail/etc/services/services.yaml | cut -d' ' -f3 | cut -d',' -f1)
    echo '[{"name": "cassandra-spectre-itest","host": "'$host'","port": 9042,"id": 1,"weight": 10}]' > $CASSANDRA_CLUSTER_CONFIG
fi

if [ "$DISABLE_STDOUT_ACCESS_LOG" = "1" ]; then
    # We already send logs to syslog from the Lua code.
    # To avoid duplicate logging, we can disable stdout access_logs.
    echo -n "Disabling access log on stdout: "
    # Search for exact string to substitue
    # If found, we run the subsitution command and jump to next line
    # If not found, we exit 3
    sed -i -e "/access_log \/dev\/stdout main_spectre;/,\${s//access_log off;/;b};\$q3" $NGINX_CONF

    # Let's exit if the substitution doesn't go exactly as planned
    [[ ! $? == 0 ]] && echo "error disabling stdout access_logs" && exit 1
    echo "done"
fi

echo "Starting casper"

SRV_CONFIGS_PATH=$SRV_CONFIGS_PATH \
    SERVICES_YAML_PATH=$SERVICES_YAML_PATH \
    /usr/local/openresty/nginx/sbin/nginx \
        -c $NGINX_CONF \
        -p $(pwd) \
        -g "worker_processes $WORKER_PROCESSES;"
