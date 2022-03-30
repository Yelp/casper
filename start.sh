#!/usr/bin/env bash

SRV_CONFIGS_PATH=${SRV_CONFIGS_PATH:-/nail/srv/configs/spectre}
SERVICES_YAML_PATH=${SERVICES_YAML_PATH:-/nail/etc/services/services.yaml}
ENVOY_CONFIGS_PATH=${ENVOY_CONFIGS_PATH:-/nail/srv/configs}
# We run 2 worker per container in production
WORKER_PROCESSES=${WORKER_PROCESSES:-2}
NGINX_CONF=config/nginx.conf

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

cleanup() {
    # kill all processes whose parent is this process
    pkill -P $$
}

for sig in INT QUIT HUP TERM; do
  trap "
    cleanup
    trap - $sig EXIT
    kill -s $sig "'"$$"' "$sig"
done
trap cleanup EXIT

echo "Starting Casper.v2"
backend() {
    while true; do
        ./luarocks/bin/casper-runtime
    done
}
backend &

echo "Starting casper"

SRV_CONFIGS_PATH=$SRV_CONFIGS_PATH \
    SERVICES_YAML_PATH=$SERVICES_YAML_PATH \
    ENVOY_CONFIGS_PATH=$ENVOY_CONFIGS_PATH \
    /usr/local/openresty/nginx/sbin/nginx \
        -c $NGINX_CONF \
        -p $(pwd) \
        -g "worker_processes $WORKER_PROCESSES;"
