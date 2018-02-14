#!/usr/bin/env bash

set -e

apt-get install -yq iptables

echo 'Adding iptable rule to drop all traffic to :9042'
iptables -A OUTPUT -p tcp --destination-port 9042 -j DROP
