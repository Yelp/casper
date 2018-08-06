#
# Run cassandra on the default port, with default config
#

FROM    ubuntu:bionic

RUN     DEBIAN_FRONTEND=noninteractive apt-get update \
        && DEBIAN_FRONTEND=noninteractive apt-get -y install \
            curl \
            gnupg2 \
            openjdk-8-jdk \
            wget

RUN     curl https://www.apache.org/dist/cassandra/KEYS | apt-key add -
RUN     echo "deb http://www.apache.org/dist/cassandra/debian 311x main" | tee -a /etc/apt/sources.list.d/cassandra.sources.list

RUN     DEBIAN_FRONTEND=noninteractive apt-get update \
        && DEBIAN_FRONTEND=noninteractive apt-get -y install \
            cassandra-tools \
            cassandra \
        && apt-get clean

# Manually install dumb-init as it's not in the public APT repo
RUN     wget https://github.com/Yelp/dumb-init/releases/download/v1.2.1/dumb-init_1.2.1_amd64.deb
RUN     dpkg -i dumb-init_*.deb

ADD     ./setup.sh                      /opt/setup.sh
ADD     ./cassandra.yaml                /etc/cassandra/cassandra.yaml
ADD     ./cassandra-rackdc.properties   /etc/cassandra/cassandra-rackdc.properties
ADD     ./setup.cql                     /etc/cassandra/setup.cql
ADD     ./spectre_db.cache_store.cql    /etc/cassandra/spectre_db.cache_store.cql

RUN     useradd dckruser -m -s /bin/bash
RUN     chown -R dckruser /etc/cassandra/
RUN     chown -R dckruser /var/log/cassandra/

RUN     mkdir -p /nail/cassandra
RUN     chown -R dckruser /nail/cassandra/

# Disable waiting for gossip, which is useless in a one node cluster
RUN     echo "JVM_OPTS=\"\$JVM_OPTS -Dcassandra.skip_wait_for_gossip_to_settle=0\"" >> /etc/cassandra/cassandra-env.sh

USER    dckruser

# Creates cassandra keyspace and tables
RUN     cassandra && /opt/setup.sh

# CMD only provides a default command to run when you start a container from this image.
# It doesn't get executed during the build, so the 2 cassandra processes won't conflict.
CMD     ["dumb-init", "cassandra", "-f"]
