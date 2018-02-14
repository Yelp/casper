FROM docker-dev.yelpcorp.com/xenial_yelp:latest
MAINTAINER Performance Team <team-perf@yelp.com>

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    syslog2scribe && \
    apt-get clean

COPY syslog2scribe.yaml /etc/syslog2scribe.yaml

CMD syslog2scribe --listen-addrs 10.5.0.5:1514 --scribe-host 10.5.0.6
