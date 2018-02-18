FROM ubuntu:xenial
LABEL maintainer="Yelp Performance Team"

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    syslog-ng

COPY syslog-ng.conf /etc/syslog-ng/syslog-ng.conf

CMD syslog-ng && while true; do sleep 1; done
