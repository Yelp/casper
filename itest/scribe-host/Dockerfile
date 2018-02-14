FROM docker-dev.yelpcorp.com/xenial_yelp:latest
MAINTAINER Performance Team <team-perf@yelp.com>

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    libthrift0=0.9.1 \
    scribe && \
    apt-get clean

ADD scribe.conf /etc/scribe.conf

CMD scribed -c /etc/scribe.conf
