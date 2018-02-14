FROM docker-dev.yelpcorp.com/xenial_yelp:latest
MAINTAINER Performance Team <team-perf@yelp.com>

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    python \
    python-yaml && \
    apt-get clean

RUN mkdir -p /code
WORKDIR /code

ADD . /code
RUN chmod 777 -R /code

CMD python echo_server.py
