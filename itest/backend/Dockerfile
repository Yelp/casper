FROM ubuntu:bionic
LABEL maintainer="Yelp Performance Team"

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    python \
    python-yaml && \
    apt-get clean

RUN mkdir -p /code
WORKDIR /code

ADD . /code
RUN chmod 777 -R /code

CMD python echo_server.py
