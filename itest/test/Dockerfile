FROM ubuntu:xenial
LABEL maintainer="Yelp Performance Team"

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    python-dev \
    python-setuptools \
    python-pip && \
    apt-get clean

RUN pip install -U pip wheel

RUN pip install \
    bravado \
    pytest \
    requests

RUN mkdir -p /code
WORKDIR /code

ADD . /code
RUN chmod 777 -R /code
