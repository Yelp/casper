FROM docker-dev.yelpcorp.com/xenial_yelp:latest
MAINTAINER Performance Team <team-perf@yelp.com>

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    python \
    python-setuptools \
    python-pkg-resources \
    python-pip && \
    apt-get clean

RUN pip install pytest
RUN pip install requests
RUN pip install -i https://pypi.yelpcorp.com/simple/ thriftpy bravado bravado-decorators

RUN mkdir -p /code
WORKDIR /code

ADD . /code
RUN chmod 777 -R /code
