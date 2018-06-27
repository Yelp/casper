FROM ubuntu:bionic
LABEL maintainer="Yelp Performance Team"

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    python3

COPY listen.py /opt/listen.py

CMD python3 /opt/listen.py
