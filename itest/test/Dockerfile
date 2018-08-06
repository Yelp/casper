FROM    ubuntu:bionic
LABEL   maintainer="Yelp Performance Team"

RUN     DEBIAN_FRONTEND=noninteractive apt-get update \
        && DEBIAN_FRONTEND=noninteractive apt-get install -y \
            python3-dev \
            python3-setuptools \
            virtualenv \
        && apt-get clean

RUN     mkdir -p /code
WORKDIR /code

RUN     virtualenv -p python3.6 venv
ENV     PATH /code/venv/bin:$PATH
RUN     pip install -U \
            pip==18.0 \
            wheel==0.31.1

COPY    requirements.txt requirements.txt
RUN     pip install -r requirements.txt

COPY    . /code
RUN     chmod 777 -R /code
