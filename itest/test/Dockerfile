FROM    ubuntu:bionic
LABEL   maintainer="Yelp Performance Team"

ARG     PIP_INDEX_URL

RUN     DEBIAN_FRONTEND=noninteractive apt-get update \
        && DEBIAN_FRONTEND=noninteractive apt-get install -y \
            curl \
            python3-dev \
            python3-setuptools \
            virtualenv \
        && apt-get clean

RUN     mkdir -p /code
WORKDIR /code

RUN     virtualenv --no-download -p python3.6 venv
ENV     PATH /code/venv/bin:$PATH
RUN     pip install -i $PIP_INDEX_URL -U \
            pip==19.3.1 \
            wheel==0.31.1

COPY    requirements.txt requirements.txt
RUN     pip install -i $PIP_INDEX_URL -r requirements.txt

RUN curl https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh > /tmp/wait-for-it.sh
RUN chmod +x /tmp/wait-for-it.sh

COPY    . /code
RUN     chmod 777 -R /code

ENTRYPOINT ["/tmp/wait-for-it.sh", "10.5.0.2:8888", "--"]
