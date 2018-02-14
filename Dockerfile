FROM docker-dev.yelpcorp.com/xenial_yelp:latest
LABEL maintainer="Yelp Performance Team"

# Set timezone to YST
RUN ln -fs /usr/share/zoneinfo/US/Pacific /etc/localtime && \
    DEBIAN_FRONTEND=noninteractive dpkg-reconfigure tzdata

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        dnsutils \
        dumb-init \
        git \
        libluajit-5.1-2 \
        # libyaml-dev is needed to install lyaml
        libyaml-dev \
        luarocks \
        # for installing the add-apt-repository command
        software-properties-common \
        tzdata \
        unzip \
        wget

# Install openresty from openresty's public APT repo
RUN wget -qO - https://openresty.org/package/pubkey.gpg | apt-key add -
RUN add-apt-repository -y "deb http://openresty.org/package/ubuntu $(lsb_release -sc) main"
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    openresty=1.13.6.1-1~xenial1 \
    openresty-opm=1.13.6.1-1~xenial1 \
    openresty-resty=1.13.6.1-1~xenial1

RUN apt-get clean

# We directly pin both lua and python dependencies to allow for reproducible
# deploy builds. See PERF-1454.
RUN luarocks install lyaml 6.1.1-4
RUN luarocks install luasocket 3.0rc1-2
RUN luarocks install luafilesystem 1.6.3-2
RUN luarocks install lua-resty-http 0.10-0
RUN luarocks install crc32 1.0
RUN luarocks install busted 2.0.rc12-1
RUN luarocks install cluacov
RUN luarocks install luacheck
RUN opm get detailyang/lua-resty-rfc5424=0.1.0

RUN mkdir -p /code
WORKDIR /code

# Revert after https://github.com/thibaultcha/lua-cassandra/pull/104 gets merged.
# Ticket: PERF-2005
ADD lua-cassandra-dev-0.rockspec lua-cassandra-dev-0.rockspec
RUN luarocks build lua-cassandra-dev-0.rockspec

ADD . /code

# Change ownership of code folders
RUN chown -R nobody:nogroup /code /usr/local/openresty

# See https://github.com/moby/moby/issues/2259
# This folder is used as a volume in itests
RUN mkdir -p /var/log/nginx
RUN chown -R nobody:nogroup /var/log/nginx

USER nobody
CMD ["dumb-init", "/code/start.sh"]
