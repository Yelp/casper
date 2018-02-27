# Casper (a friendly Spectre)

[![Build Status](https://travis-ci.org/Yelp/casper.svg?branch=master)](https://travis-ci.org/Yelp/casper)

This repo contains Casper, a caching proxy internal to Yelp. It is built with
Nginx and Openresty at its core and contains some logic in Lua to fit in our
ecosystem.

_Important note about naming: Casper was initially baptized "Spectre"
internally._ It's only after the [Spectre/Meltdown](https://spectreattack.com/)
vulnerabilities were published that we decided to change the name to "Casper", a
more friendly and less confusing name for the outside world. If you see
remnants of "Spectre" in the code, that's why!

## Requirements

Casper uses Docker. Make sure it's installed on your system by following the
instruction for your platform [here](https://docs.docker.com/install/).

We also use `make` targets to test and build Casper (see below), so ensure you
can run make targets with your OS (on Windows, use `nmake`.)

## Building locally


To start Casper, run

    $ make dev

To make sure it's up (change the port accordingly if you change the default):

    $ curl -v localhost:32927/status

For basic debugging (is it missing/hitting the cache? Am I being correctly
proxied? etc), curl, and pay attention to the "Spectre-Cache-Status" header:

    $ curl -o /dev/null -iv -H 'X-Source-Id: test' -H 'X-Smartstack-Destination: yelp-main.internalapi' -H 'X-Smartstack-Source: spectre.main' -H 'Host: internalapi' 'localhost:32927/category_yelp/?locale=en_US' 2>&1 | grep 'Spectre-Cache-Status'
    < Spectre-Cache-Status: hit

To debug deeper it's highly recommended that you hop in the docker container
in which nginx/lua are running with:

    $ make inspect

It can help, to get as much information as possible, to set nginx logging to
debug granularity. In `nginx.conf`, replace the `error_log` directive by:

    error_log /var/log/nginx.debug.log debug;

## Running Tests

To run unit tests:

    $ make test

To run integration tests:

    $ make itest

## Contributors

Casper was built and developed internally for about a year before being
opensourced. In addition to the [contributors listed on
Github](https://github.com/Yelp/casper/graphs/contributors), here is a list
of people who contributed to Casper before its public life:

* Angelo Licastro ([@angelolicastro](https://github.com/angelolicastro))
* Anthony Tan ([@DevelopingCoder](https://github.com/DevelopingCoder))
* Arnaud Brousseau ([@arnaudbrousseau](https://github.com/arnaudbrousseau))
* Avadhut Phatarpekar ([@avadhutp](https://github.com/avadhutp))
* Bai Li ([@luckytoilet](https://github.com/luckytoilet))
* Ben Plotnick ([@bplotnick](https://github.com/bplotnick))
* Chris Kuehl ([@chriskuehl](https://github.com/chriskuehl))
* Daniele Rolando ([@drolando](https://github.com/drolando))
* Ishan Vashishtha ([@ishanvas](https://github.com/ishanvas))
* Mayank Sindwani ([@msindwan](https://github.com/msindwan))
* Michael Bryant ([@mjbryant](https://github.com/mjbryant))

## Contributing

We welcome contributions to Casper, but keep in mind that this is a production
system run inside of Yelp's infrastructure. Please get in touch with us to
discuss your feature request, bug fix or enhancement by opening a Github issue.

## TODOs

* [ ] verify `make test`
* [ ] try out the instructions to run Casper on a stock macbook
