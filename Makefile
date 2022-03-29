export PWD=$(shell pwd)
export TEST_LUA=$(PWD)/testing/luajit
export DOCKER_TAG ?= spectre-dev-$(USER)
export PORT ?= 32927
export DOCKER_COMPOSE_YML ?= itest/docker-compose.yml
export SRV_CONFIGS_PATH_FOR_TESTS=$(PWD)/tests/data/srv-configs
export ENVOY_CONFIGS_PATH_FOR_TESTS=$(PWD)/tests/data/srv-configs
export SMARTSTACK_CONFIG_PATH_FOR_TESTS=$(PWD)/tests/data/services.yaml
export GIT_SHA ?= $(shell git rev-parse --short HEAD)
export PIP_INDEX_URL ?= https://pypi.org/simple

DOCKER_COMPOSE_VERSION := 1.19.0
DOCKER_COMPOSE := bin/docker-compose-$(DOCKER_COMPOSE_VERSION)

.PHONY: all
all: test itest dev

.PHONY: dev
dev: minimal
	@mkdir -p logs
	@PAASTA_SERVICE=casper \
		PAASTA_INSTANCE=test \
		./start.sh

.PHONY: test
test: deps
	rm -f luacov.stats.out luacov.report.out
	@ls -1 tests/lua/*.lua | xargs -i sh -c "\
		echo 'Running tests for {}'; \
		ENVOY_CONFIGS_PATH=$(ENVOY_CONFIGS_PATH_FOR_TESTS) \
		PAASTA_SERVICE=spectre \
		PAASTA_INSTANCE=test \
		SRV_CONFIGS_PATH=$(SRV_CONFIGS_PATH_FOR_TESTS) \
		SERVICES_YAML_PATH=$(SMARTSTACK_CONFIG_PATH_FOR_TESTS) \
		METEORITE_WORKER_PORT='-1' \
		./luawrapper resty {}"

	./luawrapper luacheck lua --exclude-files lua/vendor/*
	./luawrapper luacov
	@# We only want to print the summary, not the entire file
	@# "grep -n" returns the line number, then we print all successive lines with awk
	@awk "NR>$$(grep -n 'Summary' luacov.report.out | cut -d':' -f1)" luacov.report.out

$(DOCKER_COMPOSE):
	# From https://docs.docker.com/compose/install/#prerequisites
	# docker-compose is a statically linked go binary, so we can simply download the binary and use it
	curl -L https://github.com/docker/compose/releases/download/$(DOCKER_COMPOSE_VERSION)/docker-compose-`uname -s`-`uname -m` -o $(DOCKER_COMPOSE)
	chmod +x $(DOCKER_COMPOSE)

.PHONY: itest
itest: clean-docker $(DOCKER_COMPOSE) cook-image run-itest

.PHONY: run-itest
run-itest: $(DOCKER_COMPOSE)
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) build
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) up -d spectre backend syslog redis_1 redis_2
	sleep 10
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) run test python3 -m pytest -vv spectre
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) exec --user=root -T spectre /opt/drop_all.sh
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) kill
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) rm -f

.PHONY: cook-image
cook-image: clean-docker
	docker build -f Dockerfile.opensource -t $(DOCKER_TAG) .

.PHONY: clean
clean: clean-docker
	rm -rf .cache
	rm -rf luarocks
	rm -rf resty_modules

.PHONY: clean-docker
clean-docker: $(DOCKER_COMPOSE)
	@echo "Cleaning $(DOCKER_TAG)"
	docker stop $(DOCKER_TAG) || true
	docker rm $(DOCKER_TAG) || true
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) kill
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) rm -f

luarocks: luarocks-dependencies.txt
	rm -rf $@
	mkdir $@
	# Pin lua version so that it can work on macos' homebrew lua
	cat luarocks-dependencies.txt | xargs -L 1 luarocks install --lua-version=5.1 --tree=$@
	# Symlink in our Lua executable so that scripts that install with
	# a "#!/usr/bin/env lua" shebang will get the right interpreter.
	ln -s /usr/local/openresty/luajit/bin/luajit $@/bin/lua
	ln -s /usr/local/openresty/luajit/bin/luajit $@/bin/luajit
	ln -s $(CURDIR)/busted $@/bin/busted-resty
	ln -s /usr/bin/resty $@/bin/resty

casper_v2:
	cargo build --release
	cp -f target/release/casper-runtime luarocks/bin/casper-runtime

resty_modules: opm-dependencies.txt
	rm -rf $@
	mkdir $@
	cat opm-dependencies.txt | xargs -L 1 opm --cwd get

.PHONY: minimal
minimal: deps

.PHONY: deps
deps: luarocks resty_modules casper_v2
