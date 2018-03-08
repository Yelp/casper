export PWD=$(shell pwd)
export TEST_LUA=$(PWD)/testing/luajit
export DOCKER_TAG ?= spectre-dev-$(USER)
export PORT ?= 32927
export DOCKER_COMPOSE_YML ?= itest/docker-compose.yml
export SRV_CONFIGS_PATH_FOR_TESTS=$(PWD)/tests/data/srv-configs
export SMARTSTACK_CONFIG_PATH_FOR_TESTS=$(PWD)/tests/data/services.yaml
export GIT_SHA ?= $(shell git rev-parse --short HEAD)

DOCKER_COMPOSE_VERSION := 1.19.0
DOCKER_COMPOSE := bin/docker-compose-$(DOCKER_COMPOSE_VERSION)

.PHONY: all
all: test itest dev

.PHONY: dev
dev: cook-image
	docker run -t \
		-p $(PORT):8888 \
		-e "PAASTA_SERVICE=casper" \
		-e "PAASTA_INSTANCE=test" \
		-v /nail/etc:/nail/etc:ro \
		-v /nail/srv/configs/spectre:/nail/srv/configs/spectre:ro \
		-v /var/run/synapse/services/:/var/run/synapse/services/:ro \
		-v $(PWD):/code:ro \
		--name=$(DOCKER_TAG) $(DOCKER_TAG)
	@echo 'Spectre is up and running:'
	@echo '    curl localhost:$(PORT)/status'

.PHONY: inspect
inspect:
	docker exec -ti $(DOCKER_TAG) bash -c 'apt-get install -y --no-install-recommends vim && bash'

.PHONY: test
test: cook-image run-test

.PHONY: run-test
run-test:
	docker run -t \
		-v $(PWD)/tests:/code/tests \
		-v $(CURDIR)/tests/data/etc:/nail/etc \
		$(DOCKER_TAG) bash -c 'make unittest'

.PHONY: unittest
unittest:
	@ls -1 tests/lua/*.lua | xargs -i sh -c "\
		echo 'Running tests for {}'; \
		PAASTA_SERVICE=spectre \
		PAASTA_INSTANCE=test \
		SRV_CONFIGS_PATH=$(SRV_CONFIGS_PATH_FOR_TESTS) \
		SERVICES_YAML_PATH=$(SMARTSTACK_CONFIG_PATH_FOR_TESTS) \
		METEORITE_WORKER_PORT='-1' \
		resty {}"

	@luacheck lua --exclude-files lua/vendor/*
	@luacov
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
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) up -d spectre backend cassandra syslog
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) exec -T cassandra /opt/setup.sh
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) run test python -m pytest -vv spectre
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) exec --user=root -T spectre /opt/drop_all.sh
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) run test python -m pytest -vv cassandra
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) kill
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) rm -f

.PHONY: cook-image
cook-image: clean-docker
	docker build -f Dockerfile.opensource -t $(DOCKER_TAG) .

.PHONY: clean
clean: clean-docker
	rm -rf .cache

.PHONY: clean-docker
clean-docker: $(DOCKER_COMPOSE)
	@echo "Cleaning $(DOCKER_TAG)"
	docker stop $(DOCKER_TAG) || true
	docker rm $(DOCKER_TAG) || true
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) kill
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) rm -f
