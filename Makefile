export PWD=$(shell pwd)
export TEST_LUA=$(PWD)/testing/luajit
export DOCKER_TAG ?= spectre-dev-$(USER)
export PORT ?= 32927
export DOCKER_COMPOSE_YML ?= itest/docker-compose.yml
export SRV_CONFIGS_PATH_FOR_TESTS=$(PWD)/tests/data/srv-configs
export SMARTSTACK_CONFIG_PATH_FOR_TESTS=$(PWD)/tests/data/services.yaml
export GIT_SHA ?= $(shell git rev-parse --short HEAD)

DOCKER_COMPOSE := .tox/docker-compose/bin/docker-compose
ifeq ($(findstring .yelpcorp.com,$(shell hostname -f)), .yelpcorp.com)
	DOCKERFILE ?= Dockerfile
else
	DOCKERFILE ?= Dockerfile.opensource
endif

.PHONY: all
all: test itest dev

.PHONY: dev
dev: cook-image
	docker run -d -t \
		-p $(PORT):8888 \
		-e "MARATHON_HOST=$(shell hostname)" \
		-e "PAASTA_SERVICE=spectre" \
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
test: cook-image
	docker run -t -v $(PWD)/tests:/code/tests -v /nail/etc:/nail/etc:ro $(DOCKER_TAG) bash -c 'make unittest'

.PHONY: unittest
unittest:
	@ls -1 tests/lua/*.lua | xargs -i sh -c "\
		echo 'Running tests for {}'; \
		SRV_CONFIGS_PATH=$(SRV_CONFIGS_PATH_FOR_TESTS) \
		SERVICES_YAML_PATH=$(SMARTSTACK_CONFIG_PATH_FOR_TESTS) \
		METEORITE_WORKER_PORT='-1' \
		resty {}"

	@luacheck lua --exclude-files lua/vendor/*
	@luacov
	@# We only want to print the summary, not the entire file
	@# "grep -n" returns the line number, then we print all successive lines with awk
	@awk "NR>$$(grep -n 'Summary' luacov.report.out | cut -d':' -f1)" luacov.report.out

$(DOCKER_COMPOSE): tox.ini
	tox -e docker-compose --notest

.PHONY: docker_push
docker_push:
	tox -e docker-push

.PHONY: itest
itest: clean-docker $(DOCKER_COMPOSE) cook-image
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) build
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) up -d spectre backend syslog2scribe scribe-host cassandra
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) exec -T cassandra /opt/setup.sh
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) run test python -m pytest -vv spectre
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) exec --user=root -T spectre /opt/drop_all.sh
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) run test python -m pytest -vv cassandra
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) kill
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) rm -f

.PHONY: acceptance
acceptance: cook-image
	tox -e acceptance

.PHONY: cook-image
cook-image: clean-docker
	docker build -f $(DOCKERFILE) -t $(DOCKER_TAG) .

.PHONY: push-swagger-spec-to-registry
push-swagger-spec-to-registry:
	/usr/bin/sensu-shell-helper \
		-t "performance" \
		-n spectre_jenkins_swagger_post \
		-c 100 \
		-j '"email": "True", "runbook": "y/rb-swagger-registry", "source": "yelpsoa-configs.spectre.deploy",' \
		-- swagger post spectre api_docs/swagger.json

.PHONY: clean
clean: clean-docker
	rm -rf playground
	rm -rf .cache
	rm -rf .ycp_playground
	rm -rf .tox

.PHONY: clean-docker
clean-docker: $(DOCKER_COMPOSE)
	@echo "Cleaning $(DOCKER_TAG)"
	docker stop $(DOCKER_TAG) || true
	docker rm $(DOCKER_TAG) || true
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) kill
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_YML) rm -f
