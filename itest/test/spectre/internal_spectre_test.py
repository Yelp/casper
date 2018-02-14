# -*- coding: utf-8 -*-
import json

from util import get_from_spectre
from util import get_through_spectre


class TestCanReachStatuses(object):

    def test_can_reach_spectre_status(self):
        response = get_through_spectre('/status')
        assert response.status_code == 200
        assert response.text == 'Backend is alive\n'

        response = get_from_spectre('/status')
        assert response.status_code == 200
        status = json.loads(response.text)
        assert status['cassandra_status'] == 'up'
        assert status['smartstack_configs'] == 'present'
        assert status['spectre_configs'] == 'present'
        assert status['proxied_services'] == {
            'backend.main': {
                'host':'10.5.0.3',
                'port': 9080,
            },
        }


class TestConfigs(object):

    def test_can_get_spectre_configs(self):
        response = get_from_spectre('/configs')
        assert response.status_code == 200
        status = json.loads(response.text)
        # status['service_configs'] is too long and changes too quickly
        # to be worth asserting its entire content
        assert 'long_ttl' in status['service_configs']['backend.main']['cached_endpoints']
        assert status['service_configs']['backend.main']['uncacheable_headers'] == ['Uncacheable-Header']
        assert status['service_configs']['backend.main']['vary_headers'] == ['Accept-Encoding']
        # status['smartstack_configs'] should only contain enabled services
        assert status['smartstack_configs'] == {
            u'backend.main': {u'host': u'10.5.0.3', u'port': 9080},
        }
        # services.yaml and backend.main.yaml
        assert len(status['mod_time_table']) == 2
        assert isinstance(status['worker_id'], int)
