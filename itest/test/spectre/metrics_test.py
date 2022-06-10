import json
import time
from collections import namedtuple

import pytest

from util import get_through_spectre
from util import get_from_spectre

METRICS_FILE = '/var/log/metrics/metrics.log'

Metric = namedtuple('Metric', ['dimensions', 'value', 'type'])


@pytest.fixture
def log_file():
    with open(METRICS_FILE, 'r') as fp:
        # Ignore all the lines logged before the start of the test
        fp.readlines()
        yield fp


def _parse_dimensions(dims_str):
    # meteorite dimensions are encoded as a list of list
    # let's convert them back to a map so they're easier to check
    dims_map = {}
    dims_list = json.loads(dims_str)
    for el in dims_list:
        dims_map[el[0]] = el[1]

    return dims_map


def _load_metrics(log_file):
    metrics = []
    # Metrics are emitted after the response is sent back to the client, so we need
    # to wait a little bit before checking the file.
    time.sleep(0.2)
    lines = log_file.readlines()

    for line in lines:
        # statsd line format: <metric>:<value>|<type>
        dimensions, rest = line.strip().split(':')
        value, metric_type = rest.split('|')
        metrics.append(Metric(_parse_dimensions(dimensions), value, metric_type))

    return metrics


def _assert_request_timing_metrics(metrics, cache_name, cache_status, casper_version):
    assert len(metrics) == 4
    assert metrics[0].dimensions == {
        'status': '200',
        'metric_name': 'spectre.request_timing',
        'habitat': 'uswest1a',
        'service_name': 'spectre',
        'namespace': 'backend.main',
        'instance_name': 'itest',
        'cache_name': cache_name,
        'cache_status': cache_status,
        'casper_version': casper_version,
    }
    assert metrics[1].dimensions == {
        'status': '200',
        'metric_name': 'spectre.request_timing',
        'habitat': 'uswest1a',
        'service_name': 'spectre',
        'namespace': '__ALL__',
        'instance_name': 'itest',
        'cache_name': cache_name,
        'cache_status': cache_status,
        'casper_version': casper_version,
    }
    assert metrics[2].dimensions == {
        'status': '200',
        'metric_name': 'spectre.request_timing',
        'habitat': 'uswest1a',
        'service_name': 'spectre',
        'namespace': 'backend.main',
        'instance_name': 'itest',
        'cache_name': '__ALL__',
        'cache_status': cache_status,
        'casper_version': casper_version,
    }
    assert metrics[3].dimensions == {
        'status': '200',
        'metric_name': 'spectre.request_timing',
        'habitat': 'uswest1a',
        'service_name': 'spectre',
        'namespace': '__ALL__',
        'instance_name': 'itest',
        'cache_name': '__ALL__',
        'cache_status': cache_status,
        'casper_version': casper_version,
    }

def _assert_fetch_hit_rate(metrics, cache_name, backend):

    casper_version = metrics[0].dimensions['casper_version']

    assert len(metrics) == 2

    # Not yet enabled for V2
    #
    # assert metrics[0].dimensions == {
    #     'metric_name': 'spectre.fetch_body_and_headers',
    #     'habitat': 'uswest1a',
    #     'service_name': 'spectre',
    #     'namespace': 'backend.main',
    #     'instance_name': 'itest',
    #     'cache_name': cache_name,
    #     'cache_status': 'miss',
    #     'backend': backend,
    #     'casper_version': casper_version
    # }

    expected_metrics = ['spectre.hit_rate']
    for metric in metrics:
        if metric.dimensions['metric_name'] == 'spectre.hit_rate':
            assert metric.dimensions == {
                'metric_name': 'spectre.hit_rate',
                'habitat': 'uswest1a',
                'service_name': 'spectre',
                'namespace': 'backend.main',
                'instance_name': 'itest',
                'cache_name': cache_name,
                'cache_status': 'miss',
                'backend': backend,
                'casper_version': casper_version
            }
            expected_metrics.remove('spectre.hit_rate')

    assert len(expected_metrics) == 0


def _assert_store_metric(metric, cache_name, backend):

    casper_version = metric.dimensions['casper_version']

    assert metric.dimensions == {
        'metric_name': 'spectre.store_body_and_headers',
        'habitat': 'uswest1a',
        'service_name': 'spectre',
        'namespace': 'backend.main',
        'instance_name': 'itest',
        'cache_name': cache_name,
        'backend': backend,
        'casper_version': casper_version
    }


def test_cache_miss(log_file):

    response = get_through_spectre(
        '/timestamp/',
    )
    cache_status = response.headers['Spectre-Cache-Status']
    assert response.status_code == 200
    assert cache_status == 'miss'

    metrics = _load_metrics(log_file)
    backend = 'redis'

    # First 2 metrics are `spectre.fetch_body_and_headers` and `spectre.hit_rate`
    _assert_fetch_hit_rate(metrics[0:2], 'timestamp', backend)

    # Then since it's a miss we have a `spectre.store_body_and_headers`
    #
    # Not yet enabled for V2
    #_assert_store_metric(metrics[2], 'timestamp', backend)

    # Finally the `spectre.request_timing`
    # Due to a different set of enabled metrics features and sequence
    # The V1/V2 metrics log differs
    casper_version = metrics[0].dimensions['casper_version']
    if casper_version == 'v1':
        _assert_request_timing_metrics(metrics[3:7], 'timestamp', cache_status, casper_version)
    else:
        _assert_request_timing_metrics(metrics[1:5], 'timestamp', cache_status, casper_version)

    # # This assert is mostly there to make sure there are no more metrics than what I expect.
    # # The reason why it's not before the other asserts is because the error message doesn't
    # # show you what metrics are actually in the list, so it's very annoying to figure out
    # # what's missing. You'd need to comment out this check and then verify which of the
    # # other asserts is failing.
    # assert len(metrics) == 7


def test_bulk_endpoint_miss(log_file):
    response = get_through_spectre(
        '/bulk_requester_2/10,11/v1?foo=bar',
    )
    cache_status = response.headers['Spectre-Cache-Status']
    time.sleep(1)
    assert response.status_code == 200
    assert cache_status == 'miss'

    metrics = _load_metrics(log_file)
    # Dynamically discover the backend
    backend = metrics[0].dimensions['backend']
    casper_version = metrics[0].dimensions['casper_version']

    # We have `spectre.fetch_body_and_headers` and `spectre.hit_rate` twice
    # since we have 2 ids in the url.
    _assert_fetch_hit_rate(metrics[0:2], 'bulk_requester_default', backend)
    _assert_fetch_hit_rate(metrics[2:4], 'bulk_requester_default', backend)

    # Then we have 2 `spectre.store_body_and_headers`
    _assert_store_metric(metrics[4], 'bulk_requester_default', backend)
    _assert_store_metric(metrics[5], 'bulk_requester_default', backend)

    # Then the `spectre.request_timing`
    _assert_request_timing_metrics(metrics[6:10], 'bulk_requester_default', cache_status, casper_version)

    # Finally we have `spectre.bulk_hit_rate`
    assert metrics[10].dimensions == {
        'metric_name': 'spectre.bulk_hit_rate',
        'habitat': 'uswest1a',
        'service_name': 'spectre',
        'namespace': 'backend.main',
        'instance_name': 'itest',
        'cache_name': 'bulk_requester_default',
        'cache_status': 'miss',
        'casper_version': casper_version,
    }

    # This assert is mostly there to make sure there are no more metrics than what I expect.
    # The reason why it's not before the other asserts is because the error message doesn't
    # show you what metrics are actually in the list, so it's very annoying to figure out
    # what's missing. You'd need to comment out this check and then verify which of the
    # other asserts is failing.
    assert len(metrics) == 11


def test_no_cache_header_metrics(log_file):
    response = get_through_spectre(
        '/timestamp/',
        extra_headers={'Pragma': 'spectre-no-cache'},
    )
    assert response.status_code == 200

    metrics = _load_metrics(log_file)
    # Dynamically discover the backend
    backend = metrics[0].dimensions['backend']
    casper_version = metrics[0].dimensions['casper_version']


    # Since we send the no-cache header we don't have a `spectre.fetch_body_and_headers`
    # or `spectre.hit_rate`. We still update the cache though, so we have the
    # `spectre.store_body_and_headers`
    #
    # Not yet enabled for V2
    #_assert_store_metric(metrics[0], 'timestamp', backend)

    # Finally we emit the `spectre.no_cache_header`

    assert metrics[1].dimensions == {
        'metric_name': 'spectre.no_cache_header',
        'habitat': 'uswest1a',
        'service_name': 'spectre',
        'namespace': 'backend.main',
        'instance_name': 'itest',
        'reason': 'no-cache-header',
        'cache_name': 'timestamp',
        'casper_version': casper_version,
    }

    # This assert is mostly there to make sure there are no more metrics than what I expect.
    # The reason why it's not before the other asserts is because the error message doesn't
    # show you what metrics are actually in the list, so it's very annoying to figure out
    # what's missing. You'd need to comment out this check and then verify which of the
    # other asserts is failing.
    assert len(metrics) == 2

def test_prometheus_metrics_endpoint():
    # Send an initial request to generate metrics
    response = get_through_spectre('/timestamp/cached')

    # Test the metrics endpoint
    response = get_from_spectre('/metrics')
    assert response.status_code == 200
    assert "storage_requests_total" in response.text
