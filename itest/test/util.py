import time

import requests
from bravado.client import SwaggerClient

# Defined by config/nginx.conf (`listen` directive)
SPECTRE_PORT = 8888
# Defined statically in itest/docker-compose.yaml
SPECTRE_HOST = '10.5.0.2'

SPECTRE_BASE_URL = 'http://{host}:{port}'.format(host=SPECTRE_HOST, port=SPECTRE_PORT)
BACKEND_NAMESPACE = 'backend.main'

HAPROXY_ADDED_HEADERS = {
    'X-Smartstack-Destination': BACKEND_NAMESPACE,
}

NUM_ATTEMPTS_WHEN_GETTING_FROM_CACHE = 2


def get_spectre_swagger_client():
    return SwaggerClient.from_url(SPECTRE_BASE_URL + '/swagger.json')


spectre_swagger_client = get_spectre_swagger_client()


def get_through_spectre(path, extra_headers=None):
    """This simulates a client GETting a resource from a service proxied
    through Spectre.
    """
    headers = HAPROXY_ADDED_HEADERS.copy()
    if extra_headers is not None:
        headers.update(extra_headers)
    return requests.get(SPECTRE_BASE_URL + path, headers=headers)


def assert_is_in_spectre_cache(*args, **kwargs):
    """Because Cassandra writes are performed post request we can't be
    guaranteed that an object is in the cache by the time a cacheable response
    is returned from Spectre.
    This methods is here to:
        * assert that X-Spectre-Cache-Status is "hit"
        * if not, retry the request N-1 times
        * if, after N attempts, the status is still "miss": raise AssertionError
    Args/kwargs passed here are passed through to `get_through_spectre`
    """
    for _ in range(NUM_ATTEMPTS_WHEN_GETTING_FROM_CACHE):
        if 'data' in kwargs:
            response = post_through_spectre(*args, **kwargs)
        else:
            response = get_through_spectre(*args, **kwargs)
        if response.headers['Spectre-Cache-Status'] == 'hit':
            return response
        else:
            continue
    raise AssertionError(
        "No hit after {num_attempts} attempts (headers: '{headers}')".format(
            num_attempts=NUM_ATTEMPTS_WHEN_GETTING_FROM_CACHE,
            headers=response.headers,
        )
    )


def head_through_spectre(path, extra_headers=None):
    """This simulates a client HEADing a resource from a service proxied
    through Spectre.
    """
    headers = HAPROXY_ADDED_HEADERS.copy()
    if extra_headers is not None:
        headers.update(extra_headers)
    return requests.head(SPECTRE_BASE_URL + path, headers=headers)


def post_through_spectre(path, data=None, extra_headers=None):
    """This simulates a client POSTing to a service proxied
    through Spectre.
    """
    headers = HAPROXY_ADDED_HEADERS.copy()
    if extra_headers is not None:
        headers.update(extra_headers)
    return requests.post(SPECTRE_BASE_URL + path, data=data, headers=headers)


def purge_resource(args):
    return spectre_swagger_client.purge.purge(**args).result()


def get_from_spectre(path):
    """Simulates a client directly talking to spectre. No Smartstack headers
    added here."""
    return requests.get(SPECTRE_BASE_URL + path)


def get_timestamp(path, extra_headers=None):
    """Convenience function to make a request through spectre and return only
    the 'timestamp' key of the returned json.
    """
    return get_through_spectre(path, extra_headers).json()['timestamp']


def get_timestamp_until_hit(path, extra_headers=None):
    """Makes repeated request to the same URL and retries until
    it gets a cache hit (or until `num_tries` attempts are made,
    in which case an AssertionError is raised).

    Return value is a tuple composed of:
        hit_value: value of the timestamp at the first cache hit
        miss_values: values of the timestamp during cache misses
    """
    miss_values = []
    num_tries = 4

    for _ in range(num_tries):
        response = get_through_spectre(path, extra_headers)
        if response.headers['Spectre-Cache-Status'] == 'hit':
            return response.json()['timestamp'], miss_values
        else:
            miss_values.append(response.json()['timestamp'])
        # Sleep just a little before retrying
        time.sleep(0.1)

    raise AssertionError(
        "Could not get a cache hit after {num} attemtps".format(num=num_tries)
    )
