# -*- coding: utf-8 -*-
import time

import bravado.exception
import pytest
import requests
import socket

import util
from util import assert_is_in_spectre_cache
from util import get_timestamp
from util import get_timestamp_until_hit
from util import get_from_spectre
from util import get_through_spectre
from util import head_through_spectre
from util import post_through_spectre
from util import purge_resource
from util import SPECTRE_HOST
from util import SPECTRE_PORT


class TestGetMethod(object):

    @pytest.fixture(autouse=True)
    def purge(self):
        time.sleep(1)
        purge_resource({'namespace': 'backend.main', 'cache_name': 'timestamp'})
        purge_resource({'namespace': 'backend.main', 'cache_name': 'long_ttl'})
        time.sleep(1)

    def test_get_request(self):
        response = get_through_spectre('/timestamp/get')
        assert response.status_code == 200
        assert response.json()['method'] == 'GET'

    def test_hop_by_hop_headers(self):
        response = get_through_spectre('/timestamp/get')
        assert response.status_code == 200
        # Our test backend sends 'Connection: close'. Connection being a
        # hop-by-hop header, spectre should not forward it and we expect to see
        # openresty's "keep-alive" instead.
        # See http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.5.1
        assert response.headers['Connection'] == 'keep-alive'

    def test_endpoint_not_cached(self):
        # Only endpoints starting with /timestamp/ are cached
        val1 = get_timestamp('/timestamp_no_cache')
        val2 = get_timestamp('/timestamp_no_cache')
        assert val1 != val2

    def test_endpoint_cached(self):
        hit_value, miss_values = get_timestamp_until_hit('/timestamp/cached')
        assert hit_value in miss_values

    def test_expires_after_ttl(self):
        """We have set the expire times to > 1 sec because the docker image we use Cassandra
        acceptance testing does not run in-memory Cassandra. This needs to be fixed with
        PERF-1679."""

        cached_value, miss_values = get_timestamp_until_hit('/timestamp/ttl')
        assert cached_value in miss_values

        # Cache entry should be expired after 3 seconds
        time.sleep(3)
        new_value = get_timestamp('/timestamp/ttl')
        assert new_value != cached_value

    @pytest.mark.parametrize('header_name, header_value', [
        ('Pragma', 'no-cache'),
        ('Pragma', 'spectre-no-cache'),
        ('Cache-control', 'no-cache'),
        ('X-Strongly-Consistent-Read', '1'),
        ('X-Strongly-Consistent-Read', 'True'),
        ('X-Force-Master-Read', '1'),
        ('X-Force-Master-Read', 'True'),
    ])
    def test_pragma_no_cache(self, header_name, header_value):
        cached_value, miss_values = get_timestamp_until_hit('/timestamp/cached')
        assert cached_value in miss_values

        # Should fetch resource from the master
        no_cache_value = get_timestamp('/timestamp/cached', extra_headers={header_name: header_value})
        assert no_cache_value != cached_value

        # Checking that the new response was cached
        hit_value, miss_values = get_timestamp_until_hit('/timestamp/cached')
        assert hit_value in [no_cache_value] + miss_values

    def test_response_not_cached_if_config_not_present(self):
        val1 =  get_timestamp('/deals')
        val2 =  get_timestamp('/deals')

        assert val1 != val2

    @pytest.mark.parametrize('header_name, header_value', [
        ('Pragma', 'no-cache'),
        ('Pragma', 'spectre-no-cache'),
        ('Cache-control', 'no-cache'),
        ('X-Strongly-Consistent-Read', '1'),
        ('X-Strongly-Consistent-Read', 'True'),
        ('X-Force-Master-Read', '1'),
        ('X-Force-Master-Read', 'True'),
    ])
    def test_response_not_cached_for_req_with_no_cache(self, header_name, header_value):
        val1 =  get_timestamp('/deals', extra_headers={header_name: header_value})
        val2 =  get_timestamp('/deals')

        assert val1 != val2

    def test_error_code_is_passed_back_and_response_not_cached(self):
        response1 = get_through_spectre('/timestamp/no_cache?error_status=500')
        response2 = get_through_spectre('/timestamp/no_cache?error_status=500')

        assert response1.status_code == 500
        assert response2.status_code == 500

    def test_correct_error_code_is_passed_back(self):
        response = get_through_spectre('/timestamp/no_cache?error_status=502')
        assert response.status_code == 502
        assert response.headers['Spectre-Cache-Status'] == 'non-cacheable-response: status code is 502'

    def test_different_vary_headers(self):
        # same vary header
        val1 = get_through_spectre('/timestamp/cached', {'accept-encoding': 'testzip, deflate, custom'})
        assert val1.headers['Spectre-Cache-Status'] == 'miss'
        assert_is_in_spectre_cache('/timestamp/cached', {'accept-encoding': 'testzip, deflate, custom'})

        # different vary headers --> val3 is a miss
        val3 = get_through_spectre('/timestamp/cached', {'accept-encoding': 'none'})
        assert val3.headers['Spectre-Cache-Status'] == 'miss'

        # contains x-mode --> miss
        val4 = get_through_spectre('/timestamp/cached', {'X-Mode': 'ro'})
        assert val4.headers['Spectre-Cache-Status'] == 'miss'

        # Host is not a Vary header --> hit
        assert_is_in_spectre_cache('/timestamp/cached', {'X-Mode': 'ro', 'Host': 'localhost'})

    def test_default_vary_headers(self):
        assert get_through_spectre('/long_ttl/vary').headers['Spectre-Cache-Status'] == 'miss'
        assert assert_is_in_spectre_cache('/long_ttl/vary')

        # namespace vary_headers do not contain X-Mode
        assert assert_is_in_spectre_cache('/long_ttl/vary', {'x-mode': 'ro'})
        assert get_through_spectre('/long_ttl/vary', {'accept-encoding': 'text'}).headers['Spectre-Cache-Status'] == 'miss'

    def test_gzipped_responses_work(self):
        assert get_through_spectre('/gzipped').headers['Spectre-Cache-Status'] == 'miss'
        assert_is_in_spectre_cache('/gzipped')

    def test_query_params_ordering(self):
        val1 = get_through_spectre('/happy/?k1=v1&k2=v2')
        assert val1.headers['Spectre-Cache-Status'] == 'miss'
        assert_is_in_spectre_cache('/happy/?k1=v1&k2=v2')
        assert_is_in_spectre_cache('/happy/?k2=v2&k1=v1')

    def test_caching_json_with_null(self):
        response1 = get_through_spectre('/timestamp/cached')
        response2 = get_through_spectre('/timestamp/cached')

        body1 = response1.json()
        body2 = response2.json()

        # keys with null values should be persisted
        assert 'null_value' in body1
        assert 'null_value' in body2

        assert body1['null_value'] is None
        assert body2['null_value'] is None

    def test_http_timeouts_return_504(self):
        start = time.time()
        response = get_through_spectre('/timestamp/cached?sleep=1500')
        duration = time.time() - start

        assert response.text == 'Error requesting /timestamp/cached?sleep=1500: timeout'
        assert response.status_code == 504
        # Duration should be >= 1.0 seconds since that's the HTTP_TIMEOUT_MS
        # value we set in start.sh
        assert duration >= 1.0

    def test_connection_drops_return_502(self):
        response = get_through_spectre('/timestamp/cached?drop_connection=true')

        assert response.text == 'Error requesting /timestamp/cached?drop_connection=true: closed'
        assert response.status_code == 502

    def test_caching_works_with_id_extraction(self):
        response = get_through_spectre('/biz?foo=bar&business_id=1234')
        assert response.headers['Spectre-Cache-Status'] == 'miss'

        # ensure extracting the id is not messing up the caching logic
        assert_is_in_spectre_cache('/biz?foo=bar&business_id=1234')

        # check that invalidation is actually supported
        purge_resource({
            'namespace': 'backend.main',
            'cache_name': 'url_with_id_extraction',
            'id': '1234',
        })

        # now this should be a cache miss
        response = get_through_spectre('/biz?foo=bar&business_id=1234')
        assert response.headers['Spectre-Cache-Status'] == 'miss'

    def test_dont_drop_underscored_headers(self):
        response = get_through_spectre(
            '/business?foo=bar&business_id=1234',
            extra_headers={
                'Test-Header': 'val1',
                'Header_with_underscores': 'val2',
            },
        )

        headers = response.json()['received_headers']
        assert 'test-header' in headers
        assert 'header_with_underscores' in headers


class TestPostMethod(object):

    @pytest.fixture(autouse=True)
    def purge(self):
        time.sleep(1)
        purge_resource({'namespace': 'backend.main', 'cache_name': 'post_no_id'})
        purge_resource({'namespace': 'backend.main', 'cache_name': 'post_with_id'})
        purge_resource({'namespace': 'backend.main', 'cache_name': 'post_with_id_varying_body'})
        time.sleep(1)

    def test_post_request(self):
        response = post_through_spectre('/timestamp/post')
        assert response.status_code == 200
        assert response.json()['method'] == 'POST'
        assert response.headers['Spectre-Cache-Status'] == 'non-cacheable-uri (backend.main)'

    def test_post_request_passes_body_data(self):
        POST_DATA = 'a lot of data\n'
        val = post_through_spectre('/timestamp/post', data=POST_DATA).json()
        assert val['body'] == POST_DATA

    def test_post_request_not_cached(self):
        resp1 = post_through_spectre('/timestamp/post').json()
        resp2 = post_through_spectre('/timestamp/post').json()
        assert resp1['timestamp'] != resp2['timestamp']

    # Test all cached post endpoints. Content-Type should be application/json
    def test_post_always_cached(self):
        response = post_through_spectre(
            '/post_always_cache/',
            data={},
            extra_headers={'content-type': 'application/json'}
        )
        assert response.status_code == 200
        assert response.headers['Spectre-Cache-Status'] == 'miss'

        # When calling again the result should be cached
        assert_is_in_spectre_cache(
            '/post_always_cache/',
            data={},
            extra_headers={'content-type': 'application/json'}
        )

    # Test all cached post endpoints. Content-Type should be application/json; charset=utf-8
    def test_post_always_cached_for_extended_json_content_type(self):
        response = post_through_spectre(
            '/post_always_cache/',
            data={},
            extra_headers={'content-type': 'application/json; charset=utf-8'}
        )
        assert response.status_code == 200
        assert response.headers['Spectre-Cache-Status'] == 'miss'

        # When calling again the result should be cached
        assert_is_in_spectre_cache(
            '/post_always_cache/',
            data={},
            extra_headers={'content-type': 'application/json; charset=utf-8'}
        )

    def test_post_cache_hit_even_if_body_doesnt_match_without_vary(self):
        response = post_through_spectre(
            '/post_always_cache/',
            data='{"field1":"key1"}',
            extra_headers={'content-type': 'application/json'}
        )
        assert response.status_code == 200
        assert response.headers['Spectre-Cache-Status'] == 'miss'

        # When calling again the result should be cached
        assert_is_in_spectre_cache(
            '/post_always_cache/',
            data='{"field1":"key2"}',
            extra_headers={'content-type': 'application/json'}
        )

    def test_post_cached_with_id(self):
        response = post_through_spectre(
            '/post_id_cache/',
            data='{"request_id":123, "vary_id":"abc"}',
            extra_headers={'content-type': 'application/json'}
        )
        assert response.status_code == 200
        assert response.headers['Spectre-Cache-Status'] == 'miss'

        # When calling with different data, we will see a cache miss.
        response = post_through_spectre(
            '/post_id_cache/',
            data='{"request_id":234, "vary_id":"abc"}',
            extra_headers={'content-type': 'application/json'}
        )
        assert response.status_code == 200
        assert response.headers['Spectre-Cache-Status'] == 'miss'

        # When calling with different data with same id, we will see a cache miss.
        response = post_through_spectre(
            '/post_id_cache/',
            data='{"request_id":234, "vary_id":"def"}',
            extra_headers={'content-type': 'application/json'}
        )
        assert response.status_code == 200
        assert response.headers['Spectre-Cache-Status'] == 'miss'

        # Calling again with same request_id should be a cache hit
        assert_is_in_spectre_cache(
            '/post_id_cache/',
            data='{"request_id":234, "vary_id":"abc"}',
            extra_headers={'content-type': 'application/json'}
        )

    def test_post_cached_with_id_ignore_fields(self):
        response = post_through_spectre(
            '/post_id_cache_variable_body/',
            data='{"request_id":234, "ignore_field1":"abc"}',
            extra_headers={'content-type': 'application/json'}
        )
        assert response.status_code == 200
        assert response.headers['Spectre-Cache-Status'] == 'miss'

        # Calling again with more fields in body which are ignored would also be a cache hit
        assert_is_in_spectre_cache(
            '/post_id_cache_variable_body/', 
            data='{"request_id":234, "ignore_field1":"xyz", "ignore_field3":"21"}',
            extra_headers={'content-type': 'application/json'}
        )

    def test_post_cached_with_id_can_be_purged(self):
        response = post_through_spectre(
            '/post_id_cache/',
            data='{"request_id":123, "vary_id":"abc"}',
            extra_headers={'content-type': 'application/json'}
        )
        assert response.status_code == 200
        assert response.headers['Spectre-Cache-Status'] == 'miss'

        # When calling with different data with same id, we will see a cache miss.
        response = post_through_spectre(
            '/post_id_cache/',
            data='{"request_id":123, "vary_id":"def"}',
            extra_headers={'content-type': 'application/json'}
        )
        assert response.status_code == 200
        assert response.headers['Spectre-Cache-Status'] == 'miss'

        # Calling again with same request_id should be a cache hit
        assert_is_in_spectre_cache(
            '/post_id_cache/',
            data='{"request_id":123, "vary_id":"abc"}',
            extra_headers={'content-type': 'application/json'}
        )

        # Purge all resources with same id.
        purge_resource({'namespace': 'backend.main', 'cache_name': 'post_with_id', 'id': '123'})

        # All resources with same id should be a miss now.
        response = post_through_spectre(
            '/post_id_cache/',
            data='{"request_id":123, "vary_id":"def"}',
            extra_headers={'content-type': 'application/json'}
        )
        assert response.status_code == 200
        assert response.headers['Spectre-Cache-Status'] == 'miss'

        response = post_through_spectre(
            '/post_id_cache/',
            data='{"request_id":123, "vary_id":"abc"}',
            extra_headers={'content-type': 'application/json'}
        )
        assert response.status_code == 200
        assert response.headers['Spectre-Cache-Status'] == 'miss'



class TestHeadMethod(object):

    def test_head_request(self):
        # Since Spectre does not pass back response headers, HEAD is no-op.
        response = head_through_spectre('/timestamp/head')
        assert response.status_code == 200


class TestResponseHeaders(object):

    def test_spectre_status_response_header(self):
        response = get_through_spectre('/not_cacheable')
        assert response.headers['Spectre-Cache-Status'] == 'non-cacheable-uri (backend.main)'

        response = get_through_spectre('/timestamp/spectre_status_header')
        assert response.headers['Spectre-Cache-Status'] == 'miss'

        assert_is_in_spectre_cache('/timestamp/spectre_status_header')

        response = get_through_spectre('/timestamp/spectre_status_header', extra_headers={'Pragma': 'no-cache'})
        assert response.headers['Spectre-Cache-Status'] == 'no-cache-header'

        response = post_through_spectre('/timestamp/spectre_status_header')
        assert response.headers['Spectre-Cache-Status'] == 'non-cacheable-uri (backend.main)'

    def test_response_headers_passed_back(self):
        response = get_through_spectre('/not_cacheable')
        assert response.headers['Some-Header'] == 'abc'
        assert response.headers['Spectre-Cache-Status'] == 'non-cacheable-uri (backend.main)'

        response = get_through_spectre('/timestamp/response_header')
        assert response.headers['Spectre-Cache-Status'] == 'miss'
        assert response.headers['Some-Header'] == 'abc'

        response = assert_is_in_spectre_cache('/timestamp/response_header')
        assert response.headers['Some-Header'] == 'abc'

    def test_uncacheable_headers_not_passed_back(self):
        """Spectre doesn't store uncacheable headers in the cache, but it always
        passes back all response headers in the uncached (or uncacheable) case.
        """
        response = get_through_spectre('/not_cacheable')
        assert response.headers['Spectre-Cache-Status'] == 'non-cacheable-uri (backend.main)'
        assert 'Uncacheable-Header' in response.headers

        response = get_through_spectre('/timestamp/uncacheable_header')
        assert response.headers['Spectre-Cache-Status'] == 'miss'
        assert 'Uncacheable-Header' in response.headers

        # This is the cached case, where the uncacheable header isn't expected
        # in the response.
        response = assert_is_in_spectre_cache('/timestamp/uncacheable_header')
        assert 'Uncacheable-Header' not in response.headers


class TestSpectreBadRequets(object):

    def test_spectre_404s_when_destination_is_missing(self):
        response = get_through_spectre('/not_cacheable')
        assert response.status_code == 200

        response = get_through_spectre('/not_cacheable', extra_headers={'X-Smartstack-Destination': None})
        assert response.status_code == 404
        assert 'Not found: GET /not_cacheable' in response.text

    def test_spectre_400s_when_dupe_headers_are_passed(self):
        """Integration test to prevent PERF-1614 from happening again.
        This test has to use barebone `socket` because higher-level libs
        (requests, urllib3, etc) store headers as case-insensitive dicts.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect((SPECTRE_HOST, SPECTRE_PORT))
        s.send('GET /not_cacheable HTTP/1.0\r\n'.encode('utf-8'))
        s.send('X-Smartstack-Destination: srv.main\r\n'.encode('utf-8'))
        s.send('X-Smartstack-Destination: srv.alt\r\n'.encode('utf-8'))
        s.send('\r\n'.encode('utf-8'))

        data = s.recv(4096).decode('utf-8')
        assert 'HTTP/1.1 400 Bad Request' in data
        assert 'X-Smartstack-Destination has multiple values: srv.main srv.alt;' in data


class TestPostRequestCodeDoesntBreakOrSlowDownTheRequest(object):

    def test_slow_post_request_code(self):
        # A slow post-request code shouldn't slow down the request
        start = time.time()
        response = get_from_spectre('/internal_error/dogslow')
        # /internal_error/dogslow sleeps for 10 seconds
        assert time.time() - start < 2
        assert response.status_code == 200
        assert response.text == 'OK'

    def test_crash_post_request_code(self):
        # A crash in the post-request code shouldn't affect the response
        response = get_from_spectre('/internal_error/crash')
        assert response.status_code == 200
        assert response.text == 'OK'


class TestRegexesWorkAsExpected(object):

    @pytest.mark.parametrize('url, cache_status', [
        ('/get_user_info/v1?ids=1', 'miss'),
        ('/get_user_info/v1?business_ids=1', 'non-cacheable-uri (backend.main)'),
        ('/get_user_info/v1?ids=1,2,3', 'non-cacheable-uri (backend.main)'),
        ('/get_user_info/v1?ids=1&locale=en_US', 'miss'),
        ('/get_user_info/v1?locale=it_IT&ids=1', 'miss'),
    ])
    def test_bulk_endpoint_regex_can_work(self, url, cache_status):
        assert get_through_spectre(url).headers['Spectre-Cache-Status'] == cache_status


class TestGetBulkRequest(object):

    UNICODE_BIZ_ID = 'd%C3%A9lf%C3%ADn%C3%A4-san-francisco-2'

    @pytest.fixture(autouse=True)
    def setup_and_clean(self):
        # Places ids 1,2,3 individually in the spectre cache
        self.base_headers, self.base_body = self.make_request([1,2,3])
        yield
        self.purge()

    def make_request(self, ids, cache=True, extra_headers=None, assert_hit=False):
        base_path = '/bulk_requester'
        ids = [str(i) for i in ids]
        ids = "%2C".join(ids)
        if not cache:
            cache_headers = {'Cache-Control': 'no-cache'}
            if extra_headers is None:
                extra_headers = {}
            extra_headers.update(cache_headers)

        if assert_hit:
            resp = assert_is_in_spectre_cache("{}?ids={}".format(base_path, ids), extra_headers)
        else:
            resp = get_through_spectre("{}?ids={}".format(base_path, ids), extra_headers)

        return resp.headers, resp.json()

    def make_request_and_assert_hit(self, *args, **kwargs):
        """See util.assert_is_in_spectre_cache for rationale. This method wraps
        this class' `make_request` method and adds assertion and retries to
        overcome race conditions.
        """
        kwargs.update({'assert_hit': True})
        headers, body = self.make_request(*args, **kwargs)
        return headers, body

    def purge(self):
        time.sleep(1)
        purge_resource({'namespace': 'backend.main', 'cache_name': 'bulk_requester_does_not_cache_missing_ids'})
        purge_resource({'namespace': 'backend.main', 'cache_name': 'bulk_requester_default'})
        time.sleep(1)

    def test_unicode_chars_in_bulk_response(self):
        # Test retrieval of unicode url arguments returned during bulk requests)
        miss_headers, miss_body = self.make_request([self.UNICODE_BIZ_ID])
        assert miss_headers['Spectre-Cache-Status'] == 'miss'
        assert miss_body[0]['bulk_id'] == u'délfínä-san-francisco-2'

        headers, body = self.make_request_and_assert_hit([self.UNICODE_BIZ_ID])
        assert miss_body == body

    def test_simple_endpoint_cached(self):
        # Makes the same request twice, one a hit, one a miss
        miss_headers, miss_body = self.make_request([4])
        assert miss_headers['Spectre-Cache-Status'] == 'miss'

        headers, body = self.make_request_and_assert_hit([4])
        assert miss_body == body

    def test_basic_case(self):
        # Tests that individual ids are being cached on a bulk endpoint request
        # Test correctness of response based on bulk endpoint
        # (ids 1, 2 and 3 are placed in the cache during setup)
        headers, body = self.make_request_and_assert_hit([2])

        # Correctness
        assert len(body) == 1
        assert self.base_body[1] == body[0]

    def test_bulk_request_with_json_charset_response_body(self):
        # Tests that individual ids are being cached on a bulk endpoint request
        # The response type is application/json charset=utf-8
        # Test correctness of response based on bulk endpoint
        # (ids 1, 2 and 3 are placed in the cache during setup)
        headers, body = self.make_request_and_assert_hit(
            [2],
            extra_headers={'test-content-type': 'application/json; charset=utf-8'}
        )

        # Correctness
        assert len(body) == 1
        assert self.base_body[1] == body[0]

    def test_purge_works(self):
        # Test that purging a bulk endpoint also purges them individually
        headers, body = self.make_request([1])
        headers, body = self.make_request_and_assert_hit([1])

        self.purge()

        headers, body = self.make_request([1])
        assert headers['Spectre-Cache-Status'] == 'miss'

    def test_different_ordering(self):
        # Tests that different orderings of ids result in cache hit
        headers, body = self.make_request_and_assert_hit([3, 2, 1])

        # Correctness
        for i in range(3):
            assert self.base_body[i] == body[2 - i]

        # Different ordering with a miss
        headers, body = self.make_request([3,4,5,2,6])
        assert headers['Spectre-Cache-Status'] == 'miss'

        assert [body[0]] == self.make_request([3])[1]
        assert [body[1]] == self.make_request([4])[1]
        assert [body[2]] == self.make_request([5])[1]
        assert [body[3]] == self.make_request([2])[1]
        assert [body[4]] == self.make_request([6])[1]

    def test_new_notion_of_miss(self):
        # Ensure that there's a miss if any of the requests are a miss
        headers, body = self.make_request([3, 4])
        assert headers['Spectre-Cache-Status'] == 'miss'

    def test_different_params_same_id(self):
        base_path = '/bulk_requester'

        resp1 = get_through_spectre("{}?ids={}&data=false".format(base_path, '5%2C6'))
        assert resp1.headers['Spectre-Cache-Status'] == 'miss'
        resp2 = get_through_spectre("{}?ids={}&data=true".format(base_path, '5'))
        assert resp2.headers['Spectre-Cache-Status'] == 'miss'

        assert_is_in_spectre_cache("{}?data=false&ids={}".format(base_path, '5'))
        assert_is_in_spectre_cache("{}?data=false&ids={}".format(base_path, '6'))
        assert_is_in_spectre_cache("{}?ids={}&data=false".format(base_path, '6'))

    def test_with_invalid_id_when_cache_missing_ids_is_true(self):
        path = '/bulk_requester_2/{ids}/v1?k1=v1'

        # 0 < ids < 1000 are valid
        response = get_through_spectre(path.format(ids='10,5000,11'))
        assert len(response.json()) == 2
        assert response.headers['Spectre-Cache-Status'] == 'miss'

        # "5000" is an invalid id; but by default cache_missing_ids is set to true, so the
        # empty response would've been cached from the above request
        hit_response = assert_is_in_spectre_cache(path.format(ids='5000'))
        assert hit_response.json() == []

    def test_with_invalid_id_when_cache_missing_ids_is_false(self):
        # 0 < ids < 1000 are valid
        bulk_headers, bulk_body = self.make_request([4,0,5])
        assert len(bulk_body) == 2
        assert bulk_headers['Spectre-Cache-Status'] == 'miss'

        # "0" is an invalid ID
        headers1, body1 = self.make_request([0])
        assert headers1['Spectre-Cache-Status'] == 'miss'

        headers2, body2 = self.make_request([0], cache=False)
        assert headers2['Spectre-Cache-Status'] == 'no-cache-header'

        assert body1 == body2

        assert bulk_body[0] == self.make_request([4], cache=False)[1][0]
        assert bulk_body[1] == self.make_request([5], cache=False)[1][0]

        # Check that Spectre is not caching invalid IDs
        headers1, body1 = self.make_request([1001])
        assert headers1['Spectre-Cache-Status'] == 'miss'
        headers2, body2 = self.make_request([1001])
        assert headers2['Spectre-Cache-Status'] == 'miss'

        headers3, body3 = self.make_request([1001], cache=False)
        assert headers3['Spectre-Cache-Status'] == 'no-cache-header'
        assert body1 == body2
        assert body2 == body3

        bulk_headers, bulk_body = self.make_request([2,3,2000, 4])
        assert len(bulk_body) == 3
        assert bulk_headers['Spectre-Cache-Status'] == 'miss'
        assert bulk_body[0] == self.make_request([2], cache=False)[1][0]
        assert bulk_body[1] == self.make_request([3], cache=False)[1][0]
        assert bulk_body[2] == self.make_request([4], cache=False)[1][0]

    def test_cjson_empty_array(self):
        # Ensures that we receive an empty array instead of an empty dict
        # This ensures that lua doesn't convert [] -> {}
        headers, body = self.make_request_and_assert_hit([1])
        assert body[0]['empty_array'] == []

        headers, body = self.make_request([4])
        assert headers['Spectre-Cache-Status'] == 'miss'
        assert body[0]['empty_array'] == []

    def test_single_correctness(self):
        for i in range(1, 15):
            self.make_request([i])
            cache_headers, cache_body = self.make_request_and_assert_hit([i])
            headers, body = self.make_request([i], cache=False)
            assert headers['Spectre-Cache-Status'] != 'hit'
            assert cache_body == body

    def test_multiple_same_ids(self):
        headers, body = self.make_request([4, 4, 4])
        assert headers['Spectre-Cache-Status'] == 'miss'
        for resp in body:
            assert resp == self.make_request([4], cache=False)[1][0]

        # Test with combination
        headers, body = self.make_request([6, 5, 6, 5])
        assert headers['Spectre-Cache-Status'] == 'miss'
        for i in range(4):
            if i % 2 == 0:
                assert body[i] == self.make_request([6], cache=False)[1][0]
            else:
                assert body[i] == self.make_request([5], cache=False)[1][0]

        # Test with all hits
        headers, body = self.make_request_and_assert_hit([6,5,6,5])
        for i in range(4):
            if i % 2 == 0:
                assert body[i] == self.make_request([6], cache=False)[1][0]
            else:
                assert body[i] == self.make_request([5], cache=False)[1][0]

        # Test with combination of invalid ids
        headers, body = self.make_request([0, 1, 0, 5])
        assert headers['Spectre-Cache-Status'] == 'miss'
        assert body[0] == self.make_request([1], cache=False)[1][0]
        assert body[1] == self.make_request([5], cache=False)[1][0]

    def test_gzip_is_disabled(self):
        extra_header = {'accept-encoding': 'gzip'}
        headers1, body1 = self.make_request([4], extra_headers=extra_header)
        assert headers1['Spectre-Cache-Status'] == 'miss'

        _, body2 = self.make_request_and_assert_hit([4])

        headers3, body3 = self.make_request([4], cache=False)
        assert body1 == body2
        assert body2 == body3

    def test_non_application_json(self):
        extra_header = {'test-content-type': 'text'}
        base_path = '/bulk_requester'
        resp = get_through_spectre("{}?ids={}".format(base_path, '4%2C5'), extra_headers=extra_header)
        assert resp.text == 'this is text'
        assert resp.status_code == 200
        assert resp.headers['Spectre-Cache-Status'] == "unable to process response; content-type is text"

        # Single id isn't cached either
        resp = get_through_spectre("{}?ids={}".format(base_path, '4'), extra_headers=extra_header)
        assert resp.text == 'this is text'
        assert resp.status_code == 200
        assert resp.headers['Spectre-Cache-Status'] == "unable to process response; content-type is text"

    def test_bulk_ids_in_path(self):
        base_path = '/bulk_requester_2'

        resp_1 = get_through_spectre("{}/{}/v1?k1=v2".format(base_path, '1,2,3'))
        # Bulk was a miss
        bulk_headers, bulk_body = resp_1.headers, resp_1.json()
        assert bulk_headers['Spectre-Cache-Status'] == 'miss'

        resp_2 = assert_is_in_spectre_cache("{}/{}/v1?k1=v2".format(base_path, '2'))
        _, body = resp_2.headers, resp_2.json()

        # Check for correctness
        assert len(body) == 1
        assert bulk_body[1] == body[0]

    def test_caching_json_with_null(self):
        base_path = '/bulk_requester_2'
        resp_1 = get_through_spectre("{}/{}/v1?k1=v2".format(base_path, '1,2,3'))
        resp_2 = get_through_spectre("{}/{}/v1?k1=v2".format(base_path, '2'))
        _, bulk_body = resp_1.headers, resp_1.json()
        _, body = resp_2.headers, resp_2.json()

        # keys with null values should be persisted
        assert 'null_value' in bulk_body[0]
        assert 'null_value' in bulk_body[1]
        assert 'null_value' in bulk_body[2]
        assert 'null_value' in body[0]

        assert bulk_body[1]['null_value'] is None
        assert body[0]['null_value'] is None

    def test_doesnt_validate_json_on_non_200(self):
        resp = get_through_spectre("/not_authorized?ids=1")

        assert resp.status_code == 403
        assert resp.headers['Spectre-Cache-Status'] == 'non-cacheable-response: status code is 403'

    def test_bulk_request_error_codes(self):
        resp = get_through_spectre('/bulk_requester_2/1/v1?error_status=502')

        assert resp.status_code == 502
        assert resp.headers['Spectre-Cache-Status'] == 'non-cacheable-response: status code is 502'


class TestPurge(object):
    def test_purge_datastore(self):
        val1_before = get_timestamp('/timestamp/purge/1')
        val2_before = get_timestamp('/timestamp/purge/2')
        val3_before = get_timestamp('/long_ttl/no-purge')

        time.sleep(1)
        response = purge_resource({'namespace': 'backend.main', 'cache_name': 'timestamp'})
        assert response == 'Purged namespace: backend.main & cache_name: timestamp'
        time.sleep(1)

        val1_after = get_timestamp('/timestamp/purge/1')
        val2_after = get_timestamp('/timestamp/purge/2')
        val3_after, miss_values = get_timestamp_until_hit('/long_ttl/no-purge')

        # Both val1 and val2 should be deleted after purge;
        # however, val3 should remain as is before and after the purge.
        assert val1_before != val1_after
        assert val2_before != val2_after
        assert val3_before in [val3_after] + miss_values

    def test_purge_datastore_by_id(self):
        get_through_spectre('/bulk_requester?ids=1')
        assert_is_in_spectre_cache('/bulk_requester?ids=1')
        time.sleep(1)

        # Purge id 1
        purge_resource({'namespace': 'backend.main', 'cache_name': 'bulk_requester_does_not_cache_missing_ids', 'id': '1'})
        time.sleep(1)
        get_resp_3 = get_through_spectre('/bulk_requester?ids=1')
        # resp 3 was no longer cached.
        assert get_resp_3.headers['Spectre-Cache-Status'] == 'miss'

    def test_purge_datastore_by_id_backward_compatible(self):
        # Delete after PERF-2453 is done
        get_through_spectre('/bulk_requester?ids=1')
        assert_is_in_spectre_cache('/bulk_requester?ids=1')
        time.sleep(1)

        # Purge id 1
        res = requests.request(
            'PURGE',
            util.SPECTRE_BASE_URL + '?cache_name=bulk_requester_does_not_cache_missing_ids&id=1',
            headers=util.HAPROXY_ADDED_HEADERS
        )
        assert res.status_code == 200
        time.sleep(1)

        get_resp_3 = get_through_spectre('/bulk_requester?ids=1')
        # resp 3 was no longer cached.
        assert get_resp_3.headers['Spectre-Cache-Status'] == 'miss'

        res = requests.request(
            'PURGE',
            util.SPECTRE_BASE_URL + '?cache_name=bulk_requester_does_not_cache_missing_ids',
            headers=util.HAPROXY_ADDED_HEADERS
        )
        assert res.status_code == 200

    def test_purge_returns_400_on_invalid_namespace(self):
        with pytest.raises(bravado.exception.HTTPBadRequest) as e:
            purge_resource({'namespace': 'backend.invalid', 'cache_name': 'timestamp'})
        assert e.value.response.status_code == 400
        assert e.value.response.text == 'Unknown namespace backend.invalid'

    def test_purge_returns_400_on_invalid_cache_name(self):
        with pytest.raises(bravado.exception.HTTPBadRequest) as e:
            purge_resource({'namespace': 'backend.main', 'cache_name': 'timestamp_invalid'})
        assert e.value.response.status_code == 400
        assert e.value.response.text == 'Unknown cache_name timestamp_invalid for namespace backend.main'
