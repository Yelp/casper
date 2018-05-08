import codecs
import re
import os

import pytest

from util import get_through_spectre
from zipkin_util import load_zipkin_spans

SYSLOG_FILE = '/var/log/syslog/spectre.log'


class TestZipkinLogging(object):

    @pytest.fixture
    def clean_log_files(self):
        # Clean the log file to avoid pollution
        # We can't delete it because scribe doesn't recreate it
        if os.path.isfile(SYSLOG_FILE):
            fp = open(SYSLOG_FILE, 'w')
            fp.close()
        yield

    def _get_random_zipkin_ids(self):
        """Gets a trio of random zipkin ids - trace, span, parent_span."""
        return [
            codecs.encode(os.urandom(8), 'hex_codec')
            for _ in range(3)
        ]

    def _call_with_zipkin(
        self,
        trace_id,
        span_id,
        parent_span_id,
        sampled='1',
        url='/not_cacheable',
    ):
        """Calls Spectre-fronted service with specified Zipkin HTTP headers."""
        zipkin_headers = {
            'X-B3-TraceId': trace_id,
            'X-B3-SpanId': span_id,
            'X-B3-ParentSpanId': parent_span_id,
            'X-B3-Flags': '0',
            'X-B3-Sampled': sampled,
        }
        response = get_through_spectre(
            url,
            extra_headers=zipkin_headers,
        )
        return response

    def _check_backend_headers(
        self,
        incoming_trace_id,
        incoming_span_id,
        incoming_parent_id,
        backend_headers,
    ):
        """Checks that Spectre properly extracts, transforms, and injects
        Zipkin headers into downstream service calls.
        :param incoming_trace_id: trace ID passed to Spectre call
        :param incoming_span_id: the span ID of the Spectre span
        :param incoming_parent_id: the span ID of the Spectre span's parent
        :param backend_headers: all HTTP headers received by the backend server
        """
        # BaseHTTPServer lowercases all incoming headers, but case shouldn't
        # matter in headers anyways, right?
        assert backend_headers['x-b3-traceid'] == incoming_trace_id
        # Parent span ID should be equal to Spectre's span ID
        assert backend_headers['x-b3-parentspanid'] == incoming_span_id
        # span ID should be a new random string, so just check for neither of
        # the previous values.
        assert backend_headers['x-b3-spanid'] != incoming_parent_id
        assert backend_headers['x-b3-spanid'] != incoming_span_id

    def _assert_no_span_in_logs(self):
        assert load_zipkin_spans(SYSLOG_FILE) == []

    def _check_span_logs(self, trace_id, span_id, parent_span_id, num_lines=1):
        """Check that Zipkin span information is logged to the local error log
        file. We clear out the error file's contents before individual tests,
        so we only check for single log lines.
        """
        # This file path is specified in docker-compose.yml
        spans = load_zipkin_spans(SYSLOG_FILE)
        assert len(spans) == num_lines

        assert spans[-1].trace_id == trace_id
        assert spans[-1].id == span_id
        assert spans[-1].parent_id == parent_span_id 

    def test_logs_zipkin_info_to_error_log(self, clean_log_files):
        trace_id, span_id, parent_span_id = self._get_random_zipkin_ids()
        self._call_with_zipkin(trace_id, span_id, parent_span_id, sampled='1')
        self._check_span_logs(trace_id, span_id, parent_span_id)

    def test_still_log_even_if_not_sampled(self, clean_log_files):
        trace_id, span_id, parent_span_id = self._get_random_zipkin_ids()
        self._call_with_zipkin(trace_id, span_id, parent_span_id, sampled='0')
        self._check_span_logs(trace_id, span_id, parent_span_id)

    def test_propagates_zipkin_headers(self, clean_log_files):
        """Make sure Spectre passes properly transformed Zipkin headers
        to the downstream service.
        """
        trace_id, span_id, parent_span_id = self._get_random_zipkin_ids()
        response = self._call_with_zipkin(trace_id, span_id, parent_span_id, sampled='1')
        backend_headers = response.json()['received_headers']
        self._check_backend_headers(trace_id, span_id, parent_span_id, backend_headers)

    def test_propagates_zipkin_headers_if_not_sampled(self, clean_log_files):
        """Make sure Spectre still propagates downstream Zipkin headers, even if
        the Zipkin trace isn't sampled.
        """
        trace_id, span_id, parent_span_id = self._get_random_zipkin_ids()
        response = self._call_with_zipkin(trace_id, span_id, parent_span_id, sampled='0')
        backend_headers = response.json()['received_headers']
        self._check_backend_headers(trace_id, span_id, parent_span_id, backend_headers)

    def test_logs_zipkin_span_if_cached(self, clean_log_files):
        """Regression test to make sure Zipkin spans still get logged if the
        backend server is never hit.
        """
        # Hit an uncached endpoint
        trace_id, span_id, parent_span_id = self._get_random_zipkin_ids()
        uncached_response = self._call_with_zipkin(
            trace_id,
            span_id,
            parent_span_id,
            url='/long_ttl/zipkin',
        )
        assert uncached_response.headers['Spectre-Cache-Status'] == 'miss'
        self._check_span_logs(trace_id, span_id, parent_span_id)
        uncached_backend_headers = uncached_response.json()['received_headers']
        self._check_backend_headers(
            trace_id,
            span_id,
            parent_span_id,
            uncached_backend_headers,
        )

        # Hit the same endpoint, which should now be cached
        trace_id, span_id, parent_span_id = self._get_random_zipkin_ids()
        cached_response = self._call_with_zipkin(
            trace_id,
            span_id,
            parent_span_id,
            url='/long_ttl/zipkin',
        )
        assert cached_response.headers['Spectre-Cache-Status'] == 'hit'
        # In the cached case, Spectre courteously sets the X-Zipkin-Id response
        # header. In the uncached case, that's the responsibility of the
        # backend service to set it themselves.
        assert cached_response.headers['X-Zipkin-Id'] == trace_id
        self._check_span_logs(trace_id, span_id, parent_span_id, num_lines=2)

    def test_dont_emit_span_if_no_headers(self, clean_log_files):
        response = get_through_spectre(
            '/not_cacheable',
        )
        assert response.status_code == 200

        self._assert_no_span_in_logs()
