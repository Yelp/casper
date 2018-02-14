# -*- coding: utf-8 -*-
import time

from util import get_through_spectre


class TestCassandraTimeouts(object):
    def test_cassandra_timeouts_dont_slow_down_spectre(self):
        # Run the test twice to make sure the slow write from the first request
        # doesn't affect the next request.
        for i in range(2):
            start = time.time()
            response = get_through_spectre('/timestamp/get')
            duration = time.time() - start

            # Cassandra timeouts are treated as a miss.
            assert response.headers['Spectre-Cache-Status'] == 'miss'

            # The timeouts are set at 500ms in itests.
            # Duration should be very close to that value. If it's much higher it
            # means the timeouts are not working, if it's lower it means the
            # iptable rule is not working.
            assert duration - 0.5 < 0.01
