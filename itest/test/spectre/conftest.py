import time

import pytest

from util import get_from_spectre


@pytest.fixture(scope='session', autouse=True)
def wait_for_casper():
    """ Wait for casper and cassandra to be ready.

    It takes a bit for casper and cassandra to be ready to serve requests,
    while the tests usually start immediately without waiting.
    """
    for i in range(60):
        response = get_from_spectre('/status?check_cassandra=true')
        if response.status_code == 200:
            return
        else:
            time.sleep(1)
    else:
        raise RuntimeError("Spectre was not ready after 60 seconds")
