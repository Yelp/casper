import time

import pytest

from util import get_from_spectre


@pytest.fixture(scope='session', autouse=True)
def wait_for_casper():
    """ Wait for casper to be ready.

    It takes a bit for casper to be ready to serve requests,
    while the tests usually start immediately without waiting.
    """
    for i in range(60):
        try:
            response = get_from_spectre('/status')
            if response.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(1)
    else:
        raise RuntimeError("Spectre was not ready after 60 seconds")
