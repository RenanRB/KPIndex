"""Test helpers: serve captured upstream documents without touching the network.

The files under fixtures/ are verbatim captures of the real feeds, so the
parsers are exercised against the actual formats rather than against an
idealised version of them.
"""

import json
import logging
import os
from datetime import datetime
from unittest.mock import patch

FIXTURE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures')

# The captures were all taken at 2026-10-11T13:03Z. The pipelines publish a
# window anchored on the current time, so any test that runs one has to pin
# the clock inside the period the fixtures cover, or the window would fall
# entirely outside the data.
#
#   gfz_realtime.json   8 Oct 00:00 - 11 Oct 12:00  (measured)
#   gfz_forecast.json  11 Oct 09:00 - 14 Oct 09:00
#   noaa_3day.txt      11 Oct - 13 Oct       (issued 10 Oct)
#   noaa_27day.txt      5 Oct - 31 Oct       (issued  5 Oct)
#   embrace_ksa_*.txt   9, 10, 11 Oct        (measured, local K)
#
# Together they cover the 2-days-back to 7-days-ahead window around:
FIXTURE_NOW = datetime(2026, 10, 11, 12, 0)

# Matched against the requested URL, longest-lived identifiers first.
FIXTURES = [
    ('HpoForecast', 'gfz_forecast.json'),
    # The EMBRACE feed is one file per day, matched on the date in the URL.
    ('ksa/2026/2026-10-09', 'embrace_ksa_2026-10-09.txt'),
    ('ksa/2026/2026-10-10', 'embrace_ksa_2026-10-10.txt'),
    ('ksa/2026/2026-10-11', 'embrace_ksa_2026-10-11.txt'),
    ('kp.gfz-potsdam.de', 'gfz_realtime.json'),
    ('27-day-outlook', 'noaa_27day.txt'),
    ('3-day-geomag', 'noaa_3day.txt'),
]


def silence_logging():
    """Keeps expected warnings from cluttering the test report."""
    logging.disable(logging.CRITICAL)


def read_fixture(name):
    with open(os.path.join(FIXTURE_DIR, name)) as handle:
        return handle.read()


class FakeResponse:
    """Minimal stand-in for requests.Response.

    json() is evaluated lazily so that handing a CSV to a JSON endpoint
    fails where the real code would fail, not at construction time.
    """

    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        pass

    def json(self):
        return json.loads(self.text)


def fake_get(overrides=None):
    """Builds a requests.get replacement backed by the fixtures.

    `overrides` maps a URL fragment to literal text, so a single test can
    swap one feed for a malformed variant while the others stay realistic.
    """
    overrides = overrides or {}

    def _get(url, **kwargs):
        for fragment, text in overrides.items():
            if fragment in url:
                return FakeResponse(text)

        for fragment, filename in FIXTURES:
            if fragment in url:
                return FakeResponse(read_fixture(filename))

        if '/ksa/' in url:
            # A day we did not capture stands in for one INPE has not written
            # yet, which the reader must tolerate rather than fail on.
            raise FileNotFoundError(f"no EMBRACE fixture for {url}")

        raise AssertionError(f"No fixture registered for {url}")

    return _get


def patch_http(overrides=None):
    """Patches the single place the project performs HTTP."""
    return patch('kp_core.requests.get', side_effect=fake_get(overrides))



def patch_now(module, moment=None):
    """Pins the clock a pipeline reads, for window-dependent tests.

    The module is explicit because each pipeline imports utcnow by name, so
    the binding to replace lives in the pipeline, not in kp_core.
    """
    return patch.object(module, 'utcnow', return_value=moment or FIXTURE_NOW)
