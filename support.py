"""Test helpers: serve captured upstream documents without touching the network.

The files under fixtures/ are verbatim captures of the real feeds, so the
parsers are exercised against the actual formats rather than against an
idealised version of them.
"""

import json
import logging
import os
from unittest.mock import patch

FIXTURE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures')

# Matched against the requested URL, longest-lived identifiers first.
FIXTURES = [
    ('spaceweather.gfz-potsdam.de', 'gfz_forecast.csv'),
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

        raise AssertionError(f"No fixture registered for {url}")

    return _get


def patch_http(overrides=None):
    """Patches the single place the project performs HTTP."""
    return patch('kp_core.requests.get', side_effect=fake_get(overrides))

