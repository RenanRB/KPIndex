"""Tests for the shared Kp invariants.

These are the rules that stop bad data reaching data/, so they are tested
against the hostile cases rather than the happy path: sentinels, off-grid
timestamps, duplicated bins and values outside the scale.
"""

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import requests

import kp_core
import support
from kp_core import (
    DataValidationError,
    clean_kp,
    fetch_json,
    fetch_lines,
    fetch_source,
    fetch_url,
    format_timestamp,
    is_aligned,
    parse_timestamp,
    snap_to_bin,
    utcnow,
    validate_series,
    write_json_atomic,
)


def setUpModule():
    support.silence_logging()


def series(start='2026-03-15T00:00:00Z', count=8, kp=2.0):
    """Builds a valid contiguous series, for tests to then corrupt."""
    first = parse_timestamp(start)
    return [
        {"datetime": format_timestamp(first + timedelta(hours=3 * index)), "kp": kp}
        for index in range(count)
    ]


class TestSnapToBin(unittest.TestCase):

    def test_exact_bins_are_untouched(self):
        for hour in kp_core.VALID_BIN_HOURS:
            moment = datetime(2026, 3, 15, hour)
            self.assertEqual(snap_to_bin(moment), moment)

    def test_rounds_to_the_nearest_bin(self):
        cases = [
            (datetime(2026, 3, 15, 1, 20), datetime(2026, 3, 15, 0)),
            (datetime(2026, 3, 15, 1, 30), datetime(2026, 3, 15, 3)),
            (datetime(2026, 3, 15, 4, 30), datetime(2026, 3, 15, 6)),
            (datetime(2026, 3, 15, 23, 45), datetime(2026, 3, 16, 0)),
        ]
        for moment, expected in cases:
            self.assertEqual(snap_to_bin(moment), expected, moment)

    def test_late_evening_stays_on_its_own_day(self):
        """Regression: 22:00 used to jump back to 00:00 of the same day.

        The old nearest-hour search included 23 as a candidate and then
        rewrote it to 0 without advancing the date, so a reading could be
        republished 21 hours earlier, on the wrong day.
        """
        self.assertEqual(snap_to_bin(datetime(2026, 3, 15, 22, 0)), datetime(2026, 3, 15, 21))
        self.assertEqual(snap_to_bin(datetime(2026, 3, 15, 22, 30)), datetime(2026, 3, 16, 0))
        self.assertEqual(snap_to_bin(datetime(2026, 3, 15, 23, 0)), datetime(2026, 3, 16, 0))

    def test_never_moves_a_reading_more_than_half_a_bin(self):
        """No input may be displaced by more than 90 minutes."""
        moment = datetime(2026, 3, 15, 0, 0)
        limit = timedelta(minutes=90)

        for step in range(24 * 12):  # every five minutes across a full day
            candidate = moment + timedelta(minutes=5 * step)
            self.assertLessEqual(
                abs(snap_to_bin(candidate) - candidate), limit,
                f"{candidate} moved too far"
            )

    def test_output_is_always_a_valid_bin(self):
        moment = datetime(2026, 3, 15, 0, 0)
        for step in range(24 * 12):
            self.assertTrue(is_aligned(snap_to_bin(moment + timedelta(minutes=5 * step))))

    def test_snapping_across_a_leap_day(self):
        self.assertEqual(snap_to_bin(datetime(2028, 2, 28, 23, 30)), datetime(2028, 2, 29, 0))
        self.assertEqual(snap_to_bin(datetime(2028, 2, 29, 23, 30)), datetime(2028, 3, 1, 0))


class TestIsAligned(unittest.TestCase):

    def test_accepts_only_the_eight_real_bins(self):
        for hour in range(24):
            expected = hour in (0, 3, 6, 9, 12, 15, 18, 21)
            self.assertEqual(is_aligned(datetime(2026, 3, 15, hour)), expected, hour)

    def test_rejects_sub_hour_components(self):
        self.assertFalse(is_aligned(datetime(2026, 3, 15, 3, 1)))
        self.assertFalse(is_aligned(datetime(2026, 3, 15, 3, 0, 1)))
        self.assertFalse(is_aligned(datetime(2026, 3, 15, 3, 0, 0, 1)))


class TestTimestamps(unittest.TestCase):

    def test_round_trip(self):
        moment = datetime(2026, 3, 15, 21)
        self.assertEqual(parse_timestamp(format_timestamp(moment)), moment)

    def test_accepts_both_utc_spellings(self):
        expected = datetime(2026, 3, 15, 12)
        self.assertEqual(parse_timestamp('2026-03-15T12:00:00Z'), expected)
        self.assertEqual(parse_timestamp('2026-03-15T12:00:00+00:00'), expected)

    def test_rejects_garbage(self):
        for value in ['', 'not a date', '2026-13-45T00:00:00Z', None, 42, []]:
            with self.assertRaises(DataValidationError, msg=repr(value)):
                parse_timestamp(value)


class TestCleanKp(unittest.TestCase):

    def test_accepts_the_whole_scale(self):
        for value in [0, 0.0, 2.667, 9, 9.0, '3.33']:
            self.assertIsNotNone(clean_kp(value), value)

    def test_rejects_missing_data_sentinels(self):
        """GFZ reports unmeasured bins as null or a negative sentinel."""
        for value in [None, -1, -1.0, '-1']:
            self.assertIsNone(clean_kp(value), value)

    def test_rejects_values_off_the_scale(self):
        for value in [9.1, 10, 99, -0.1]:
            self.assertIsNone(clean_kp(value), value)

    def test_rejects_non_numbers(self):
        for value in ['', 'abc', [], {}, True, False]:
            self.assertIsNone(clean_kp(value), value)

    def test_rejects_nan_and_infinity(self):
        for value in [float('nan'), float('inf'), float('-inf'), 'nan', 'inf']:
            self.assertIsNone(clean_kp(value), value)

    def test_returns_a_float(self):
        self.assertIsInstance(clean_kp(3), float)
        self.assertIsInstance(clean_kp('3'), float)


class TestValidateSeries(unittest.TestCase):

    def test_accepts_a_clean_series(self):
        self.assertEqual(validate_series(series()), [])

    def test_rejects_a_short_series(self):
        with self.assertRaises(DataValidationError):
            validate_series(series(count=3))

    def test_rejects_duplicate_timestamps(self):
        records = series()
        records[3]['datetime'] = records[2]['datetime']
        with self.assertRaises(DataValidationError) as caught:
            validate_series(records)
        self.assertIn('Duplicate', str(caught.exception))

    def test_rejects_timestamps_going_backwards(self):
        records = series()
        records[4], records[5] = records[5], records[4]
        with self.assertRaises(DataValidationError) as caught:
            validate_series(records)
        self.assertIn('backwards', str(caught.exception))

    def test_rejects_off_grid_timestamps(self):
        records = series()
        records[2]['datetime'] = '2026-03-15T07:00:00Z'
        with self.assertRaises(DataValidationError) as caught:
            validate_series(records)
        self.assertIn('three-hour', str(caught.exception))

    def test_rejects_kp_off_the_scale(self):
        for bad in [-1, 9.5, 100]:
            records = series()
            records[2]['kp'] = bad
            with self.assertRaises(DataValidationError, msg=repr(bad)):
                validate_series(records)

    def test_rejects_non_numeric_kp(self):
        for bad in ['3.0', None, True, []]:
            records = series()
            records[2]['kp'] = bad
            with self.assertRaises(DataValidationError, msg=repr(bad)):
                validate_series(records)

    def test_rejects_missing_fields(self):
        records = series()
        del records[2]['kp']
        with self.assertRaises(DataValidationError):
            validate_series(records)

    def test_reports_gaps_as_warnings_not_errors(self):
        records = series(count=10)
        del records[4]
        warnings = validate_series(records)
        self.assertEqual(len(warnings), 1)
        self.assertIn('Gap of 1 bin', warnings[0])

    def test_rejects_wrong_container_types(self):
        with self.assertRaises(DataValidationError):
            validate_series({'not': 'a list'})
        with self.assertRaises(DataValidationError):
            validate_series([1, 2, 3, 4, 5, 6, 7, 8])


class TestFetchUrl(unittest.TestCase):

    def test_retries_then_succeeds(self):
        attempts = []

        def flaky(url, **kwargs):
            attempts.append(url)
            if len(attempts) < 3:
                raise requests.exceptions.ConnectTimeout("boom")
            response = MagicMock()
            response.raise_for_status.return_value = None
            return response

        with patch('kp_core.requests.get', side_effect=flaky):
            fetch_url('https://example.test/data', sleep=lambda _: None)

        self.assertEqual(len(attempts), 3)

    def test_gives_up_after_the_configured_attempts(self):
        with patch('kp_core.requests.get', side_effect=requests.exceptions.ConnectTimeout("boom")) as mock_get:
            with self.assertRaises(requests.exceptions.ConnectTimeout):
                fetch_url('https://example.test/data', sleep=lambda _: None)

        self.assertEqual(mock_get.call_count, kp_core.MAX_ATTEMPTS)

    def test_backoff_grows_between_attempts(self):
        delays = []
        with patch('kp_core.requests.get', side_effect=requests.exceptions.ConnectTimeout("boom")):
            with self.assertRaises(requests.exceptions.ConnectTimeout):
                fetch_url('https://example.test/data', sleep=delays.append)

        self.assertEqual(delays, [5, 10, 20])

    def test_http_errors_are_retried_too(self):
        with patch('kp_core.requests.get', side_effect=requests.exceptions.HTTPError("503")) as mock_get:
            with self.assertRaises(requests.exceptions.HTTPError):
                fetch_url('https://example.test/data', sleep=lambda _: None)

        self.assertEqual(mock_get.call_count, kp_core.MAX_ATTEMPTS)


class TestFetchGuards(unittest.TestCase):
    """A maintenance page answered with 200 must never reach a parser."""

    def _respond(self, text):
        response = MagicMock()
        response.text = text
        response.raise_for_status.return_value = None
        response.json.side_effect = lambda: json.loads(text)
        return response

    def test_rejects_html_served_as_the_document(self):
        html = '<html><head><title>503 Service Unavailable</title></head></html>'
        with patch('kp_core.requests.get', return_value=self._respond(html)):
            with self.assertRaises(DataValidationError) as caught:
                fetch_lines('https://example.test/data', expected_marker='Time (UTC)')

        self.assertIn('does not look like', str(caught.exception))

    def test_accepts_the_real_document(self):
        with patch('kp_core.requests.get', return_value=self._respond('Time (UTC),median\na,b')):
            lines = fetch_lines('https://example.test/data', expected_marker='Time (UTC)')

        self.assertEqual(lines, ['Time (UTC),median', 'a,b'])

    def test_rejects_non_json_from_a_json_endpoint(self):
        with patch('kp_core.requests.get', return_value=self._respond('<html>nope</html>')):
            with self.assertRaises(DataValidationError) as caught:
                fetch_json('https://example.test/data')

        self.assertIn('not valid JSON', str(caught.exception))


class TestFetchSource(unittest.TestCase):

    def test_passes_through_the_value(self):
        self.assertEqual(fetch_source('ok', lambda: [1, 2]), [1, 2])

    def test_turns_any_failure_into_none(self):
        def explode():
            raise requests.exceptions.ConnectTimeout("down")

        self.assertIsNone(fetch_source('broken', explode))

    def test_distinguishes_empty_from_missing(self):
        """An empty list is data; None means the source was unreachable."""
        self.assertEqual(fetch_source('empty', lambda: []), [])
        self.assertIsNotNone(fetch_source('empty', lambda: []))


class TestWriteJsonAtomic(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.path = os.path.join(self.directory, 'out.json')

    def test_writes_readable_json(self):
        write_json_atomic(self.path, series())
        with open(self.path) as handle:
            self.assertEqual(len(json.load(handle)), 8)

    def test_replaces_an_existing_file(self):
        write_json_atomic(self.path, series(count=8))
        write_json_atomic(self.path, series(count=16))
        with open(self.path) as handle:
            self.assertEqual(len(json.load(handle)), 16)

    def test_leaves_the_previous_file_intact_when_serialisation_fails(self):
        write_json_atomic(self.path, series())

        with self.assertRaises(TypeError):
            write_json_atomic(self.path, [{"datetime": "x", "kp": {1, 2}}])  # sets are not JSON

        with open(self.path) as handle:
            self.assertEqual(len(json.load(handle)), 8)

    def test_leaves_no_temporary_files_behind(self):
        write_json_atomic(self.path, series())
        with self.assertRaises(TypeError):
            write_json_atomic(self.path, [{"kp": {1, 2}}])

        leftovers = [name for name in os.listdir(self.directory) if name.endswith('.tmp')]
        self.assertEqual(leftovers, [])


class TestUtcNow(unittest.TestCase):

    def test_is_naive_and_utc(self):
        now = utcnow()
        self.assertIsNone(now.tzinfo)
        self.assertLess(abs((now - datetime.utcnow()).total_seconds()), 5)


if __name__ == '__main__':
    unittest.main()
