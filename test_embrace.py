"""Tests for the Brazilian local K index reader and its pipeline.

This feed differs from every other one here in two ways that the tests exist
to pin down: it measures rather than predicts, so it never reaches into the
future, and it publishes a `k` field that must never be confused with `kp`.
"""

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import embrace
import search_embrace
import support
from kp_core import DataValidationError, parse_timestamp, validate_series


def setUpModule():
    support.silence_logging()


class TestParseK(unittest.TestCase):
    """The index is written in thirds: '4-' is 3.67, '4o' is 4, '4+' is 4.33."""

    def test_reads_the_thirds_notation(self):
        self.assertAlmostEqual(embrace.parse_k('4-'), 3.667, places=2)
        self.assertEqual(embrace.parse_k('4o'), 4.0)
        self.assertAlmostEqual(embrace.parse_k('4+'), 4.333, places=2)

    def test_reads_the_whole_scale(self):
        self.assertEqual(embrace.parse_k('0o'), 0.0)
        self.assertEqual(embrace.parse_k('9o'), 9.0)

    def test_rejects_unreadable_tokens(self):
        for token in ['', '4', 'x', '4x', 'oo', None, 5, '-', '+']:
            self.assertIsNone(embrace.parse_k(token), repr(token))

    def test_never_returns_a_value_off_the_scale(self):
        """A malformed high digit must not slip through as a real reading."""
        self.assertAlmostEqual(embrace.parse_k('9+'), 9.333, places=2)
        # 9+ is above the scale, so the caller's clean_kp must drop it.
        from kp_core import clean_kp
        self.assertIsNone(clean_kp(embrace.parse_k('9+')))


class TestParseDay(unittest.TestCase):

    def test_parses_a_real_file(self):
        text = support.read_fixture('embrace_ksa_2026-10-11.txt')
        records = embrace.parse_day(text)

        self.assertEqual(len(records), 8)
        self.assertEqual(records[0]['datetime'], '2026-10-11T00:00:00Z')
        self.assertIn('k', records[0])
        self.assertNotIn('kp', records[0])

    def test_skips_unreadable_lines(self):
        text = (
            "2026-10-11T00:00:00 3o\n"
            "garbage line here\n"
            "not-a-date 4o\n"
            "2026-10-11T03:00:00 xx\n"
            "2026-10-11T06:00:00 5-\n"
        )
        records = embrace.parse_day(text)

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], {"datetime": "2026-10-11T00:00:00Z", "k": 3.0})
        self.assertAlmostEqual(records[1]['k'], 4.667, places=2)

    def test_drops_values_off_the_scale(self):
        records = embrace.parse_day("2026-10-11T00:00:00 9+\n2026-10-11T03:00:00 2o\n")
        self.assertEqual(records, [{"datetime": "2026-10-11T03:00:00Z", "k": 2.0}])

    def test_empty_file_yields_nothing(self):
        """A day INPE has not written yet is legitimately empty."""
        self.assertEqual(embrace.parse_day(''), [])


class TestFetchLocalK(unittest.TestCase):

    NOW = datetime(2026, 10, 11, 12, 0)

    def test_fetches_across_day_boundaries(self):
        with support.patch_http():
            records = embrace.fetch_local_k(self.NOW, days=2)

        self.assertEqual(len(records), 24)
        self.assertEqual(records[0]['datetime'], '2026-10-09T00:00:00Z')
        self.assertEqual(records[-1]['datetime'], '2026-10-11T21:00:00Z')

    def test_output_is_chronological(self):
        with support.patch_http():
            records = embrace.fetch_local_k(self.NOW, days=2)

        stamps = [r['datetime'] for r in records]
        self.assertEqual(stamps, sorted(stamps))

    def test_tolerates_a_day_that_does_not_exist_yet(self):
        """Asking for more days than exist must not fail the run."""
        with support.patch_http():
            records = embrace.fetch_local_k(self.NOW, days=6)

        self.assertEqual(len(records), 24)

    def test_fails_when_no_day_can_be_fetched(self):
        with patch('kp_core.requests.get', side_effect=OSError("down")):
            with self.assertRaises(DataValidationError) as caught:
                embrace.fetch_local_k(self.NOW, days=2)

        self.assertIn('No EMBRACE daily file', str(caught.exception))

    def test_fails_when_files_are_reachable_but_empty(self):
        with support.patch_http({'ksa/': ''}):
            with self.assertRaises(DataValidationError) as caught:
                embrace.fetch_local_k(self.NOW, days=2)

        self.assertIn('no usable readings', str(caught.exception))


class TestPipeline(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.output = os.path.join(self.directory, 'new_k_br.json')
        patcher = patch.object(search_embrace, 'OUTPUT_FILE', self.output)
        patcher.start()
        self.addCleanup(patcher.stop)

    def load(self):
        with open(self.output) as handle:
            return json.load(handle)

    def run_pipeline(self, overrides=None, now=None):
        with support.patch_http(overrides):
            with support.patch_now(search_embrace, now):
                return search_embrace.get_local_k_pipeline()

    def test_writes_a_valid_series(self):
        self.assertTrue(self.run_pipeline())
        records = self.load()

        self.assertEqual(validate_series(records, value_field='k'), [])
        self.assertTrue(records)

    def test_publishes_k_and_never_kp(self):
        """The field name is the only thing telling the two indices apart."""
        self.run_pipeline()
        for record in self.load():
            self.assertIn('k', record)
            self.assertNotIn('kp', record)

    def test_covers_the_history_window_and_no_future(self):
        self.run_pipeline()
        records = self.load()

        first = parse_timestamp(records[0]['datetime'])
        last = parse_timestamp(records[-1]['datetime'])

        self.assertEqual(
            support.FIXTURE_NOW - first, timedelta(days=search_embrace.HISTORY_DAYS)
        )
        self.assertLessEqual(last, support.FIXTURE_NOW)

    def test_values_stay_on_the_scale(self):
        self.run_pipeline()
        for record in self.load():
            self.assertIsInstance(record['k'], float)
            self.assertTrue(0.0 <= record['k'] <= 9.0)

    def test_keeps_previous_data_when_the_source_is_down(self):
        with patch('kp_core.requests.get', side_effect=OSError("down")):
            with support.patch_now(search_embrace):
                self.assertFalse(search_embrace.get_local_k_pipeline())

        self.assertFalse(os.path.exists(self.output))

    def newest_reading(self):
        with support.patch_http():
            records = embrace.fetch_local_k(support.FIXTURE_NOW, days=2)
        return parse_timestamp(records[-1]['datetime'])

    def test_refuses_to_publish_a_feed_that_stopped_reporting(self):
        """Measured data is judged on how far behind its newest reading is.

        The clock is advanced past the newest reading, not past FIXTURE_NOW,
        because the window moves with the clock and the fixtures run several
        hours beyond it.
        """
        stale_now = self.newest_reading() + search_embrace.MAX_LAG + timedelta(hours=2)

        with support.patch_http():
            with support.patch_now(search_embrace, stale_now):
                self.assertFalse(search_embrace.get_local_k_pipeline())

        self.assertFalse(os.path.exists(self.output))

    def test_the_refusal_is_about_lag_not_an_empty_window(self):
        """Guards the test above: the window must still contain readings."""
        stale_now = self.newest_reading() + search_embrace.MAX_LAG + timedelta(hours=2)
        window_start = stale_now - timedelta(days=search_embrace.HISTORY_DAYS)

        with support.patch_http():
            records = embrace.fetch_local_k(stale_now, days=search_embrace.FETCH_DAYS)

        inside = [r for r in records if parse_timestamp(r['datetime']) >= window_start]
        self.assertTrue(inside, 'window is empty, so the test proves nothing')

    def test_publishes_while_the_lag_is_within_the_limit(self):
        lagging = support.FIXTURE_NOW + timedelta(hours=6)

        with support.patch_http():
            with support.patch_now(search_embrace, lagging):
                self.assertTrue(search_embrace.get_local_k_pipeline())

    def test_never_overwrites_good_data_with_a_failed_run(self):
        self.assertTrue(self.run_pipeline())
        with open(self.output) as handle:
            before = handle.read()

        with patch('kp_core.requests.get', side_effect=OSError("down")):
            with support.patch_now(search_embrace):
                self.assertFalse(search_embrace.get_local_k_pipeline())

        with open(self.output) as handle:
            self.assertEqual(handle.read(), before)


class TestLocalKDiffersFromPlanetary(unittest.TestCase):
    """The reason this file exists at all."""

    def test_brazilian_k_does_not_track_global_kp(self):
        import search

        with support.patch_http():
            local = {r['datetime']: r['k'] for r in embrace.fetch_local_k(
                support.FIXTURE_NOW, days=2)}
            planetary = {r['datetime']: r['kp'] for r in search.fetch_gfz_realtime_json()}

        shared = sorted(set(local) & set(planetary))
        self.assertTrue(shared, 'fixtures should overlap')

        differences = [local[t] - planetary[t] for t in shared]
        biggest = max(abs(d) for d in differences)
        self.assertGreater(
            biggest, 0.5,
            'local K tracking global Kp exactly would mean the feed adds nothing'
        )


if __name__ == '__main__':
    unittest.main()
