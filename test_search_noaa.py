"""Tests for the NOAA-only pipeline behind data/kp_noaa.json.

The parsers it uses are covered in test_noaa.py; this file is about the
window the pipeline publishes and what it refuses to publish.
"""

import json
import os
import tempfile
import unittest
from datetime import timedelta
from unittest.mock import patch

import search_noaa
import support
from kp_core import DataValidationError, parse_timestamp, series_horizon, validate_series


def setUpModule():
    support.silence_logging()


class TestPipeline(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.output = os.path.join(self.directory, 'new_kp.json')
        patcher = patch.object(search_noaa, 'OUTPUT_FILE', self.output)
        patcher.start()
        self.addCleanup(patcher.stop)

    def load(self):
        with open(self.output) as handle:
            return json.load(handle)

    def run_pipeline(self, overrides=None, now=None):
        with support.patch_http(overrides):
            with support.patch_now(search_noaa, now):
                return search_noaa.merge_and_save_data()

    def test_writes_a_valid_series_from_the_real_documents(self):
        self.assertTrue(self.run_pipeline())
        self.assertEqual(validate_series(self.load()), [])

    def test_covers_the_configured_window(self):
        self.run_pipeline()
        records = self.load()

        first = parse_timestamp(records[0]['datetime'])
        self.assertEqual(support.FIXTURE_NOW - first, timedelta(days=search_noaa.HISTORY_DAYS))
        self.assertEqual(
            series_horizon(records, support.FIXTURE_NOW),
            timedelta(days=search_noaa.FORECAST_DAYS),
        )

    def test_series_is_contiguous(self):
        self.run_pipeline()
        moments = [parse_timestamp(r['datetime']) for r in self.load()]

        for earlier, later in zip(moments, moments[1:]):
            self.assertEqual(later - earlier, timedelta(hours=3))

    def test_output_is_chronological_unique_and_in_range(self):
        self.run_pipeline()
        records = self.load()

        stamps = [record['datetime'] for record in records]
        self.assertEqual(stamps, sorted(stamps))
        self.assertEqual(len(stamps), len(set(stamps)))
        for record in records:
            self.assertIsInstance(record['kp'], float)
            self.assertTrue(0.0 <= record['kp'] <= 9.0)

    def test_the_three_day_profile_wins_over_the_daily_tail(self):
        """Where both cover a bin, the real per-bin profile must be used."""
        self.run_pipeline()
        published = {record['datetime']: record['kp'] for record in self.load()}

        with support.patch_http():
            profile = search_noaa.noaa.fetch_3day_forecast(search_noaa.url_hour)

        overlapping = [p for p in profile if p['datetime'] in published]
        self.assertTrue(overlapping, 'fixtures should overlap the window')
        for record in overlapping:
            self.assertEqual(published[record['datetime']], record['kp'])

    def test_survives_the_loss_of_the_three_day_forecast(self):
        with patch.object(search_noaa.noaa, 'fetch_3day_forecast', side_effect=OSError("down")):
            self.assertTrue(self.run_pipeline())

        self.assertEqual(validate_series(self.load()), [])

    def test_keeps_previous_data_when_every_source_is_down(self):
        with patch('kp_core.requests.get', side_effect=OSError("down")):
            with support.patch_now(search_noaa):
                self.assertFalse(search_noaa.merge_and_save_data())

        self.assertFalse(os.path.exists(self.output))

    def test_refuses_a_series_that_barely_reaches_the_future(self):
        with patch.object(search_noaa, 'FORECAST_DAYS', 1):
            self.assertFalse(self.run_pipeline())

        self.assertFalse(os.path.exists(self.output))

    def test_never_overwrites_good_data_with_an_unpublishable_run(self):
        self.assertTrue(self.run_pipeline())
        with open(self.output) as handle:
            before = handle.read()

        with patch('kp_core.requests.get', side_effect=OSError("down")):
            with support.patch_now(search_noaa):
                self.assertFalse(search_noaa.merge_and_save_data())

        with open(self.output) as handle:
            self.assertEqual(handle.read(), before)

    def test_refuses_to_publish_a_corrupt_series(self):
        corrupt = ([{"datetime": "2026-09-16T07:00:00Z", "kp": 1.0}] * 8, {})

        with patch('search_noaa.build_series', return_value=corrupt):
            with self.assertRaises(DataValidationError):
                self.run_pipeline()

        self.assertFalse(os.path.exists(self.output))


if __name__ == '__main__':
    unittest.main()
