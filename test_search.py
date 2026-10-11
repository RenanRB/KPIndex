"""Tests for the GFZ + NOAA pipeline behind data/kp.json."""

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import search
import support
from kp_core import DataValidationError, parse_timestamp, series_horizon, validate_series


def setUpModule():
    support.silence_logging()


class TestFetchGfzForecast(unittest.TestCase):

    def test_parses_the_real_document(self):
        with support.patch_http():
            records = support_records = search.fetch_gfz_forecast()

        self.assertEqual(len(records), 25)
        self.assertEqual(records[0], {"datetime": "2026-10-11T09:00:00Z", "kp": 2.0})
        self.assertEqual(records[-1]['datetime'], '2026-10-14T09:00:00Z')
        for record in support_records:
            self.assertTrue(0 <= record['kp'] <= 9)

    def test_reads_the_consensus_key_by_name(self):
        """MAX is the pessimistic bound; picking it would overstate activity."""
        payload = json.dumps({
            "MEDIAN": {"2026-10-11T09:00:00.000": 2.0},
            "MAX": {"2026-10-11T09:00:00.000": 8.0},
        })
        with support.patch_http({'HpoForecast': payload}):
            records = search.fetch_gfz_forecast()

        self.assertEqual(records, [{"datetime": "2026-10-11T09:00:00Z", "kp": 2.0}])

    def test_rejects_a_document_without_the_consensus_key(self):
        payload = json.dumps({"MAX": {"2026-10-11T09:00:00.000": 8.0}})
        with support.patch_http({'HpoForecast': payload}):
            with self.assertRaises(DataValidationError) as caught:
                search.fetch_gfz_forecast()

        self.assertIn('MEDIAN', str(caught.exception))

    def test_rejects_an_empty_consensus(self):
        with support.patch_http({'HpoForecast': json.dumps({"MEDIAN": {}})}):
            with self.assertRaises(DataValidationError):
                search.fetch_gfz_forecast()

    def test_rejects_an_error_page(self):
        with support.patch_http({'HpoForecast': '<html>502 Bad Gateway</html>'}):
            with self.assertRaises(DataValidationError):
                search.fetch_gfz_forecast()

    def test_rejects_a_non_object_payload(self):
        for payload in ['[]', 'null', '3']:
            with support.patch_http({'HpoForecast': payload}):
                with self.assertRaises(DataValidationError, msg=payload):
                    search.fetch_gfz_forecast()

    def test_skips_unusable_bins_but_keeps_the_good_ones(self):
        payload = json.dumps({"MEDIAN": {
            "2026-10-11T09:00:00.000": 3.0,
            "2026-10-11T12:00:00.000": -1,
            "2026-10-11T15:00:00.000": 99,
            "2026-10-11T18:00:00.000": None,
            "2026-10-11T21:00:00.000": 5.0,
        }})
        with support.patch_http({'HpoForecast': payload}):
            records = search.fetch_gfz_forecast()

        self.assertEqual(
            records,
            [
                {"datetime": "2026-10-11T09:00:00Z", "kp": 3.0},
                {"datetime": "2026-10-11T21:00:00Z", "kp": 5.0},
            ],
        )

    def test_rejects_a_document_with_no_usable_bins(self):
        payload = json.dumps({"MEDIAN": {"2026-10-11T09:00:00.000": -1}})
        with support.patch_http({'HpoForecast': payload}):
            with self.assertRaises(DataValidationError):
                search.fetch_gfz_forecast()

    def test_output_is_chronological(self):
        """JSON object order is not guaranteed, so it must be sorted."""
        payload = json.dumps({"MEDIAN": {
            "2026-10-11T21:00:00.000": 1.0,
            "2026-10-11T09:00:00.000": 2.0,
            "2026-10-11T15:00:00.000": 3.0,
        }})
        with support.patch_http({'HpoForecast': payload}):
            records = search.fetch_gfz_forecast()

        stamps = [record['datetime'] for record in records]
        self.assertEqual(stamps, sorted(stamps))

    def test_off_grid_bins_are_snapped_onto_real_bins(self):
        payload = json.dumps({"MEDIAN": {
            "2026-10-11T22:00:00.000": 2.0,
            "2026-10-11T23:45:00.000": 3.0,
        }})
        with support.patch_http({'HpoForecast': payload}):
            records = search.fetch_gfz_forecast()

        # 22:00 belongs to its own evening, not to midnight of the same day.
        self.assertEqual(records[0]['datetime'], '2026-10-11T21:00:00Z')
        self.assertEqual(records[1]['datetime'], '2026-10-12T00:00:00Z')


class TestFetchGfzRealtimeJson(unittest.TestCase):

    def test_parses_the_real_document(self):
        with support.patch_http():
            records = search.fetch_gfz_realtime_json()

        self.assertEqual(len(records), 29)
        self.assertEqual(records[0], {"datetime": "2026-10-08T00:00:00Z", "kp": 1.0})

    def test_drops_unmeasured_bins(self):
        """Future or missing bins arrive as null or -1 and are not readings."""
        payload = json.dumps({
            "datetime": [
                "2026-03-15T00:00:00Z", "2026-03-15T03:00:00Z",
                "2026-03-15T06:00:00Z", "2026-03-15T09:00:00Z",
            ],
            "Kp": [2.0, -1, None, 3.0],
        })
        with support.patch_http({'kp.gfz-potsdam.de': payload}):
            records = search.fetch_gfz_realtime_json()

        self.assertEqual(
            records,
            [
                {"datetime": "2026-03-15T00:00:00Z", "kp": 2.0},
                {"datetime": "2026-03-15T09:00:00Z", "kp": 3.0},
            ],
        )

    def test_off_grid_realtime_bins_are_snapped(self):
        payload = json.dumps({
            "datetime": ["2026-03-15T22:00:00Z", "2026-03-15T23:45:00Z"],
            "Kp": [2.0, 3.0],
        })
        with support.patch_http({'kp.gfz-potsdam.de': payload}):
            records = search.fetch_gfz_realtime_json()

        self.assertEqual(records[0]['datetime'], '2026-03-15T21:00:00Z')
        self.assertEqual(records[1]['datetime'], '2026-03-16T00:00:00Z')

    def test_rejects_mismatched_arrays(self):
        payload = json.dumps({"datetime": ["2026-03-15T00:00:00Z"], "Kp": [1.0, 2.0]})
        with support.patch_http({'kp.gfz-potsdam.de': payload}):
            with self.assertRaises(DataValidationError) as caught:
                search.fetch_gfz_realtime_json()

        self.assertIn('1 timestamps for 2', str(caught.exception))

    def test_rejects_a_missing_payload(self):
        for payload in ['{}', '[]', '{"datetime": []}', 'null']:
            with support.patch_http({'kp.gfz-potsdam.de': payload}):
                with self.assertRaises(DataValidationError, msg=payload):
                    search.fetch_gfz_realtime_json()

    def test_requests_enough_history_to_fill_the_window(self):
        """The request must reach past the window's start, never stop short."""
        captured = {}

        def capture(url, **kwargs):
            captured['url'] = url
            return support.FakeResponse('{"datetime": [], "Kp": []}')

        with patch('kp_core.requests.get', side_effect=capture):
            with support.patch_now(search, datetime(2026, 3, 15, 12)):
                search.fetch_gfz_realtime_json()

        requested = parse_timestamp(captured['url'].split('start=')[1][:10] + 'T00:00:00Z')
        window_start = datetime(2026, 3, 15, 12) - timedelta(days=search.HISTORY_DAYS)
        self.assertLessEqual(requested, window_start)


class TestPipeline(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.output = os.path.join(self.directory, 'new_kp.json')
        patcher = patch.object(search, 'OUTPUT_FILE', self.output)
        patcher.start()
        self.addCleanup(patcher.stop)

    def load(self):
        with open(self.output) as handle:
            return json.load(handle)

    def run_pipeline(self, overrides=None, now=None):
        with support.patch_http(overrides):
            with support.patch_now(search, now):
                return search.get_kp_pipeline()

    def test_writes_a_valid_series_from_the_real_documents(self):
        self.assertTrue(self.run_pipeline())
        records = self.load()

        self.assertEqual(validate_series(records), [])
        self.assertTrue(records)

    def test_covers_the_configured_window(self):
        self.run_pipeline()
        records = self.load()

        first = parse_timestamp(records[0]['datetime'])
        horizon = series_horizon(records, support.FIXTURE_NOW)

        self.assertEqual(support.FIXTURE_NOW - first, timedelta(days=search.HISTORY_DAYS))
        self.assertEqual(horizon, timedelta(days=search.FORECAST_DAYS))

    def test_series_is_contiguous(self):
        """No gaps: every three-hour bin in the window must be present."""
        self.run_pipeline()
        records = self.load()

        self.assertEqual(validate_series(records), [])
        moments = [parse_timestamp(record['datetime']) for record in records]
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

    def test_measured_kp_wins_over_every_forecast(self):
        """A bin GFZ has measured must carry the measurement."""
        self.run_pipeline()
        records = {record['datetime']: record['kp'] for record in self.load()}

        with support.patch_http():
            measured = search.fetch_gfz_realtime_json()

        overlapping = [m for m in measured if m['datetime'] in records]
        self.assertTrue(overlapping, 'fixtures should overlap the window')
        for record in overlapping:
            self.assertEqual(records[record['datetime']], record['kp'])

    def forecast_only_bins(self, published):
        """Published bins that no measurement covers.

        Measured Kp outranks every forecast, so a comparison between two
        forecast feeds has to leave the measured bins out of it.
        """
        with support.patch_http():
            measured = {record['datetime'] for record in search.fetch_gfz_realtime_json()}

        return {stamp: kp for stamp, kp in published.items() if stamp not in measured}

    def test_the_gfz_model_outranks_the_noaa_profile(self):
        """GFZ's per-bin model is preferred where both feeds cover a bin."""
        self.run_pipeline()
        published = self.forecast_only_bins(
            {record['datetime']: record['kp'] for record in self.load()}
        )

        with support.patch_http():
            model = search.fetch_gfz_forecast()

        overlapping = [m for m in model if m['datetime'] in published]
        self.assertTrue(overlapping, 'fixtures should overlap the window')
        for record in overlapping:
            self.assertEqual(published[record['datetime']], record['kp'])

    def test_the_noaa_profile_takes_over_when_the_gfz_model_is_gone(self):
        with patch.object(search, 'fetch_gfz_forecast', side_effect=OSError("down")):
            self.assertTrue(self.run_pipeline())
        published = self.forecast_only_bins(
            {record['datetime']: record['kp'] for record in self.load()}
        )

        with support.patch_http():
            profile = search.noaa.fetch_3day_forecast()

        overlapping = [p for p in profile if p['datetime'] in published]
        self.assertTrue(overlapping)
        for record in overlapping:
            self.assertEqual(published[record['datetime']], record['kp'])

    def test_a_frozen_gfz_forecast_no_longer_costs_us_the_future(self):
        """Regression for the October 2026 outage.

        GFZ kept serving a forecast file that had not changed in eight days.
        Because the window was anchored on the earliest record, that stale
        feed dragged the window back and the NOAA tail was filtered out: the
        published series kept its past and lost its entire forecast.
        """
        stale_day = support.FIXTURE_NOW - timedelta(days=8)
        frozen = {
            (stale_day + timedelta(hours=3 * index)).strftime('%Y-%m-%dT%H:%M:00.000'): 1.0
            for index in range(25)
        }
        stale_json = json.dumps({"MEDIAN": frozen})

        self.assertTrue(self.run_pipeline({'HpoForecast': stale_json}))
        records = self.load()

        horizon = series_horizon(records, support.FIXTURE_NOW)
        self.assertEqual(horizon, timedelta(days=search.FORECAST_DAYS))
        self.assertEqual(validate_series(records), [])

        # Nothing from the stale file may appear in the published window.
        earliest = parse_timestamp(records[0]['datetime'])
        self.assertGreater(earliest, stale_day + timedelta(days=1))

    def test_survives_the_loss_of_any_single_source(self):
        """Each feed can vanish and still leave a publishable series."""
        feeds = [
            'fetch_gfz_realtime_json',
            'fetch_gfz_forecast',
        ]
        for name in feeds:
            with self.subTest(source=name):
                with patch.object(search, name, side_effect=OSError("down")):
                    self.assertTrue(self.run_pipeline(), f"{name} down broke the run")
                self.assertEqual(validate_series(self.load()), [])

    def test_survives_the_loss_of_the_gfz_entirely(self):
        with patch.object(search, 'fetch_gfz_realtime_json', side_effect=OSError("down")):
            with patch.object(search, 'fetch_gfz_forecast', side_effect=OSError("down")):
                self.assertTrue(self.run_pipeline())

        self.assertEqual(validate_series(self.load()), [])

    def test_keeps_previous_data_when_every_source_is_down(self):
        with patch('kp_core.requests.get', side_effect=OSError("down")):
            with support.patch_now(search):
                self.assertFalse(search.get_kp_pipeline())

        self.assertFalse(os.path.exists(self.output))

    def test_refuses_a_series_that_barely_reaches_the_future(self):
        """Too little forecast is worse than the file it would replace."""
        with patch.object(search, 'FORECAST_DAYS', 1):
            self.assertFalse(self.run_pipeline())

        self.assertFalse(os.path.exists(self.output))

    def test_never_overwrites_good_data_with_an_unpublishable_run(self):
        self.assertTrue(self.run_pipeline())
        with open(self.output) as handle:
            before = handle.read()

        with patch('kp_core.requests.get', side_effect=OSError("down")):
            with support.patch_now(search):
                self.assertFalse(search.get_kp_pipeline())

        with open(self.output) as handle:
            self.assertEqual(handle.read(), before)

    def test_refuses_to_publish_a_corrupt_series(self):
        corrupt = ([{"datetime": "2026-10-11T07:00:00Z", "kp": 1.0}] * 8, {})

        with patch('search.build_series', return_value=corrupt):
            with self.assertRaises(DataValidationError):
                self.run_pipeline()

        self.assertFalse(os.path.exists(self.output))


if __name__ == '__main__':
    unittest.main()
