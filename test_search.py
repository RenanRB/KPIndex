"""Tests for the GFZ + NOAA pipeline behind data/kp.json."""

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import search
import support
from kp_core import DataValidationError, parse_timestamp, validate_series


def setUpModule():
    support.silence_logging()


class TestFetchGfzForecastCsv(unittest.TestCase):

    def test_parses_the_real_document(self):
        with support.patch_http():
            records = search.fetch_gfz_forecast_csv()

        self.assertTrue(records)
        self.assertEqual(records[0]['datetime'], '2026-09-16T00:00:00Z')
        self.assertAlmostEqual(records[0]['kp'], 3.333333)
        for record in records:
            self.assertTrue(0 <= record['kp'] <= 9)

    def test_reads_the_median_column_by_name(self):
        """A reordered CSV must not silently shift us onto another quantile."""
        csv = (
            "Time (UTC),median,minimum,maximum\n"
            "15-03-2026 00:00,4.0,1.0,9.0\n"
            "15-03-2026 03:00,5.0,1.0,9.0\n"
        )
        with support.patch_http({'spaceweather': csv}):
            records = search.fetch_gfz_forecast_csv()

        self.assertEqual([record['kp'] for record in records], [4.0, 5.0])

    def test_rejects_a_csv_without_a_median_column(self):
        csv = "Time (UTC),minimum,maximum\n15-03-2026 00:00,1.0,9.0\n"
        with support.patch_http({'spaceweather': csv}):
            with self.assertRaises(DataValidationError) as caught:
                search.fetch_gfz_forecast_csv()

        self.assertIn('median', str(caught.exception))

    def test_rejects_an_error_page(self):
        with support.patch_http({'spaceweather': '<html>502 Bad Gateway</html>'}):
            with self.assertRaises(DataValidationError):
                search.fetch_gfz_forecast_csv()

    def test_skips_unusable_rows_but_keeps_the_good_ones(self):
        csv = (
            "Time (UTC),minimum,q,median\n"
            "15-03-2026 00:00,x,x,3.0\n"
            "not-a-date,x,x,4.0\n"
            "15-03-2026 06:00,x,x,-1\n"
            "15-03-2026 09:00,x,x,99\n"
            "15-03-2026 12:00,x,x,5.0\n"
            "\n"
        )
        with support.patch_http({'spaceweather': csv}):
            records = search.fetch_gfz_forecast_csv()

        self.assertEqual(
            records,
            [
                {"datetime": "2026-03-15T00:00:00Z", "kp": 3.0},
                {"datetime": "2026-03-15T12:00:00Z", "kp": 5.0},
            ],
        )

    def test_rejects_a_csv_with_no_usable_rows(self):
        csv = "Time (UTC),median\nnot-a-date,4.0\n"
        with support.patch_http({'spaceweather': csv}):
            with self.assertRaises(DataValidationError):
                search.fetch_gfz_forecast_csv()

    def test_rejects_an_empty_csv(self):
        with support.patch_http({'spaceweather': ''}):
            with self.assertRaises(DataValidationError):
                search.fetch_gfz_forecast_csv()

    def test_ignores_rows_too_short_to_hold_a_value(self):
        csv = (
            "Time (UTC),minimum,q,median\n"
            "15-03-2026 00:00,x\n"
            "15-03-2026 03:00,x,x,4.0\n"
        )
        with support.patch_http({'spaceweather': csv}):
            records = search.fetch_gfz_forecast_csv()

        self.assertEqual(records, [{"datetime": "2026-03-15T03:00:00Z", "kp": 4.0}])

    def test_off_grid_times_are_snapped_onto_real_bins(self):
        csv = (
            "Time (UTC),median\n"
            "15-03-2026 22:00,2.0\n"
            "15-03-2026 23:45,3.0\n"
        )
        with support.patch_http({'spaceweather': csv}):
            records = search.fetch_gfz_forecast_csv()

        # 22:00 belongs to its own evening, not to midnight of the same day.
        self.assertEqual(records[0]['datetime'], '2026-03-15T21:00:00Z')
        self.assertEqual(records[1]['datetime'], '2026-03-16T00:00:00Z')


class TestFetchGfzRealtimeJson(unittest.TestCase):

    def test_parses_the_real_document(self):
        with support.patch_http():
            records = search.fetch_gfz_realtime_json()

        self.assertEqual(len(records), 16)
        self.assertEqual(records[0], {"datetime": "2026-09-14T00:00:00Z", "kp": 1.0})

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

    def test_requests_the_configured_window_in_utc(self):
        captured = {}

        def capture(url, **kwargs):
            captured['url'] = url
            return support.FakeResponse('{"datetime": [], "Kp": []}')

        with patch('kp_core.requests.get', side_effect=capture):
            with patch('search.utcnow', return_value=datetime(2026, 3, 15, 12)):
                search.fetch_gfz_realtime_json()

        self.assertIn('start=2026-03-13', captured['url'])
        self.assertIn('end=2026-03-15', captured['url'])


class TestFetchNoaa27DayOutlook(unittest.TestCase):

    def test_parses_the_real_document(self):
        with support.patch_http():
            records = search.fetch_noaa_27day_outlook()

        self.assertEqual(len(records), 27 * 8)
        self.assertEqual(records[0], {"datetime": "2026-09-14T00:00:00Z", "kp": 4.0})
        self.assertEqual(records[7]['datetime'], '2026-09-14T21:00:00Z')

    def test_spreads_a_day_across_its_eight_bins(self):
        text = ":Issued: 2026 Mar 15 1200 UTC\n2026 Mar 16  100  10  3\n"
        with support.patch_http({'27-day-outlook': text}):
            records = search.fetch_noaa_27day_outlook()

        self.assertEqual(len(records), 8)
        self.assertEqual([record['kp'] for record in records], [3.0] * 8)
        self.assertEqual(records[0]['datetime'], '2026-03-16T00:00:00Z')
        self.assertEqual(records[-1]['datetime'], '2026-03-16T21:00:00Z')

    def test_kp_is_always_a_float(self):
        """kp.json used to mix ints from NOAA with floats from GFZ."""
        text = ":Issued: 2026 Mar 15 1200 UTC\n2026 Mar 16  100  10  3\n"
        with support.patch_http({'27-day-outlook': text}):
            records = search.fetch_noaa_27day_outlook()

        for record in records:
            self.assertIsInstance(record['kp'], float)

    def test_rejects_a_document_without_an_issue_header(self):
        with support.patch_http({'27-day-outlook': '2026 Mar 16  100  10  3\n'}):
            with self.assertRaises(DataValidationError):
                search.fetch_noaa_27day_outlook()

    def test_handles_a_leap_day(self):
        text = ":Issued: 2028 Feb 28 1200 UTC\n2028 Feb 29  100  10  3\n"
        with support.patch_http({'27-day-outlook': text}):
            records = search.fetch_noaa_27day_outlook()

        self.assertEqual(records[0]['datetime'], '2028-02-29T00:00:00Z')

    def test_skips_rows_with_an_unreadable_date(self):
        text = (
            ":Issued: 2026 Mar 15 1200 UTC\n"
            "2026 Xxx 16  100  10  3\n"
            "2026 Mar 17  100  10  4\n"
        )
        with support.patch_http({'27-day-outlook': text}):
            records = search.fetch_noaa_27day_outlook()

        self.assertEqual(len(records), 8)
        self.assertEqual(records[0]['datetime'], '2026-03-17T00:00:00Z')

    def test_rejects_a_document_with_no_usable_rows(self):
        with support.patch_http({'27-day-outlook': ":Issued: 2026 Mar 15 1200 UTC\n# nothing\n"}):
            with self.assertRaises(DataValidationError):
                search.fetch_noaa_27day_outlook()

    def test_skips_rows_with_an_unusable_kp(self):
        text = (
            ":Issued: 2026 Mar 15 1200 UTC\n"
            "2026 Mar 16  100  10  3\n"
            "2026 Mar 17  100  10  bad\n"
        )
        with support.patch_http({'27-day-outlook': text}):
            records = search.fetch_noaa_27day_outlook()

        self.assertEqual(len(records), 8)


class TestCombineShortTerm(unittest.TestCase):

    def test_measurements_win_over_forecasts(self):
        forecast = [
            {"datetime": "2026-03-15T00:00:00Z", "kp": 1.0},
            {"datetime": "2026-03-15T03:00:00Z", "kp": 1.5},
        ]
        realtime = [{"datetime": "2026-03-15T00:00:00Z", "kp": 7.0}]

        combined = search.combine_short_term(forecast, realtime)

        self.assertEqual(
            combined,
            [
                {"datetime": "2026-03-15T00:00:00Z", "kp": 7.0},
                {"datetime": "2026-03-15T03:00:00Z", "kp": 1.5},
            ],
        )

    def test_result_is_sorted_and_unique(self):
        forecast = [
            {"datetime": "2026-03-15T09:00:00Z", "kp": 1.0},
            {"datetime": "2026-03-15T00:00:00Z", "kp": 2.0},
            {"datetime": "2026-03-15T09:00:00Z", "kp": 3.0},
        ]
        combined = search.combine_short_term(forecast, [])

        stamps = [record['datetime'] for record in combined]
        self.assertEqual(stamps, sorted(stamps))
        self.assertEqual(len(stamps), len(set(stamps)))


class TestMergeKpData(unittest.TestCase):

    def test_tail_starts_after_the_short_term_series(self):
        short_term = [
            {"datetime": "2026-03-15T00:00:00Z", "kp": 1.0},
            {"datetime": "2026-03-15T03:00:00Z", "kp": 1.0},
        ]
        long_term = [
            {"datetime": "2026-03-14T00:00:00Z", "kp": 9.0},   # before, dropped
            {"datetime": "2026-03-15T03:00:00Z", "kp": 9.0},   # overlaps, dropped
            {"datetime": "2026-03-15T06:00:00Z", "kp": 5.0},   # kept
            {"datetime": "2026-03-30T00:00:00Z", "kp": 9.0},   # past window, dropped
        ]

        merged = search.merge_kp_data(short_term, long_term)

        self.assertEqual(len(merged), 3)
        self.assertEqual(merged[-1], {"datetime": "2026-03-15T06:00:00Z", "kp": 5.0})

    def test_window_is_bounded_from_the_first_bin(self):
        short_term = [{"datetime": "2026-03-15T00:00:00Z", "kp": 1.0}]
        limit = parse_timestamp("2026-03-15T00:00:00Z") + timedelta(days=search.SERIES_WINDOW_DAYS)

        long_term = [
            {"datetime": (limit - timedelta(hours=3)).strftime('%Y-%m-%dT%H:%M:%SZ'), "kp": 2.0},
            {"datetime": limit.strftime('%Y-%m-%dT%H:%M:%SZ'), "kp": 3.0},
        ]

        merged = search.merge_kp_data(short_term, long_term)

        self.assertEqual(len(merged), 2)
        self.assertEqual(merged[-1]['kp'], 2.0)

    def test_empty_short_term_falls_back_to_the_tail(self):
        long_term = [{"datetime": "2026-03-15T00:00:00Z", "kp": 1.0}]
        self.assertEqual(search.merge_kp_data([], long_term), long_term)


class TestPipeline(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.output = os.path.join(self.directory, 'new_kp.json')
        patcher = patch.object(search, 'OUTPUT_FILE', self.output)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_writes_a_valid_series_from_the_real_documents(self):
        with support.patch_http():
            self.assertTrue(search.get_kp_pipeline())

        with open(self.output) as handle:
            records = json.load(handle)

        self.assertEqual(validate_series(records), [])
        self.assertEqual(records[0]['datetime'], '2026-09-14T00:00:00Z')

    def test_output_is_chronological_unique_and_in_range(self):
        with support.patch_http():
            search.get_kp_pipeline()

        with open(self.output) as handle:
            records = json.load(handle)

        stamps = [record['datetime'] for record in records]
        self.assertEqual(stamps, sorted(stamps))
        self.assertEqual(len(stamps), len(set(stamps)))
        for record in records:
            self.assertIsInstance(record['kp'], float)
            self.assertTrue(0.0 <= record['kp'] <= 9.0)

    def test_measured_history_precedes_the_forecast(self):
        """The series must start in the past and reach into the future."""
        with support.patch_http():
            search.get_kp_pipeline()

        with open(self.output) as handle:
            records = json.load(handle)

        first = parse_timestamp(records[0]['datetime'])
        last = parse_timestamp(records[-1]['datetime'])
        self.assertLess(first, last)
        self.assertGreater(last - first, timedelta(days=5))

    def test_keeps_previous_data_when_a_source_is_down(self):
        with patch.object(search, 'fetch_gfz_forecast_csv', side_effect=OSError("down")):
            with support.patch_http():
                self.assertFalse(search.get_kp_pipeline())

        self.assertFalse(os.path.exists(self.output))

    def test_never_overwrites_good_data_with_a_partial_run(self):
        with support.patch_http():
            search.get_kp_pipeline()
        with open(self.output) as handle:
            before = handle.read()

        with patch.object(search, 'fetch_noaa_27day_outlook', side_effect=OSError("down")):
            with support.patch_http():
                self.assertFalse(search.get_kp_pipeline())

        with open(self.output) as handle:
            self.assertEqual(handle.read(), before)

    def test_refuses_to_publish_a_corrupt_series(self):
        """Validation failure must raise, not quietly write bad data."""
        corrupt = [{"datetime": "2026-03-15T07:00:00Z", "kp": 1.0}] * 8

        with patch.object(search, 'merge_kp_data', return_value=corrupt):
            with support.patch_http():
                with self.assertRaises(DataValidationError):
                    search.get_kp_pipeline()

        self.assertFalse(os.path.exists(self.output))


if __name__ == '__main__':
    unittest.main()
