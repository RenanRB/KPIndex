"""Tests for the NOAA-only pipeline behind data/kp_noaa.json."""

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import search_noaa
import support
from kp_core import DataValidationError, parse_timestamp, validate_series


def setUpModule():
    support.silence_logging()


def forecast_document(issued, header, rows):
    """Builds a 3-day forecast document in NOAA's real layout."""
    body = [f":Issued: {issued} UTC", "#", "NOAA Kp index forecast", header]
    body.extend(rows)
    return "\n".join(body) + "\n"


class TestParseIssueDate(unittest.TestCase):

    def test_reads_the_issue_date(self):
        lines = [":Issued: 2026 Sep 15 2205 UTC"]
        self.assertEqual(search_noaa.parse_issue_date(lines), datetime(2026, 9, 15, 22, 5))

    def test_rejects_a_document_without_one(self):
        with self.assertRaises(DataValidationError):
            search_noaa.parse_issue_date(["# nothing here"])

    def test_rejects_an_unreadable_issue_date(self):
        with self.assertRaises(DataValidationError):
            search_noaa.parse_issue_date([":Issued: not a date UTC"])


class TestParseColumnDates(unittest.TestCase):

    def test_reads_the_real_header(self):
        dates = search_noaa.parse_column_dates(
            "             Sep 16    Sep 17    Sep 18", datetime(2026, 9, 15, 22, 5)
        )
        self.assertEqual(dates, [datetime(2026, 9, 16), datetime(2026, 9, 17), datetime(2026, 9, 18)])

    def test_a_forecast_covering_its_own_issue_day(self):
        """NOAA does not always start the forecast the day after issuance.

        The previous parser assumed issue date + 1 and would have shifted
        every reading one day into the future.
        """
        dates = search_noaa.parse_column_dates(
            "Sep 16  Sep 17  Sep 18", datetime(2026, 9, 16, 0, 30)
        )
        self.assertEqual(dates[0], datetime(2026, 9, 16))

    def test_rolls_over_the_new_year(self):
        dates = search_noaa.parse_column_dates(
            "Dec 31  Jan 01  Jan 02", datetime(2026, 12, 30, 22, 5)
        )
        self.assertEqual(dates, [datetime(2026, 12, 31), datetime(2027, 1, 1), datetime(2027, 1, 2)])

    def test_rolls_over_when_no_december_column_is_present(self):
        """Issued on 31 Dec, every column already belongs to the next year."""
        dates = search_noaa.parse_column_dates(
            "Jan 01  Jan 02  Jan 03", datetime(2026, 12, 31, 22, 5)
        )
        self.assertEqual(dates, [datetime(2027, 1, 1), datetime(2027, 1, 2), datetime(2027, 1, 3)])

    def test_handles_a_leap_day(self):
        """'%b %d' alone would default to 1900 and reject 29 February."""
        dates = search_noaa.parse_column_dates(
            "Feb 28  Feb 29  Mar 01", datetime(2028, 2, 27, 22, 5)
        )
        self.assertEqual(dates[1], datetime(2028, 2, 29))

    def test_rejects_a_real_impossible_date(self):
        with self.assertRaises(DataValidationError):
            search_noaa.parse_column_dates("Feb 30", datetime(2026, 2, 27))

    def test_rejects_malformed_headers(self):
        for header in ["", "Sep", "Sep 16 Sep", "Xxx 16"]:
            with self.assertRaises(DataValidationError, msg=repr(header)):
                search_noaa.parse_column_dates(header, datetime(2026, 9, 15))

    def test_dates_are_strictly_increasing(self):
        dates = search_noaa.parse_column_dates(
            "Dec 30  Dec 31  Jan 01", datetime(2026, 12, 29)
        )
        self.assertEqual(dates, sorted(dates))
        self.assertEqual(len(dates), len(set(dates)))


class TestFetchHourData(unittest.TestCase):

    def test_parses_the_real_document(self):
        with support.patch_http():
            records = search_noaa.fetch_and_process_hour_data(search_noaa.url_hour)

        self.assertEqual(len(records), 24)
        self.assertEqual(records[0], {"datetime": "2026-09-16T00:00:00Z", "kp": 3.67})
        self.assertEqual(records[-1], {"datetime": "2026-09-18T21:00:00Z", "kp": 3.0})

    def test_each_row_is_anchored_on_its_own_label(self):
        """Rows out of order must still land on the right bin."""
        document = forecast_document(
            "2026 Sep 15 2205", "Sep 16", ["21-00UT  5.00", "00-03UT  1.00", "09-12UT  3.00"]
        )
        with support.patch_http({'3-day-geomag': document}):
            records = search_noaa.fetch_and_process_hour_data(search_noaa.url_hour)

        self.assertEqual(
            records,
            [
                {"datetime": "2026-09-16T00:00:00Z", "kp": 1.0},
                {"datetime": "2026-09-16T09:00:00Z", "kp": 3.0},
                {"datetime": "2026-09-16T21:00:00Z", "kp": 5.0},
            ],
        )

    def test_the_last_bin_stays_on_its_own_day(self):
        """'21-00UT' starts at 21:00, it does not wrap to midnight."""
        document = forecast_document("2026 Sep 15 2205", "Sep 16", ["21-00UT  5.00"])
        with support.patch_http({'3-day-geomag': document}):
            records = search_noaa.fetch_and_process_hour_data(search_noaa.url_hour)

        self.assertEqual(records[0]['datetime'], '2026-09-16T21:00:00Z')

    def test_skips_a_row_with_an_unreadable_label(self):
        document = forecast_document(
            "2026 Sep 15 2205", "Sep 16", ["xx-yyUT  5.00", "00-03UT  1.00"]
        )
        with support.patch_http({'3-day-geomag': document}):
            records = search_noaa.fetch_and_process_hour_data(search_noaa.url_hour)

        self.assertEqual(records, [{"datetime": "2026-09-16T00:00:00Z", "kp": 1.0}])

    def test_rejects_a_forecast_with_no_usable_rows(self):
        document = forecast_document("2026 Sep 15 2205", "Sep 16", ["xx-yyUT  5.00"])
        with support.patch_http({'3-day-geomag': document}):
            with self.assertRaises(DataValidationError):
                search_noaa.fetch_and_process_hour_data(search_noaa.url_hour)

    def test_rejects_a_row_whose_width_does_not_match_the_header(self):
        document = forecast_document(
            "2026 Sep 15 2205", "Sep 16  Sep 17  Sep 18", ["00-03UT  1.00  2.00"]
        )
        with support.patch_http({'3-day-geomag': document}):
            with self.assertRaises(DataValidationError) as caught:
                search_noaa.fetch_and_process_hour_data(search_noaa.url_hour)

        self.assertIn('2 values for 3', str(caught.exception))

    def test_rejects_an_error_page(self):
        with support.patch_http({'3-day-geomag': '<html>503</html>'}):
            with self.assertRaises(DataValidationError):
                search_noaa.fetch_and_process_hour_data(search_noaa.url_hour)

    def test_rejects_a_document_with_no_kp_section(self):
        document = ":Issued: 2026 Sep 15 2205 UTC\nNOAA Kp index forecast\n"
        with support.patch_http({'3-day-geomag': document}):
            with self.assertRaises(DataValidationError):
                search_noaa.fetch_and_process_hour_data(search_noaa.url_hour)

    def test_skips_unusable_values(self):
        document = forecast_document(
            "2026 Sep 15 2205", "Sep 16  Sep 17", ["00-03UT  1.00  -1", "03-06UT  99  2.00"]
        )
        with support.patch_http({'3-day-geomag': document}):
            records = search_noaa.fetch_and_process_hour_data(search_noaa.url_hour)

        self.assertEqual(
            records,
            [
                {"datetime": "2026-09-16T00:00:00Z", "kp": 1.0},
                {"datetime": "2026-09-17T03:00:00Z", "kp": 2.0},
            ],
        )

    def test_output_is_chronological(self):
        with support.patch_http():
            records = search_noaa.fetch_and_process_hour_data(search_noaa.url_hour)

        stamps = [record['datetime'] for record in records]
        self.assertEqual(stamps, sorted(stamps))


class TestFetchDailyData(unittest.TestCase):

    def test_parses_the_real_document(self):
        with support.patch_http():
            records = search_noaa.fetch_and_process_daily_data(search_noaa.url_daily)

        self.assertEqual(len(records), 27 * 8)
        self.assertEqual(records[0], {"datetime": "2026-09-14T00:00:00Z", "kp": 4.0})

    def test_rejects_a_document_without_an_issue_header(self):
        with support.patch_http({'27-day-outlook': '2026 Sep 16 100 10 3\n'}):
            with self.assertRaises(DataValidationError):
                search_noaa.fetch_and_process_daily_data(search_noaa.url_daily)

    def test_skips_rows_with_an_unreadable_date(self):
        text = (
            ":Issued: 2026 Mar 15 1200 UTC\n"
            "2026 Xxx 16  100  10  3\n"
            "2026 Mar 17  100  10  4\n"
        )
        with support.patch_http({'27-day-outlook': text}):
            records = search_noaa.fetch_and_process_daily_data(search_noaa.url_daily)

        self.assertEqual(len(records), 8)
        self.assertEqual(records[0]['datetime'], '2026-03-17T00:00:00Z')

    def test_rejects_a_document_with_no_usable_rows(self):
        with support.patch_http({'27-day-outlook': ":Issued: 2026 Mar 15 1200 UTC\n# none\n"}):
            with self.assertRaises(DataValidationError):
                search_noaa.fetch_and_process_daily_data(search_noaa.url_daily)


class TestMergeInfos(unittest.TestCase):

    def test_tail_starts_after_the_hourly_forecast(self):
        hourly = [
            {"datetime": "2026-03-15T00:00:00Z", "kp": 1.0},
            {"datetime": "2026-03-15T03:00:00Z", "kp": 1.0},
        ]
        daily = [
            {"datetime": "2026-03-15T03:00:00Z", "kp": 9.0},   # overlaps, dropped
            {"datetime": "2026-03-15T06:00:00Z", "kp": 5.0},   # kept
            {"datetime": "2026-03-30T00:00:00Z", "kp": 9.0},   # past window, dropped
        ]

        merged = search_noaa.merge_infos(hourly, daily)

        self.assertEqual(len(merged), 3)
        self.assertEqual(merged[-1]['kp'], 5.0)

    def test_uses_utc_not_local_time_when_the_forecast_is_empty(self):
        """A runner in another timezone must not cut the tail on a wrong day."""
        fixed = datetime(2026, 3, 15, 12)
        daily = [
            {"datetime": "2026-03-15T09:00:00Z", "kp": 1.0},   # before now, dropped
            {"datetime": "2026-03-16T00:00:00Z", "kp": 2.0},   # kept
        ]

        with patch.object(search_noaa, 'utcnow', return_value=fixed):
            merged = search_noaa.merge_infos([], daily)

        self.assertEqual(merged, [{"datetime": "2026-03-16T00:00:00Z", "kp": 2.0}])

    def test_window_is_bounded(self):
        hourly = [{"datetime": "2026-03-15T00:00:00Z", "kp": 1.0}]
        limit = parse_timestamp("2026-03-15T00:00:00Z") + timedelta(
            days=search_noaa.SERIES_WINDOW_DAYS
        )
        daily = [{"datetime": limit.strftime('%Y-%m-%dT%H:%M:%SZ'), "kp": 2.0}]

        self.assertEqual(search_noaa.merge_infos(hourly, daily), hourly)


class TestPipeline(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.output = os.path.join(self.directory, 'new_kp.json')
        patcher = patch.object(search_noaa, 'OUTPUT_FILE', self.output)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_writes_a_valid_series_from_the_real_documents(self):
        with support.patch_http():
            self.assertTrue(search_noaa.merge_and_save_data())

        with open(self.output) as handle:
            records = json.load(handle)

        self.assertEqual(validate_series(records), [])
        self.assertEqual(len(records), 56)

    def test_output_is_chronological_unique_and_in_range(self):
        with support.patch_http():
            search_noaa.merge_and_save_data()

        with open(self.output) as handle:
            records = json.load(handle)

        stamps = [record['datetime'] for record in records]
        self.assertEqual(stamps, sorted(stamps))
        self.assertEqual(len(stamps), len(set(stamps)))
        for record in records:
            self.assertIsInstance(record['kp'], float)
            self.assertTrue(0.0 <= record['kp'] <= 9.0)

    def test_keeps_previous_data_when_a_source_is_down(self):
        with patch.object(search_noaa, 'fetch_and_process_hour_data', side_effect=OSError("down")):
            with support.patch_http():
                self.assertFalse(search_noaa.merge_and_save_data())

        self.assertFalse(os.path.exists(self.output))

    def test_refuses_to_publish_a_corrupt_series(self):
        corrupt = [{"datetime": "2026-03-15T07:00:00Z", "kp": 1.0}] * 8

        with patch.object(search_noaa, 'merge_infos', return_value=corrupt):
            with support.patch_http():
                with self.assertRaises(DataValidationError):
                    search_noaa.merge_and_save_data()

        self.assertFalse(os.path.exists(self.output))


if __name__ == '__main__':
    unittest.main()
