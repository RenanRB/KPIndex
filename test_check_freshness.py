"""Tests for the staleness gate.

This check exists to fail, so the interesting cases are the failing ones:
a file that stopped changing, and a series that no longer covers now.
"""

import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta

import check_freshness
import support
from check_freshness import FreshnessError, check_file, max_age_for
from kp_core import format_timestamp

NOW = datetime(2026, 9, 15, 12, 0)


def setUpModule():
    support.silence_logging()


class TestMaxAgeFor(unittest.TestCase):

    def test_each_file_gets_its_measured_budget(self):
        """The two files move at very different rates."""
        self.assertEqual(max_age_for('data/kp.json'), 18)
        self.assertEqual(max_age_for('data/kp_noaa.json'), 72)

    def test_budget_is_resolved_by_name_not_by_path(self):
        self.assertEqual(max_age_for('kp.json'), 18)
        self.assertEqual(max_age_for('/tmp/anywhere/kp_noaa.json'), 72)

    def test_unknown_files_fall_back_to_the_default(self):
        self.assertEqual(
            max_age_for('data/something_else.json'),
            check_freshness.DEFAULT_MAX_AGE_HOURS,
        )


class TestCheckFile(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp()

    def write_series(self, name='kp.json', horizon_hours=168):
        """Writes a series whose last bin sits `horizon_hours` beyond NOW."""
        path = os.path.join(self.directory, name)
        last = NOW + timedelta(hours=horizon_hours)
        records = [
            {"datetime": format_timestamp(last - timedelta(hours=3 * index)), "kp": 2.0}
            for index in reversed(range(8))
        ]
        with open(path, 'w') as handle:
            json.dump(records, handle)
        return path

    def aged(self, hours):
        """A commit-time function reporting a file last changed `hours` ago."""
        return lambda path: NOW - timedelta(hours=hours)

    def test_accepts_a_fresh_file(self):
        path = self.write_series()
        summary = check_file(path, now=NOW, commit_time=self.aged(1))
        self.assertIn('last changed 1.0h ago', summary)

    def test_rejects_a_file_that_stopped_changing(self):
        path = self.write_series()
        with self.assertRaises(FreshnessError) as caught:
            check_file(path, now=NOW, commit_time=self.aged(19))

        self.assertIn('has not changed for 19.0h', str(caught.exception))

    def test_accepts_a_file_just_inside_its_budget(self):
        path = self.write_series()
        check_file(path, now=NOW, commit_time=self.aged(17.9))

    def test_the_noaa_budget_is_more_generous(self):
        """26h is normal for kp_noaa.json and would be a false alarm at 24h."""
        path = self.write_series(name='kp_noaa.json')
        check_file(path, now=NOW, commit_time=self.aged(26))

        stale = self.write_series(name='kp.json')
        with self.assertRaises(FreshnessError):
            check_file(stale, now=NOW, commit_time=self.aged(26))

    def test_rejects_a_series_that_no_longer_covers_the_present(self):
        path = self.write_series(horizon_hours=6)
        with self.assertRaises(FreshnessError) as caught:
            check_file(path, now=NOW, commit_time=self.aged(1))

        self.assertIn('only reaches 6.0h into the future', str(caught.exception))

    def test_rejects_a_series_entirely_in_the_past(self):
        path = self.write_series(horizon_hours=-48)
        with self.assertRaises(FreshnessError):
            check_file(path, now=NOW, commit_time=self.aged(1))

    def test_a_recent_commit_does_not_excuse_a_dead_series(self):
        """Both dimensions are checked; neither alone is sufficient."""
        path = self.write_series(horizon_hours=1)
        with self.assertRaises(FreshnessError):
            check_file(path, now=NOW, commit_time=self.aged(0))

    def test_rejects_an_empty_file(self):
        path = os.path.join(self.directory, 'kp.json')
        with open(path, 'w') as handle:
            json.dump([], handle)

        with self.assertRaises(FreshnessError):
            check_file(path, now=NOW, commit_time=self.aged(1))

    def test_explains_a_shallow_checkout(self):
        path = self.write_series()

        def no_history(_):
            raise FreshnessError(
                "kp.json has no commit history. If the checkout is shallow, "
                "fetch more history (actions/checkout with fetch-depth: 0)."
            )

        with self.assertRaises(FreshnessError) as caught:
            check_file(path, now=NOW, commit_time=no_history)

        self.assertIn('fetch-depth', str(caught.exception))


class TestLastCommitTime(unittest.TestCase):

    def test_reads_this_repository(self):
        """Exercises the real git path against a file that is committed."""
        root = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(root, 'kp_core.py')

        if not os.path.exists(os.path.join(root, '.git')):
            self.skipTest('not a git checkout')

        moment = check_freshness.last_commit_time(path)
        self.assertIsInstance(moment, datetime)
        self.assertIsNone(moment.tzinfo)

    def test_points_at_fetch_depth_for_a_file_with_no_history(self):
        """Inside the repo but uncommitted, git succeeds and returns nothing.

        That is exactly what a shallow CI checkout looks like, so the error
        has to name the fix rather than just report emptiness.
        """
        root = os.path.dirname(os.path.abspath(__file__))
        if not os.path.exists(os.path.join(root, '.git')):
            self.skipTest('not a git checkout')

        handle, path = tempfile.mkstemp(dir=root, suffix='.json')
        os.close(handle)
        self.addCleanup(os.unlink, path)

        with self.assertRaises(FreshnessError) as caught:
            check_freshness.last_commit_time(path)

        self.assertIn('fetch-depth', str(caught.exception))

    def test_reports_a_file_outside_the_repository(self):
        with tempfile.NamedTemporaryFile(suffix='.json') as handle:
            with self.assertRaises(FreshnessError) as caught:
                check_freshness.last_commit_time(handle.name)

        self.assertIn('Could not read git history', str(caught.exception))


class TestMain(unittest.TestCase):

    def run_main(self, paths):
        with contextlib.redirect_stdout(io.StringIO()) as out:
            with contextlib.redirect_stderr(io.StringIO()) as err:
                code = check_freshness.main(paths)
        return code, out.getvalue() + err.getvalue()

    def test_the_committed_data_is_currently_fresh(self):
        root = os.path.dirname(os.path.abspath(__file__))
        if not os.path.exists(os.path.join(root, '.git')):
            self.skipTest('not a git checkout')

        code, _ = self.run_main([os.path.join(root, name) for name in check_freshness.DATA_FILES])
        self.assertEqual(code, 0)

    def test_fails_on_a_missing_file(self):
        code, output = self.run_main(['/nonexistent-kpindex/kp.json'])
        self.assertEqual(code, 1)
        self.assertIn('FAIL', output)

    def test_reports_every_failure_not_just_the_first(self):
        code, output = self.run_main([
            '/nonexistent-kpindex/a.json',
            '/nonexistent-kpindex/b.json',
        ])
        self.assertEqual(code, 1)
        self.assertIn('2 file(s) are stale', output)


if __name__ == '__main__':
    unittest.main()
