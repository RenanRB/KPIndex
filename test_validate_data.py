"""Tests for the data audit gate.

This is the last check before a file replaces known-good data, so its
failure paths matter more than its success path.
"""

import contextlib
import io
import json
import os
import tempfile
import unittest
import unittest.mock

import support
import validate_data
from kp_core import DataValidationError, format_timestamp, parse_timestamp


def setUpModule():
    support.silence_logging()


def series(count=8, kp=2.0):
    first = parse_timestamp('2026-03-15T00:00:00Z')
    from datetime import timedelta
    return [
        {"datetime": format_timestamp(first + timedelta(hours=3 * index)), "kp": kp}
        for index in range(count)
    ]


class TestValidateFile(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp()

    def write(self, name, content):
        path = os.path.join(self.directory, name)
        with open(path, 'w') as handle:
            if isinstance(content, str):
                handle.write(content)
            else:
                json.dump(content, handle)
        return path

    def test_accepts_a_clean_file(self):
        path = self.write('good.json', series())
        self.assertEqual(validate_data.validate_file(path), [])

    def test_rejects_malformed_json(self):
        path = self.write('broken.json', '{not json')
        with self.assertRaises(DataValidationError) as caught:
            validate_data.validate_file(path)
        self.assertIn('not valid JSON', str(caught.exception))

    def test_rejects_an_empty_file(self):
        path = self.write('empty.json', '')
        with self.assertRaises(DataValidationError):
            validate_data.validate_file(path)

    def test_rejects_a_corrupt_series(self):
        records = series()
        records[3]['kp'] = 42
        path = self.write('bad.json', records)
        with self.assertRaises(DataValidationError):
            validate_data.validate_file(path)

    def test_reports_gaps_without_failing(self):
        records = series(count=10)
        del records[4]
        path = self.write('gappy.json', records)
        self.assertEqual(len(validate_data.validate_file(path)), 1)


class TestMain(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp()

    def run_main(self, paths):
        """Runs the CLI entry point without its report reaching the test log."""
        with contextlib.redirect_stdout(io.StringIO()):
            with contextlib.redirect_stderr(io.StringIO()):
                return validate_data.main(paths)

    def write(self, name, content):
        path = os.path.join(self.directory, name)
        with open(path, 'w') as handle:
            json.dump(content, handle)
        return path

    def test_succeeds_on_good_files(self):
        paths = [self.write('a.json', series()), self.write('b.json', series())]
        self.assertEqual(self.run_main(paths), 0)

    def test_fails_on_a_bad_file(self):
        good = self.write('a.json', series())
        bad = self.write('b.json', [{"datetime": "2026-03-15T07:00:00Z", "kp": 1.0}] * 8)
        self.assertEqual(self.run_main([good, bad]), 1)

    def test_fails_on_a_missing_file(self):
        self.assertEqual(self.run_main([os.path.join(self.directory, 'nope.json')]), 1)

    def test_checks_every_file_before_reporting(self):
        """One bad file must not hide a second one."""
        bad_one = self.write('a.json', [{"datetime": "x", "kp": 1.0}] * 8)
        bad_two = self.write('b.json', [{"datetime": "2026-03-15T07:00:00Z", "kp": 1.0}] * 8)
        self.assertEqual(self.run_main([bad_one, bad_two]), 1)


class TestMainDefaults(unittest.TestCase):

    def run_main(self, paths):
        with contextlib.redirect_stdout(io.StringIO()):
            with contextlib.redirect_stderr(io.StringIO()):
                return validate_data.main(paths)

    def test_fails_when_there_is_no_data_directory(self):
        with unittest.mock.patch.object(validate_data, 'DATA_DIR', '/nonexistent-kpindex'):
            self.assertEqual(self.run_main([]), 1)

    def test_fails_when_the_data_directory_is_empty(self):
        empty = tempfile.mkdtemp()
        with unittest.mock.patch.object(validate_data, 'DATA_DIR', empty):
            self.assertEqual(self.run_main([]), 1)

    def test_checks_the_whole_data_directory_by_default(self):
        root = os.path.dirname(os.path.abspath(__file__))
        with unittest.mock.patch.object(validate_data, 'DATA_DIR', os.path.join(root, 'data')):
            self.assertEqual(self.run_main([]), 0)


class TestPublishedData(unittest.TestCase):
    """The files actually committed to data/ must always be valid."""

    def test_committed_data_satisfies_every_invariant(self):
        root = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(root, 'data')

        if not os.path.isdir(data_dir):
            self.skipTest('no data/ directory')

        names = [name for name in os.listdir(data_dir) if name.endswith('.json')]
        self.assertTrue(names, 'data/ has no JSON files')

        for name in names:
            with self.subTest(file=name):
                validate_data.validate_file(os.path.join(data_dir, name))


if __name__ == '__main__':
    unittest.main()
