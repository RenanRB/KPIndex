"""Audits the published data files against every Kp invariant.

Run with no arguments it checks everything under data/; given paths it
checks those instead, which is how the hourly workflows inspect a freshly
built file before it is allowed to replace the previous one.

Exits non-zero on the first file that fails, so it works as a CI gate.
"""

import json
import logging
import os
import sys

from kp_core import DataValidationError, validate_series

DATA_DIR = 'data'
# Freshness is only meaningful for a file the scheduler keeps rewriting, so
# it is reported rather than enforced here.
EXPECTED_MINIMUM_RECORDS = 8


def validate_file(path: str) -> list[str]:
    """Validates one JSON file, returning its non-blocking warnings.

    Reporting belongs to the caller so this stays usable as a plain check.
    """
    with open(path) as handle:
        try:
            records = json.load(handle)
        except ValueError as error:
            raise DataValidationError(f"{path} is not valid JSON: {error}")

    return validate_series(records, minimum_records=EXPECTED_MINIMUM_RECORDS)


def describe(path: str) -> str:
    """One-line summary of a file's coverage, for the run log."""
    with open(path) as handle:
        records = json.load(handle)

    return (
        f"{len(records)} records, "
        f"{records[0]['datetime']} to {records[-1]['datetime']}"
    )


def main(paths: list[str]) -> int:
    if not paths:
        if not os.path.isdir(DATA_DIR):
            print(f"No {DATA_DIR}/ directory to check.", file=sys.stderr)
            return 1
        paths = sorted(
            os.path.join(DATA_DIR, name)
            for name in os.listdir(DATA_DIR)
            if name.endswith('.json')
        )

    if not paths:
        print("No JSON files to check.", file=sys.stderr)
        return 1

    failures = 0

    for path in paths:
        try:
            warnings = validate_file(path)
        except (DataValidationError, OSError) as error:
            print(f"FAIL {path}: {error}", file=sys.stderr)
            failures += 1
            continue

        print(f"OK  {path}: {describe(path)}")
        for warning in warnings:
            print(f"    warning: {warning}")

    if failures:
        print(f"\n{failures} file(s) failed validation.", file=sys.stderr)
        return 1

    print(f"\nAll {len(paths)} file(s) passed.")
    return 0


if __name__ == '__main__':  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    sys.exit(main(sys.argv[1:]))
