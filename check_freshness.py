"""Fails when the published data has stopped moving.

The pipelines deliberately finish green when an upstream feed is down, so
that a five-minute outage does not raise a false alarm. The cost of that
choice is a blind spot: if a feed stays down for days, the files simply
stop changing and nothing says so.

This closes it, from two directions:

  * staleness - how long since the file last changed, read from git;
  * horizon   - how far beyond now the series still reaches.

Staleness catches a stuck pipeline early. Horizon catches the case that
actually hurts a consumer: data that no longer covers the present.
"""

import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone

from kp_core import parse_timestamp, utcnow

# Thresholds calibrated from the real update cadence of each file, measured
# over its last 400 commits. kp.json tracks measured Kp and moves every few
# hours; kp_noaa.json is pure forecast and normally moves once a day.
#
#            observed median   observed max   threshold
#  kp.json         1.3h            11.8h         18h
#  kp_noaa.json   24.0h            48.0h         72h
MAX_AGE_HOURS = {
    'kp.json': 18,
    'kp_noaa.json': 72,
}
DEFAULT_MAX_AGE_HOURS = 24

# A series that no longer reaches a day into the future is of no use to a
# consumer, whatever its file timestamp says.
MIN_HORIZON_HOURS = 24

DATA_FILES = ['data/kp.json', 'data/kp_noaa.json']


class FreshnessError(Exception):
    """Raised when a file is stale or no longer covers the present."""


def last_commit_time(path: str) -> datetime:
    """When git last recorded a change to this file, in naive UTC."""
    try:
        result = subprocess.run(
            ['git', 'log', '-1', '--format=%ct', '--', path],
            capture_output=True, text=True, check=True,
        )
    except (subprocess.CalledProcessError, OSError) as error:
        raise FreshnessError(f"Could not read git history for {path}: {error}")

    stamp = result.stdout.strip()
    if not stamp:
        raise FreshnessError(
            f"{path} has no commit history. If the checkout is shallow, "
            f"fetch more history (actions/checkout with fetch-depth: 0)."
        )

    # Naive UTC, to match every other timestamp in the project.
    return datetime.fromtimestamp(int(stamp), timezone.utc).replace(tzinfo=None)


def series_horizon(path: str, now: datetime) -> timedelta:
    """How far past `now` the last bin in the file reaches."""
    with open(path) as handle:
        records = json.load(handle)

    if not records:
        raise FreshnessError(f"{path} is empty")

    return parse_timestamp(records[-1]['datetime']) - now


def max_age_for(path: str) -> int:
    """The staleness budget for a file, by name."""
    name = path.replace('\\', '/').rsplit('/', 1)[-1]
    return MAX_AGE_HOURS.get(name, DEFAULT_MAX_AGE_HOURS)


def check_file(path: str, now=None, commit_time=last_commit_time) -> str:
    """Checks one file, returning a one-line report.

    Raises FreshnessError if the file is stale or no longer covers the
    present. `commit_time` is injectable so tests need no git history.
    """
    now = now or utcnow()

    age = now - commit_time(path)
    age_hours = age.total_seconds() / 3600
    budget = max_age_for(path)

    horizon = series_horizon(path, now)
    horizon_hours = horizon.total_seconds() / 3600

    summary = (
        f"{path}: last changed {age_hours:.1f}h ago (budget {budget}h), "
        f"reaches {horizon_hours:.1f}h ahead (minimum {MIN_HORIZON_HOURS}h)"
    )

    if age_hours > budget:
        raise FreshnessError(
            f"{path} has not changed for {age_hours:.1f}h, over its {budget}h "
            f"budget. The upstream feed is most likely down."
        )

    if horizon_hours < MIN_HORIZON_HOURS:
        raise FreshnessError(
            f"{path} only reaches {horizon_hours:.1f}h into the future, under "
            f"the {MIN_HORIZON_HOURS}h minimum. The data no longer covers the present."
        )

    return summary


def main(paths: list[str]) -> int:
    paths = paths or DATA_FILES
    now = utcnow()
    failures = []

    for path in paths:
        try:
            print(f"OK  {check_file(path, now=now)}")
        except (FreshnessError, OSError, ValueError, KeyError) as error:
            print(f"FAIL {error}", file=sys.stderr)
            failures.append(path)

    if failures:
        print(
            f"\n{len(failures)} file(s) are stale. "
            f"Check the scheduled workflow runs and the upstream feeds.",
            file=sys.stderr,
        )
        return 1

    print(f"\nAll {len(paths)} file(s) are fresh.")
    return 0


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv[1:]))
