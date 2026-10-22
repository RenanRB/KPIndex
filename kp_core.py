"""Shared building blocks for the Kp index pipelines.

Kp is a planetary geomagnetic index published in fixed three-hour bins
(00, 03, 06, 09, 12, 15, 18 and 21 UTC) on a scale from 0 to 9. Every rule
enforced here exists to protect those two invariants: a record whose
timestamp is not a real bin, or whose value falls outside the scale, is
corrupt and must never reach the published data.

The guiding principle is that publishing nothing is always better than
publishing something wrong, so validation failures abort the run and leave
the previous file untouched.
"""

import json
import logging
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone

import requests
import urllib3

# The GFZ and NOAA endpoints present certificates that fail verification on
# the runner, so requests are made unverified and the noisy warning muted.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Kp domain rules ---
KP_MIN = 0.0
KP_MAX = 9.0
BIN_HOURS = 3
VALID_BIN_HOURS = (0, 3, 6, 9, 12, 15, 18, 21)
TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

# --- Network behaviour ---
# The upstream hosts drop connections fairly often, so every request is
# retried before the run is considered a failure.
REQUEST_TIMEOUT = (15, 45)  # (connect, read) seconds
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 5


class DataValidationError(Exception):
    """Raised when fetched data violates a Kp invariant."""


def utcnow() -> datetime:
    """Current UTC time as a naive datetime.

    Naive UTC is used throughout because the upstream feeds publish naive
    UTC stamps; `datetime.utcnow` is deprecated, hence the explicit path.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def fetch_url(url: str, sleep=None) -> requests.Response:
    """Fetches a URL, retrying transient network errors with exponential backoff.

    `sleep` is injectable so tests do not have to wait through the backoff.
    """
    if sleep is None:
        sleep = time.sleep

    last_error = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(url, verify=False, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response
        except requests.RequestException as error:
            last_error = error
            if attempt == MAX_ATTEMPTS:
                break
            delay = BACKOFF_SECONDS * (2 ** (attempt - 1))
            logging.warning(
                "Attempt %d/%d failed for %s (%s). Retrying in %ds...",
                attempt, MAX_ATTEMPTS, url, error.__class__.__name__, delay
            )
            sleep(delay)

    raise last_error


def fetch_lines(url: str, expected_marker: str = None, sleep=None) -> list[str]:
    """Fetches a text document and splits it into lines.

    A maintenance page or captive portal can answer 200 with HTML, which a
    lenient parser would happily turn into nonsense records. `expected_marker`
    is a string the real document must contain, checked before parsing.
    """
    text = fetch_url(url, sleep=sleep).text

    if expected_marker and expected_marker not in text:
        preview = text[:200].replace('\n', ' ')
        raise DataValidationError(
            f"Response from {url} does not look like the expected document "
            f"(missing {expected_marker!r}). First 200 chars: {preview!r}"
        )

    return text.splitlines()


def fetch_json(url: str, sleep=None) -> dict:
    """Fetches and parses a JSON document."""
    response = fetch_url(url, sleep=sleep)
    try:
        return response.json()
    except ValueError as error:
        preview = response.text[:200].replace('\n', ' ')
        raise DataValidationError(
            f"Response from {url} is not valid JSON ({error}). "
            f"First 200 chars: {preview!r}"
        )


def snap_to_bin(moment: datetime) -> datetime:
    """Rounds a timestamp to the nearest three-hour Kp bin.

    The feeds normally publish exact bins; this tolerates small drifts
    without ever moving a reading to a different day by accident.
    """
    shifted = moment + timedelta(minutes=BIN_HOURS * 60 // 2)
    shifted = shifted.replace(minute=0, second=0, microsecond=0)
    return shifted.replace(hour=(shifted.hour // BIN_HOURS) * BIN_HOURS)


def floor_to_bin(moment: datetime) -> datetime:
    """Truncates a timestamp down to the Kp bin containing it."""
    return moment.replace(
        hour=(moment.hour // BIN_HOURS) * BIN_HOURS,
        minute=0, second=0, microsecond=0,
    )


def build_series(sources, now: datetime, history_days: int, forecast_days: int,
                 value_field: str = 'kp'):
    """Fills a window anchored on `now` from sources in priority order.

    `sources` is a sequence of (name, records), most trustworthy first. Each
    bin is taken from the first source that offers it, and anything outside
    the window is discarded.

    Anchoring the window on the clock rather than on the data is the whole
    point: a feed that freezes used to drag the window's edges back with it,
    silently shortening the forecast. Now a stale feed simply contributes
    nothing in range and the next source fills its place.

    Returns (records, provenance) where provenance maps a timestamp to the
    name of the source it came from, for logging.
    """
    start = floor_to_bin(now) - timedelta(days=history_days)
    end = floor_to_bin(now) + timedelta(days=forecast_days)

    chosen = {}
    provenance = {}

    for name, records in sources:
        for record in records or []:
            moment = parse_timestamp(record['datetime'])
            if moment < start or moment > end:
                continue

            key = format_timestamp(moment)
            if key in chosen:
                continue  # a higher-priority source already owns this bin

            chosen[key] = {"datetime": key, value_field: record[value_field]}
            provenance[key] = name

    ordered = [chosen[key] for key in sorted(chosen)]
    return ordered, provenance


def reject_stale_sources(sources, now: datetime, minimum_horizons: dict):
    """Drops forecast feeds that have stopped reaching into the future.

    A feed can answer 200 with a perfectly well-formed document and still be
    worthless, because nobody is updating it any more. HTTP status says
    nothing about that; only the newest timestamp inside the document does.

    `minimum_horizons` maps a source name to how far past `now` its data must
    reach. A name absent from the map is never judged — that is how measured
    feeds, which by definition do not predict, are left alone.

    Returns (kept, rejected_notes).
    """
    kept = []
    notes = []

    for name, records in sources:
        requirement = minimum_horizons.get(name)

        if requirement is None or not records:
            kept.append((name, records))
            continue

        horizon = series_horizon(records, now)
        if horizon < requirement:
            notes.append(
                f"{name} only reaches {horizon.total_seconds() / 3600:.1f}h ahead, "
                f"under the {requirement.total_seconds() / 3600:.0f}h it should - "
                f"treating it as stale"
            )
            continue

        kept.append((name, records))

    return kept, notes


def series_horizon(records: list[dict], now: datetime) -> timedelta:
    """How far beyond `now` the last bin of a series reaches."""
    if not records:
        return timedelta(0)

    return parse_timestamp(records[-1]['datetime']) - now


def is_aligned(moment: datetime) -> bool:
    """True when the timestamp already sits exactly on a Kp bin."""
    return (
        moment.hour in VALID_BIN_HOURS
        and moment.minute == 0
        and moment.second == 0
        and moment.microsecond == 0
    )


def format_timestamp(moment: datetime) -> str:
    """Renders a datetime in the published UTC format."""
    return moment.strftime(TIMESTAMP_FORMAT)


def parse_timestamp(value: str) -> datetime:
    """Parses a published UTC timestamp back into a naive datetime."""
    if not isinstance(value, str):
        raise DataValidationError(f"Timestamp must be a string, got {value!r}")

    text = value.strip()
    if text.endswith('Z'):
        text = text[:-1]
    elif text.endswith('+00:00'):
        text = text[:-6]

    try:
        return datetime.fromisoformat(text)
    except ValueError as error:
        raise DataValidationError(f"Unparseable timestamp {value!r}: {error}")


def clean_kp(value) -> float:
    """Normalises a Kp reading, returning None when it is not usable.

    The feeds use sentinels such as null or -1 for bins that have not been
    measured yet. Those must be dropped rather than published as real Kp.
    """
    if value is None or isinstance(value, bool):
        return None

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    # Rejects NaN and infinities, which compare false against every bound.
    if not (KP_MIN <= number <= KP_MAX):
        return None

    return number


def validate_series(records: list[dict], minimum_records: int = 8,
                    value_field: str = 'kp') -> list[str]:
    """Checks a finished series against every Kp invariant.

    Raises DataValidationError for corruption that must block publication,
    and returns a list of non-blocking warnings (currently timeline gaps,
    which are visible to consumers but not wrong).

    `value_field` is the key holding the index. It is 'kp' for the planetary
    series and 'k' for a local one: both live on the same 0-9 scale and obey
    the same bin rules, but they are different quantities and must never be
    published under the same name.
    """
    if not isinstance(records, list):
        raise DataValidationError(f"Series must be a list, got {type(records).__name__}")

    if len(records) < minimum_records:
        raise DataValidationError(
            f"Series has only {len(records)} records, expected at least {minimum_records}"
        )

    warnings = []
    previous = None

    for position, record in enumerate(records):
        if not isinstance(record, dict):
            raise DataValidationError(f"Record {position} is not an object: {record!r}")

        missing = {'datetime', value_field} - set(record)
        if missing:
            raise DataValidationError(
                f"Record {position} is missing {sorted(missing)}: {record!r}"
            )

        moment = parse_timestamp(record['datetime'])

        if not is_aligned(moment):
            raise DataValidationError(
                f"Record {position} is not on a three-hour Kp bin: {record['datetime']}"
            )

        value = record[value_field]
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise DataValidationError(
                f"Record {position} has a non-numeric Kp: {value!r}"
            )
        if not (KP_MIN <= value <= KP_MAX):
            raise DataValidationError(
                f"Record {position} has Kp {value} outside the {KP_MIN}-{KP_MAX} scale"
            )

        if previous is not None:
            if moment == previous:
                raise DataValidationError(
                    f"Duplicate timestamp at record {position}: {record['datetime']}"
                )
            if moment < previous:
                raise DataValidationError(
                    f"Timestamps go backwards at record {position}: "
                    f"{format_timestamp(previous)} then {record['datetime']}"
                )
            gap = moment - previous
            if gap > timedelta(hours=BIN_HOURS):
                missing_bins = int(gap.total_seconds() // 3600 // BIN_HOURS) - 1
                warnings.append(
                    f"Gap of {missing_bins} bin(s) between "
                    f"{format_timestamp(previous)} and {record['datetime']}"
                )

        previous = moment

    return warnings


def write_json_atomic(path: str, data) -> None:
    """Writes JSON via a temporary file so readers never see a partial write."""
    directory = os.path.dirname(os.path.abspath(path))
    handle, temporary_path = tempfile.mkstemp(dir=directory, suffix='.tmp')

    try:
        with os.fdopen(handle, 'w') as file:
            json.dump(data, file, indent=4)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary_path, path)
    except BaseException:
        if os.path.exists(temporary_path):
            os.unlink(temporary_path)
        raise


def fetch_source(name: str, fetcher):
    """Runs a fetcher, converting an unrecoverable failure into None."""
    try:
        return fetcher()
    except Exception as error:
        logging.error(
            "Source '%s' is unavailable: %s: %s",
            name, error.__class__.__name__, error
        )
        return None
