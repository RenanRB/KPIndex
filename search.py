"""Builds the combined Kp index series published in data/kp.json.

Three feeds are stitched together, in increasing order of uncertainty:

  1. GFZ realtime   - Kp already measured, for the last two days.
  2. GFZ forecast   - modelled Kp for roughly the next three days.
  3. NOAA 27-day    - a single daily figure, used to extend the tail.

Measurements always win over forecasts for the same bin. The result is
validated as a whole before anything is written, and a run that cannot
produce a trustworthy series writes nothing at all.
"""

import csv
import logging
from datetime import datetime, timedelta

from kp_core import (
    DataValidationError,
    clean_kp,
    fetch_json,
    fetch_lines,
    fetch_source,
    format_timestamp,
    is_aligned,
    parse_timestamp,
    snap_to_bin,
    utcnow,
    validate_series,
    write_json_atomic,
)

# --- Configuration Constants ---
GFZ_FORECAST_URL = 'https://spaceweather.gfz-potsdam.de/fileadmin/Kp-Forecast/CSV/kp_product_file_FORECAST_PAGER_SWIFT_LAST.csv'
GFZ_REALTIME_URL_TEMPLATE = 'https://kp.gfz-potsdam.de/app/json/?start={start}T00:00:00Z&end={end}T23%3A59%3A59Z&index=Kp#kpdatadownload-143'
NOAA_OUTLOOK_URL = 'https://services.swpc.noaa.gov/text/27-day-outlook.txt'
OUTPUT_FILE = 'new_kp.json'

# Strings the genuine documents contain, used to reject error pages served
# with a 200 status before they reach a parser.
GFZ_FORECAST_MARKER = 'Time (UTC)'
NOAA_OUTLOOK_MARKER = ':Issued:'

# The forecast publishes several quantiles per bin; we track the median.
FORECAST_VALUE_COLUMN = 'median'
FORECAST_TIME_FORMAT = '%d-%m-%Y %H:%M'

# How many days of realtime history to request, and how far past the start of
# the series the NOAA tail is allowed to reach.
REALTIME_LOOKBACK_DAYS = 2
SERIES_WINDOW_DAYS = 9


def fetch_gfz_forecast_csv() -> list[dict]:
    """Fetches the short-term GFZ forecast, keeping the median Kp per bin."""
    lines = fetch_lines(GFZ_FORECAST_URL, expected_marker=GFZ_FORECAST_MARKER)

    rows = csv.reader(lines)
    try:
        header = next(rows)
    except StopIteration:
        raise DataValidationError("GFZ forecast CSV is empty")

    # Resolved by name so a reordered or extended CSV cannot silently shift
    # us onto the wrong quantile.
    try:
        value_column = [column.strip() for column in header].index(FORECAST_VALUE_COLUMN)
    except ValueError:
        raise DataValidationError(
            f"GFZ forecast CSV has no {FORECAST_VALUE_COLUMN!r} column. Header: {header}"
        )

    forecast_list = []

    for position, row in enumerate(rows, start=2):
        if not row or len(row) <= value_column:
            continue

        try:
            moment = datetime.strptime(row[0].strip(), FORECAST_TIME_FORMAT)
        except ValueError:
            logging.warning("GFZ forecast line %d has an unreadable time: %r", position, row[0])
            continue

        kp = clean_kp(row[value_column])
        if kp is None:
            logging.warning(
                "GFZ forecast line %d has an unusable Kp: %r", position, row[value_column]
            )
            continue

        if not is_aligned(moment):
            logging.warning(
                "GFZ forecast line %d is off-grid (%s); snapping to the nearest bin",
                position, moment
            )

        forecast_list.append({
            "datetime": format_timestamp(snap_to_bin(moment)),
            "kp": kp,
        })

    if not forecast_list:
        raise DataValidationError("GFZ forecast CSV produced no usable rows")

    return forecast_list


def fetch_gfz_realtime_json() -> list[dict]:
    """Fetches the observed Kp index from GFZ for the recent past."""
    today = utcnow()
    first_day = today - timedelta(days=REALTIME_LOOKBACK_DAYS)

    url = GFZ_REALTIME_URL_TEMPLATE.format(
        start=first_day.strftime("%Y-%m-%d"),
        end=today.strftime("%Y-%m-%d")
    )

    data = fetch_json(url)

    if not isinstance(data, dict):
        raise DataValidationError(
            f"GFZ realtime returned {type(data).__name__}, expected an object"
        )

    moments = data.get('datetime')
    values = data.get('Kp')

    if not isinstance(moments, list) or not isinstance(values, list):
        raise DataValidationError(
            f"GFZ realtime is missing the datetime/Kp arrays. Keys: {sorted(data)}"
        )
    if len(moments) != len(values):
        raise DataValidationError(
            f"GFZ realtime returned {len(moments)} timestamps for {len(values)} Kp values"
        )

    result = []

    for raw_moment, raw_kp in zip(moments, values):
        # Bins that have not been measured yet arrive as null or a negative
        # sentinel; publishing those as real readings would be a data defect.
        kp = clean_kp(raw_kp)
        if kp is None:
            logging.info("Skipping unmeasured realtime bin %s (Kp=%r)", raw_moment, raw_kp)
            continue

        moment = parse_timestamp(raw_moment)
        if not is_aligned(moment):
            logging.warning(
                "GFZ realtime bin %s is off-grid; snapping to the nearest bin", raw_moment
            )

        result.append({
            "datetime": format_timestamp(snap_to_bin(moment)),
            "kp": kp,
        })

    return result


def fetch_noaa_27day_outlook() -> list[dict]:
    """Fetches the NOAA 27-day outlook and spreads each day across its bins.

    NOAA publishes one figure per day, the largest Kp expected that day. It
    is repeated across all eight bins because the consumer expects a uniform
    three-hour series; the tail is therefore an upper bound, not a profile.
    """
    lines = fetch_lines(NOAA_OUTLOOK_URL, expected_marker=NOAA_OUTLOOK_MARKER)

    issued = False
    kp_data = []

    for line in lines:
        parts = line.split()

        if line.startswith(':Issued:'):
            issued = True
            continue

        # Data rows look like: 2026 Sep 16  95  20  5
        if not issued or len(parts) != 6 or not parts[0].isdigit():
            continue

        try:
            day = datetime.strptime(f"{parts[0]} {parts[1]} {parts[2]}", '%Y %b %d')
        except ValueError:
            logging.warning("NOAA outlook row has an unreadable date: %r", line)
            continue

        kp = clean_kp(parts[5])
        if kp is None:
            logging.warning("NOAA outlook row has an unusable Kp: %r", line)
            continue

        for bin_index in range(8):
            kp_data.append({
                "datetime": format_timestamp(day + timedelta(hours=3 * bin_index)),
                "kp": kp,
            })

    if not issued:
        raise DataValidationError("NOAA outlook has no ':Issued:' header")
    if not kp_data:
        raise DataValidationError("NOAA outlook produced no usable rows")

    return kp_data


def merge_kp_data(short_term_data: list[dict], long_term_data: list[dict]) -> list[dict]:
    """Appends the long-term tail to the short-term series without overlap.

    The tail starts after the last short-term bin and stops SERIES_WINDOW_DAYS
    after the first one, which bounds how far ahead the coarse daily figures
    are allowed to reach.
    """
    if not short_term_data:
        return long_term_data

    moments = [parse_timestamp(entry["datetime"]) for entry in short_term_data]
    first_date = min(moments)
    last_date = max(moments)
    limit_date = first_date + timedelta(days=SERIES_WINDOW_DAYS)

    long_term_filtered = [
        entry for entry in long_term_data
        if last_date < parse_timestamp(entry["datetime"]) < limit_date
    ]

    return short_term_data + long_term_filtered


def combine_short_term(forecast: list[dict], realtime: list[dict]) -> list[dict]:
    """Overlays measured Kp on top of the forecast for the same bins."""
    short_term = {entry['datetime']: entry for entry in forecast}

    if len(short_term) != len(forecast):
        logging.warning(
            "GFZ forecast contained %d duplicate bin(s); keeping the last of each",
            len(forecast) - len(short_term)
        )

    overridden = 0
    for entry in realtime:
        if entry['datetime'] in short_term:
            overridden += 1
        short_term[entry['datetime']] = entry

    if overridden:
        logging.info("Measured Kp replaced the forecast for %d bin(s)", overridden)

    return sorted(short_term.values(), key=lambda entry: entry['datetime'])


def get_kp_pipeline() -> bool:
    """Runs the full pipeline.

    Returns True when a validated file was written, and False when a source
    was unavailable and the previous data should be kept. Raises
    DataValidationError when the data was fetched but cannot be trusted.
    """
    logging.info("Starting Kp fetch pipeline...")

    gfz_forecast = fetch_source('GFZ forecast', fetch_gfz_forecast_csv)
    gfz_realtime = fetch_source('GFZ realtime', fetch_gfz_realtime_json)
    noaa_outlook = fetch_source('NOAA 27-day outlook', fetch_noaa_27day_outlook)

    # Each source covers a different slice of the timeline, so a partial run
    # would silently publish a truncated series over a complete one.
    missing = [
        name for name, data in (
            ('GFZ forecast', gfz_forecast),
            ('GFZ realtime', gfz_realtime),
            ('NOAA 27-day outlook', noaa_outlook),
        ) if data is None
    ]
    if missing:
        logging.warning("Skipping update, unavailable source(s): %s", ', '.join(missing))
        return False

    short_term_merged = combine_short_term(gfz_forecast, gfz_realtime)
    final_result = merge_kp_data(short_term_merged, noaa_outlook)

    for warning in validate_series(final_result):
        logging.warning("Series warning: %s", warning)

    write_json_atomic(OUTPUT_FILE, final_result)

    logging.info(
        "Successfully saved %d Kp records to %s (%s to %s)",
        len(final_result), OUTPUT_FILE,
        final_result[0]['datetime'], final_result[-1]['datetime']
    )
    return True


if __name__ == '__main__':  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if not get_kp_pipeline():
        # Exit successfully so a transient upstream outage is not reported as
        # a broken build, but flag it on the GitHub Actions summary.
        print("::warning::Kp sources unavailable, data/kp.json kept unchanged.")
