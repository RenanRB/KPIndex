"""Builds the combined Kp index series published in data/kp.json.

The series covers a window anchored on the clock: HISTORY_DAYS of past and
FORECAST_DAYS of future. Four feeds compete to fill it, and each bin is
taken from the most trustworthy source that offers it:

  1. GFZ realtime   - Kp already measured. Wins outright.
  2. GFZ forecast   - modelled per-bin Kp, a few days ahead.
  3. NOAA 3-day     - a real per-bin profile, three days ahead.
  4. NOAA 27-day    - one figure per day, an upper bound, fills the tail.

That ordering is what makes the output resilient: when a feed freezes, as
the GFZ forecast file did for eight days in October 2026, it stops offering
bins in range and the next source takes over instead of the series quietly
losing its future.

The result is validated as a whole before anything is written, and a run
that cannot produce a trustworthy series writes nothing at all.
"""

import csv
import logging
from datetime import datetime, timedelta

import noaa
from kp_core import (
    DataValidationError,
    build_series,
    clean_kp,
    fetch_json,
    fetch_lines,
    fetch_source,
    format_timestamp,
    is_aligned,
    parse_timestamp,
    series_horizon,
    snap_to_bin,
    utcnow,
    validate_series,
    write_json_atomic,
)

# --- Configuration Constants ---
GFZ_FORECAST_URL = 'https://spaceweather.gfz-potsdam.de/fileadmin/Kp-Forecast/CSV/kp_product_file_FORECAST_PAGER_SWIFT_LAST.csv'
GFZ_REALTIME_URL_TEMPLATE = 'https://kp.gfz-potsdam.de/app/json/?start={start}T00:00:00Z&end={end}T23%3A59%3A59Z&index=Kp#kpdatadownload-143'
OUTPUT_FILE = 'new_kp.json'

# A string the genuine document contains, used to reject an error page
# served with a 200 status before it reaches the parser.
GFZ_FORECAST_MARKER = 'Time (UTC)'

# The forecast publishes several quantiles per bin; we track the median.
FORECAST_VALUE_COLUMN = 'median'
FORECAST_TIME_FORMAT = '%d-%m-%Y %H:%M'

# The published window, both ends anchored on the current time.
HISTORY_DAYS = 2
FORECAST_DAYS = 7

# Below this much future the series is not worth publishing over the
# previous one; the run keeps the old file and says so.
MIN_FORECAST_DAYS = 3

# How many days of realtime history to request. One extra day is asked for
# so the window's start is never clipped by the request boundary.
REALTIME_LOOKBACK_DAYS = HISTORY_DAYS + 1


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


def report_coverage(provenance: dict) -> None:
    """Logs how many bins each source ended up contributing."""
    counts = {}
    for name in provenance.values():
        counts[name] = counts.get(name, 0) + 1

    for name, count in sorted(counts.items(), key=lambda item: -item[1]):
        logging.info("  %-18s %3d bin(s)", name, count)


def get_kp_pipeline() -> bool:
    """Runs the full pipeline.

    Returns True when a validated file was written, and False when the data
    on offer cannot produce a series worth publishing. Raises
    DataValidationError when the data was fetched but cannot be trusted.
    """
    logging.info("Starting Kp fetch pipeline...")
    now = utcnow()

    # Ordered by how much we trust them; build_series resolves the overlaps.
    sources = [
        ('GFZ realtime', fetch_source('GFZ realtime', fetch_gfz_realtime_json)),
        ('GFZ forecast', fetch_source('GFZ forecast', fetch_gfz_forecast_csv)),
        ('NOAA 3-day', fetch_source('NOAA 3-day', noaa.fetch_3day_forecast)),
        ('NOAA 27-day', fetch_source('NOAA 27-day', noaa.fetch_27day_outlook)),
    ]

    available = [(name, records) for name, records in sources if records is not None]
    if not available:
        logging.error("Every source is unavailable; keeping the previous data.")
        return False

    unavailable = [name for name, records in sources if records is None]
    if unavailable:
        logging.warning("Unavailable source(s): %s", ', '.join(unavailable))

    final_result, provenance = build_series(
        available, now, history_days=HISTORY_DAYS, forecast_days=FORECAST_DAYS
    )

    if not final_result:
        logging.error("No source covered the publishable window; keeping the previous data.")
        return False

    report_coverage(provenance)

    # Integrity first: corrupt data is a defect that must be seen, so it
    # raises. Only then is the series judged on whether it is useful, which
    # a mere upstream outage can make it not be.
    for warning in validate_series(final_result):
        logging.warning("Series warning: %s", warning)

    # A series that barely reaches past now is worse than the file it would
    # replace, so it is refused rather than published.
    horizon = series_horizon(final_result, now)
    if horizon < timedelta(days=MIN_FORECAST_DAYS):
        logging.error(
            "Series only reaches %.1fh ahead, under the %d-day minimum. "
            "Keeping the previous data.",
            horizon.total_seconds() / 3600, MIN_FORECAST_DAYS
        )
        return False

    write_json_atomic(OUTPUT_FILE, final_result)

    logging.info(
        "Successfully saved %d Kp records to %s (%s to %s, %.1f days ahead)",
        len(final_result), OUTPUT_FILE,
        final_result[0]['datetime'], final_result[-1]['datetime'],
        horizon.total_seconds() / 86400
    )
    return True


if __name__ == '__main__':  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if not get_kp_pipeline():
        # Exit successfully so a transient upstream outage is not reported as
        # a broken build. The scheduled freshness check is what escalates a
        # problem that persists, so this cannot go unnoticed for long.
        print("::warning::Could not build a publishable Kp series, data/kp.json kept unchanged.")
