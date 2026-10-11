"""Builds the NOAA-only Kp index series published in data/kp_noaa.json.

Two NOAA products fill a window anchored on the clock, best source first:

  1. 3-day geomagnetic forecast - a real three-hour Kp profile.
  2. 27-day outlook            - one daily figure, used to extend the tail.

Unlike the GFZ pipeline this series is entirely predicted; there is no
measured segment, so it carries no history.
"""

import logging
from datetime import timedelta

import noaa
from kp_core import (
    build_series,
    fetch_source,
    reject_stale_sources,
    series_horizon,
    utcnow,
    validate_series,
    write_json_atomic,
)

url_hour = noaa.FORECAST_URL
url_daily = noaa.OUTLOOK_URL
OUTPUT_FILE = 'new_kp.json'

# The published window. This series is forecast-only, so it starts at the
# current bin rather than reaching into the past.
HISTORY_DAYS = 0
FORECAST_DAYS = 7

# Below this much future the series is not worth publishing over the
# previous one.
MIN_FORECAST_DAYS = 3

# How far past now each feed must reach to be trusted at all; being served
# is not the same as being maintained.
SOURCE_MIN_HORIZONS = {
    'NOAA 3-day': timedelta(hours=24),
    'NOAA 27-day': timedelta(days=5),
}


def merge_and_save_data() -> bool:
    """Runs the full pipeline.

    Returns True when a validated file was written, and False when the data
    on offer cannot produce a series worth publishing. Raises
    DataValidationError when the data was fetched but cannot be trusted.
    """
    logging.info("Starting NOAA Kp fetch pipeline...")
    now = utcnow()

    sources = [
        ('NOAA 3-day', fetch_source('NOAA 3-day', lambda: noaa.fetch_3day_forecast(url_hour))),
        ('NOAA 27-day', fetch_source('NOAA 27-day', lambda: noaa.fetch_27day_outlook(url_daily))),
    ]

    unavailable = [name for name, records in sources if records is None]
    if unavailable:
        logging.warning("Unavailable source(s): %s", ', '.join(unavailable))

    available = [(name, records) for name, records in sources if records is not None]

    available, stale_notes = reject_stale_sources(available, now, SOURCE_MIN_HORIZONS)
    for note in stale_notes:
        logging.warning("%s", note)

    if not available:
        logging.error("No source is both reachable and current; keeping the previous data.")
        return False

    result, provenance = build_series(
        available, now, history_days=HISTORY_DAYS, forecast_days=FORECAST_DAYS
    )

    if not result:
        logging.error("No source covered the publishable window; keeping the previous data.")
        return False

    counts = {}
    for name in provenance.values():
        counts[name] = counts.get(name, 0) + 1
    for name, count in sorted(counts.items(), key=lambda item: -item[1]):
        logging.info("  %-18s %3d bin(s)", name, count)

    # Integrity first: corrupt data is a defect that must be seen, so it
    # raises. Only then is the series judged on whether it is useful, which
    # a mere upstream outage can make it not be.
    for warning in validate_series(result):
        logging.warning("Series warning: %s", warning)

    horizon = series_horizon(result, now)
    if horizon < timedelta(days=MIN_FORECAST_DAYS):
        logging.error(
            "Series only reaches %.1fh ahead, under the %d-day minimum. "
            "Keeping the previous data.",
            horizon.total_seconds() / 3600, MIN_FORECAST_DAYS
        )
        return False

    write_json_atomic(OUTPUT_FILE, result)

    logging.info(
        "Successfully saved %d Kp records to %s (%s to %s, %.1f days ahead)",
        len(result), OUTPUT_FILE, result[0]['datetime'], result[-1]['datetime'],
        horizon.total_seconds() / 86400
    )
    return True


if __name__ == '__main__':  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if not merge_and_save_data():
        # Exit successfully so a transient upstream outage is not reported as
        # a broken build. The scheduled freshness check is what escalates a
        # problem that persists.
        print("::warning::Could not build a publishable Kp series, data/kp_noaa.json kept unchanged.")
