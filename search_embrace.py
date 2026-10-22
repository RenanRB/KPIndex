"""Builds the Brazilian local K index series published in data/k_br.json.

This series is measurement, not forecast: it covers the recent past and stops
at the latest bin INPE has published. That makes it the only file here with no
future, and the reason it carries a `k` field rather than `kp` — it is a local
index, a different quantity from the planetary one, on the same 0-9 scale.

See embrace.py for the licensing position and why the values differ from Kp.
"""

import logging
from datetime import timedelta

import embrace
from kp_core import (
    build_series,
    fetch_source,
    parse_timestamp,
    utcnow,
    validate_series,
    write_json_atomic,
)

OUTPUT_FILE = 'new_k_br.json'
VALUE_FIELD = 'k'

# How much past to publish. Matches the history the planetary series carries,
# so a consumer can line the two up bin for bin.
HISTORY_DAYS = 2

# One extra day is requested so the window's start is never clipped by a file
# boundary, and because the newest files are still filling in.
FETCH_DAYS = HISTORY_DAYS + 1

# INPE publishes a day's bins through the day, so the newest bin normally
# trails the clock. Measured over 30 days the lag has a median of 1h and a
# worst case of 9h, the peak falling late in the UTC day when the final bins
# have not been written yet. The limit is set at twice that worst case: tight
# enough to catch a feed that has stopped, loose enough that ordinary lag
# never blocks a publish and leaves the file to go stale instead.
MAX_LAG = timedelta(hours=18)


def get_local_k_pipeline() -> bool:
    """Runs the pipeline.

    Returns True when a validated file was written, and False when the data on
    offer cannot produce a series worth publishing. Raises DataValidationError
    when the data was fetched but cannot be trusted.
    """
    logging.info("Starting EMBRACE local K pipeline...")
    now = utcnow()

    records = fetch_source(
        'EMBRACE local K', lambda: embrace.fetch_local_k(now, FETCH_DAYS)
    )
    if records is None:
        logging.warning("EMBRACE unavailable; keeping the previous data.")
        return False

    # forecast_days=0: this feed measures, it does not predict.
    series, _ = build_series(
        [('EMBRACE local K', records)], now,
        history_days=HISTORY_DAYS, forecast_days=0,
        value_field=VALUE_FIELD,
    )

    if not series:
        logging.error("No reading fell inside the publishable window; keeping previous data.")
        return False

    for warning in validate_series(series, value_field=VALUE_FIELD):
        logging.warning("Series warning: %s", warning)

    # A measured feed is judged on how far behind it is, not how far ahead it
    # reaches. Too far behind and it has stopped being measurement of now.
    lag = now - parse_timestamp(series[-1]['datetime'])
    if lag > MAX_LAG:
        logging.error(
            "Newest reading is %.1fh old, over the %.0fh limit. Keeping the previous data.",
            lag.total_seconds() / 3600, MAX_LAG.total_seconds() / 3600
        )
        return False

    write_json_atomic(OUTPUT_FILE, series)

    logging.info(
        "Successfully saved %d local K records to %s (%s to %s, %.1fh behind)",
        len(series), OUTPUT_FILE,
        series[0]['datetime'], series[-1]['datetime'], lag.total_seconds() / 3600
    )
    return True


if __name__ == '__main__':  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if not get_local_k_pipeline():
        print("::warning::Could not build a publishable local K series, data/k_br.json kept unchanged.")
