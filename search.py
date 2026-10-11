"""Builds the combined Kp index series published in data/kp.json.

The series covers a window anchored on the clock: HISTORY_DAYS of past and
FORECAST_DAYS of future. Four feeds compete to fill it, and each bin is
taken from the most trustworthy source that offers it:

  1. GFZ realtime   - Kp already measured. Wins outright.
  2. GFZ forecast   - modelled per-bin Kp, three days ahead.
  3. NOAA 3-day     - a real per-bin profile, three days ahead.
  4. NOAA 27-day    - one figure per day, an upper bound, fills the tail.

That ordering is what makes the output resilient: when a feed freezes, as
GFZ's retired PAGER/SWIFT file did for eight days in October 2026, it stops
offering bins in range and the next source takes over instead of the series
quietly losing its future.

The result is validated as a whole before anything is written, and a run
that cannot produce a trustworthy series writes nothing at all.
"""

import logging
from datetime import timedelta

import noaa
from kp_core import (
    DataValidationError,
    build_series,
    clean_kp,
    fetch_json,
    fetch_source,
    reject_stale_sources,
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
# GFZ's Hpo forecast, the successor to the PAGER/SWIFT CSV this project used
# until October 2026. That older product silently stopped updating on 3 Oct
# 2026 while still answering 200, on both the gfz-potsdam.de and gfz.de
# hostnames, so it was retired here rather than merely re-pointed.
# Listed at: spaceweather.gfz.de/products-data/observatory-based-geomagnetic-
# forecasts/forecasting-global-kp-and-hpo-indices
GFZ_FORECAST_URL = 'https://isdc-data.gfz.de/geomagnetism/HpoForecast/v0102/output/Hpo/json/hpo_forecast_mean_bars_Kp.json'
GFZ_REALTIME_URL_TEMPLATE = 'https://kp.gfz-potsdam.de/app/json/?start={start}T00:00:00Z&end={end}T23%3A59%3A59Z&index=Kp#kpdatadownload-143'
OUTPUT_FILE = 'new_kp.json'

# The document holds a consensus of three models as {"MEDIAN": {...},
# "MAX": {...}}. MEDIAN is the central estimate and the direct equivalent of
# the 'median' column the retired CSV provided; MAX is the pessimistic bound.
# The per-model files alongside it disagree sharply with each other, so the
# consensus is what we publish.
GFZ_FORECAST_KEY = 'MEDIAN'

# The published window, both ends anchored on the current time.
HISTORY_DAYS = 2
FORECAST_DAYS = 7

# Below this much future the series is not worth publishing over the
# previous one; the run keeps the old file and says so.
MIN_FORECAST_DAYS = 3

# How many days of realtime history to request. One extra day is asked for
# so the window's start is never clipped by the request boundary.
REALTIME_LOOKBACK_DAYS = HISTORY_DAYS + 1

# How far past now each forecast feed must reach to be trusted at all. A feed
# that falls short is being served but no longer maintained, which an HTTP
# status cannot tell us — only the newest timestamp in the document can.
# Measured feeds are absent here on purpose: they do not predict.
SOURCE_MIN_HORIZONS = {
    'GFZ forecast': timedelta(hours=24),
    'NOAA 3-day': timedelta(hours=24),
    'NOAA 27-day': timedelta(days=5),
}


def fetch_gfz_forecast() -> list[dict]:
    """Fetches the GFZ Hpo forecast, keeping the consensus Kp per bin."""
    data = fetch_json(GFZ_FORECAST_URL)

    if not isinstance(data, dict):
        raise DataValidationError(
            f"GFZ forecast returned {type(data).__name__}, expected an object"
        )

    # Resolved by name so a reshaped document cannot silently shift us onto
    # the pessimistic bound, which would overstate activity everywhere.
    series = data.get(GFZ_FORECAST_KEY)
    if not isinstance(series, dict):
        raise DataValidationError(
            f"GFZ forecast has no {GFZ_FORECAST_KEY!r} object. Keys: {sorted(data)}"
        )
    if not series:
        raise DataValidationError(f"GFZ forecast {GFZ_FORECAST_KEY!r} object is empty")

    forecast_list = []

    for raw_moment, raw_kp in series.items():
        kp = clean_kp(raw_kp)
        if kp is None:
            logging.warning("GFZ forecast bin %s has an unusable Kp: %r", raw_moment, raw_kp)
            continue

        moment = parse_timestamp(raw_moment)
        if not is_aligned(moment):
            logging.warning(
                "GFZ forecast bin %s is off-grid; snapping to the nearest bin", raw_moment
            )

        forecast_list.append({
            "datetime": format_timestamp(snap_to_bin(moment)),
            "kp": kp,
        })

    if not forecast_list:
        raise DataValidationError("GFZ forecast produced no usable bins")

    # The document is a JSON object, whose order is not guaranteed.
    return sorted(forecast_list, key=lambda entry: entry['datetime'])


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
        ('GFZ forecast', fetch_source('GFZ forecast', fetch_gfz_forecast)),
        ('NOAA 3-day', fetch_source('NOAA 3-day', noaa.fetch_3day_forecast)),
        ('NOAA 27-day', fetch_source('NOAA 27-day', noaa.fetch_27day_outlook)),
    ]

    unavailable = [name for name, records in sources if records is None]
    if unavailable:
        logging.warning("Unavailable source(s): %s", ', '.join(unavailable))

    available = [(name, records) for name, records in sources if records is not None]

    # Reachable is not the same as current: drop whatever has gone stale so
    # the next source in the order gets its turn.
    available, stale_notes = reject_stale_sources(available, now, SOURCE_MIN_HORIZONS)
    for note in stale_notes:
        logging.warning("%s", note)

    if not available:
        logging.error("No source is both reachable and current; keeping the previous data.")
        return False

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
