"""Reader for the Brazilian local K index, from INPE's EMBRACE programme.

This is a *local* K index, measured over Brazil. It shares the 0-9 scale with
the planetary Kp but is a different quantity, so it is published under its own
field name (`k`) in its own file. Over four months of 2026 it ran a mean +1.08
above global Kp, reaching +3.3 during quiet periods, because low geomagnetic
latitude sits under the equatorial electrojet and the South Atlantic Anomaly.

Unlike every other feed in this project, this one is measurement: it covers
the recent past and never the future.

Licensing: INPE's Operations Team confirmed in writing on 2026-10-21 that the
CC BY-ND notice in the portal footer applies to the website content and not to
the scientific data, which follow the Federal Executive's Open Data Policy and
carry no use restriction, including commercial use and derived values. They
also stated there is no guarantee of continuity or advance notice of change,
which is why this feed is subject to the same staleness rules as the others.
"""

import logging
from datetime import timedelta

from kp_core import (
    DataValidationError,
    clean_kp,
    fetch_lines,
    format_timestamp,
    is_aligned,
    parse_timestamp,
    snap_to_bin,
)

BASE_URL = 'https://embracedata.inpe.br/ksa/{year}/{date}.txt'

# Values are written in the thirds notation the index is defined in:
# '4-' is 3.67, '4o' is 4.0, '4+' is 4.33.
THIRDS = {'-': -1.0 / 3.0, 'o': 0.0, '+': 1.0 / 3.0}


def parse_k(token: str) -> float:
    """Turns '4-', '4o' or '4+' into a number, or None if unreadable."""
    if not isinstance(token, str) or len(token) < 2:
        return None

    suffix = THIRDS.get(token[-1])
    if suffix is None:
        return None

    try:
        return round(float(token[:-1]) + suffix, 3)
    except ValueError:
        return None


def parse_day(text: str, source: str = '') -> list[dict]:
    """Parses one daily file into records.

    Lines look like: 2026-10-11T00:00:00 5-
    """
    records = []

    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue

        try:
            moment = parse_timestamp(parts[0])
        except DataValidationError:
            logging.warning("EMBRACE %s has an unreadable time: %r", source, parts[0])
            continue

        value = parse_k(parts[1])
        if value is None or clean_kp(value) is None:
            logging.warning("EMBRACE %s has an unusable K: %r", source, parts[1])
            continue

        if not is_aligned(moment):
            logging.warning("EMBRACE %s bin %s is off-grid; snapping", source, parts[0])

        records.append({
            "datetime": format_timestamp(snap_to_bin(moment)),
            "k": value,
        })

    return records


def fetch_local_k(now, days: int) -> list[dict]:
    """Fetches the last `days` daily files, newest day included.

    Each day is a separate file and the most recent ones are still filling in,
    so a missing or short file is normal and never fails the run. Only a total
    absence of data does.
    """
    records = []
    reachable = 0

    for offset in range(days, -1, -1):
        day = now - timedelta(days=offset)
        url = BASE_URL.format(year=day.year, date=day.strftime('%Y-%m-%d'))

        try:
            # No marker check: a valid file for a day with no readings yet is
            # legitimately empty, so there is no string guaranteed present.
            lines = fetch_lines(url)
        except Exception as error:
            logging.info("EMBRACE day %s unavailable: %s", day.date(), error)
            continue

        reachable += 1
        records.extend(parse_day("\n".join(lines), source=str(day.date())))

    if not reachable:
        raise DataValidationError("No EMBRACE daily file could be fetched")
    if not records:
        raise DataValidationError(
            f"EMBRACE returned {reachable} file(s) but no usable readings"
        )

    # Days arrive in order but a file could repeat a bin; the caller windows
    # and de-duplicates, so only ordering is guaranteed here.
    return sorted(records, key=lambda entry: entry['datetime'])
