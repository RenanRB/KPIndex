"""Parsers for the two NOAA SWPC Kp products.

Both pipelines read these, so they live here rather than in either one.

Dates are taken from the documents themselves rather than inferred from the
issue date, because NOAA does not always publish a forecast that starts the
day after it was issued.
"""

import logging
from datetime import datetime, timedelta

from kp_core import (
    BIN_HOURS,
    DataValidationError,
    clean_kp,
    fetch_lines,
    format_timestamp,
)

FORECAST_URL = 'https://services.swpc.noaa.gov/text/3-day-geomag-forecast.txt'
OUTLOOK_URL = 'https://services.swpc.noaa.gov/text/27-day-outlook.txt'

# Strings the genuine documents contain, used to reject error pages served
# with a 200 status before they reach a parser.
FORECAST_MARKER = 'NOAA Kp index forecast'
OUTLOOK_MARKER = ':Issued:'

BINS_PER_DAY = 24 // BIN_HOURS


def parse_issue_date(lines: list[str]) -> datetime:
    """Reads the issue date, the only place the documents state a year."""
    for line in lines:
        if line.startswith(':Issued:'):
            issued = line.split(':Issued: ')[1].replace(' UTC', '').strip()
            try:
                return datetime.strptime(issued, '%Y %b %d %H%M')
            except ValueError as error:
                raise DataValidationError(f"Unreadable issue date {issued!r}: {error}")

    raise DataValidationError("NOAA document has no ':Issued:' header")


def parse_column_dates(header: str, issue_date: datetime) -> list[datetime]:
    """Turns the 'Sep 16    Sep 17    Sep 18' header into real dates.

    The columns carry no year, so it is anchored on the issue date: a
    forecast never starts before it was issued, so a column that would land
    in the past belongs to the following year. That is what keeps a table
    issued on 31 December from dating its January columns a year early.
    """
    parts = header.split()
    if not parts or len(parts) % 2 != 0:
        raise DataValidationError(f"Unreadable forecast column header: {header!r}")

    dates = []
    previous = issue_date.replace(hour=0, minute=0, second=0, microsecond=0)

    for index in range(0, len(parts), 2):
        token = f"{parts[index]} {parts[index + 1]}"
        try:
            # Month and day are resolved separately because strptime would
            # otherwise default to 1900, a non-leap year that rejects 29 Feb.
            month = datetime.strptime(parts[index], '%b').month
            day_number = int(parts[index + 1])
        except ValueError:
            raise DataValidationError(f"Unreadable forecast column {token!r}")

        try:
            candidate = datetime(previous.year, month, day_number)
            if candidate < previous:
                candidate = datetime(previous.year + 1, month, day_number)
        except ValueError as error:
            raise DataValidationError(f"Invalid forecast column date {token!r}: {error}")

        dates.append(candidate)
        previous = candidate

    return dates


def fetch_3day_forecast(url: str = FORECAST_URL) -> list[dict]:
    """Parses the NOAA 3-day forecast into three-hour Kp records.

    This is a real per-bin profile, so it is the best forecast available
    short of the GFZ model. Each row is anchored on its own '00-03UT' label
    and each column on its own date, so a reordered, shortened or shifted
    table cannot silently move readings onto the wrong bin.
    """
    lines = fetch_lines(url, expected_marker=FORECAST_MARKER)
    issue_date = parse_issue_date(lines)

    column_dates = None
    records = []

    for index, line in enumerate(lines):
        # The date header is the line right after the forecast title.
        if line.startswith(FORECAST_MARKER):
            if index + 1 >= len(lines):
                raise DataValidationError("Forecast title is not followed by a date header")
            column_dates = parse_column_dates(lines[index + 1], issue_date)
            continue

        parts = line.split()
        if column_dates is None or not parts or not parts[0].endswith('UT'):
            continue

        # Rows look like: 00-03UT   3.67   4.00   3.67
        label = parts[0]
        try:
            start_hour = int(label[:2])
        except ValueError:
            logging.warning("Unreadable forecast row label: %r", label)
            continue

        values = parts[1:]
        if len(values) != len(column_dates):
            raise DataValidationError(
                f"Forecast row {label} has {len(values)} values for "
                f"{len(column_dates)} day column(s)"
            )

        for day, raw_kp in zip(column_dates, values):
            kp = clean_kp(raw_kp)
            if kp is None:
                logging.warning("Forecast row %s has an unusable Kp: %r", label, raw_kp)
                continue

            records.append({
                "datetime": format_timestamp(day + timedelta(hours=start_hour)),
                "kp": kp,
            })

    if column_dates is None:
        raise DataValidationError("NOAA forecast has no Kp index section")
    if not records:
        raise DataValidationError("NOAA forecast produced no usable rows")

    # Rows arrive grouped by bin but we publish a timeline.
    return sorted(records, key=lambda entry: entry['datetime'])


def fetch_27day_outlook(url: str = OUTLOOK_URL) -> list[dict]:
    """Parses the NOAA 27-day outlook, spreading each day across its bins.

    NOAA publishes one figure per day, the largest Kp expected that day. It
    is repeated across all eight bins because the consumer expects a uniform
    three-hour series; this tail is therefore an upper bound, not a profile.
    """
    lines = fetch_lines(url, expected_marker=OUTLOOK_MARKER)

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

        for bin_index in range(BINS_PER_DAY):
            kp_data.append({
                "datetime": format_timestamp(day + timedelta(hours=BIN_HOURS * bin_index)),
                "kp": kp,
            })

    if not issued:
        raise DataValidationError("NOAA outlook has no ':Issued:' header")
    if not kp_data:
        raise DataValidationError("NOAA outlook produced no usable rows")

    return kp_data
