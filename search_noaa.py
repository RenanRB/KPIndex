import requests
import json
import logging
from datetime import datetime, timedelta, time
from time import sleep

url_hour = 'https://services.swpc.noaa.gov/text/3-day-geomag-forecast.txt'
url_daily = 'https://services.swpc.noaa.gov/text/27-day-outlook.txt'

# NOAA occasionally refuses or stalls connections, so retry before giving up.
REQUEST_TIMEOUT = (15, 45)  # (connect, read) seconds
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 5

def fetch_lines(url):
    """Fetches a text file, retrying transient network errors with exponential backoff."""
    last_error = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(url, verify=False, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.text.splitlines()
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

def fetch_and_process_hour_data(url):
    data = fetch_lines(url)

    kp_data = [[],[],[]]
    start_date = None

    for i, line in enumerate(data):
        splited_lide = line.split()
        if line.startswith(':Issued:'):
            date_time_str = line.split(":Issued: ")[1]
            date_time_str = date_time_str.replace(" UTC", "")
            date_time = datetime.strptime(date_time_str, '%Y %b %d %H%M')
            next_day = date_time + timedelta(days=1)
            start_date = next_day.replace(hour=0, minute=0, second=0, microsecond=0)
        if start_date and len(splited_lide) == 4 and 'UT' in splited_lide[0]:
            for j, kp in enumerate(splited_lide[1:]):
                kp_value = float(kp)
                datetime_str = (start_date + timedelta(days=j)).strftime('%Y-%m-%dT%H:%M:%SZ')
                kp_data[j].append({
                    "datetime": datetime_str,
                    "kp": kp_value
                })
            start_date = start_date + timedelta(hours=3)
    
    return kp_data[0] + kp_data[1] + kp_data[2]

def fetch_and_process_daily_data(url):
    data = fetch_lines(url)
    start_date = None
    kp_data = []

    for i, line in enumerate(data):
        splitedLide = line.split()
        if line.startswith(':Issued:'):
            date_time_str = line.split(":Issued: ")[1]
            date_time_str = date_time_str.replace(" UTC", "")
            date_time = datetime.strptime(date_time_str, '%Y %b %d %H%M')
            next_day = date_time + timedelta(days=1)
            start_date = next_day.replace(hour=0, minute=0, second=0, microsecond=0)
        if start_date and len(splitedLide) == 6 and splitedLide[0].isdigit():
            date_data = datetime.combine(datetime.strptime(splitedLide[0] + ' ' + splitedLide[1] + ' ' + splitedLide[2], '%Y %b %d').date(), time.min)
            
            for i in range(8):
                kp_data.append({
                    "datetime": date_data.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "kp": float(splitedLide[5])
                })

                date_data = date_data + timedelta(hours=3)
    
    return kp_data

def merge_infos(kp_hour_data, kp_daily_data):
    kp_hour_data = [
        {**entry, "datetime": datetime.fromisoformat(entry["datetime"].replace("Z", ""))}
        for entry in kp_hour_data
    ]

    kp_daily_data = [
        {**entry, "datetime": datetime.fromisoformat(entry["datetime"].replace("Z", ""))}
        for entry in kp_daily_data
    ]
    
    if (len(kp_hour_data)):
        last_date = max(entry["datetime"] for entry in kp_hour_data)

        first_date = min(entry["datetime"] for entry in kp_hour_data)
    else:
        last_date = datetime.today()

        first_date = datetime.today()

    limit_date = first_date + timedelta(days=7)

    kp_diario_filtrado = [
        entry for entry in kp_daily_data if last_date < entry["datetime"] < limit_date
    ]

    join_data = kp_hour_data + kp_diario_filtrado

    join_data = [
        {**entry, "datetime": entry["datetime"].strftime('%Y-%m-%dT%H:%M:%SZ')} for entry in join_data
    ]
    
    return join_data

def merge_and_save_data():
    """Returns True when a new file was written, False when NOAA was unreachable."""
    try:
        kp_hour_data = fetch_and_process_hour_data(url_hour)
        kp_daily_data = fetch_and_process_daily_data(url_daily)
    except Exception as error:
        logging.error("NOAA is unavailable: %s: %s", error.__class__.__name__, error)
        return False

    result = merge_infos(kp_hour_data, kp_daily_data)
    if not result:
        logging.warning("NOAA returned no usable rows.")
        return False

    with open('new_kp.json', 'w') as file:
        json.dump(result, file, indent=4)

    logging.info("Successfully saved %d Kp records to new_kp.json", len(result))
    return True

if __name__ == '__main__':  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if not merge_and_save_data():
        # Exit successfully so the scheduled run is not reported as broken for a
        # transient upstream outage, but flag it on the GitHub Actions summary.
        print("::warning::NOAA sources unavailable, data/kp_noaa.json kept unchanged.")
