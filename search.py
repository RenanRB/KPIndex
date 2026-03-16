import csv
import json
import logging
from datetime import datetime, timedelta, time
import requests
import urllib3

# Suppress warnings for unverified HTTPS requests since the GFZ and NOAA sources require it
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration Constants ---
GFZ_FORECAST_URL = 'https://spaceweather.gfz-potsdam.de/fileadmin/Kp-Forecast/CSV/kp_product_file_FORECAST_PAGER_SWIFT_LAST.csv'
GFZ_REALTIME_URL_TEMPLATE = 'https://kp.gfz-potsdam.de/app/json/?start={start}T00:00:00Z&end={end}T23%3A59%3A59Z&index=Kp#kpdatadownload-143'
NOAA_OUTLOOK_URL = 'https://services.swpc.noaa.gov/text/27-day-outlook.txt'
OUTPUT_FILE = 'new_kp.json'


def fetch_csv_data(url: str) -> list[str]:
    """Helper to fetch text data and split it into lines."""
    response = requests.get(url, verify=False, timeout=15)
    response.raise_for_status()
    return response.text.splitlines()


def fetch_json_data(url: str) -> dict:
    """Helper to fetch and parse JSON data."""
    response = requests.get(url, verify=False, timeout=15)
    response.raise_for_status()
    return response.json()


def fetch_gfz_forecast_csv() -> list[dict]:
    """
    Fetches the 3-day short-term GFZ forecast.
    Extracts datetime and Kp median, enforcing 3-hour fixed bins.
    """
    fixed_hours = [0, 3, 6, 9, 12, 15, 18, 21]
    lines = fetch_csv_data(GFZ_FORECAST_URL)
    
    forecast_list = []
    # Skip header row
    for row in csv.reader(lines[1:]):
        if not row or len(row) < 4:
            continue
            
        date_time_str = row[0]
        kp = float(row[3])
        date_time_obj = datetime.strptime(date_time_str, '%d-%m-%Y %H:%M')
        
        # Snap the hour to the closest 3-hour bin
        closest_hour = min([23] + fixed_hours, key=lambda x: abs(x - date_time_obj.hour))
        
        if closest_hour == 23 or date_time_obj.hour == 1:
            closest_hour = 0
            if date_time_obj.hour >= 23:
                date_time_obj += timedelta(days=1)
                
        date_time_obj = date_time_obj.replace(hour=closest_hour, minute=0, second=0, microsecond=0)
        
        forecast_list.append({
            "datetime": date_time_obj.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "kp": kp
        })
        
    return forecast_list


def fetch_gfz_realtime_json() -> list[dict]:
    """
    Fetches the actual real-time observed Kp index from GFZ for the past two days.
    """
    today = datetime.utcnow()
    last_day = today - timedelta(days=2)
    
    url = GFZ_REALTIME_URL_TEMPLATE.format(
        start=last_day.strftime("%Y-%m-%d"),
        end=today.strftime("%Y-%m-%d")
    )
    
    data = fetch_json_data(url)
    
    result = []
    for dt, kp_value in zip(data.get('datetime', []), data.get('Kp', [])):
        result.append({
            'datetime': dt,
            'kp': kp_value
        })
        
    return result


def fetch_noaa_27day_outlook() -> list[dict]:
    """
    Fetches the NOAA 27-day Kp outlook text file.
    Parses the issuance date and generates subsequent 3-hour bin estimates.
    """
    lines = fetch_csv_data(NOAA_OUTLOOK_URL)
    start_date = None
    kp_data = []

    for line in lines:
        parts = line.split()
        
        if line.startswith(':Issued:'):
            date_time_str = line.split(":Issued: ")[1].replace(" UTC", "")
            issue_date = datetime.strptime(date_time_str, '%Y %b %d %H%M')
            # The forecast starts the day after issuance
            start_date = (issue_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            
        if start_date and len(parts) == 6 and parts[0].isdigit():
            # Parse the actual row representing a day forecast
            row_date_str = f"{parts[0]} {parts[1]} {parts[2]}"
            row_date = datetime.strptime(row_date_str, '%Y %b %d')
            date_data = datetime.combine(row_date.date(), time.min)

            daily_max_kp = int(parts[5])
            # Duplicate the single daily maximum into 8 3-hour bins for consistency
            for _ in range(8):
                kp_data.append({
                    "datetime": date_data.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "kp": daily_max_kp
                })
                date_data += timedelta(hours=3)

    return kp_data


def merge_kp_data(short_term_data: list[dict], long_term_data: list[dict]) -> list[dict]:
    """
    Merges short-term and long-term datasets.
    It takes the timeframe bounded by the short_term_data (+9 days limits)
    and combines them, ensuring no overlapping dates duplicate.
    """
    if not short_term_data:
        return long_term_data
        
    # Convert string dates to datetime objects for accurate comparison
    short_term_parsed = [
        {**entry, "dt_obj": datetime.fromisoformat(entry["datetime"].replace("Z", ""))}
        for entry in short_term_data
    ]
    
    first_date = min(entry["dt_obj"] for entry in short_term_parsed)
    last_date = max(entry["dt_obj"] for entry in short_term_parsed)
    limit_date = first_date + timedelta(days=9)

    long_term_filtered = []
    for entry in long_term_data:
        dt_obj = datetime.fromisoformat(entry["datetime"].replace("Z", ""))
        if last_date < dt_obj < limit_date:
            long_term_filtered.append(entry)

    # Return ordered result by recombining dicts
    merged = short_term_data + long_term_filtered
    return merged


def get_kp_pipeline():
    """Main pipeline execution for composing the output JSON."""
    logging.info("Starting Kp fetch pipeline...")
    
    # 1. Fetch data from all sources
    gfz_forecast = fetch_gfz_forecast_csv()
    gfz_realtime = fetch_gfz_realtime_json()
    noaa_outlook = fetch_noaa_27day_outlook()
    
    # 2. Integrate real-time over forecast (real-time trumps predicted)
    # Using a dictionary automatically handles overwriting duplicate timestamps
    short_term_dict = {item['datetime']: item for item in gfz_forecast}
    for item in gfz_realtime:
        short_term_dict[item['datetime']] = item
        
    short_term_merged = sorted(short_term_dict.values(), key=lambda x: x['datetime'])
    
    # 3. Merge short-term and long-term
    final_result = merge_kp_data(short_term_merged, noaa_outlook)
    
    # 4. Save to JSON
    with open(OUTPUT_FILE, 'w') as file:
        json.dump(final_result, file, indent=4)
        
    logging.info(f"Successfully saved {len(final_result)} Kp records to {OUTPUT_FILE}")


if __name__ == '__main__':  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    get_kp_pipeline()
