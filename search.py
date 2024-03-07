import csv
import json
import requests
from datetime import datetime, timedelta

def fetch_site1_data():
    fixed_hours = [3, 6, 9, 12, 15, 18, 21, 23]
    url = 'https://spaceweather.gfz-potsdam.de/fileadmin/Kp-Forecast/CSV/kp_product_file_FORECAST_PAGER_SWIFT_LAST.csv'
    response = requests.get(url, verify=False)
    lines = response.text.split('\n')

    forecast_list = []
    for line in csv.reader(lines[1:]):
        if len(line) > 1:
            date_time_str = line[0]
            kp = float(line[5])
            date_time_obj = datetime.strptime(date_time_str, '%d-%m-%Y %H:%M')
            closest_hour = min(fixed_hours, key=lambda x: abs(x - date_time_obj.hour))
            if closest_hour == 23:
                closest_hour = 0
                date_time_obj += timedelta(days=1)
            date_time_obj = date_time_obj.replace(hour=closest_hour)
            forecast_list.append({
                "datetime": date_time_obj.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "kp": kp
            })
    return forecast_list

def fetch_site2_data():
    today = datetime.now().strftime('%Y-%m-%d')
    url = f'https://kp.gfz-potsdam.de/app/json/?start={today}T00%3A00%3A00Z&end={today}T23%3A59%3A59Z&index=Kp#kpdatadownload-143'
    response = requests.get(url, verify=False)
    data = json.loads(response.text)
    date_time_obj = data['datetime']
    kp = data['Kp']
    result = []
    for dt, kp_value in zip(date_time_obj, kp):
        result.append({'datetime': dt, 'kp': kp_value})
    return result

def merge_and_save_data():
    site1_data = fetch_site1_data()
    site2_data = fetch_site2_data()
    indexInsert = 0
    for item in site2_data:
        if item['datetime'] not in [i['datetime'] for i in site1_data]:
            site1_data.insert(indexInsert, item)
            indexInsert += 1

    with open('new_kp.json', 'w') as file:
        json.dump(site1_data, file, indent=4)

def get_date(year, day_of_year, hour):
    date = datetime.datetime(year, 1, 1) + datetime.timedelta(day_of_year - 1)
    return f'{date.strftime("%Y-%m-%d")}T{str(hour).zfill(2)}:00:00Z'

# Call the function to merge and save the data
merge_and_save_data()
