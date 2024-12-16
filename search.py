import csv
import json
import requests
from datetime import datetime, timedelta, time

def fetch_site1_data():
    fixed_hours = [3, 6, 9, 12, 15, 18, 21, 23]
    url = 'https://spaceweather.gfz-potsdam.de/fileadmin/Kp-Forecast/CSV/kp_product_file_FORECAST_PAGER_SWIFT_LAST.csv'
    response = requests.get(url, verify=False)
    lines = response.text.split('\n')

    forecast_list = []
    for line in csv.reader(lines[1:]):
        if len(line) > 1:
            date_time_str = line[0]
            kp = float(line[3])
            date_time_obj = datetime.strptime(date_time_str, '%d-%m-%Y %H:%M')
            closest_hour = min(fixed_hours, key=lambda x: abs(x - date_time_obj.hour))
            if closest_hour == 23:
                closest_hour = 0
                date_time_obj += timedelta(days=1)
            if date_time_obj.hour == 1:
                closest_hour = 0
            date_time_obj = date_time_obj.replace(hour=closest_hour)
            forecast_list.append({
                "datetime": date_time_obj.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "kp": kp
            })
    return forecast_list

def fetch_site2_data():
    today = datetime.now()
    lastDay = today - timedelta(days=1)
    url = f'https://kp.gfz-potsdam.de/app/json/?start={ lastDay.strftime("%Y-%m-%d") }T00:00:00Z&end={ today.strftime("%Y-%m-%d") }T23%3A59%3A59Z&index=Kp#kpdatadownload-143'
    response = requests.get(url, verify=False)
    data = json.loads(response.text)
    date_time_obj = data['datetime']
    kp = data['Kp']
    result = []
    for dt, kp_value in zip(date_time_obj, kp):
        result.append({'datetime': dt, 'kp': kp_value})
    return result

def fetch_site3_data():
    url = 'https://services.swpc.noaa.gov/text/27-day-outlook.txt'
    response = requests.get(url, verify=False)
    data = response.text.splitlines()
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
                    "kp": int(splitedLide[5])
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

    limit_date = first_date + timedelta(days=9)


    kp_diario_filtrado = [
        entry for entry in kp_daily_data if last_date < entry["datetime"] < limit_date
    ]

    join_data = kp_hour_data + kp_diario_filtrado

    join_data = [
        {**entry, "datetime": entry["datetime"].strftime('%Y-%m-%dT%H:%M:%SZ')} for entry in join_data
    ]

    return join_data

def merge_and_save_data():
    site1_data = fetch_site1_data()
    site2_data = fetch_site2_data()
    site3_data = fetch_site3_data()
    indexInsert = 0
    for item in site2_data:
        if item['datetime'] not in [i['datetime'] for i in site1_data]:
            site1_data.insert(indexInsert, item)
            indexInsert += 1

    result = merge_infos(site1_data, site3_data)

    with open('new_kp.json', 'w') as file:
        json.dump(result, file, indent=4)

def get_date(year, day_of_year, hour):
    date = datetime.datetime(year, 1, 1) + datetime.timedelta(day_of_year - 1)
    return f'{date.strftime("%Y-%m-%d")}T{str(hour).zfill(2)}:00:00Z'

# Call the function to merge and save the data
merge_and_save_data()
