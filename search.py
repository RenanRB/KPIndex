import requests
import json
import datetime

# Function to fetch data from the first site
def fetch_site1_data():
    url = 'https://spaceweather.gfz-potsdam.de/fileadmin/ruggero/Kp_forecast/forecast_figures/KP_FORECAST_CURRENT.dat'
    response = requests.get(url)
    data = response.text.split('\n')[:-1]
    result = []
    for line in data:
        year, day, hour, kp = line.split()
        date = get_date(int(year), int(day), int(hour))
        result.append({'datetime': f'{date}', 'kp': float(kp)})
    return result

# Function to fetch data from the second site
def fetch_site2_data():
    url = 'https://kp.gfz-potsdam.de/app/json/?start=2023-05-15T00%3A00%3A00Z&end=2023-05-15T23%3A59%3A59Z&index=Kp#kpdatadownload-143'
    response = requests.get(url)
    data = json.loads(response.text)
    datetime = data['datetime']
    kp = data['Kp']
    result = []
    for dt, kp_value in zip(datetime, kp):
        result.append({'datetime': dt, 'kp': kp_value})
    return result

# Function to merge the data and save it to a local JSON file
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
    print('Merged data successfully saved!')

def get_date(year, day_of_year, hour):
    date = datetime.datetime(year, 1, 1) + datetime.timedelta(day_of_year - 1)
    return f'{date.strftime("%Y-%m-%d")}T{str(hour).zfill(2)}:00:00Z'

# Call the function to merge and save the data
merge_and_save_data()
