import requests
import json

url = 'https://spaceweather.gfz-potsdam.de/fileadmin/ruggero/Kp_forecast/forecast_figures/KP_FORECAST_CURRENT.dat'
response = requests.get(url)

data = response.text

# Transformar a resposta em uma lista de dicionários
forecast_list = []
lines = data.split('\n')
for line in lines:
    if line:
        values = line.split()
        forecast = {
            'year': int(values[0]),
            'doy': int(values[1]),
            'hour': int(values[2]),
            'kp': float(values[3])
        }
        forecast_list.append(forecast)

# Salvar o resultado em formato JSON no diretório 'data' do repositório
with open('new_kp.json', 'w') as file:
    json.dump(forecast_list, file)
