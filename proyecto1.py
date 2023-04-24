import requests
import pandas as pd
import json
from keys import api_key as key
from datetime import date, timedelta

url="https://api.weatherbit.io/v2.0/history/daily?lat={0}&lon={1}&start_date={2}&end_date={3}&key={4}"

latitude = 40.776676
longitude = -73.971321

year = 2022
month = 1
day = 1

start_date = date(year,month,day) # 2023-04-04
end_date = start_date + timedelta(1) # Request required furter date

full_url = url.format(latitude,longitude,start_date,end_date,key)
print(full_url)

response = requests.get(full_url)
print(response.json())

df = pd.read_json(response.json())






