import requests
import pandas as pd
import json
from keys import claves as keys
from datetime import date, timedelta
import psycopg2
import time
key = keys.ver(303)

url="https://api.weatherbit.io/v2.0/history/daily?lat={0}&lon={1}&start_date={2}&end_date={3}&key={4}"

# Manhattan
latitude = 40.776676
longitude = -73.971321


# Dictionary to transform all dtypes to usable types
dic_types = {
    'clouds': 'float',
    'datetime': 'string',
    'dewpt': 'float',
    'dhi': 'float',
    'dni': 'float',
    'ghi': 'float',
    'max_dhi': 'float',
    'max_dni': 'float',
    'max_ghi': 'float',
    'max_temp': 'float',
    'max_temp_ts': 'float',
    'max_uv': 'float',
    'max_wind_dir': 'float',
    'max_wind_spd': 'float',
    'max_wind_spd_ts': 'float',
    'min_temp': 'float',
    'min_temp_ts': 'float',
    'precip': 'float',
    'precip_gpm': 'float',
    'pres': 'float',
    'revision_status': 'string',
    'rh': 'float',
    'slp': 'float',
    'snow': 'float',
    'snow_depth': 'float',
    'solar_rad': 'float',
    't_dhi': 'float',
    't_dni': 'float',
    't_ghi': 'float',
    't_solar_rad': 'float',
    'temp': 'float',
    'ts': 'float',
    'wind_dir': 'float',
    'wind_gust_spd': 'float',
    'wind_spd': 'float'
}
# Dictionary with dtypes from pandas and sql data types.
dic_pdtosql = {
    'object': 'VARCHAR',
    'int64': 'INTEGER',
    'float64': 'FLOAT',
    'bool': 'BIT',
    'datetime64': 'DATETIME',
    'timedelta64': 'INTERVAL',
    'category': 'VARCHAR',
    'string' : 'VARCHAR'
}
# Conection to the database.
def conection():
    conn = psycopg2.connect(
        host="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
        database="data-engineer-database",
        user= keys.ver(101),
        password= keys.ver(202),
        port="5439")
    return conn
# Table creation.
def create_table(table_name):

    cursor.execute("DROP TABLE IF EXISTS daily_weather")
    print(" Table droped")
    columns = []
    df = apic(start_date, end_date)
    for column in df.columns:
        data_type = dic_pdtosql[str(df[column].dtype)]
        columns_sql = '{} {}'.format(column, data_type)
        columns.append(columns_sql)

    table_query = "CREATE TABLE {} ({})".format(table_name, ", ".join(columns))
    cursor.execute(table_query)
#
def apic(start_date, end_date):
        end_date = start_date + timedelta(1)
        full_url = url.format(latitude,longitude,start_date,end_date,key)

        response = requests.get(full_url)
        json_data = json.loads(response.text, parse_constant=lambda x: x.replace("'", '"'))
        df = pd.json_normalize(json_data, record_path =['data'])
        df = df.astype(dic_types)
        print("df normalizado")
        return df
#
def intert_data(start_date, end_date, table_name):
    fin = end_date
    while fin >= start_date:
        df = apic(start_date, end_date,)
        print(start_date, end_date)
        add_row(table_name, df)
        start_date = start_date + timedelta(1)
        time.sleep(5)
#
def add_row(table_name, df):
    row = df.iloc[0]
    column = ', '.join(row.index)
    values = ', '.join(['%({0})s'.format(col) for col in row.index])
    query = "INSERT INTO {} ({}) VALUES ({})".format(table_name, column, values)
    print("preparado")
    cursor.execute(query, row)
    print("listo")

start_date = date(2022,1,1) # 2023-04-04
end_date = date(2022,12,31) # Request required furter date
table_name = "daily_weather"

# Try/Expect.
conn = conection()
cursor = conn.cursor()

with cursor:
    print("Creating {} table".format(table_name))
    #create_table(table_name)
    print("Table {} successfully created.".format(table_name))
    print("Initializing data insertion")
    intert_data(start_date, end_date, table_name)

conn.commit()
conn.close()
