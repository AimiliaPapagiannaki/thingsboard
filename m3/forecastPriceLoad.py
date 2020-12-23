#!/usr/bin/env python3

from entsoe import EntsoePandasClient
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import sys
import json
import requests


def transform_df(df):
   
    for col in df.columns:
        df[col] = df[col].apply(str)
    
    mydict = df.to_dict('index')

    return mydict



def thingsboard_http(mydict):
    address = "http://localhost:8080"
    dev_token = 'cuCNBRaiJ04PQtsKWVNv' # DEH meter-->Aggregated meter
    
    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazon@thingsboard.org', 'password': 'meazon'}).json()
    acc_token = 'Bearer' + ' ' + r['token']


    for key, value in mydict.items():
        # time.sleep(0.015)
        my_json = json.dumps({'ts': key, 'values': value})
        
        r = requests.post(url=address + "/api/v1/" + dev_token + "/telemetry",
                           data=my_json, headers={'Content-Type': 'application/json', 'Accept': '*/*',
                                                  'X-Authorization': acc_token})
        

    print('Finished writing descriptors')

    return
  
    
    
def fetch_meas(start_ts, end_ts, country_code, client):

    start = pd.Timestamp(start_ts, tz='Europe/Athens')  
    end = pd.Timestamp(end_ts, tz='Europe/Athens')   
      
    print('time range in msec:',start,end)
    df=pd.DataFrame()

    df['priceForecast']=client.query_day_ahead_prices(country_code, start=start,end=end)
    df['totaLoadForecast'] = client.query_load_forecast(country_code, start=start,end=end)
    
    df['ts'] = df.index.astype(np.int64) // 10 ** 6
    df.set_index('ts', drop=True, inplace=True)
    mydict = transform_df(df)
    
    thingsboard_http(mydict)
    
    
    

def main():


    TOKEN="b713d78a-28ef-41c8-b408-d5c4258696ba"
    client = EntsoePandasClient(api_key=TOKEN)
    
    dt = datetime.datetime.utcnow()
    country_code = 'GR'
    print('Running script at utc time:',dt)
    #######################
    # try to fetch next day's measurements
    start_ts = datetime.datetime(year = dt.year, month=dt.month, day=dt.day+1) # tomorrow
    end_ts = datetime.datetime(year = dt.year, month=dt.month, day=dt.day+2) # the day after tomorrow
    print('time range:',start_ts,end_ts)

    
    try:
        fetch_meas(start_ts, end_ts, country_code, client)
    except:
        print('Next day not available yet')
    
    
    #######################
    # try to fetch current day's measurements
    start_ts = datetime.datetime(year = dt.year, month=dt.month, day=dt.day) # tomorrow
    end_ts = datetime.datetime(year = dt.year, month=dt.month, day=dt.day+1) # the day after tomorrow
    print('time range:',start_ts,end_ts)
  
    try:
        fetch_meas(start_ts, end_ts, country_code, client)
    except:
        print('Current day not available yet')
    
    
if __name__ == "__main__":
    sys.exit(main())
    