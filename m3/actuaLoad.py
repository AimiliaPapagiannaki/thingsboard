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
    
    
def main():


    TOKEN="b713d78a-28ef-41c8-b408-d5c4258696ba"
    client = EntsoePandasClient(api_key=TOKEN)
    
    dt = datetime.datetime.utcnow()
    
    start_ts = datetime.datetime(year = dt.year, month=dt.month, day=dt.day-1) # today
    end_ts = datetime.datetime(year = dt.year, month=dt.month, day=dt.day,hour = dt.hour) # today up to now
    
    
    start = pd.Timestamp(start_ts, tz='Europe/Athens')  
    end = pd.Timestamp(end_ts, tz='Europe/Athens')   
    country_code = 'GR'  
    
    try:
        df=pd.DataFrame()
        df['totaLoad'] = client.query_load(country_code, start=start,end=end)
        
        df['ts'] = df.index.astype(np.int64) // 10 ** 6
        df.set_index('ts', drop=True, inplace=True)
        print(df)
        mydict = transform_df(df)
        
        thingsboard_http(mydict)
    except:
        print('Unable to retrieve data.')
    
    
if __name__ == "__main__":
    sys.exit(main())
    