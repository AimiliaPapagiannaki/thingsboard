#!/usr/bin/env python

import os
import re
import sys
import pandas as pd
import datetime
import os.path
from os import path
import requests
import numpy as np
import time
import pytz
import json
from dateutil.relativedelta import relativedelta
from dateutil.tz import gettz
import thingsAPI

ADDRESS = "http://localhost:8080" 

def read_data(device, acc_token, start_time, end_time, descriptors, entity):
    """
    Reads data from the API and returns a DataFrame.
    """
    devid = thingsAPI.get_devid(ADDRESS, device, entity)
    entity = entity.upper()
    
    df = pd.DataFrame([])

   
    response = requests.get(
        url=f"{ADDRESS}/api/plugins/telemetry/{entity}/{devid}/values/timeseries",
        params={
            "keys": descriptors,
            "startTs": start_time,
            "endTs": end_time,
            "agg": "NONE",
            "limit": 1000000
        },
        headers={
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'X-Authorization': acc_token
        }
    )
    r2 = response.json()

    if r2:
        for desc in r2.keys():
            df1 = pd.DataFrame(r2[desc])
            df1['ts'] = pd.to_datetime(df1['ts'], unit='ms')
            df1.set_index('ts', inplace=True, drop=True)
            df1 = df1.rename(columns = {'value':str(desc)})
            df = pd.concat([df,df1], axis = 1)

        df.sort_index(inplace=True)
        #df.set_index('ts', inplace=True, drop=True)
        

    if not df.empty:
        df = df.apply(pd.to_numeric, errors='coerce')

        
        
    else:
        print('empty df! must retrieve estimated value')
           
    return df
    



def send_data(mydf, device, entity):
    """
    Write telemetry data to the API.
    """
    df = mydf.copy()
    
    # convert to int
    for col in df.columns:
        df[col] = df[col].astype(np.int64)
    
    
    #print(device, df)
    # transform ts and write telemetry
    df['ts'] = df.index
    
    df['ts'] = df.apply(lambda row: int(row['ts'].timestamp()) * 1000, axis=1)
    df.set_index('ts', inplace=True, drop=True)
    df = df.sort_index()
    mydict = df.to_dict('index')

    l=[]
    for i, (key, value) in enumerate(mydict.items(), 1):
        newdict={}
        newdict['ts'] = key
        # newdict['values'] = value
        newdict['values'] = { k: v for k, v in value.items() if v == v }
        l.append(newdict)
    # write to json and send telemetry to TB
    my_json = json.dumps(l)
    
    print(device,my_json)
    thingsAPI.send_telemetry(ADDRESS, device, my_json, entity.upper())
    #print('Telemetry sent for device {}'.format(device))


     
     
    
    
def main():

    # define descriptors and access token
    descriptors = 'cnrgA,cnrgB,cnrgC'
    acc_token = thingsAPI.get_access_token(ADDRESS)
    start_time = '1706738400000' 
    end_time = '1720436400000'
    
    
    meter = '102.402.002036'
    print(meter)
    # if meter in ['VIRTUAL METER 41 - ΛΟΙΠΑ ΦΟΡΤΙΑ ΕΓΚΑΤΑΣΤΑΣΗΣ','Λοιπά Φορτία Eγκατάστασης','Υποσύνολο']:
    entity = 'device'
    devid = thingsAPI.get_devid(ADDRESS, meter, entity)
    df = read_data(meter, acc_token, start_time, end_time,descriptors, entity)
    print(df.tail()) 
    meter2 = '102.402.002050'
    send_data(df, meter2, entity)

    
if __name__ == "__main__":
    sys.exit(main())