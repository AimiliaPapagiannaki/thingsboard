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
        df['totalCleanNrg'] = df['clean_nrgA']+df['clean_nrgB']+df['clean_nrgC']
        
    else:
        print('empty df! must retrieve estimated value')
           
    return df
    



def send_data(mydf, device, entity):
    """
    Write telemetry data to the API.
    """
    df = mydf[['totalCleanNrg']].copy()
    
    # convert to int
    
    df['totalCleanNrg'] = df['totalCleanNrg'].astype(np.int64)
    
    
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
    # print('Telemetry sent for device {}'.format(device))


     
     
    
    
def main():

    # define descriptors and access token
    descriptors = 'clean_nrgA,clean_nrgB,clean_nrgC'
    acc_token = thingsAPI.get_access_token(ADDRESS)
    start_time = '1708380000000' 
    end_time = '1719781200000'
    
    
    # load meter info
    filepath = '/home/azureuser/BEMS/'
    os.chdir(filepath)
    filename = 'meters_info.json'
    with open(filename, 'r', encoding='utf-8') as file:
        loaded_data = json.load(file)

    # Access the loaded data
    physical_meters = loaded_data["physical_meters"] # physical meters
    virtualMeters = loaded_data["virtualMeters"] # virtual meters aggregations with physical meters
    unwriteable = loaded_data["unwriteable"] # intermediate virtual meters, no need to write data 
    assetdevs = loaded_data["assetdevs"] # Virtual rooms, represented as assets, not devices
    
    # Create list of meters (physical+virtual) whose telemetry will be stored in TB
    lst = [value for value in list(virtualMeters.keys()) if value not in unwriteable]
    totalist = physical_meters + lst
    
    for meter in totalist:
        print(meter)
        if meter=='VIRTUAL METER 41 - ΛΟΙΠΑ ΦΟΡΤΙΑ ΕΓΚΑΤΑΣΤΑΣΗΣ':
            if meter in assetdevs:
                entity = 'asset'
            else:
                entity = 'device'
            devid = thingsAPI.get_devid(ADDRESS, meter, entity)
            df = read_data(meter, acc_token, start_time, end_time,descriptors, entity)
            
            send_data(df, meter, entity)
    
    
    
    #meter = 'Πλανητάριο'
    #entity = 'asset'
    #devid = thingsAPI.get_devid(ADDRESS, meter, entity)
    #df = read_data(meter, acc_token, start_time, end_time,descriptors, entity)
    #send_data(df, meter, entity)

    
if __name__ == "__main__":
    sys.exit(main())