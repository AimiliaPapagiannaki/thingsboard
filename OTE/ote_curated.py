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

# ADDRESS = "http://localhost:8080"    
ADDRESS = "https://mi3.meazon.com"

def align_resample(df,interv):
    """
    Aligns and resamples the DataFrame to the specified interval.
    """
    df['ts'] = df.index
    df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert('Europe/Athens')
    df.set_index('ts',inplace = True, drop = True)
    df = df.resample(interv).max()

    return df


def handle_missing(df, tmp1):
    '''
    Apply a 3-step fill of missing values
    '''
    # interpolate middle nans
    [df, avgDiffs] = interp_missing(df)
    #print('avgdiffs', avgDiffs)

    tmp1 = align_resample(tmp1,'1h')
    tmp1 = pd.concat([tmp1,df], axis=1)
    tmp1 = tmp1.sort_index()
    
    nan_mask = tmp1['cnrgA'].isna()
    # Find the index of the first and last non-NaN value
    first_non_nan_index = nan_mask.idxmin()
    last_non_nan_index = nan_mask.sort_index(ascending=False).idxmin()

    # Select preceding nan rows
    if nan_mask[0]==True:
        back_df = tmp1.loc[:first_non_nan_index].copy()
        back_df = interp_edges(back_df, avgDiffs, 'start')

        df = pd.concat([df,back_df])
        df = df.sort_index()
    # Select succeeding nan rows
    if nan_mask[-1]==True:
        forw_df = tmp1.loc[last_non_nan_index:].copy()
        forw_df = interp_edges(forw_df, avgDiffs, 'end')

        df = pd.concat([df,forw_df])
        df = df.sort_index()
    return df



def interp_missing(df):
    '''
    Interpolate middle missing values and calculate average diff
    '''
    # interpolate backwards
    df = df.interpolate(method='linear', limit_direction='backward')
    avgDiffs={}
    for ph in ['A','B','C']:
        df['diff'+ph] = df['cnrg'+ph]-df['cnrg'+ph].shift()
        avgDiffs[ph] = df['diff'+ph].mean().round().astype(int)
    
    df = df[['cnrgA','cnrgB','cnrgC']]

    return df, avgDiffs
                
def interp_edges(df, fillvalue, edge):
    if not df.empty:
        if edge == 'start': # preceding nan values
            df = df.sort_index(ascending=False)
            for i in range(1, len(df)):
                for ph in ['A','B','C']:
                    df['cnrg'+ph].iloc[i] = df['cnrg'+ph].iloc[i-1]-fillvalue[ph]
            df = df.sort_index()

        elif edge == 'end':
            for i in range(1, len(df)):
                for ph in ['A','B','C']:
                    df['cnrg'+ph].iloc[i] = df['cnrg'+ph].iloc[i-1]+fillvalue[ph]
    return df


def legacy_info(df, device, legadict):
    tmp = df.copy()
    tmp = tmp.round()
    for col in tmp.columns:
        tmp['diff_'+col] = tmp[col]-tmp[col].shift()
    tmp = tmp.iloc[-1]
    mydict = tmp.to_dict()
    legadict[device] = mydict
    
    return legadict


def read_data(device, acc_token, start_time, end_time, descriptors, legadict):
    """
    Reads data from the API and returns a DataFrame.
    """
    devid = thingsAPI.get_devid(ADDRESS, device)
    print(device)
    # Create date range
    start_dt = pd.to_datetime(start_time, unit='ms')
    end_dt = pd.to_datetime(end_time, unit='ms')

    # Create a datetime index with a desired frequency (e.g., 'D' for daily)
    datetime_index = pd.date_range(start=start_dt, end=end_dt, freq='H')
    datetime_index = datetime_index[:-1]

    tmp1 = pd.DataFrame(index=datetime_index)
    df = pd.DataFrame([])
    offset = 30 * 86400000 if int(end_time) - int(start_time) > 30 * 86400000 else 86400000
    svec = np.arange(int(start_time), int(end_time), offset)
    
    for st in svec:
        en = st + offset - 1
        en = int(end_time) if int(end_time) - en <= 0 else en

        try:
            response = requests.get(
                url=f"{ADDRESS}/api/plugins/telemetry/DEVICE/{devid}/values/timeseries",
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
                tmp = pd.concat(
                    [pd.DataFrame(r2[desc]).rename(columns={'value': desc}).set_index('ts') for desc in r2],
                    axis=1
                )
                tmp.reset_index(drop=False, inplace=True)
                tmp['ts'] = pd.to_datetime(tmp['ts'], unit='ms')
                tmp.sort_values(by=['ts'], inplace=True)
                tmp.set_index('ts', inplace=True, drop=True)

                df = pd.concat([df, tmp])

        except Exception as e:
            print(f"Error reading data for device {devid}: {e}")
            continue

    if not df.empty:
        df = df.apply(pd.to_numeric, errors='coerce')
        df = align_resample(df, '1h')
        
        df = handle_missing(df, tmp1)
        

        # rename columns and resample daily
        df.rename(columns={'cnrgA':'clean_nrgA','cnrgB':'clean_nrgB','cnrgC':'clean_nrgC'}, inplace=True)
        df = df.resample('1D').max()
        legadict = legacy_info(df, device, legadict)
        #df = df.tail(1)
        # print('cleaned df daily:\n', df)
    else:
        print('empty df! must retrieve estimated value')
           
    return df
 


def create_virtual(vrtl, cleaned, operation):
    """
    Creates a virtual DataFrame by aggregating data from multiple devices.
    """
    agg = pd.DataFrame([])
    try:
        for submeter in vrtl:
            df = cleaned[submeter]

            if not df.empty:
                if not agg.empty:
                    if operation == 1:
                        agg = agg.add(df)
                    else:
                        agg = agg.sub(df)                        
                else:
                    agg = df
            else:
                return pd.DataFrame([])
            
    except Exception as e:
        print(f"Unable to retrieve data for all virtuals: {e}")
        
    return agg.dropna()



def send_data(mydf, device):
    """
    Write telemetry data to the API.
    """
    df = mydf.copy()
    df = df.round()
    
    # convert to int
    for col in ['clean_nrgA','clean_nrgB','clean_nrgC']:
        df[col] = df[col].astype(np.int64)
         # sanity check for negative nrg
        df.loc[df[col]<df[col].shift(), col]=df[col].shift()
    
    df = df.iloc[:-1] # remove current day's measurement until noon
    df = df.tail(1) # write only energy of the previous day
    
    df = df.dropna()
    if df.empty:
        return
    
    try:
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
        
        
        thingsAPI.send_telemetry(ADDRESS, device, my_json)
        # print('Telemetry sent for device {}'.format(device))

    except Exception as e:
        print(f"Error sending data for device {device}: {e}")
     

def main():
    
    # load meter info
    filepath = '/home/mcubeazon/Analytics/OTE/curatedVirtuals/'
    os.chdir(filepath)
    filename = 'meters_info.json'
    with open(filename, 'r', encoding='utf-8') as file:
        loaded_data = json.load(file)

    # Access the loaded data
    physical_meters = loaded_data["physical_meters"] # physical meters
    virtualMeters = loaded_data["virtualMeters"] # virtual meters aggregations with physical meters
    subtract_meters = loaded_data["subtract_meters"] # meters that need subtracting in aggregation
    unwriteable = loaded_data["unwriteable"] # intermediate virtual meters, no need to write data 
    
    legadict = {} # legacy dictionary
    cleaned = {}

    # define descriptors and access token
    descriptors = 'cnrgA,cnrgB,cnrgC'
    acc_token = thingsAPI.get_access_token(ADDRESS)
    
    
    
    #define start - end date
    end_time = datetime.datetime.now(pytz.timezone('Europe/Athens'))
    end_time = end_time - datetime.timedelta(hours=end_time.hour, minutes=end_time.minute, seconds=end_time.second,
                                                    microseconds=end_time.microsecond)
    
    start_time = end_time +relativedelta(days=-5)
    end_time = end_time + datetime.timedelta(hours=13)
    
    # convert datetime to unix timestamp
    start_time = str(int(start_time.timestamp()) * 1000)
    end_time = str(int(end_time.timestamp()) * 1000)
    
    #month = 12
    #year = 2024
    #startm = datetime.datetime(year = year, month=month, day=1)
    #endm = startm + relativedelta(months=1)
    #tmzn = pytz.timezone('Europe/Athens')    
    #endm = tmzn.localize(endm)
    #startm = tmzn.localize(startm)
    #end_time = str(int((endm ).timestamp() * 1000))
    #start_time = str(int((startm ).timestamp() * 1000))
    #start_time = '1714510800000' 
    #end_time = '1716843600000'
    

    print(start_time,end_time)
    
    # iterate over physical meters to clean energy values
    for meter in physical_meters:
        devid = thingsAPI.get_devid(ADDRESS, meter)
        df = read_data(meter, acc_token, start_time, end_time,descriptors, legadict)
        cleaned[meter] = df
    

    # Write the data to the file
    filename = 'meters_info.json'
    with open('legacy_info.json', 'w', encoding='utf-8') as file:
        json.dump(legadict, file, ensure_ascii=False, indent=4)

    # iterate over meters
    for virtualName,submeters in virtualMeters.items():
        if virtualName in subtract_meters:
            operation=0 # sub
        else:
            operation=1 #add

        agg = create_virtual(submeters, cleaned, operation)
        if not agg.empty:
            cleaned[virtualName] = agg
        # agg = postproc(agg, virtualName)
        #send_data(agg,virtualName, address)
        
    
    # Create list of meters (physical+virtual) whose telemetry will be stored in TB
    lst = [value for value in list(cleaned.keys()) if value not in unwriteable]
    filtered = {key: cleaned[key] for key in lst if key in cleaned}

    for key,data in filtered.items():
        send_data(data, key)


    # for key,value in cleaned.items():
    #     print(key,value)
    
if __name__ == "__main__":
    sys.exit(main())