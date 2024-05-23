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
import thingsio

# ADDRESS = "http://localhost:8080"    
ADDRESS = "https://mi3.meazon.com"

def align_resample(df,interv):
    """
    Aligns and resamples the DataFrame to the specified interval.
    """
   
    df['ts'] = df.index
    df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert('Europe/Athens')
    # df['ts'] = df['ts'].dt.tz_localize('Europe/Athens')
    df.set_index('ts',inplace = True, drop = True)
    df = df.resample(interv).max()
#     df = df.fillna(method="ffill")

    return df


def handle_missing(df, tmp1):
    '''
    Apply a 3-step fill of missing values
    '''
    # interpolate middle nans
    [df, avgDiffs] = interp_missing(df)
    # print('df and avgdiffs', df, avgDiffs)

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

def read_data(devid, acc_token, start_time, end_time, descriptors):
    """
    Reads data from the API and returns a DataFrame.
    """
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
        # print('df before fillna:\n', df)
        df = handle_missing(df, tmp1)
        # print('df after fillna:\n', df)

        # rename columns and resample daily
        df.rename(columns={'cnrgA':'clean_nrgA','cnrgB':'clean_nrgB','cnrgC':'clean_nrgC'}, inplace=True)
        df = df.resample('1D').max()
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
    #     # sanity check for negative nrg
    #     print(df.loc[df[col]<df[col].shift()])
    #     df.loc[df[col]<df[col].shift(), col]=np.nan


    df = df.dropna()
    if df.empty:
        return
    
    try:
        print(device, df)
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
        
        # print(my_json)
        #thingsio.send_telemetry(ADDRESS, device, my_json)
        # print('Telemetry sent for device {}'.format(device))

    except Exception as e:
        print(f"Error sending data for device {device}: {e}")
     

def main():
   
    physical_meters = ['102.216.000649',
                       '102.216.000651', 
                       '102.216.000652', 
                       '102.301.000850', 
                       '102.301.000865',
                       '102.301.000867', 
                       '102.301.000868', 
                       '102.301.000869', 
                       '102.301.000870',
                       '102.301.000896', 
                       '102.301.000916', 
                       '102.301.000921', 
                       '102.301.000923', 
                       '102.301.000935', 
                       '102.301.000963', 
                       '102.301.000968', 
                       '102.301.000969', 
                       '102.301.000970', 
                       '102.301.000976', 
                       '102.301.000978', 
                       '102.301.001102',
                       '102.301.001108', 
                       '102.301.001128', 
                       '102.402.000028', 
                       '102.402.000031', 
                       '102.402.000035', 
                       '102.402.000037', 
                       '102.402.000039']
    
    cleaned = {}

    # define descriptors and access token
    descriptors = 'cnrgA,cnrgB,cnrgC'
    acc_token = thingsio.get_access_token(ADDRESS)
    
    #define start - end date
    end_time = datetime.datetime.now(pytz.timezone('Europe/Athens'))
    end_time = end_time - datetime.timedelta(hours=end_time.hour, minutes=end_time.minute, seconds=end_time.second,
                                                    microseconds=end_time.microsecond)
    start_time = end_time +relativedelta(days=-5)
    
    print(start_time,end_time)
    # convert datetime to unix timestamp
    start_time = str(int(start_time.timestamp()) * 1000)
    end_time = str(int(end_time.timestamp()) * 1000)

    # iterate over physical meters to clean energy values
    for meter in physical_meters:
        devid = thingsio.get_devid(ADDRESS, meter)
        df = read_data(devid, acc_token, start_time, end_time,descriptors)
        cleaned[meter] = df


    # Basic virtual meters
    virtualMeters = {'Απαραίτητα':
                                ['102.216.000652', # M06
                                 '102.216.000649'], # M07
                    'Κλιματισμός Γραφείων': #V01
                                ['102.301.000976',# M02
                                 '102.301.000968',# M09
                                 '102.301.000969'], # M10
                    'Φωτισμός Γραφείων': # V02
                                ['102.301.000963',# M03
                                 '102.301.000869',# M04
                                 '102.301.000870',# M05
                                 '102.301.000850',# M11
                                 '102.301.000865',# M12
                                 '102.301.000868'],# M14
                    'UPS Γραφείων': # V03
                                ['102.402.000037',# M20
                                 '102.402.000028'], # M21
                    'Σύνολο Παρόχων': # V09
                                ['102.402.000039',# Μ22
                                 '102.301.000921'], # Μ18
                    'Σύνολο Σταθερής': # V10
                                ['102.301.000923',# Μ19
                                 '102.301.000916',# Μ17
                                 '102.301.000935',# Μ15
                                 '102.301.000978'], # Μ16
                    'Κλιματισμός Σταθερής': # V11
                                ['102.301.000916',# Μ17
                                 '102.301.000935',# Μ15
                                 '102.301.000978'], # Μ16
                    'Φωτισμός Καταστήματος':
                                ['102.301.001108',# Φωτισμός - Πρίζες
                                 '102.301.001128'], # Φωτισμός - Φώτα Νυκτός 
                    'Κλιματισμός Κινητής': # V07
                                ['102.402.000031', # M23
                                 '102.402.000035'], # Μ24
                    'Σύνολο Τεχνολογίας':
                                ['Σύνολο Σταθερής',# V10
                                 '102.402.000031'],#  23
                    'Υποσύνολο γραφείων': # (M2+M9+M10)+(M3+M4+M5+M11+M12+M14)+(M20+M21)+M01+M13
                                ['102.301.000870',# Fwtismos pinaka - M05
                                '102.301.000963',# Mparokivotia fwtismos - M03
                                '102.301.000850',# Fwtismos isogeio - M11
                                '102.301.000869',# Fwtismos 5ou - M04
                                '102.301.000868',# Fwtismos 3ou - M14
                                '102.301.000865',# Fwtismos 1ou - M12
                                '102.301.000970',# Fortia kinisis grafeiwn 3os - M01
                                '102.301.000867',# Fortia kinisis 1os - M13
                                '102.301.000969',#Klimatismos 5ou - M10
                                '102.301.000976',#Klimatismos 3ou - M02
                                '102.301.000968',#Klimatismos 1ou - M09
                                '102.402.000037', #UPS 2 - M20
                                '102.402.000028'], # UPS 1 - M21
                    'Υποσύνολο κιν-σταθ-κατ': # M23+(M22+M18)+(M19+M15+M16+M17)+SYNOLO COSMOTE SHOP
                                ['102.402.000039','102.301.000921', # V09 
                                '102.301.000923','102.301.000916','102.301.000935','102.301.000978', # V10 
                                '102.402.000031', # M23 
                                '102.301.000896'] ,# cosmote shop
                    'Υποσύνολο για υπόλοιπα': # intermediate addition
                                ['Υποσύνολο γραφείων',
                                'Υποσύνολο κιν-σταθ-κατ'],
                    'Υπόλοιπα Φορτία Γραφείων': # V05
                                ['102.216.000651', # M08
                                'Υποσύνολο για υπόλοιπα'],
                    'Φωτισμός-Κλιματισμός': # Φωτισμός καταστήματος + Κλιματισμός
                                ['102.301.001108', # Φωτισμός - Πρίζες 
                                '102.301.001128', # Φωτισμός - Φώτα Νυκτός 
                                '102.301.001102'], # Κλιματισμός καταστήματος
                    'Υπόλοιπα Καταστήματος': # Cosmote shop - Φωτισμός-Κλιματισμός
                                ['102.301.000896', # Cosmote shop
                                'Φωτισμός-Κλιματισμός'],
                    'Σύνολο Γραφείων':
                                ['Υπόλοιπα Φορτία Γραφείων',
                                'Υποσύνολο γραφείων']
    }

    # list of virtual meters that subtraction must be applied, not addition
    subtract_meters = ['Κλιματισμός Κινητής','Υπόλοιπα Καταστήματος','Υπόλοιπα Φορτία Γραφείων']
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
        
    nosendlist = ['Υποσύνολο γραφείων', 'Υποσύνολο κιν-σταθ-κατ', 'Υποσύνολο για υπόλοιπα', 'Φωτισμός-Κλιματισμός']    
    
    # Create list of meters (physical+virtual) whose telemetry will be stored in TB
    lst = [value for value in list(cleaned.keys()) if value not in nosendlist]
    filtered = {key: cleaned[key] for key in lst if key in cleaned}

    for key,data in filtered.items():
        send_data(data, key)


    # for key,value in cleaned.items():
    #     print(key,value)
    
if __name__ == "__main__":
    sys.exit(main())