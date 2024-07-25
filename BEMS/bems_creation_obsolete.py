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
#ADDRESS = "https://mi6.meazon.com"

def align_resample(df,interv, label):
    """
    Aligns and resamples the DataFrame to the specified interval.
    """
    df['ts'] = df.index
    df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert('Europe/Athens')
    df.set_index('ts',inplace = True, drop = True)
    df = df.resample(interv, label=label).max()

    return df


def handle_missing(df, tmp1):
    '''
    Apply a 3-step fill of missing values
    '''
    # interpolate middle nans
    [df, avgDiffs] = interp_missing(df)
    #print('avgdiffs', avgDiffs)

    tmp1 = align_resample(tmp1,'1h','left')
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
    tmp = tmp.iloc[-2]
    mydict = tmp.to_dict()
    legadict[device] = mydict
    
    return legadict



def read_virtual_data(device, acc_token, start_time, end_time, descriptors, entity):
    """
    Reads data of virtual freezer devices from the API and returns a DataFrame.
    """
    devid = thingsAPI.get_devid(ADDRESS, device, entity)
    url = f"{ADDRESS}/api/plugins/telemetry/DEVICE/{devid}/values/timeseries"
        
    try:
        response = requests.get(
            url=url,
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
                df = pd.concat(
                    [pd.DataFrame(r2[desc]).rename(columns={'value': desc}).set_index('ts') for desc in r2],
                    axis=1
                )
                df.reset_index(drop=False, inplace=True)
                df['ts'] = pd.to_datetime(df['ts'], unit='ms')
                df.sort_values(by=['ts'], inplace=True)
                df.set_index('ts', inplace=True, drop=True)

                if not df.empty:
                    df = df.apply(pd.to_numeric, errors='coerce')
                    df['ts'] = df.index
                    df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert('Europe/Athens')
                    df.set_index('ts',inplace = True, drop = True)
                    df = df.rename(columns={'totalCleanNrg':'historicNrg'})
                    print(df)

    except Exception as e:
        print(f"Error reading data for virtual device {device}: {e}")
        
    return df


def read_data(device, acc_token, start_time, end_time, descriptors, legadict, entity):
    """
    Reads data from the API and returns a DataFrame.
    """
    devid = thingsAPI.get_devid(ADDRESS, device, entity)
    
    # Create date range
    start_dt = pd.to_datetime(int(start_time), unit='ms')
    end_dt = pd.to_datetime(int(end_time), unit='ms')
    

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
        
        url = f"{ADDRESS}/api/plugins/telemetry/DEVICE/{devid}/values/timeseries"
        
        try:
            response = requests.get(
                url=url,
                params={
                    "keys": descriptors,
                    "startTs": st,
                    "endTs": en,
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
        df = align_resample(df, '1h', 'left')
        
        df = handle_missing(df, tmp1)
        

        # rename columns and resample daily
        df.rename(columns={'cnrgA':'clean_nrgA','cnrgB':'clean_nrgB','cnrgC':'clean_nrgC'}, inplace=True)
        df = df.resample('1D', label='left').max()
        
        ## Check if device is 102.402.002050, to correct cnrg after replacement
        if device=='102.402.002050':
            deltaA = 250206-5941
            deltaB = 250206-6213
            deltaC = 250206-5905
            avgdelta = (deltaA + deltaB + deltaC)/3

            df['total'] = df['clean_nrgA'] + df['clean_nrgB'] + df['clean_nrgC']
            for ph in ['A','B','C']:
                df['clean_nrg'+ph] = df['total']/3 + avgdelta
            df = df.drop('total', axis=1)

        legadict = legacy_info(df, device, legadict)
        #df = df.tail(1)
        # print('cleaned df daily:\n', df)
    else:
        print('empty df! must retrieve estimated value')
        filename = 'legacy_info.json'
        with open(filename, 'r', encoding='utf-8') as file:
            loaded_data = json.load(file)
            
        device_info = loaded_data[device]
        df = tmp1.copy()
        df = align_resample(df,'1D', 'left')
        
        
        for ph in ['A','B','C']:
            df['clean_nrg'+ph] = np.nan
            df['clean_nrg'+ph].iloc[-3] = device_info['clean_nrg'+ph]#[-2]
            df['clean_nrg'+ph].iloc[-2] = device_info['clean_nrg'+ph]+device_info['diff_clean_nrg'+ph]
            df['clean_nrg'+ph].iloc[-1] = device_info['clean_nrg'+ph]+2*device_info['diff_clean_nrg'+ph]
        
        df = df.sort_index(ascending=False)
        for i in range(3, len(df)):
            for ph in ['A','B','C']:
                df['clean_nrg'+ph].iloc[i] = device_info['clean_nrg'+ph]-(i-2)*device_info['diff_clean_nrg'+ph]
            
        df = df.sort_index()
        legadict = legacy_info(df, device, legadict)
           
    return df,legadict
 


def create_virtual(vrtl, cleaned, operation, historic):
    """
    Creates a virtual DataFrame by aggregating data from multiple devices.
    """
    agg = pd.DataFrame([])
    #try:
    if operation<2: # add or sub
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
    else:

        
        agg = cleaned[vrtl[0]].copy() # AC of unit
        agg['unitAC'] = agg['clean_nrgA']+agg['clean_nrgB']+agg['clean_nrgC']
        agg = agg[['unitAC']]
        
        totalAC = cleaned[vrtl[1]].copy() # total AC
        totalAC['AC'] = totalAC['clean_nrgA']+totalAC['clean_nrgB']+totalAC['clean_nrgC']
        totalAC = totalAC[['AC']]

        totalFreeze = cleaned[vrtl[2]].copy() # Sum of Freezers
        totalFreeze['Freezer'] = totalFreeze['clean_nrgA']+totalFreeze['clean_nrgB']+totalFreeze['clean_nrgC']
        totalFreeze = totalFreeze[['Freezer']]

        
        # First calculate deltas, then calculate complex freezer
        if (not agg.empty) and (not totalAC.empty) and (not totalFreeze.empty):
            agg = pd.concat([agg, totalAC, totalFreeze],axis=1)
            agg['delta_unitAC'] = agg['unitAC']-agg['unitAC'].shift()
            agg['delta_AC'] = agg['AC']-agg['AC'].shift()
            agg['delta_Freezer'] = agg['Freezer']-agg['Freezer'].shift()
            agg['totalCleanNrg'] = (agg['delta_unitAC']/agg['delta_AC'])*agg['delta_Freezer']
            
            # bring historic data to accumulate energy
            agg = pd.concat([historic, agg], axis=1)
            agg['test'] = agg['totalCleanNrg']
            agg.loc[agg['historicNrg'].notna(),'test'] = np.nan
            for i in range(1,len(agg)):
                if np.isnan(agg['historicNrg'].iloc[i] ):
                    agg['historicNrg'].iloc[i] = agg['historicNrg'].iloc[i-1]+agg['test'].iloc[i]
            agg = agg.drop(['test','totalCleanNrg'], axis=1)
            agg = agg.rename(columns={'historicNrg':'totalCleanNrg'})
            print('after concatenation with historic:',agg)
            # agg['totalCleanNrg'] = agg['totalCleanNrg'].cumsum()

            # agg = df.copy()
            # agg = agg.div(totalAC)
            # agg = agg.mul(totalFreeze)

            agg['clean_nrgA'] = agg['totalCleanNrg']/3
            agg['clean_nrgB'] = agg['totalCleanNrg']/3
            agg['clean_nrgC'] = agg['totalCleanNrg']/3
            
            agg = agg[['clean_nrgA','clean_nrgB','clean_nrgC']]
            print('ready df:',agg)
            # agg = agg.drop('totalCleanNrg',axis=1)
            
        else:
            return pd.DataFrame([])

            
    #except Exception as e:
    #    print(f"Unable to retrieve data for all virtuals: {e}")
        
    return agg.dropna()



def create_virtual_diff(vrtl, cleaned):
    agg = pd.DataFrame([])
    df1 = cleaned[vrtl[0]].copy() # central meter
    df1['totalCleanNrg'] = df1['clean_nrgA']+df1['clean_nrgB']+df1['clean_nrgC']

    df2 = cleaned[vrtl[1]].copy() # central meter
    df2['totalCleanNrg'] = df2['clean_nrgA']+df2['clean_nrgB']+df2['clean_nrgC']
    agg = df1[['totalCleanNrg']].copy()
    
    agg = agg.sub(df2[['totalCleanNrg']])
    
    agg['clean_nrgA'] = agg['totalCleanNrg']/3
    agg['clean_nrgB'] = agg['totalCleanNrg']/3
    agg['clean_nrgC'] = agg['totalCleanNrg']/3
    

    return agg.dropna()


def send_data(mydf, device, entity):
    """
    Write telemetry data to the API.
    """
    df = mydf.copy()
    df = df.round()
    
    
    # convert to int
    for col in ['clean_nrgA','clean_nrgB','clean_nrgC']:
        df[col] = df[col].astype(np.int64)
         # sanity check for negative nrg
        while not df.loc[df[col]<df[col].shift()].empty: 
            # df.loc[df[col]<df[col].shift(), col]=df[col].shift()
            
            df.loc[df[col]<df[col].shift(), col]=np.nan
            df[col] = df[col].ffill()
            df[col] = df[col].bfill()
            
    df['totalCleanNrg'] = df['clean_nrgA']+df['clean_nrgB']+df['clean_nrgC']
    df = df.iloc[1:]
    df = df.iloc[:-1] # remove current day's measurement until noon
    # df = df.tail(1) # write only energy of the previous day
    print('yesterdays value to write:', df)
    
    df = df.dropna()
    
    if df.empty:
        return
    
    #try:
    #print(device, df)
    # transform ts and write telemetry
    df['ts'] = df.index
    df['ts'] = df.apply(lambda row: int(row['ts'].timestamp()) * 1000, axis=1)
    df.set_index('ts', inplace=True, drop=True)
    df = df.astype(np.int64)
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
    
    thingsAPI.send_telemetry(ADDRESS, device, my_json,entity)
    print('Telemetry sent for device {}'.format(device))

    #except Exception as e:
    #    print(f"Error sending data for device {device}: {e}")
     

def main():
    
    # load meter info
    filepath = '/home/azureuser/BEMS/'
    os.chdir(filepath)
    filename = 'meters_info.json'
    with open(filename, 'r', encoding='utf-8') as file:
        loaded_data = json.load(file)

    # Access the loaded data
    physical_meters = loaded_data["physical_meters"] # physical meters
    virtualMeters = loaded_data["virtualMeters"] # virtual meters aggregations with physical meters
    subtract_meters = loaded_data["subtract_meters"] # meters that need subtracting in aggregation
    complex_calc_meters = loaded_data["complex_calc_meters"] # meters that need complex calculation (div/mul)
    unwriteable = loaded_data["unwriteable"] # intermediate virtual meters, no need to write data 
    assetdevs = loaded_data["assetdevs"] # Virtual rooms, represented as assets, not devices
    
    legadict = {} # legacy dictionary
    cleaned = {}

    # define descriptors and access token
    descriptors = 'cnrgA,cnrgB,cnrgC'
    acc_token = thingsAPI.get_access_token(ADDRESS)
    
    
    
    #define start - end date
    end_time = datetime.datetime.now(pytz.timezone('Europe/Athens'))
    end_time = end_time - datetime.timedelta(hours=end_time.hour, minutes=end_time.minute, seconds=end_time.second,
                                                    microseconds=end_time.microsecond)
    
    start_time = end_time +relativedelta(days=-8)
    start_time = start_time + datetime.timedelta(hours=13)
    end_time = end_time + datetime.timedelta(hours=13)
    
    # convert datetime to unix timestamp
    start_time = str(int(start_time.timestamp()) * 1000)
    end_time = str(int(end_time.timestamp()) * 1000)
    
    #month = 3
    year = 2024
    # for month in [3,4,5,6]:
    #     start_time = datetime.datetime(year = year, month=month, day=1)
    #     end_time = start_time + relativedelta(months=1)
    #     tmzn = pytz.timezone('Europe/Athens')    
    #     end_time = tmzn.localize(end_time)
    #     start_time = tmzn.localize(start_time)
    #     start_time = start_time +relativedelta(days=-1)
    #     start_time = start_time + datetime.timedelta(hours=13)
    #     end_time = end_time + datetime.timedelta(hours=13)
    #     end_time = str(int((end_time ).timestamp() * 1000))
    #     start_time = str(int((start_time ).timestamp() * 1000))
    # start_time = '1706785200000' 
    # end_time = '1721286000000'
    # end_time = '1709290800000'
    

    print(start_time,end_time)
    
    # iterate over physical meters to clean energy values
    for meter in physical_meters:
        # print(meter)
        [df, legadict] = read_data(meter, acc_token, start_time, end_time,descriptors, legadict, 'device')
        cleaned[meter] = df
    
    
    # Write the data to the file
    filename = 'meters_info.json'
    with open('legacy_info.json', 'w', encoding='utf-8') as file:
        json.dump(legadict, file, ensure_ascii=False, indent=4)

    # iterate over meters
    for virtualName,submeters in virtualMeters.items():
        print(virtualName)
        if virtualName in subtract_meters:
            print('SUBTRACTING')
            agg = create_virtual_diff(submeters, cleaned)
        elif virtualName in complex_calc_meters:
            vrtl_desc = 'totalCleanNrg'
            vrtl_fr = read_virtual_data(virtualName, acc_token, start_time, end_time, vrtl_desc, 'device')
            
            operation = 2 # div then mul
            agg = create_virtual(submeters, cleaned, operation, vrtl_fr)
        else:
            operation=1 #add
            agg = create_virtual(submeters, cleaned, operation, {})

        
        
        if not agg.empty:
            cleaned[virtualName] = agg
        # agg = postproc(agg, virtualName)
        #send_data(agg,virtualName, address)
        
    
    # Create list of meters (physical+virtual) whose telemetry will be stored in TB
    lst = [value for value in list(cleaned.keys()) if value not in unwriteable]
    filtered = {key: cleaned[key] for key in lst if key in cleaned}
    
    # write telemetry
    for key,data in filtered.items():
    
        if key in assetdevs:
            entity = 'ASSET'
            # send_data(data, key, entity)
        else:
            entity = 'DEVICE'
        #print(key, entity, data.head())
        send_data(data, key, entity)
            
    #     if key in ['VIRTUAL METER 41 - ΛΟΙΠΑ ΦΟΡΤΙΑ ΕΓΚΑΤΑΣΤΑΣΗΣ','Λοιπά Φορτία Eγκατάστασης','Υποσύνολο','Κέντρο Ερευνας & Τεχνολογίας (ΚΕΤ)',"Virtual Meter 36 - ΚΑΤΑΝΑΛΩΣΗ ΨΥΚΤΩΝ ΚΕΤ",
    #     "Virtual Meter 37 - ΚΑΤΑΝΑΛΩΣΗ ΨΥΚΤΩΝ ΑΜΦΙΘΕΑΤΡΟ",
    #     "Virtual Meter 38 - ΚΑΤΑΝΑΛΩΣΗ ΨΥΚΤΩΝ ΑΙΘ. ΔΙΑΛΕΞΕΩΝ  Α' ΟΡΟΦΟΣ",
    #     "Virtual Meter 39 - ΚΑΤΑΝΑΛΩΣΗ ΨΥΚΤΩΝ U-TECH LAB",
    #     "Virtual Meter 40 - ΚΑΤΑΝΑΛΩΣΗ ΨΥΚΤΩΝ ΒΙΒΛΙΟΘΗΚΗΣ",
    #     "Κέντρο Ερευνας & Τεχνολογίας (ΚΕΤ)",
    #     "Αμφιθέατρο",
    #     "Αίθουσα Διαλέξεων",
    #     "U - TECH LAB",
    #     "Βιβλιοθήκη"]:
    #         print(key, entity, data.head(10))
    #         send_data(data, key, entity)
        
    
     
if __name__ == "__main__":
    sys.exit(main())