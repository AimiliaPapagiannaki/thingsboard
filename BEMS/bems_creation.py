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

pd.set_option('display.max_rows', None)
ADDRESS = "http://localhost:8080"    
#ADDRESS = "https://mi6.meazon.com"

def correct_cumulative_column(df, column_name):
    # Extract the column to a list for easier manipulation
    values = df[column_name].tolist()

    # Iterate through the list and correct the values
    for i in range(1, len(values)):
        if values[i] < values[i-1]:
            diff = values[i-1] - values[i]
            for j in range(i, len(values)):
                values[j] += diff
    
    # Assign the corrected values back to the DataFrame
    df[column_name] = values
    return df

def preprocess_nrg(df):
    for ph in ['A','B','C']:
        # check if there are incidents of negative delta nrg
        if not df.loc[df['cnrg'+ph]<df['cnrg'+ph].shift()].empty:
            df = correct_cumulative_column(df, 'cnrg'+ph)
    
    return df


def align_resample(df):
    """
    Aligns and resamples the DataFrame to the specified interval.
    """
    df['ts'] = df.index
    df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert('Europe/Athens')
    df.set_index('ts',inplace = True, drop = True)
    #df = df.resample(interv, label=label).max()

    return df



def handle_missing(df, tmp1):
    '''
    Apply a 3-step fill of missing values
    '''
    # interpolate middle nans
    [df, avgDiffs] = interp_missing(df)
    #print('avgdiffs', avgDiffs)

    tmp1 = align_resample(tmp1)
    # tmp1 = tmp1.resample('5T').max()
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
    df = df.resample('5T').max()
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
    
    ind = tmp.index[-1]
    tmp = tmp.iloc[-1]
    mydict = tmp.to_dict()
    mydict['ts'] = ind
    legadict[device] = mydict
    
    return legadict

# Function to convert Timestamp to string
def convert_timestamp(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    raise TypeError("Type not serializable")

 

def read_data(device, acc_token, start_time, end_time, descriptors, legadict, entity):
    """
    Reads data from the API and returns a DataFrame.
    """
    devid = thingsAPI.get_devid(ADDRESS, device, entity)
    
    # Create date range
    start_dt = pd.to_datetime(int(start_time), unit='ms')
    end_dt = pd.to_datetime(int(end_time), unit='ms')
    

    # Create a datetime index with a desired frequency (e.g., 'D' for daily)
    # datetime_index = pd.date_range(start=start_dt, end=end_dt, freq='H')
    datetime_index = pd.date_range(start=start_dt, end=end_dt, freq='5T')
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
        
        # address globack problem if existent
        df = preprocess_nrg(df)
        # resample on hourly basis
        df = align_resample(df)
        
        # fill missing values, if any
        df = handle_missing(df, tmp1)

        

        # rename columns and resample daily
        df.rename(columns={'cnrgA':'clean_nrgA','cnrgB':'clean_nrgB','cnrgC':'clean_nrgC'}, inplace=True)
        #df = df.resample('1D', label='left').max()
        
        # Convert clean_nrg to delta nrg
        # print('df before delta:',df)
        for ph in ['A','B','C']:
            df['clean_nrg'+ph] = df['clean_nrg'+ph]-df['clean_nrg'+ph].shift()
        
        df = df.resample('1D', label='left').sum()
        
        # write last row to dictionary, to have it as reference in case of missing values
        legadict = legacy_info(df, device, legadict)
        
        
    else:
        print('empty df! must retrieve estimated value')
        filename = 'legacy_info.json'
        with open(filename, 'r', encoding='utf-8') as file:
            loaded_data = json.load(file) 
        
        # Convert the dictionary to a DataFrame
        device_info = loaded_data[device]
        tmp1 = pd.DataFrame(index=datetime_index)
        df = tmp1.copy()
        df = align_resample(df)
        df = df.resample('1D', label='left').sum()
        
        # Create a DataFrame with the timestamp as the index
        tmpdf = pd.DataFrame(device_info, index=['ts'])
        tmpdf['ts'] = pd.to_datetime(tmpdf['ts'])
        # Set the 'timestamp' column as the index
        tmpdf.set_index('ts', inplace=True)
        
        # Create a DataFrame with the timestamp as the index
        df = pd.concat([df, tmpdf], axis=1)
        df = df.ffill() # forward fill
        df = df.bfill() # backward fill
        
        legadict = legacy_info(df, device, legadict)
           
    return df,legadict
 


def create_virtual(vrtl, cleaned, operation):
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
                        agg = agg.add(df, fill_value=0)
                    else:
                        agg = agg.sub(df, fill_value=0)                        
                else:
                    agg = df
                
            else:
                return pd.DataFrame([])
        agg['totalCleanNrg'] = agg['clean_nrgA']+agg['clean_nrgB']+agg['clean_nrgC']
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

        
        
        if (not agg.empty) and (not totalAC.empty) and (not totalFreeze.empty):
            agg = pd.concat([agg, totalAC, totalFreeze],axis=1)
            agg['totalCleanNrg'] = (agg['unitAC']/agg['AC'])*agg['Freezer']
                        
            # agg = df.copy()
            # agg = agg.div(totalAC)
            # agg = agg.mul(totalFreeze)

            agg['clean_nrgA'] = agg['totalCleanNrg']/3
            agg['clean_nrgB'] = agg['totalCleanNrg']/3
            agg['clean_nrgC'] = agg['totalCleanNrg']/3
            
            agg = agg[['clean_nrgA','clean_nrgB','clean_nrgC','totalCleanNrg']]
            
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
    
    agg = agg.sub(df2[['totalCleanNrg']], fill_value=0)
    
    agg['clean_nrgA'] = agg['totalCleanNrg']/3
    agg['clean_nrgB'] = agg['totalCleanNrg']/3
    agg['clean_nrgC'] = agg['totalCleanNrg']/3
    

    return agg.dropna()


def send_data(mydf, device, entity):
    """
    Write telemetry data to the API.
    """
    df = mydf.copy()
    # df = df.round()
    
    
    # convert to int
    for col in ['clean_nrgA','clean_nrgB','clean_nrgC']:
        df[col] = np.round(df[col],2)
       
    
    if not 'totalCleanNrg'  in df.columns:
        df['totalCleanNrg'] = df['clean_nrgA']+df['clean_nrgB']+df['clean_nrgC']
    

    # df = df.tail(1) # write only energy of the previous day
    # print('yesterdays value to write:', df)
    
    df = df.dropna()
    
    if df.empty:
        return
    
    #try:
    #print(device, df)
    # transform ts and write telemetry
    df['ts'] = df.index
    df['ts'] = df.apply(lambda row: int(row['ts'].timestamp()) * 1000, axis=1)
    df.set_index('ts', inplace=True, drop=True)
    # df = df.astype(np.int64)
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

    
def store_primitive_cleaned(cleaned, month, year):
    # Initialize an empty dataframe to store the combined data
    combined_sums = pd.DataFrame()
    # copied_dict = copy.deepcopy(cleaned)
    copied_dict = cleaned.copy()
    # Iterate through the dictionary
    for name, mydf in copied_dict.items():
        # Calculate the sum of columns A, B, C elementwise
        mydf['Sum'] = mydf[['clean_nrgA', 'clean_nrgB', 'clean_nrgC']].sum(axis=1)
        mydf['Date'] = mydf.index.astype(str)
        
        # Add the 'Sum' column to the combined_sums dataframe
        combined_sums[name] = mydf['Sum']
    combined_sums['Date'] = mydf['Date']
    
    date_col = combined_sums.pop('Date') 
    combined_sums.insert(0, 'Date', date_col)
    
    # Save the combined dataframe to an Excel file
    combined_sums.to_excel('combined_sums_'+str(month)+'_'+str(year)+'.xlsx', index=False)
    del combined_sums

 
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
    start_time = end_time +relativedelta(days=-5)

    
    # convert datetime to unix timestamp
    start_time = str(int(start_time.timestamp()) * 1000)
    end_time = str(int(end_time.timestamp()) * 1000)
    
    month = 7
    year = 2024
    # for month in [3,4,5,6]:
    #     start_time = datetime.datetime(year = year, month=month, day=1)
    #     end_time = start_time + relativedelta(months=1)
    #     tmzn = pytz.timezone('Europe/Athens')    
    #     end_time = tmzn.localize(end_time)
    #     start_time = tmzn.localize(start_time)
    #     start_time = start_time +relativedelta(days=-1)
        
    #     end_time = str(int((end_time ).timestamp() * 1000))
    #     start_time = str(int((start_time ).timestamp() * 1000))
    start_time = '1719781200000' 
    end_time = '1721854800000'

    
 
    print(start_time,end_time)
    
    # iterate over physical meters to clean energy values
    for meter in physical_meters:
        print(meter)
        [df, legadict] = read_data(meter, acc_token, start_time, end_time,descriptors, legadict, 'device')
        cleaned[meter] = df
        
    

    # Write the data to the file
    with open('legacy_info.json', 'w', encoding='utf-8') as file:
        json.dump(legadict, file, ensure_ascii=False, indent=4, default=convert_timestamp)

    

    # iterate over meters
    for virtualName,submeters in virtualMeters.items():
        print(virtualName)
        if virtualName in subtract_meters:
            
            agg = create_virtual_diff(submeters, cleaned)
        elif virtualName in complex_calc_meters:            
            operation = 2 # div then mul
            agg = create_virtual(submeters, cleaned, operation)
        else:
            operation=1 #add
            agg = create_virtual(submeters, cleaned, operation)

        print(agg)
        
        if not agg.empty:
            cleaned[virtualName] = agg

    store_primitive_cleaned(cleaned, month, year)
        
    
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
        # send_data(data, key, entity)
        
        
     
if __name__ == "__main__":
    sys.exit(main())