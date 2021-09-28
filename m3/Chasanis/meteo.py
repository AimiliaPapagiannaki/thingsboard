#!/usr/bin/env python3
import glob, os, sys
import pandas as pd
import datetime
import requests
import json
from dateutil.relativedelta import relativedelta
import random
import numpy as np
import pytz
import calendar






def align_resample(df, interval,tmzn):

    df = df.groupby(df.index).max()
    df.sort_index(inplace=True)
    
    ##########set timezone
    df['ts'] = df.index
    df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
    df.reset_index(drop=True, inplace=True)
    df.set_index('ts',inplace = True, drop = True)

    res = interval+'T'
    side = 'left' 
    
    # resample df to given interval 
    
    df = df.resample(res,label = side, closed = side).mean()
    df.reset_index(inplace = True, drop = False)
    df.set_index('ts',inplace = True, drop = False)
    
    return df


def read_data(devid, acc_token, address, start_time, end_time, interval, descriptors, tmzn):

    # request all descriptors that have ever been assigned to this device
    r1 = requests.get(url = address+"/api/plugins/telemetry/DEVICE/"+devid+"/keys/timeseries",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    

    descriptors = descriptors.split(",")
    descriptors = [x for x in descriptors if x in r1]
           
    descriptors = ','.join(descriptors)
    
    # mapping is a dictionary to map all occurencies of descriptors to descent column names
    mapping = {}
    mapping['pwrA'] = 'pwrA'
    mapping['pwrB'] = 'pwrB'
    mapping['pwrC'] = 'pwrC'
    

    
    r2 = requests.get(url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys="+descriptors+"&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    print("keys length ",len(r2.keys()))
    if ((len(r2.keys())>0) & (len(descriptors)>0)):
        df = pd.DataFrame([])
        

        # read all descriptors at once
        for desc in r2.keys():
            df1 = pd.DataFrame(r2[desc])
            df1.set_index('ts', inplace=True)
            df1.columns = [mapping.get(str(desc))]
            df = pd.concat([df,df1], axis = 1)
                   

        if df.empty == False:
        
            df.reset_index(drop=False, inplace=True)
            df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
            # Set timestamp as index, convert all columns to float
            df = df.sort_values(by=['ts'])
            df.reset_index(drop=True, inplace=True)
            df.set_index('ts',inplace = True, drop = True)
            for col in df.columns:
                df[col] = df[col].astype('float')
            
            df = align_resample(df, interval,tmzn)
            
            df = df.drop('ts',axis = 1)
            
            
        else:
            df = pd.DataFrame([])
            side = ' '
    else:
        df = pd.DataFrame([])
        side = ' '
        print('Empty json!')
    return df


def split_powers(df):

    # the meteorological station is present on phase C
    df['meteo']=df['pwrC']
    df.loc[df['meteo']>1600,'meteo']=np.nan
    df['nulls']=0
    df.loc[((df['meteo'].shift().isna()) | (df['meteo'].shift(-1).isna())),'nulls']=1
    df.loc[((df['meteo'].shift().isna()) | (df['meteo'].shift(-1).isna())),'meteo']=np.nan
    
    df.loc[df['meteo'].isna(),'nulls']=1
    # df['meteo'] = df['meteo'].interpolate()
    df.fillna(method='ffill',inplace=True)
    df.fillna(method='bfill',inplace=True)
    
   # after filling nans, insert some randnomness to create real-like values
    for i in range(0,df.shape[0]):
        if df['nulls'].iloc[i]>0.5:
            try:
                df['meteo'].iloc[i] = random.randint(int(df['meteo'].iloc[i].astype(int)-(0.1*df['meteo'].iloc[i].astype(int))),int(df['meteo'].iloc[i].astype(int)+(0.1*df['meteo'].iloc[i].astype(int))))
            except:
                continue;
        
    # the third phase of lights is now the subtraction of pwrC minus the m.station        
    df['nlight'] = df['pwrC']-df['meteo']
    df = df.dropna()
    df.drop(['nulls'],axis=1,inplace=True)
    
    # if negative values are present, make them zero
    df.loc[df['meteo']<0,'meteo']=0
    df.loc[df['pwrA']<0,'pwrA']=0
    df.loc[df['pwrB']<0,'pwrB']=0
    df.loc[df['nlight']<0,'nlight']=0

    # calculate energy of lights and meteorological station
    df['nrgLights'] = (df['pwrA']+df['pwrB']+df['nlight'])*60/3600
    df['nrgMeteo'] = df['meteo']*60/3600
    
    return df
    
    

def get_dev_info(devname):
    address = "http://localhost:8080"
    r = requests.post(address + "/api/auth/login",
                      json={'username': 'a.papagianaki_tenant@meazon.com', 'password': 'meazonmeazon'}).json()

    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']
    # get devid by serial name
    r1 = requests.get(
        url=address + "/api/tenant/devices?deviceName=" + devname,
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()

    devid = r1['id']['id']
    r1 = requests.get(
        url=address + "/api/device/" + devid + "/credentials",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    devtoken = r1['credentialsId']

    return devid,devtoken,acc_token,address


def transform_df(tmp):

    for col in tmp.columns:
        tmp[col] = tmp[col].apply(str)
        
    tmp['ts'] = tmp.index.values.astype(np.int64) // 10 ** 6    
    
    tmp.set_index('ts',inplace = True, drop = True)
    mydict = tmp.to_dict('index')
    
    return mydict  



def send_data(mydict,devtoken,address,acc_token):

    for key, value in mydict.items():
        my_json = json.dumps({'ts': key, 'values': value})
        #print(my_json)
        r = requests.post(url=address + "/api/v1/" + devtoken + "/telemetry",
                          data=my_json, headers={'Content-Type': 'application/json', 'Accept': '*/*',
                                                 'X-Authorization': acc_token})
    print('Finished')

if __name__ == '__main__':

    devname = '102.402.000475'
    interval = '1'
    descriptors = 'pwrA,pwrB,pwrC'
    tmzn = 'Europe/Athens' 
    
    # devname=sys.argv[1]
    [devid,devtoken,acc_token,address] = get_dev_info(devname)
    
    end_time = datetime.datetime.now()
    end_time = end_time - datetime.timedelta(hours=end_time.hour, minutes=end_time.minute, seconds=end_time.second+1,
                                                 microseconds=end_time.microsecond)
    start_time = end_time +relativedelta(days=-1)
    #end_time = end_time-datetime.timedelta(seconds=1)


    start_time = str(int(start_time.timestamp()) * 1000)
    #start_time = '1626364200000'
    end_time = str(int(end_time.timestamp()) * 1000)
    
    df = read_data(devid,acc_token,address, start_time, end_time, interval, descriptors,tmzn)
    
    df = split_powers(df)
    
    # write power data
    tmp = df[['meteo','nlight']].copy()
    pwrdict = transform_df(tmp)
    #send_data(pwrdict,devtoken,address,acc_token)
    
    # write energy data
    ndf=df.resample('1D').sum()
    print(ndf)
    tmp = ndf[['nrgLights','nrgMeteo']].copy()
    nrgdict = transform_df(tmp)
    #send_data(nrgdict,devtoken,address,acc_token)
    
    
    
    
    

    