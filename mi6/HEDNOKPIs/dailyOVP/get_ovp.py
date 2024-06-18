#!/usr/bin/env python3 

import sys
import pandas as pd
import numpy as np
import requests
import os
import datetime
import glob
from dateutil.relativedelta import relativedelta
import pytz
import matplotlib.dates as mdates
import json
import math
import ast
from sys import stdout as out
import matplotlib.pyplot as plt
#plt.rcParams.update({'font.size': 14})
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

import warnings
warnings.filterwarnings("ignore")


def pwrAlarms(df, nominal, label, device):
    dictph = {}
    dictph['A'] = 'L1'
    dictph['B'] = 'L2'
    dictph['C'] = 'L3'
    
    globaldf = pd.DataFrame([])
    # calculate new variables
    for ph in ['A','B','C']:
        df['apwr'+ph] = df['vlt'+ph]*df['cur'+ph]

    
    nomphase = 1000*np.round(nominal/3,2)
    threshold = 0.8*nomphase
    #print('threshold:',threshold)
    # Find power alarms
    
    for ph in ['A','B','C']:
        tmp = pd.DataFrame([])
        df['pwrAlarm'+ph] = np.nan
        df['exceed_perc'+ph] = np.nan # percentage of exceeding the threshold
        df.loc[df['apwr'+ph]>threshold, 'pwrAlarm'+ph] = 1
        df.loc[df['apwr'+ph]>threshold, 'exceed_perc'+ph] = 100*(df['apwr'+ph]/nomphase)
        
        # create groups
        ind=0
        df['alarm'+ph] = np.nan
        for i in range(1,df.shape[0]):
            if ((df['pwrAlarm'+ph].iloc[i]==1) & (df['pwrAlarm'+ph].iloc[i-1]!=1)):
                ind+=1
                df['alarm'+ph].iloc[i]=ind
            elif((df['pwrAlarm'+ph].iloc[i]==1) & (df['pwrAlarm'+ph].iloc[i-1]==1)):
                df['alarm'+ph].iloc[i]=ind
       
        # number of distinct events/alarms
        events = df.groupby('alarm'+ph).ngroups
        df['events'+ph] = events
        
        dtPerc = 100*np.round(df.dropna(subset=['cur'+ph]).describe()['pwrAlarm'+ph]['count']/df.dropna(subset=['cur'+ph]).describe()['cur'+ph]['count'],3)
        df['% of time'+ph] = dtPerc
        tmp = df.loc[df['pwrAlarm'+ph]==1,['apwr'+ph,'exceed_perc'+ph,'% of time'+ph,'events'+ph]].copy()
        if not tmp.empty:
            tmp['Transformer'] = label
            tmp.to_excel(device+'_'+ph+'.xlsx', index=False)
    return 
        


def get_dev_info(device, address):
    
    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()
    
    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']
    
    # get devid by serial name
    r1 = requests.get(
        url=address + "/api/tenant/devices?deviceName=" + device,
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    label = r1['label']
    devid = r1['id']['id']
    r1 = requests.get(
        url=address + "/api/device/" + devid + "/credentials",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    devtoken = r1['credentialsId']

    
    return devid,acc_token,label
    
    

def get_attr(acc_token,devid,address):
    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/attributes?keys=nominal",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    nominal = r2[0]['value']

    return nominal


            

def read_data(acc_token, devid, address, start_time, end_time, descriptors):

        
    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys=" + descriptors + "&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    if r2:
        df = pd.DataFrame([])
        
        for desc in r2.keys():
            df1 = pd.DataFrame(r2[desc])
            df1.set_index('ts', inplace=True)
            df1.columns = [str(desc)]
            
            df1.reset_index(drop=False, inplace=True)
            df1['ts'] = pd.to_datetime(df1['ts'], unit='ms')
            df1['ts'] = df1['ts'].dt.tz_localize('utc').dt.tz_convert('Europe/Athens')
            df1 = df1.sort_values(by=['ts'])
            df1.reset_index(drop=True, inplace=True)
            df1.set_index('ts', inplace=True, drop=True)            
            
            df = pd.concat([df, df1], axis=1)

        if df.empty:
            df = pd.DataFrame([])
        else:
            for col in df.columns:
                df[col] = df[col].astype('float64')
    else:
        df = pd.DataFrame([])
        # print('Empty json!')
    return df


def main(device, start_time, end_time, assetname,devid, acc_token, label, mainpath):
#def main(argv):
    
    
    
    
    interval = 1 # interval in minutes
    descriptors = 'vltA,vltB,vltC,curA,curB,curC'
    address = 'http://localhost:8080'

    
    nominal = get_attr(acc_token,devid,address)
        
    timethres = 12*3600000
    svec = np.arange(int(start_time),int(end_time),timethres)
    
    df = pd.DataFrame([])
    
    for st in svec:
        en = st+timethres-1
        
        if int(end_time)-en<=0: en = int(end_time)
        tmp = read_data(acc_token, devid, address,  str(st), str(en), descriptors)
        if not tmp.empty:
            tmp = tmp.resample(str(interval)+'T').mean()
            # tmp = tmp.dropna()
            df = pd.concat([df,tmp])
    
    del tmp
    df.sort_index(inplace=True)
    
    if not df.empty:
        pwrAlarms(df, nominal, label, device)
        
    
    
    return 
    
  
#if __name__ == '__main__':
 #   sys.exit(main())
    
    
