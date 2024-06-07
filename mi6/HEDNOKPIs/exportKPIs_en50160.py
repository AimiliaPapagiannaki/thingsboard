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
from fpdf import FPDF
import matplotlib.pyplot as plt
plt.rcParams.update({'font.size': 14})
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

import warnings
warnings.filterwarnings("ignore")



def get_lat_lon(devid, acc_token, address):

    r2 = requests.get(url=address + "/api/plugins/telemetry/DEVICE/"+devid+"/values/attributes",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    key_to_find = 'key'
    value_to_find1 = 'latitude'
    value_to_find2 = 'longitude'

    # Iterate through the list of dictionaries
    for dictionary in r2:
        if dictionary.get(key_to_find) == value_to_find1:
            lat = dictionary['value']
        if dictionary.get(key_to_find) == value_to_find2:
            lon = dictionary['value']
            
    return lat,lon


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





def countAlarms(alarms):
   
   
   ndips = alarms.loc[(alarms['alarm_id']>=2) & (alarms['alarm_id']<=4) & (alarms['alarm_duration']>0),'alarm_id'].count()
   print('NDIPS:',ndips)
   nswells = alarms.loc[(alarms['alarm_id']>=5) & (alarms['alarm_id']<=7)  & (alarms['alarm_duration']>0),'alarm_id'].count()
    
   nfreqover = alarms.loc[(alarms['alarm_id']>=14) & (alarms['alarm_id']<=16) & (alarms['alarm_duration']>0),'alarm_id'].count()
   nfrequnder = alarms.loc[(alarms['alarm_id']>=17) & (alarms['alarm_id']<=19)  & (alarms['alarm_duration']>0),'alarm_id'].count()
    
   
   return ndips,nswells,nfreqover,nfrequnder
   




def countpfails(pfail,alarms):
    
    tmp = alarms.loc[((alarms['alarm_id']==1) & (alarms['alarm_status']>1))].copy()
    tmp = tmp.drop_duplicates(subset='alarm_time', keep='first')
    
    
    tmp2 = alarms.loc[((alarms['alarm_id']==1) & (alarms['alarm_status']>1) & (alarms['alarm_duration']>0))].copy()
    tmp2 = tmp2.drop_duplicates(subset='alarm_time', keep='first')
    tmp2.rename(columns={'alarm_duration':'timedif'}, inplace=True)
    
    
    if not tmp.empty:
        #print('alarms not empty')
        pfail['allvlt'] = np.nan
        pfail.loc[(pfail['vltA'].isna()==0) | (pfail['vltB'].isna()==0) | (pfail['vltC'].isna()==0),'allvlt'] = 1
        pfail = pfail[['allvlt']]
        
        pfail = pfail.dropna()
        
        
        pfail = pd.concat([pfail,tmp[['alarm_id']]],axis=1)
        pfail = pfail.sort_index()
        
        pfail['timedif'] = pfail.index.to_series().diff().astype('timedelta64[ms]')

        fails = pfail.loc[pfail['alarm_id'].shift()==1,['timedif']]
        if not tmp2.empty:
            fails = pd.concat([fails, tmp2[['timedif']]])
        
        # number of fails
        nr_fails = fails.shape[0]
        min_pfails = (fails['timedif']/1000).min()
        max_pfails = (fails['timedif']/1000).max()
        
        
        
    else:
        #print('alarms empty')
        nr_fails = 0 
        min_pfails = 0 
        max_pfails = 0
    
    return nr_fails,min_pfails,max_pfails
        
        
        



def main(device, start_time, end_time, assetname,devid, acc_token, label,kpis,isdist):
#def main(argv):

    interval = 1 # interval in minutes
    descriptors = 'vltA,vltB,vltC,frqA,frqB,frqC,vthdA,vthdB,vthdC,ithdA,ithdB,ithdC'
    address = 'http://localhost:8080'

    
    if not isdist:
        nominal = get_attr(acc_token,devid,address)
        nomcur = np.round((nominal*1000/230)/2,2)
    else:
        nominal = ''
        nomcur = 60
        
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
        #make a copy for power fail check later
        pfail = df[['vltA','vltB','vltC']].copy()

        

    
        df['totalVlt'] = (df['vltA']+df['vltB']+df['vltC'])/3
        df['totalVthd'] = (df['vthdA']+df['vthdB']+df['vthdC'])/3
        df['totalIthd'] = (df['ithdA']+df['ithdB']+df['ithdC'])/3
        df['deviation'] = ((df['totalVlt']-230)/230)*100

        df['difA'] = np.abs(df['vltA']-df['totalVlt'])
        df['difB'] = np.abs(df['vltB']-df['totalVlt'])
        df['difC'] = np.abs(df['vltC']-df['totalVlt'])
        df['imbalance'] = (df[['difA', 'difB', 'difC']].max(axis=1)/df['totalVlt'])*100
        if 'frqA' in df.columns:
            frqflag=1
            dfreq = df[['frqA','frqB','frqC']].copy()
            
            # plot min max frequencies
            df = df.drop(['frqA','frqB','frqC'],axis=1)
            dfreq['efrq'] = (dfreq['frqA']+dfreq['frqB']+dfreq['frqC'])/3
            dfreq['frqdev'] = ((dfreq['efrq']-50)/50)*100
            
            # dfreq = dfreq.dropna()
            
        else:
            frqflag=0
        
        df = df.resample('10T').mean()
        df.drop(['difA','difB','difC'], axis=1, inplace=True)

        dev95 = np.round(np.abs(df['deviation']).quantile(0.95),3)
        dev100pos = np.round(df['deviation'].max(),3)
        dev100neg = np.round(df['deviation'].min(),3) if df['deviation'].min()<0 else 0

        vimb95 = np.round(df['imbalance'].quantile(.95),3)
        
        vthd95 = np.round(df['totalVthd'].quantile(.95),3)
        vthd100 = np.round(df['totalVthd'].max(),3)
        
        ithd100 = np.round(df['totalIthd'].max(),3)
    
        if frqflag==1:
            frqdev995 = np.round(dfreq['frqdev'].quantile(0.995),3)
            frqdev100pos = np.round(dfreq['frqdev'].max(),3)
            frqdev100neg = np.round(dfreq['frqdev'].min(),3) if dfreq['frqdev'].min()<0 else 0

        kpis['Max voltage deviation (95% of 10min intervals)'] = dev95
        kpis['Max positive voltage deviation (100% of 10min intervals)'] = dev100pos
        kpis['Max negative voltage deviation (100% of 10min intervals)'] = dev100neg
        kpis['Max voltage imbalance (95% of 10min intervals)'] = vimb95
        kpis['Max voltage THD (95% of 10min intervals)'] = vthd95
        kpis['Max voltage THD (100% of 10min intervals)'] = vthd100
        kpis['Max current THD (100% of 10min intervals)'] = ithd100
        
    else:
        kpis['Max voltage deviation (95% of 10min intervals)'] = '-'
        kpis['Max positive voltage deviation (100% of 10min intervals)'] = '-'
        kpis['Max negative voltage deviation (100% of 10min intervals)'] = '-'
        kpis['Max voltage imbalance (95% of 10min intervals)'] = '-'
        kpis['Max voltage THD (95% of 10min intervals)'] = '-'
        kpis['Max voltage THD (100% of 10min intervals)'] = '-' 
        kpis['Max current THD (100% of 10min intervals)'] = '-'
        
        frqflag=0
    
    if frqflag==1:
        kpis['Max frequency deviation (99.5% of 10min intervals)'] = frqdev995
        kpis['Max positive frequency deviation (100% of 10min intervals)'] = frqdev100pos
        kpis['Max negative frequency deviation (100% of 10min intervals)'] = frqdev100neg
    else:
        kpis['Max frequency deviation (99.5% of 10min intervals)'] = '-'
        kpis['Max positive frequency deviation (100% of 10min intervals)'] = '-'
        kpis['Max negative frequency deviation (100% of 10min intervals)'] = '-'

    # Alarms
    alarmDesc = 'alarm_time,alarm_id,alarm_duration,alarm_value,alarm_status'
    alarms = read_data(acc_token, devid, address,  start_time, end_time, alarmDesc)
    
    if not alarms.empty:
        alarms = alarms.sort_index()
        
        alarms['alarm_id'] = alarms['alarm_id'].astype(int)
        alarms['alarm_time'] = pd.to_datetime(alarms['alarm_time'], unit='ms')
        alarms['alarm_time'] = alarms['alarm_time'].dt.tz_localize('utc').dt.tz_convert('Europe/Athens')
        
        
        # number of power fails, aka outage
        [nr_fails,min_pfails,max_pfails] = countpfails(pfail,alarms)
        
        
        # number of dips/swells        
        #alarms = alarms.drop('alarm_status', axis=1)
        alarms = alarms[(alarms['alarm_id']>1) & (alarms['alarm_id']<8)]

        [ndips,nswells,nfreqover,nfrequnder] = countAlarms(alarms)
    else:
        nr_fails = 0
        ndips = 0
        nswells = 0
        nfreqover = 0
        nfrequnder = 0

    
    kpis['Nr. of Power Fails (outage)'] = nr_fails
    kpis['Nr. of Voltage dips'] = ndips
    kpis['Nr. of Voltage swells'] = nswells
    kpis['Occurences of Frequency over limit'] = nfreqover
    kpis['Occurences of Frequency under limit'] = nfrequnder  
     
    del df,alarms
    
    return kpis
    
       
    
    
        
       

    
#if __name__ == '__main__':
#    sys.exit(main(sys.argv))
    
    
