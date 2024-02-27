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


def calc_nrg(df,kpis):
    df['totalnrg'] = df['cnrgA']+df['cnrgB']+df['cnrgC']
    df = df[['totalnrg']]
    df = df.dropna()
    totalnrg = df['totalnrg'].max()-df['totalnrg'].min()
    
    kpis['Total energy consumption (MWh)'] = np.round(totalnrg/1e6,2)
    print('Energy consumption MWh',np.round(totalnrg/1e6,2))
    return kpis


def vimbalance(df,kpis,isdist):

    if not isdist:
        tmp = df.copy()
        tmp['totalVlt'] = (tmp['vltA']+tmp['vltB']+tmp['vltC'])/3
        tmp['difA'] = np.abs(tmp['vltA']-tmp['totalVlt'])
        tmp['difB'] = np.abs(tmp['vltB']-tmp['totalVlt'])
        tmp['difC'] = np.abs(tmp['vltC']-tmp['totalVlt'])
        tmp['imbalance'] = (tmp[['difA', 'difB', 'difC']].max(axis=1)/tmp['totalVlt'])*100
        
        
        
        tmp['vimb_alarm'] = np.nan
        tmp.loc[tmp['imbalance']>2,'vimb_alarm'] = 1
        # create groups
        
        ind=0
        
        for i in range(1,tmp.shape[0]):
            if ((tmp['vimb_alarm'].iloc[i]==1) & (tmp['vimb_alarm'].iloc[i-1]!=1)):
                ind+=1
                tmp['vimb_alarm'].iloc[i]=ind
            elif((tmp['vimb_alarm'].iloc[i]==1) & (tmp['vimb_alarm'].iloc[i-1]==1)):
                tmp['vimb_alarm'].iloc[i]=ind


        # compute time and number of events per phase
        dtPerc = np.round(100*(tmp.describe()['vimb_alarm']['count']/tmp.describe()['vltA']['count']),2)
        
        # number of distinct events/alarms
        events = tmp.groupby('vimb_alarm').ngroups
        
        kpis['Nr. of Voltage unbalance alarms'] = events
        
        kpis['% of time of Voltage unbalance alarms'] = dtPerc
    
    return kpis
    
    

def cimbalance(df,kpis,nomcur):

    
    tmp = df.copy()
    tmp['totalcur'] = tmp['curA']+tmp['curB']+tmp['curC']
    tmp['meancur'] = tmp['totalcur']/3
    tmp['devA'] = np.abs(tmp['curA']-tmp['meancur'])
    tmp['devB'] = np.abs(tmp['curB']-tmp['meancur'])
    tmp['devC'] = np.abs(tmp['curC']-tmp['meancur'])
    tmp['curunb'] = 100*tmp[['devA','devB','devC']].max(axis=1)/tmp['meancur']
    tmp = tmp.loc[tmp['totalcur']>=nomcur]
    
    
    
    tmp['cimb_alarm'] = np.nan
    tmp.loc[tmp['curunb']>35,'cimb_alarm'] = 1
    # create groups
    
    ind=0
    
    for i in range(1,tmp.shape[0]):
        if ((tmp['cimb_alarm'].iloc[i]==1) & (tmp['cimb_alarm'].iloc[i-1]!=1)):
            ind+=1
            tmp['cimb_alarm'].iloc[i]=ind
        elif((tmp['cimb_alarm'].iloc[i]==1) & (tmp['cimb_alarm'].iloc[i-1]==1)):
            tmp['cimb_alarm'].iloc[i]=ind


    # compute time and number of events per phase
    dtPerc = np.round(100*(tmp.describe()['cimb_alarm']['count']/tmp.describe()['curA']['count']),2)
    
    # number of distinct events/alarms
    events = tmp.groupby('cimb_alarm').ngroups
    
    kpis['Nr. of Current unbalance alarms'] = events
    
    kpis['% of time of Current unbalance alarms'] = dtPerc
    
    return kpis


def curAlarms(df, kpis,isdist):
    dictph = {}
    dictph['A'] = 'L1'
    dictph['B'] = 'L2'
    dictph['C'] = 'L3'
    
    if isdist:
        nom = 200
        # Find fuse alarms
        for ph in ['A','B','C']:
            df['fuseAlarm'+ph] = np.nan
            df.loc[df['cur'+ph]>nom, 'fuseAlarm'+ph] = 1
    
    
        # create groups
        for ph in ['A','B','C']:
            ind=0
            df['alarm'+ph] = np.nan
            for i in range(1,df.shape[0]):
                if ((df['fuseAlarm'+ph].iloc[i]==1) & (df['fuseAlarm'+ph].iloc[i-1]!=1)):
                    ind+=1
                    df['alarm'+ph].iloc[i]=ind
                elif((df['fuseAlarm'+ph].iloc[i]==1) & (df['fuseAlarm'+ph].iloc[i-1]==1)):
                    df['alarm'+ph].iloc[i]=ind


            # compute time and number of events per phase
            #dtPerc = np.round(100*(df.describe()['alarm'+ph]['count']/df.describe()['cur'+ph]['count']),2)
            
            # number of distinct events/alarms
            events = df.groupby('alarm'+ph).ngroups
            mintime = df.groupby('alarm'+ph).size().min()
            maxtime = df.groupby('alarm'+ph).size().max()
            
            
            kpis['Nr. of overcurrent alarms '+dictph[ph]] = events 
            kpis['Min duration of overcurrent alarms '+dictph[ph]+' (min)'] = mintime
            kpis['Max duration of overcurrent alarms '+dictph[ph]+' (min)'] = maxtime
    else:
        for ph in ['A','B','C']:
            kpis['Nr. of overcurrent alarms '+dictph[ph]] = '' 
            kpis['Min duration of overcurrent alarms '+dictph[ph]+' (min)'] = ''
            kpis['Max duration of overcurrent alarms '+dictph[ph]+' (min)'] = ''
            
    return kpis
            
            
            
        

def pwrAlarms(df, nominal, kpis,isdist):
    dictph = {}
    dictph['A'] = 'L1'
    dictph['B'] = 'L2'
    dictph['C'] = 'L3'
    if not isdist:
        # calculate new variables
        for ph in ['A','B','C']:
            df['apwr'+ph] = df['vlt'+ph]*df['cur'+ph]
    
        df['totalcur'] = df['curA']+df['curB']+df['curC']
        nomphase = 1000*np.round(nominal/3,2)
    
    
        # Find power alarms
        for ph in ['A','B','C']:
            df['pwrAlarm'+ph] = np.nan
            df.loc[df['apwr'+ph]>0.8*nomphase, 'pwrAlarm'+ph] = 1
    
            # create groups
            ind=0
            df['alarm'+ph] = np.nan
            for i in range(1,df.shape[0]):
                if ((df['pwrAlarm'+ph].iloc[i]==1) & (df['pwrAlarm'+ph].iloc[i-1]!=1)):
                    ind+=1
                    df['alarm'+ph].iloc[i]=ind
                elif((df['pwrAlarm'+ph].iloc[i]==1) & (df['pwrAlarm'+ph].iloc[i-1]==1)):
                    df['alarm'+ph].iloc[i]=ind
            
            
            
            # compute time and number of events per phase
            #dtPerc = np.round(100*(df.describe()['alarm'+ph]['count']/df.describe()['cur'+ph]['count']),2)
            
            # number of distinct events/alarms
            events = df.groupby('alarm'+ph).ngroups
            mintime = df.groupby('alarm'+ph).size().min()
            maxtime = df.groupby('alarm'+ph).size().max()
            
            
            
            
            kpis['Nr. of power alarms '+dictph[ph]] = events 
            kpis['Min duration of power alarms '+dictph[ph]+' (min)'] = mintime
            kpis['Max duration of power alarms '+dictph[ph]+' (min)'] = maxtime
            
    else:
        for ph in ['A','B','C']:
            kpis['Nr. of power alarms '+dictph[ph]] = '' 
            kpis['Min duration of power alarms '+dictph[ph]+' (min)'] = ''
            kpis['Max duration of power alarms '+dictph[ph]+' (min)'] = ''
#    kpis.rename(columns = {'npwrA':'Nr. of power alarms L1', 'npwrB':'Nr. of power alarms L2', 'npwrC':'Nr. of power alarms L3','timeA':'% of time of power alarms L1','timeB':'% of time of power alarms L2','timeC':'% of time of power alarms L3'}, inplace=True)
    
    return kpis
        



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
   #print('DIPS:',alarms.loc[(alarms['alarm_id']>=2) & (alarms['alarm_id']<=4)]) 
   ndips = alarms.loc[(alarms['alarm_id']>=2) & (alarms['alarm_id']<=4),'alarm_id'].count()
   if not alarms.loc[(alarms['alarm_id']>=2) & (alarms['alarm_id']<=4),'alarm_duration'].empty:
       timedips = alarms.loc[(alarms['alarm_id']>=2) & (alarms['alarm_id']<=4),'alarm_duration'].mean()
   else:
       timedips = 0
   
   nswells = alarms.loc[(alarms['alarm_id']>=5) & (alarms['alarm_id']<=7),'alarm_id'].count()
   if not alarms.loc[(alarms['alarm_id']>=5) & (alarms['alarm_id']<=7),'alarm_duration'].empty:
       timeswells = alarms.loc[(alarms['alarm_id']>=5) & (alarms['alarm_id']<=7),'alarm_duration'].mean()
   else:
       timeswells = 0
   
   return ndips,nswells,timedips,timeswells
   




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
    descriptors = 'vltA,vltB,vltC,curA,curB,curC,cnrgA,cnrgB,cnrgC'
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
    
    kpis = calc_nrg(df,kpis)
    
    kpis = pwrAlarms(df, nominal, kpis,isdist)
    kpis = curAlarms(df, kpis,isdist)
    kpis = vimbalance(df,kpis,isdist)
    kpis = cimbalance(df,kpis,nomcur)
    if not df.empty:
        #make a copy for power fail check later
        pfail = df[['vltA','vltB','vltC']].copy()

        df = df.reset_index()

    

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
        alarms = alarms.drop('alarm_status', axis=1)
        alarms = alarms[(alarms['alarm_id']>1) & (alarms['alarm_id']<8)]

        [ndips,nswells,timedips,timeswells] = countAlarms(alarms)
    else:
        nr_fails = 0
        min_pfails = 0
        max_pfails = 0
        ndips = 0
        nswells = 0
        timedips = 0
        timeswells = 0
    
    kpis['Nr. of Power Fails (outage)'] = nr_fails
    kpis['Min time of Power Fails (sec)'] = min_pfails
    kpis['Max time of Power Fails (sec)'] = max_pfails
    kpis['Nr. of Voltage dips'] = ndips
    kpis['Avg time of Voltage dips (msec)'] = timedips
    kpis['Nr. of Voltage swells'] = nswells
    kpis['Avg time of Voltage swells (msec)'] = timeswells
  
     
    del df,alarms
    
    return kpis
    
       
    
    
        
       

    
#if __name__ == '__main__':
#    sys.exit(main(sys.argv))
    
    
