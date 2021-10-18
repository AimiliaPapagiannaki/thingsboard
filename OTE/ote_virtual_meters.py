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
import json
from dateutil.relativedelta import relativedelta
from dateutil.tz import gettz
import timeit
import pytz





def align_resample(df,interv):
   
    df['ts'] = df.index
    df['ts'] = df['ts'].dt.tz_localize('utc')
    df.set_index('ts',inplace = True, drop = True)

    df = df.resample(interv,label='right').max()
#     df = df.fillna(method="ffill")

    return df



def read_data(devid, acc_token, address, start_time, end_time,descriptors):
    df = pd.DataFrame([])
    
    if int(end_time)-int(start_time)>(30*86400000):
        offset = 30*86400000
    else:
        offset = 86400000
    svec = np.arange(int(start_time), int(end_time), offset) # 1 day
    for st in svec:
        en = st + offset - 1

        if int(end_time) - en <= 0: en = int(end_time)
        tmp = pd.DataFrame([])

        r2 = requests.get(url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys="+descriptors+"&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",
            headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
        if ((len(r2.keys())>0) & (len(descriptors)>0)):


            # read all descriptors at once
            for desc in r2.keys():
                try:
                    df1 = pd.DataFrame(r2[desc])
                    df1.set_index('ts', inplace=True)
                    df1.columns = [str(desc)]
                    tmp = pd.concat([tmp,df1], axis = 1)
                except:
                    continue


            if not tmp.empty:

                tmp.reset_index(drop=False, inplace=True)
                tmp['ts'] = pd.to_datetime(tmp['ts'], unit='ms')

                # Set timestamp as index, convert all columns to float
                tmp = tmp.sort_values(by=['ts'])
                tmp.reset_index(drop=True, inplace=True)
                tmp.set_index('ts',inplace = True, drop = True)
                df = pd.concat([df, tmp])

                      
    if not df.empty:
        for col in df.columns:
            df[col] = df[col].astype('float')
        #df = df.add_suffix('_'+str(incr))
                
        df = align_resample(df, '5T')
    return df




def create_virtual(vrtl,operation,layer):
    
    agg = pd.DataFrame([])
    
    for virtual in vrtl:
        if layer==1:
            print(virtual)
            # request devid
            r1 = requests.get(
                url=address + "/api/tenant/devices?deviceName=" + virtual,
                headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
            devid = r1['id']['id']
        else:
            devid = virtual

        df = read_data(devid, acc_token, address, start_time, end_time,descriptors)
#         print(df.head())
        if not agg.empty:
            if operation == 1:
                agg = agg.add(df)#,fill_value=0)
            else:
                cols = ['cnrgA','cnrgB','cnrgC','pwrA','pwrB','pwrC','curA','curB','curC']
                agg[cols] = agg[cols].sub(df[cols])#, fill_value=0)
                agg[['vltA','vltB','vltC']] = agg[['vltA','vltB','vltC']].add(df[['vltA','vltB','vltC']])
                
        else:
            agg = df
        del df
    for vlt in ['vltA','vltB','vltC']:
        agg[vlt] = agg[vlt]/2
    return agg





def send_data(mydf,devid):
    df = mydf.copy()
    
    r = requests.post(address + "/api/auth/login",json={'username': 'a.andrikopoulos19@meazon.com', 'password': 'andrikopMeazon13'}).json()
    acc_token = 'Bearer' + ' ' + r['token']

    r1 = requests.get(
        url=address + "/api/device/" + devid + "/credentials",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    devtoken = r1['credentialsId']
#     print('devtoken:',devtoken)
    
    df['ts'] = df.index
    df['ts'] = df.apply(lambda row: int(row['ts'].timestamp()) * 1000, axis=1)

    df.set_index('ts', inplace=True, drop=True)

    mydict = df.to_dict('index')

    for key, value in mydict.items():
        my_json = json.dumps({'ts': key, 'values': value})
        r = requests.post(url=address + "/api/v1/" + devtoken + "/telemetry",
                          data=my_json, headers={'Content-Type': 'application/json', 'Accept': '*/*',
                                                 'X-Authorization': acc_token})
    return 
    


address = "http://localhost:8080"

#Klimatismos grafeiwn
vrtl1 = ['102.301.000976','102.301.000968','102.301.000969']

#Fwtismos grafeiwn
vrtl2 = ['102.301.000963','102.301.000869','102.301.000870','102.301.000850','102.301.000865','102.301.000868']

#UPS grafeiwn
vrtl3 = ['102.402.000037','102.402.000028']

#Fortia kinisis (3) grafeiwn
vrtl4 = ['102.301.000970','102.301.000923']

#Synolo paroxwn
vrtl5 = ['102.402.000039','102.301.000921']

#Synolo statheris
vrtl6 = ['102.301.000923','102.301.000916','102.301.000935','102.301.000978']

#Klimatismos kinitis (-)
vrtl7 = ['102.402.000031','102.402.000035']


# devids for virtual meters
vdevids = ['e8652270-19e5-11ec-ab8f-ef1ea7f487fc','0445f9b0-19e6-11ec-ab8f-ef1ea7f487fc','0c1aacd0-19e6-11ec-ab8f-ef1ea7f487fc','1415aca0-19e6-11ec-ab8f-ef1ea7f487fc','69dd5c50-19e6-11ec-ab8f-ef1ea7f487fc','7df3e0b0-19e6-11ec-ab8f-ef1ea7f487fc','486cdff0-19e6-11ec-ab8f-ef1ea7f487fc']


virtualMeters=[vrtl1]+[vrtl2]+[vrtl3]+[vrtl4]+[vrtl5]+[vrtl6]+[vrtl7]

# define descriptors and request token
descriptors = 'cnrgA,cnrgB,cnrgC,pwrA,pwrB,pwrC,curA,curB,curC,vltA,vltB,vltC'
r = requests.post(address + "/api/auth/login",json={'username': 'a.andrikopoulos19@meazon.com', 'password': 'andrikopMeazon13'}).json()
acc_token = 'Bearer' + ' ' + r['token']
    
start_time = '1577829600000'
end_time = '1633035600000'


for i in range(0,len(virtualMeters)):
#for i in range(5,7):
    if i==len(virtualMeters)-1:
        operation=0 # sub
    else:
        operation=1 #add
    agg = create_virtual(virtualMeters[i],operation,layer=0)
    agg = agg.dropna()
    
    # convert cnrg to integer
    for cnrg in ['cnrgA','cnrgB','cnrgC']:
        agg[cnrg] = agg[cnrg].astype(int)
        
    send_data(agg,vdevids[i])
    print('Completed ',i)

# end of layer 1
################################################################

# Athroisma grafeiwn (den grafetai)
athr_grafeiwn = ['e8652270-19e5-11ec-ab8f-ef1ea7f487fc','0445f9b0-19e6-11ec-ab8f-ef1ea7f487fc','0c1aacd0-19e6-11ec-ab8f-ef1ea7f487fc','1415aca0-19e6-11ec-ab8f-ef1ea7f487fc','813f3ce0-1c2e-11ea-8762-6bf954fc5af1']
operation=1

agg1 = create_virtual(athr_grafeiwn,operation,layer=2)
agg1 = agg1.dropna()
    
for cnrg in ['cnrgA','cnrgB','cnrgC']:
    agg1[cnrg] = agg1[cnrg].astype(int)


# Synolo kinitis statheris
synolo_kinstath = ['69dd5c50-19e6-11ec-ab8f-ef1ea7f487fc','7df3e0b0-19e6-11ec-ab8f-ef1ea7f487fc','9997b670-942c-11ea-8c14-6192a0efe6e6']
agg2 = create_virtual(synolo_kinstath,operation,layer=2)
agg2 = agg2.dropna()
    
for cnrg in ['cnrgA','cnrgB','cnrgC']:
    agg2[cnrg] = agg2[cnrg].astype(int)
    

# Ypoloipa fortia xrisi fortiwn
# 102.216.000654 - athroisma grafeiwn - synolo kinitis statheris
devid = 'da260200-57e6-11ea-8762-6bf954fc5af1'
df = read_data(devid, acc_token, address, start_time, end_time,descriptors)
df = df.dropna()
for cnrg in ['cnrgA','cnrgB','cnrgC']:
    df[cnrg] = df[cnrg].astype(int)

agg = pd.DataFrame([])
agg = df.copy()
cols = ['cnrgA','cnrgB','cnrgC','pwrA','pwrB','pwrC','curA','curB','curC']
agg[cols] = agg[cols].sub(agg1[cols])
agg[['vltA','vltB','vltC']] = agg[['vltA','vltB','vltC']].add(agg1[['vltA','vltB','vltC']])
agg[cols] = agg[cols].sub(agg2[cols])
agg[['vltA','vltB','vltC']] = agg[['vltA','vltB','vltC']].add(agg2[['vltA','vltB','vltC']])

for vlt in ['vltA','vltB','vltC']:
        agg[vlt] = agg[vlt]/2

agg.dropna(inplace=True)
del agg2
send_data(agg,'21c14710-19e6-11ec-ab8f-ef1ea7f487fc')

# end of layer 2
################################################################
#Synolo grafeiwn

agg = agg.add(agg1)
for vlt in ['vltA','vltB','vltC']:
        agg[vlt] = agg[vlt]/2

del agg1
agg.dropna(inplace=True)
send_data(agg,'288d15b0-19e6-11ec-ab8f-ef1ea7f487fc')
    
