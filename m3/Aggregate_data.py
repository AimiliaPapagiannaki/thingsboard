#!/usr/bin/env python3

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
# from datetime import datetime

#address = "https://m3.meazon.com"
address = "http://localhost:8080"



def align_resample(df, cnrg,pwr,frun):
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y/%m/%d %H:%M:%S.%f')
    #df['Timestamp'] = df['Timestamp'].dt.tz_localize('utc')
    df = df.sort_values(by="Timestamp")

    df['Timestamp'] = df['Timestamp'].astype('datetime64[s]')
    df = df.set_index('Timestamp', drop=True)

    for col in df.columns:
        df[col] = df[col].astype(float)

    # identify report interval and round to closest minute
    #tmpdf = pd.DataFrame(df[df.columns[0]].dropna())
    #tmpdf['minutes'] = tmpdf.index.minute
    #tmpdf['interv'] = tmpdf['minutes'].shift(-1) - tmpdf['minutes']
    #interval = int(tmpdf['interv'].value_counts().idxmax())
    #del tmpdf
    df.index = df.index.map(lambda x: x.replace(second=0))

    df = df.groupby(df.index).max()
    df.sort_index(inplace=True)

    # if a spike is detected, assign previous values
    for nrg in cnrg:
        df.loc[(df[nrg].shift(-1)-df[nrg])<0,nrg] = df[nrg].shift()
    
    if frun==0:
        tmp = pd.DataFrame(df).copy()
        tmp['totalCnrg'] = np.zeros(tmp.shape[0])
        for nrg in cnrg:
            tmp['totalCnrg'] = tmp['totalCnrg'] + tmp[nrg]
        #print('Total nrg for this device is:',np.max(tmp['totalCnrg']))
        dif = np.max(tmp['totalCnrg']) - np.min(tmp['totalCnrg'])
        if np.isnan(dif):dif=0
        del tmp
    
    df_demand = df.resample('15T',label = 'right').max().copy()
    df_demand['totalMaxpwr'] = np.zeros(df_demand.shape[0])
    df_demand['totalCnrg'] = np.zeros(df_demand.shape[0])
    
    for nrg in cnrg:
        df_demand['totalCnrg'] = df_demand['totalCnrg'] + df_demand[nrg]
        
    ######### check if there are nan values in cnrg
    if frun==1:
    
        for i in range(1,df_demand.shape[0]):
            if np.isnan(df_demand['totalCnrg'].iloc[i]):
                df_demand['totalCnrg'].iloc[i] = df_demand['totalCnrg'].iloc[i-1]
                #print('Found nans, new value is:',df_demand['totalCnrg'].iloc[i])
    else:
        df_demand['totalCnrg'] = dif
    
        

    # calculate mean and max power
    for pr in pwr:
        df_demand['totalMaxpwr'] = df_demand['totalMaxpwr'] + df_demand[pr]

    df_demand = df_demand[['totalMaxpwr','totalCnrg']]

    df = df.resample('15T',label = 'right').mean()
    df['totalMeanpwr'] = np.zeros(df_demand.shape[0])
    for pr in pwr:
        df['totalMeanpwr'] = df['totalMeanpwr'] + df[pr]


    # df.reset_index(inplace=True, drop=False)
    # df.set_index('ts', inplace=True, drop=False)
    df = pd.concat([df, df_demand], axis=1)
    df = df[['totalCnrg', 'totalMaxpwr', 'totalMeanpwr']]
######    print('df after concat:',df.head())

    return df


def download_nrg(start_date, end_date, devid,acc_token):
    global address

    start_time = str(start_date)
    end_time = str(end_date)

    # make request to detect connection type of device
    r1 = requests.get(url=address + '/api/plugins/telemetry/DEVICE/'+devid+'/values/attributes/SERVER_SCOPE',
                      headers = {'Content-Type': 'application/json', 'Accept': '*/*',
                                                     'X-Authorization': acc_token}).json()
    # define regex to read at which Lines is the total consumption set

    tmp = next(item for item in r1 if item["key"] == "connectionType")
   
    conn_info = json.loads(tmp['value'])
    
    descr = []
    cnrg = []
    pwr = []
    
    if len(conn_info)>0:
        if conn_info['type']=='TPMSP':
    
            if 'A' in conn_info['mainLine']:
                descr = descr + ['cnrgA','pwrA']
                cnrg =  cnrg + ['cnrgA']
                pwr =  pwr + ['pwrA']
            if 'B' in conn_info['mainLine']:
                descr = descr + ['cnrgB', 'pwrB']
                cnrg = cnrg + ['cnrgB']
                pwr = pwr + ['pwrB']
            if 'C' in conn_info['mainLine']:
                descr = descr + ['cnrgC', 'pwrC']
                cnrg = cnrg + ['cnrgC']
                pwr = pwr + ['pwrC']
    
        elif conn_info['type'] == 'TWPM':
            if 'A' in conn_info['mainLine']:
                descr = descr + ['cnrgA','pwrA']
                cnrg =  cnrg + ['cnrgA']
                pwr =  pwr + ['pwrA']
            if 'B' in conn_info['mainLine']:
                descr = descr + ['cnrgB', 'pwrB']
                cnrg = cnrg + ['cnrgB']
                pwr = pwr + ['pwrB']
            if 'C' in conn_info['mainLine']:
                descr = descr + ['cnrgC', 'pwrC']
                cnrg = cnrg + ['cnrgC']
                pwr = pwr + ['pwrC']
    
        elif conn_info['type'] == 'SPM':
            descr = descr + ['cnrgA', 'pwrA']
            cnrg = cnrg + ['cnrgA']
            pwr = pwr + ['pwrA']
    
        elif conn_info['type'] == 'TPMSPP' :
            descr = descr + ['cnrgA','cnrgB','cnrgC', 'pwrA','pwrB','pwrC']
            cnrg = cnrg + ['cnrgA','cnrgB','cnrgC']
            pwr = pwr + ['pwrA','pwrB','pwrC']
    
        descr = ','.join(descr)
    print('descriptors:',descr)
    #r = requests.post(address + "/api/auth/login",
    #                  json={'username': 'meazon@thingsboard.org', 'password': 'meazon'}).json()

    # acc_token is the token to be used in the next request
    #acc_token = 'Bearer' + ' ' + r['token']

    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + str(devid) + "/values/timeseries?keys="+str(descr)+"&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()

    if len(descr)>0 and r2:
        df = pd.DataFrame([])

        if 'pwrA' in r2.keys():
            df1 = pd.DataFrame(r2['pwrA'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['pwrA']
            df = pd.concat([df, df1], axis=1, sort=True)

        if 'pwrB' in r2.keys():
            df2 = pd.DataFrame(r2['pwrB'])
            df2.set_index('ts', inplace=True)
            df2.columns = ['pwrB']
            df = pd.concat([df, df2], axis=1, sort=True)

        if 'pwrC' in r2.keys():
            df3 = pd.DataFrame(r2['pwrC'])
            df3.set_index('ts', inplace=True)
            df3.columns = ['pwrC']
            df = pd.concat([df, df3], axis=1, sort=True)

        if 'cnrgA' in r2.keys():
            df4 = pd.DataFrame(r2['cnrgA'])
            df4.set_index('ts', inplace=True)
            df4.columns = ['cnrgA']
            df = pd.concat([df, df4], axis=1, sort=True)

        if 'cnrgB' in r2.keys():
            df5 = pd.DataFrame(r2['cnrgB'])
            df5.set_index('ts', inplace=True)
            df5.columns = ['cnrgB']
            df = pd.concat([df, df5], axis=1, sort=True)

        if 'cnrgC' in r2.keys():
            df6 = pd.DataFrame(r2['cnrgC'])
            df6.set_index('ts', inplace=True)
            df6.columns = ['cnrgC']
            df = pd.concat([df, df6], axis=1, sort=True)

        df.reset_index(drop=False, inplace=True)
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')

        df = df.sort_values(by=['ts'])
        df.reset_index(drop=True, inplace=True)
        df = df.rename(columns={"ts": "Timestamp"})

    else:
        df = pd.DataFrame([])
        print('Empty json!')

    return [df,cnrg,pwr]



def get_energy_data(start_date, end_date, devid,acc_token,frun):

    if frun==1:
        energy = pd.DataFrame([])
        svec = np.arange(int(start_date),int(end_date),2592000000)
        for st in svec:
            en = st+2592000000-1
            
            if int(end_date)-en<=0: en = int(end_date)
            print('start and end of iteration:',st,en)
            [tmp,cnrg,pwr] = download_nrg(str(st), str(en),devid,acc_token)
            energy = pd.concat([energy,tmp],axis=0)
    else:
        [energy,cnrg,pwr] = download_nrg(start_date, end_date, devid,acc_token)

    if energy.empty == True:
        energy = pd.DataFrame([])
        
        return energy
    else:
        energy = align_resample(energy,cnrg,pwr,frun)
        

        # energy['Timestamp'] = energy['Timestamp'].dt.tz_localize('utc')
        # energy.set_index('Timestamp', inplace=True, drop=False)

        print('Energy df has successfully been created')
        return energy

def transform_df(df):
    import datetime

    df['totalCnrg'] = df['totalCnrg'].apply(str)
    df['totalMaxpwr'] = df['totalMaxpwr'].apply(str)
    df['totalMeanpwr'] = df['totalMeanpwr'].apply(str)

    df['ts'] = df.index.values.astype(np.int64) // 10 ** 6

    # df['ts'] = df['ts'].astype(int)
    df.columns = ['totalCnrg','totalMaxpwr','totalMeanpwr','ts']
    df.set_index('ts',inplace = True, drop = True)
    mydict = df.to_dict('index')

    return mydict

def thingsboard_http(acc_token, dev_token, mydict):
    # address = "https://m3.meazon.com"

    global address


    for key, value in mydict.items():
        # time.sleep(0.015)
        my_json = json.dumps({'ts': key, 'values': value})
        #print(my_json)
        r = requests.post(url=address + "/api/v1/" + dev_token + "/telemetry",
                           data=my_json, headers={'Content-Type': 'application/json', 'Accept': '*/*',
                                                  'X-Authorization': acc_token})
        #print('finished descriptor..')

    print('Finished writing descriptors')

    return

def main():
    startt = time.time()

    # input arguments -->DEH assetid
    entityID =  str('a457a6e0-676f-11ea-9788-2bd444f36b4e')
    DevToken = 'cuCNBRaiJ04PQtsKWVNv'

    if path.exists("/home/iotsm/Analytics/testfiles/test_agg.txt") == False:
        # First run. Create test file
        print('First run')
        frun=1
        os.mknod("/home/iotsm/Analytics/testfiles/test_agg.txt")
        # end_ = (datetime.datetime.now())
        end_ = datetime.datetime.utcnow()
        
        end_ = end_ - datetime.timedelta(seconds=end_.second%60,
                                          microseconds=end_.microsecond)
        print('Running script at time:', end_)

        # start from 1/4/2020 12:00 AM
        start_time = int(1585688400000)
        end_time = int(end_.replace(tzinfo=pytz.utc).timestamp()) * 1000
        

    else:
        print('\n\n\n')
        print('No first run')
        frun = 0
        end_ = (datetime.datetime.utcnow())
        end_ = end_ - datetime.timedelta(seconds=end_.second%60,microseconds=end_.microsecond)
        print('Running script at time:', end_)
        start_ = end_ + relativedelta(minutes=-15)

        end_time = int(end_.replace(tzinfo=pytz.utc).timestamp()) * 1000
        start_time = int(start_.replace(tzinfo=pytz.utc).timestamp()) * 1000



    global address
    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazon@thingsboard.org', 'password': 'meazon'}).json()

    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']


    r1 = requests.get(url=address + '/api/relations/info?fromId=' + entityID + '&fromType=DEVICE',
                      headers={'Content-Type': 'application/json', 'Accept': '*/*',
                               'X-Authorization': acc_token}).json()

    dfglob = pd.DataFrame([])
    for device in r1:

        # read ID and name of building's devices
        devid = str(device['to']['id'])
        devName = str(device['toName'])
        print(devName)

        # Fetch cnrg and cnrgest data
        energy = get_energy_data(start_time, end_time, devid,acc_token,frun)
        dfglob = pd.concat([dfglob, energy]).groupby(level=0).sum()
        
    
    if frun==0:
        oldt =  end_ + relativedelta(days=-9)
        oldtime= int(oldt.timestamp()) * 1000
        r2 = requests.get(url=address + "/api/plugins/telemetry/DEVICE/" + str(entityID) + "/values/timeseries?keys=totalCnrg,totalMeanpwr,totalMaxpwr&startTs=" + str(oldtime) + "&endTs=" + str(end_time) + "&agg=NONE&limit=1000000",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
        
        df = pd.DataFrame([])
            
        # read all descriptors at once
        for desc in r2.keys():
            df1 = pd.DataFrame(r2[desc])
            df1.set_index('ts', inplace=True)
            df1.columns = [str(desc)]
            df = pd.concat([df,df1], axis = 1)
        for col in df.columns:
            df[col] = df[col].astype('float64')
        df.sort_index(inplace = True)
        df = df.iloc[-1:]
        print('Old totalCnrg is:',df['totalCnrg'].values)    
    if dfglob.empty == False:
        
        if frun==0:
            dfglob = dfglob.iloc[-1:]
            dfglob['totalCnrg'] = dfglob['totalCnrg'] + df['totalCnrg'].values
            print('new total cnrg:',dfglob['totalCnrg'])
        print()
        # transform dfs
        mydict = transform_df(dfglob)
    else:
        print('empty df! old variables will be stored')
        mydict = {np.int64(end_time):{'totalCnrg':str(df['totalCnrg'].values[-1]),'totalMeanpwr':str(df['totalMeanpwr'].values[-1]),'totalMaxpwr':str(df['totalMaxpwr'].values[-1])}}
  
    # write values to thingsboard
    thingsboard_http(acc_token, DevToken, mydict)


    elapsed = time.time() - startt
    print("---  seconds ---", elapsed)


if __name__ == "__main__":
    sys.exit(main())





