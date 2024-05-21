#!/usr/bin/env python3 
import sys
import requests
import json
import datetime
import os
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta


def write_df(df, address, acc_token, devtoken):
    for col in df.columns:
        tmp = df[[col]].copy().dropna()
        if not tmp.empty:
            tmp[col] = np.round(tmp[col],2)
            mydict = tmp.to_dict('index')
            l=[]
            for i, (key, value) in enumerate(mydict.items(), 1):
                newdict={}
                newdict['ts'] = key
                # newdict['values'] = value
                newdict['values'] = { k: v for k, v in value.items() if v == v }
                l.append(newdict)
            # write to json and send telemetry to TB
            my_json = json.dumps(l)     
            print(my_json)       
            r = requests.post(url=address+"/api/v1/" + devtoken + "/telemetry",data=my_json, headers={'Content-Type': 'application/json', 'Accept': '*/*',
                                                        'X-Authorization': acc_token})


        
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

    
    return devid,acc_token,label, devtoken
    

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
                      
            if not df1.empty:
                df = pd.concat([df, df1], axis=1)

        if not df.empty:
            for col in df.columns:
                df[col] = df[col].astype('float64')
    else:
        df = pd.DataFrame([])
        # print('Empty json!')
    return df


def detect_alarms(address, start_time, end_time, devid, acc_token, label,devtoken):
    descriptors = 'vltA,vltB,vltC,curA,curB,curC'
    
    nominal = get_attr(acc_token,devid,address)
        
    df = read_data(acc_token, devid, address,  start_time, end_time, descriptors)
    if not df.empty:
        df.sort_index(inplace=True)

         # calculate new variables
        for ph in ['A','B','C']:
            df['apwr'+ph] = df['vlt'+ph]*df['cur'+ph]

        nomphase = 1000*np.round(nominal/3,2)

        # Find power alarms
        for ph in ['A','B','C']:
            df['overpwr'+ph] = np.nan
            df.loc[df['apwr'+ph]>0.8*nomphase, 'overpwr'+ph] = df['apwr'+ph]
        
        df = df[['overpwrA','overpwrB','overpwrC']]
        df = df.dropna(how='all')
        if not df.empty:
            print(df)
            write_df(df, address, acc_token, devtoken)
        


def main():
    
    #define start - end date
    end_time = datetime.datetime.now()
    end_time = end_time - datetime.timedelta(minutes=end_time.minute, seconds=end_time.second,
                                                 microseconds=end_time.microsecond)
    
    start_time = end_time +relativedelta(hours=-1)
    print(start_time, end_time)
    start_time = str(int(start_time.timestamp()) * 1000)
    end_time = str(int(end_time.timestamp()) * 1000)
    
    address = 'http://localhost:8080'
    # address = 'https://mi6.meazon.com'
    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()
    
    acc_token = 'Bearer' + ' ' + r['token']
    
    
    entityId = '4795fc10-5b7f-11ee-b2c9-653b42f73605' # DEDDHE ATHINAS
    r1 = requests.get(url=address + "/api/entityGroup/"+entityId+"/entities?pageSize=1000&page=0",headers={'Content-Type': 'application/json', 
'Accept': '*/*', 'X-Authorization': acc_token}).json()

    for i in range(0,len(r1['data'])):
        assetid = r1['data'][i]['id']['id']
        assetname = r1['data'][i]['name']
        print(assetname)
    
        if assetname[0]!='0':
        
            r2 = requests.get(url=address + "/api/relations/info?fromId="+assetid+"&fromType=ASSET",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
            
            for j in range(0, len(r2)):
                device = r2[j]['toName']
                if device[:3]=='102':
                    print(device)
                               
                    # call export KPIs function
                    try:
                        [devid, acc_token, label, devtoken] = get_dev_info(device, address)                   
                        
                        detect_alarms(address, start_time, end_time, devid, acc_token, label, devtoken)
                    except Exception as e:
                        print(f"Error reading data for device {device}: {e}")
                        continue


if __name__ == '__main__':
    sys.exit(main())