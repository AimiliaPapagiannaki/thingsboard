#!/usr/bin/env python3 
import sys
import requests
import json
import datetime
import os
import pandas as pd




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



def main():
    
    start_time = '1708765200000'
    end_time = '1708779600000'
    
    address = 'http://localhost:8080'
    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()
    
    acc_token = 'Bearer' + ' ' + r['token']
    
    
    entityId = '4795fc10-5b7f-11ee-b2c9-653b42f73605' # DEDDHE ATHINAS
    r1 = requests.get(url=address + "/api/entityGroup/"+entityId+"/entities?pageSize=1000&page=0",headers={'Content-Type': 'application/json', 
'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    summary = pd.DataFrame(columns=['Installation','Transformer','Distribution','Total energy consumption (KWh)','Nr. of power alarms L1','% of time of power alarms L1','Nr. of power alarms L2','% of time of power alarms L2','Nr. of power alarms L3','% of time of power alarms L3','Nr. of overcurrent alarms L1','% of time of overcurrent alarms L1','Nr. of overcurrent alarms L2','% of time of overcurrent alarms L2','Nr. of overcurrent alarms L3','% of time of overcurrent alarms L3','Nr. of Voltage unbalance alarms','% of time of Voltage unbalance alarms','Nr. of Current unbalance alarms','% of time of Current unbalance alarms','Nr. of Power Fails (outage)','Min time of Power Fails (sec)','Max time of Power Fails (sec)','Nr. of Voltage dips','Avg time of Voltage dips (msec)','Nr. of Voltage swells','Avg time of Voltage swells (msec)'])
    
    
  
    
    for i in range(0,len(r1['data'])):
     #   os.chdir('/home/azureuser/deddhePDF/')
        assetid = r1['data'][i]['id']['id']
        assetname = r1['data'][i]['name']
        print(assetname)
    
        if assetname[0]!='0':
        
            r2 = requests.get(url=address + "/api/relations/info?fromId="+assetid+"&fromType=ASSET",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
            
            
            for j in range(0, len(r2)):
                device = r2[j]['toName']
                if device[:3]=='102':
                                      

                    
                    # call export KPIs function
                    #try:
                    [devid, acc_token, label] = get_dev_info(device, address)
                    
                    interval = 1 # interval in minutes
                    descriptors = 'pwrA,pwrB,pwrC'
                    address = 'http://localhost:8080'
                
                    
                    df = read_data(acc_token, devid, address,  start_time, end_time, descriptors)
                    
                    
                    df = df.loc[df['pwrA']<-50]
                    
                    if not df.empty:
                        print(label)
                        print(df)
                    
        
    
        
            
            

if __name__ == '__main__':
    sys.exit(main())