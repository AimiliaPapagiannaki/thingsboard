#!/usr/bin/env python3 
import pandas as pd
import datetime
import os
import seaborn as sns
import matplotlib.pyplot as plt
import requests
import numpy as np
from dateutil.relativedelta import relativedelta
import pytz
import sys

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


def angle_analysis(info, device, address, acc_token, devid, label, start_time, end_time, descriptors):
    nomcur = info.loc[info['Serial']==device,'Ampere'].values[0]
    print('Nominal current:', nomcur)
    df = read_data(acc_token, devid, address, start_time, end_time, descriptors)
    
    combined_df = pd.DataFrame([])
    for ph in ['A','B','C']:
        tmp = df.copy()
        tmp = tmp.loc[tmp['cur'+ph]>0.01*nomcur]
        
        tmp = tmp[['angle'+ph]]
        tmp = tmp.rename(columns={'angle'+ph:'angle'})
        tmp = tmp.dropna()
        tmp['angle'] = np.abs(tmp['angle'])
        tmp['category'] = np.nan
        tmp.loc[tmp['angle']<=60,'category'] = 'angle<=60'
        tmp.loc[((tmp['angle']>60) & (tmp['angle']<=180)),'category'] = '60<angle<=180'
        tmp.loc[tmp['angle']>180,'category'] = 'angle>180'
        tmp['Phase']  = 'Phase '+ph
        combined_df = pd.concat([combined_df,tmp])
    print(combined_df.loc[combined_df['category']=='60<angle<=180'].describe())
    # Create a boxplot grouped by category and dataframe source
    plt.figure(figsize=(12, 8))
    sns.boxplot(x='Phase', y='angle', hue='category', data=combined_df)

    # Add labels and title
    plt.title('Boxplot of V-I angles '+label+'('+device+')')
    plt.xlabel('Category')
    plt.ylabel('Angle')
    plt.legend(title='Phase')
    plt.tight_layout()
    figname = label+'('+device+').png'
    plt.savefig(figname, dpi=300)


def main():
    # Read info about nominal current
    info = pd.read_excel('HEDNO_info.xlsx', engine='openpyxl')

    address = 'https://mi6.meazon.com'
    r = requests.post(address + "/api/auth/login",
                        json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()

    acc_token = 'Bearer' + ' ' + r['token']


    entityId = '47545f30-5b7f-11ee-b2c9-653b42f73605' # DEDDHE ATHINAS
    r1 = requests.get(url=address + "/api/entityGroup/"+entityId+"/entities?pageSize=1000&page=0",headers={'Content-Type': 'application/json', 
    'Accept': '*/*', 'X-Authorization': acc_token}).json()

    interval = 1 # interval in minutes
    descriptors = 'curA,curB,curC,angleA,angleB,angleC'
    month = 9
    startm = datetime.datetime(year = 2024, month=month, day=1)
    endm = startm + relativedelta(months=1)
    tmzn = pytz.timezone('Europe/Athens')    
    endm = tmzn.localize(endm)
    startm = tmzn.localize(startm)
    end_time = str(int((endm ).timestamp() * 1000))
    start_time = str(int((startm ).timestamp() * 1000))
    # start_time = '1725138000000'
    end_time = '1726559346000'

    basepath = 'C:/Meazon Projects/thingsboard/mi6/HEDNOKPIs/fault_installation_check/'
    for i in range(0,len(r1['data'])):
        assetid = r1['data'][i]['id']['id']
        assetname = r1['data'][i]['name']
        assetpath = basepath+'/'+assetname
        if not os.path.exists(assetpath):
            os.makedirs(assetpath)
        os.chdir(assetpath)
        print(assetname)

        if assetname[0]!='0':  
            r2 = requests.get(url=address + "/api/relations/info?fromId="+assetid+"&fromType=ASSET",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
            for j in range(0, len(r2)):
                device = r2[j]['toName']
                if device[:3]=='102':
                    print(device)
                    [devid, acc_token, label] = get_dev_info(device, address)
                    transformerpath = assetpath+'/'+label
                    if not os.path.exists(transformerpath):
                        os.makedirs(transformerpath)
                    os.chdir(transformerpath) 
                    angle_analysis(info, device, address, acc_token, devid, label, start_time, end_time, descriptors)
                    # search for nested devices
                    r3 = requests.get(url=address + "/api/relations/info?fromId="+devid+"&fromType=DEVICE",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
                    for k in range(0, len(r3)):                    
                        device = r3[k]['toName']
                        print(device)
                        [devid, acc_token, label] = get_dev_info(device, address)
                        angle_analysis(info, device, address, acc_token, devid, label, start_time, end_time, descriptors)
                os.chdir(assetpath)
            os.chdir(basepath)

if __name__ == '__main__':
    sys.exit(main())