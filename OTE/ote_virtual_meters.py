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
                
        df = align_resample(df, '1h')
    return df




def create_virtual(start_time, end_time,descriptors,vrtl,operation,layer, acc_token):
    address = "http://localhost:8080"
    agg = pd.DataFrame([])
    try:
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
            if not df.empty:
                if not agg.empty:
                    if operation == 1:
                        agg = agg.add(df)#,fill_value=0)
                    else:
                        cols = ['cnrgA','cnrgB','cnrgC','pwrA','pwrB','pwrC','curA','curB','curC']
                        agg[cols] = agg[cols].sub(df[cols])#, fill_value=0)
                        agg[['vltA','vltB','vltC']] = agg[['vltA','vltB','vltC']].add(df[['vltA','vltB','vltC']])
                        
                else:
                    agg = df
            else:
                agg = pd.DataFrame([])
                break
            del df
        if not agg.empty:
            
            for vlt in ['vltA','vltB','vltC']:
                agg[vlt] = agg[vlt]/2
    except:
        print('Unable to retrieve data for all virtuals')
        pass
    agg = agg.dropna()
    return agg





def send_data(mydf,devid):
    address = "http://localhost:8080"
    df = mydf.copy()
    
    r = requests.post(address + "/api/auth/login",json={'username': 'meazon-scripts@meazon.com', 'password': 'scr1pt!'}).json()
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
    


def main():


    address = "http://localhost:8080"
    
    #Aparaitita
    
    vrtl0 = ['102.216.000652','102.216.000649'] 
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
    
    #Klimatismos texnologias statheris (+)
    vrtl7 = ['102.301.000916','102.301.000935','102.301.000978']
    
    #Fwtismos katastimatos
    vrtl8 = ['102.301.001108','102.301.001128']
    
    #Klimatismos kinitis (-)
    vrtl9 = ['102.402.000031','102.402.000035']
    
    
    
    
    # devids for virtual meters
    vdevids = ['767bd9e0-829c-11ec-ab8f-ef1ea7f487fc','e8652270-19e5-11ec-ab8f-ef1ea7f487fc','0445f9b0-19e6-11ec-ab8f-ef1ea7f487fc','0c1aacd0-19e6-11ec-ab8f-ef1ea7f487fc','1415aca0-19e6-11ec-ab8f-ef1ea7f487fc','69dd5c50-19e6-11ec-ab8f-ef1ea7f487fc','7df3e0b0-19e6-11ec-ab8f-ef1ea7f487fc','84e16820-19e6-11ec-ab8f-ef1ea7f487fc', 'b4274890-beee-11ec-b33d-07b37ee02f32','486cdff0-19e6-11ec-ab8f-ef1ea7f487fc']
    
    
    virtualMeters=[vrtl0]+[vrtl1]+[vrtl2]+[vrtl3]+[vrtl4]+[vrtl5]+[vrtl6]+[vrtl7]+[vrtl8]+[vrtl9]
    
    
    # define descriptors and request token
    descriptors = 'cnrgA,cnrgB,cnrgC,pwrA,pwrB,pwrC,curA,curB,curC,vltA,vltB,vltC'
    r = requests.post(address + "/api/auth/login",json={'username': 'meazon-scripts@meazon.com', 'password': 'scr1pt!'}).json()
    acc_token = 'Bearer' + ' ' + r['token']
        
    
    
    #define start - end date
    end_time = datetime.datetime.now()
    end_time = end_time - datetime.timedelta(hours=end_time.hour, minutes=end_time.minute, seconds=end_time.second,
                                                 microseconds=end_time.microsecond)
    
    start_time = end_time +relativedelta(days=-1)
    
    start_time = str(int(start_time.timestamp()) * 1000)
    end_time = str(int(end_time.timestamp()) * 1000)
    
    #end_time = '1712523600000'# 7-8 april
    #start_time = '1712437200000'
    # iterate over meters
    
    for i in range(0,len(virtualMeters)):

        if i==len(virtualMeters)-1:
            operation=0 # sub
        else:
            operation=1 #add
        #print(operation)
        agg = create_virtual(start_time, end_time,descriptors,virtualMeters[i],operation,1,acc_token)
        print(agg)
        if not agg.empty:
            print('agg is not empty')
            agg = agg.dropna()
            
            
            # convert cnrg to integer
            for cnrg in ['cnrgA','cnrgB','cnrgC']:
                agg[cnrg] = agg[cnrg].astype(int)
            if not agg.empty:
                send_data(agg,vdevids[i])
                print('Completed ',i)
        else:
            print('agg is empty')
    
    # end of layer 1
    ################################################################
    
    # Virtual meter Xrisi Texnologias (Synolo statheris, Kiniti synolo)
    
    texnologia = ['7df3e0b0-19e6-11ec-ab8f-ef1ea7f487fc', '9997b670-942c-11ea-8c14-6192a0efe6e6']
    tmpdevid = 'a3504fe0-8437-11ec-ab8f-ef1ea7f487fc'
    operation=1
    tech_df = create_virtual(start_time, end_time,descriptors,texnologia,operation,2,acc_token)
    
    if not tech_df.empty:
        print('tech_df is not empty')
        
        tech_df = tech_df.dropna()
        
        
        # convert cnrg to integer
        for cnrg in ['cnrgA','cnrgB','cnrgC']:
            tech_df[cnrg] = tech_df[cnrg].astype(int)
        if not tech_df.empty:
            
            send_data(tech_df,tmpdevid)
            print('Completed tech',)
    
    ################################################################
    
    # Athroisma grafeiwn (den grafetai)
    #athr_grafeiwn = ['e8652270-19e5-11ec-ab8f-ef1ea7f487fc','0445f9b0-19e6-11ec-ab8f-ef1ea7f487fc','0c1aacd0-19e6-11ec-ab8f-ef1ea7f487fc','1415aca0-19e6-11ec-ab8f-ef1ea7f487fc','813f3ce0-1c2e-11ea-8762-6bf954fc5af1']
    
    athr_grafeiwn =['924afcd0-1c2f-11ea-8762-6bf954fc5af1','e0cc30d0-96b4-11eb-9140-dd89ba669722','abba5450-1c2e-11ea-8762-6bf954fc5af1','945ffa20-1c2f-11ea-8762-6bf954fc5af1','a94a8cd0-1c2e-11ea-8762-6bf954fc5af1','817ae652-1c2e-11ea-8762-6bf954fc5af1','8ef1e6c0-1c2f-11ea-8762-6bf954fc5af1','813f3ce0-1c2e-11ea-8762-6bf954fc5af1','648b68c0-1c2f-11ea-8762-6bf954fc5af1','855d9ec0-1c2e-11ea-8762-6bf954fc5af1','828ca470-1c2e-11ea-8762-6bf954fc5af1','40be03b0-20dc-11ea-8762-6bf954fc5af1','b956d620-9428-11ea-8c14-6192a0efe6e6']
    operation=1
    
    agg1 = create_virtual(start_time, end_time,descriptors,athr_grafeiwn,operation,2,acc_token)
    #print("athr graf",agg1)
    if not agg1.empty:
        agg1 = agg1.dropna()
            
        for cnrg in ['cnrgA','cnrgB','cnrgC']:
            agg1[cnrg] = agg1[cnrg].astype(int)
    
    
    # Synolo kinitis statheris  (den grafetai) #,9997b670-942c-11ea-8c14-6192a0efe6e6,,"""'69dd5c50-19e6-11ec-ab8f-ef1ea7f487fc','7df3e0b0-19e6-11ec-ab8f-ef1ea7f487fc'
    synolo_kinstath = ['63a6b040-5947-11ea-8762-6bf954fc5af1','992a9cc0-942c-11ea-8c14-6192a0efe6e6','9997b670-942c-11ea-8c14-6192a0efe6e6']
    agg2 = create_virtual(start_time, end_time,descriptors,synolo_kinstath,operation,2,acc_token)
    #print("synolo kin stath",agg2)
    if not agg2.empty:
        agg2 = agg2.dropna()
            
        for cnrg in ['cnrgA','cnrgB','cnrgC']:
            agg2[cnrg] = agg2[cnrg].astype(int)
            
#######################################################################################            
    # Fwtismos katastimatos+klimatismos
    fwtismos_klimatismos = ['102.301.001108','102.301.001128','102.301.001102']
    operation=1
    fwt_kl = create_virtual(start_time, end_time,descriptors,fwtismos_klimatismos,operation,1,acc_token)
    
    if not fwt_kl.empty:
        fwt_kl = fwt_kl.dropna()
            
        for cnrg in ['cnrgA','cnrgB','cnrgC']:
            fwt_kl[cnrg] = fwt_kl[cnrg].astype(int)
    
    # 102.301.000896 -Fwtismos_klimatismos
    devid = '53098be0-bb38-11ec-b33d-07b37ee02f32'
    df = read_data(devid, acc_token, address, start_time, end_time,descriptors)
    df = df.dropna()
    
    if not df.empty:
        for cnrg in ['cnrgA','cnrgB','cnrgC']:
            df[cnrg] = df[cnrg].astype(int)
        if (not fwt_kl.empty):
            agg = pd.DataFrame([])
            agg = df.copy()
            cols = ['cnrgA','cnrgB','cnrgC','pwrA','pwrB','pwrC','curA','curB','curC']
            agg[cols] = agg[cols].sub(fwt_kl[cols])
            agg[['vltA','vltB','vltC']] = agg[['vltA','vltB','vltC']].add(fwt_kl[['vltA','vltB','vltC']])
            
            for vlt in ['vltA','vltB','vltC']:
                    agg[vlt] = agg[vlt]/2
                    
            agg.dropna(inplace=True)
            del fwt_kl
            
            if not agg.empty:
                send_data(agg,'06a25df0-bef2-11ec-b33d-07b37ee02f32') # write to Ypoloipa katastimatos (virtual meter)
    
        
#######################################################################################
    
    # Ypoloipa fortia xrisi grafeiwn
    # 102.216.000654 - athroisma grafeiwn - synolo kinitis statheris
    devid = 'da260200-57e6-11ea-8762-6bf954fc5af1'#'da260200-57e6-11ea-8762-6bf954fc5af1'
    df = read_data(devid, acc_token, address, start_time, end_time,descriptors)
    df = df.dropna()
    #print("main",df)
    if not df.empty:
        for cnrg in ['cnrgA','cnrgB','cnrgC']:
            df[cnrg] = df[cnrg].astype(int)
    
        if ((not agg1.empty) & (not agg2.empty)):
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
            
            if not agg.empty:
                send_data(agg,'21c14710-19e6-11ec-ab8f-ef1ea7f487fc')
    
    # end of layer 2
    ################################################################
    #Synolo grafeiwn
    
    if not agg.empty:
        agg = agg.add(agg1)
        for vlt in ['vltA','vltB','vltC']:
                agg[vlt] = agg[vlt]/2
        
        del agg1
        agg.dropna(inplace=True)
        if not agg.empty:
            #print("sunolo grafio",agg)
            send_data(agg,'288d15b0-19e6-11ec-ab8f-ef1ea7f487fc') #288d15b0-19e6-11ec-ab8f-ef1ea7f487fc
    
    
    
if __name__ == "__main__":
    sys.exit(main())