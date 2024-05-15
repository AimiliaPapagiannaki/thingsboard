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

def get_devid(address,acc_token, device):
    r1 = requests.get( url=address + "/api/tenant/devices?deviceName=" + device,
                    headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    devid = r1['id']['id']
    
    return devid
    

def create_virtual(start_time, end_time,descriptors,vrtl,operation,layer, acc_token):
    address = "http://localhost:8080"
    agg = pd.DataFrame([])
    try:
        for virtual in vrtl:
            if layer==1:
                print(virtual)
                # request devid
                devid = get_devid(address,acc_token, virtual)
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




def send_data(mydf,device):
    address = "http://localhost:8080"
    df = mydf.copy()
    
    r = requests.post(address + "/api/auth/login",json={'username': 'meazon-scripts@meazon.com', 'password': 'scr1pt!'}).json()
    acc_token = 'Bearer' + ' ' + r['token']

    # get devid of virtual
    devid = get_devid(address, acc_token, device)

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
    
    # virtual devices' names
    virtualMeters = {'Απαραίτητα':
                                ['102.216.000652','102.216.000649'], # M06, M07
                    'Κλιματισμός Γραφείων': #V01
                                ['102.301.000976','102.301.000968','102.301.000969'], # M02, M09, M10
                    'Φωτισμός Γραφείων': # V02
                                ['102.301.000963','102.301.000869','102.301.000870','102.301.000850','102.301.000865','102.301.000868'], # M03, M04, M05, M11, M12, M14
                    'UPS Γραφείων': # V03
                                ['102.402.000037','102.402.000028'], # M20, M21
                    'Φορτία κίνησης (3) γραφείων': # V04
                                ['102.301.000970'], # M01
                    'Σύνολο Παρόχων': # V09
                                ['102.402.000039','102.301.000921'], # Μ22, Μ18
                    'Σύνολο Σταθερής': # V10
                                ['102.301.000923','102.301.000916','102.301.000935','102.301.000978'], # Μ19, Μ17, Μ15, Μ16
                    'Κλιματισμός Σταθερής': # V11
                                ['102.301.000916','102.301.000935','102.301.000978'], # Μ17, Μ15, Μ16
                    'Φωτισμός Καταστήματος':
                                ['102.301.001108','102.301.001128'], # Φωτισμός - Πρίζες, Φωτισμός - Φώτα Νυκτός 
                    'Κλιματισμός Κινητής': # V07
                                ['102.402.000031','102.402.000035'] # Μ23, Μ24
    }
    

    # define descriptors and access token
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
    

    # iterate over meters
    for virtualName,submeters in virtualMeters.items():

        if virtualName=='Κλιματισμός Κινητής':
            operation=0 # sub
        else:
            operation=1 #add
        #print(operation)
        agg = create_virtual(start_time, end_time,descriptors,submeters,operation,1,acc_token)
        print(agg)
        if not agg.empty:
            print('agg is not empty')
            agg = agg.dropna()
            
            
            # convert cnrg to integer
            for cnrg in ['cnrgA','cnrgB','cnrgC']:
                agg[cnrg] = agg[cnrg].astype(int)
            if not agg.empty:
                send_data(agg,virtualName)
                print('Completed ',virtualName)
        else:
            print('agg is empty')
    
    # end of layer 1
    ################################################################
    
    # Virtual meter Synolo Texnologias (Synolo statheris, Kiniti synolo)

    tmp_virtual = 'Σύνολο Τεχνολογίας'
    texnologia = ['Σύνολο Σταθερής', # Synolo statheris
                   'Κινητή Σύνολο'] # Kinitis synolo
    
    operation=1 # add meters
    tech_df = create_virtual(start_time, end_time,descriptors,texnologia,operation,1,acc_token)
    
    if not tech_df.empty:
        print('tech_df is not empty')
        tech_df = tech_df.dropna()
        
        # convert cnrg to integer
        for cnrg in ['cnrgA','cnrgB','cnrgC']:
            tech_df[cnrg] = tech_df[cnrg].astype(int)

        if not tech_df.empty:
            send_data(tech_df,tmp_virtual)
            print('Completed tech')
    
    ################################################################
    
    # Synolo grafeiwn (den grafetai)
    athr_grafeiwn = ['102.301.000870',# Fwtismos pinaka - M05
                     '102.301.000963',# Mparokivotia fwtismos - M03
                     '102.301.000850',# Fwtismos isogeio - M11
                     '102.301.000869',# Fwtismos 5ou - M04
                     '102.301.000868',# Fwtismos 3ou - M14
                     '102.301.000865',# Fwtismos 1ou - M12
                     '102.301.000970',# Fortia kinisis grafeiwn 3os - M01
                     '102.301.000867',# Fortia kinisis 1os - M13
                     '102.301.000969',#Klimatismos 5ou - M10
                     '102.301.000976',#Klimatismos 3ou - M02
                     '102.301.000968',#Klimatismos 1ou - M09
                     '102.402.000037', #UPS 2 - M20
                     '102.402.000028'] # UPS 1 - M21
    
    operation=1
    
    agg1 = create_virtual(start_time, end_time,descriptors,athr_grafeiwn,operation,1,acc_token)
    #print("athr graf",agg1)
    if not agg1.empty:
        agg1 = agg1.dropna()
            
        for cnrg in ['cnrgA','cnrgB','cnrgC']:
            agg1[cnrg] = agg1[cnrg].astype(int)
    
    
    # Synolo kinitis, statheris, paroxwn, katastimatos  (den grafetai) 
    synolo_kinstath = ['Σύνολο Παρόχων', # V09
                       'Σύνολο Σταθερής', # V10
                       'Κινητή Σύνολο', # M23
                       '102.301.000896'] # Synolo katastimatos cosmote shop
    agg2 = create_virtual(start_time, end_time,descriptors,synolo_kinstath,operation,1,acc_token)
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
    
    # Ypoloipa fortia xrisi grafeiwn: Genikos - athroisma grafeiwn - synolo kinitis/statheris/paroxwn/katastimatos
    
    device = 'Συνολική κατανάλωση κτ. Αθηνάς'
    devid = get_devid(address, acc_token, device)
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
                tmp_virtual = 'Υπόλοιπα Φορτία Γραφείων'
                send_data(agg,tmp_virtual)
    
    # end of layer 2
    ################################################################
    #Synolo grafeiwn
    tmp_virtual = 'Σύνολο Γραφείων'
    if not agg.empty:
        agg = agg.add(agg1)
        for vlt in ['vltA','vltB','vltC']:
                agg[vlt] = agg[vlt]/2
        
        del agg1
        agg.dropna(inplace=True)
        if not agg.empty:
            #print("sunolo grafio",agg)
            send_data(agg,tmp_virtual) 
    
    
    
if __name__ == "__main__":
    sys.exit(main())