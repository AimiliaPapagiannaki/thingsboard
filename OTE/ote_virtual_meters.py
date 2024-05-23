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
    """
    Aligns and resamples the DataFrame to the specified interval.
    """
   
    df['ts'] = df.index
    df['ts'] = df['ts'].dt.tz_localize('utc')
    df.set_index('ts',inplace = True, drop = True)

    df = df.resample(interv,label='right').max()
#     df = df.fillna(method="ffill")

    return df



def read_data(devid, acc_token, address, start_time, end_time, descriptors):
    """
    Reads data from the API and returns a DataFrame.
    """
    df = pd.DataFrame([])
    offset = 30 * 86400000 if int(end_time) - int(start_time) > 30 * 86400000 else 86400000
    svec = np.arange(int(start_time), int(end_time), offset)
    
    for st in svec:
        en = st + offset - 1
        en = int(end_time) if int(end_time) - en <= 0 else en

        try:
            response = requests.get(
                url=f"{address}/api/plugins/telemetry/DEVICE/{devid}/values/timeseries",
                params={
                    "keys": descriptors,
                    "startTs": start_time,
                    "endTs": end_time,
                    "agg": "NONE",
                    "limit": 1000000
                },
                headers={
                    'Content-Type': 'application/json',
                    'Accept': '*/*',
                    'X-Authorization': acc_token
                }
            )
            r2 = response.json()

            if r2:
                tmp = pd.concat(
                    [pd.DataFrame(r2[desc]).rename(columns={'value': desc}).set_index('ts') for desc in r2],
                    axis=1
                )
                tmp.reset_index(drop=False, inplace=True)
                tmp['ts'] = pd.to_datetime(tmp['ts'], unit='ms')
                tmp.sort_values(by=['ts'], inplace=True)
                tmp.set_index('ts', inplace=True, drop=True)
                df = pd.concat([df, tmp])

        except Exception as e:
            print(f"Error reading data for device {devid}: {e}")
            continue

    if not df.empty:
        df = df.apply(pd.to_numeric, errors='coerce')
        df = align_resample(df, '1h')
    
    return df


def get_devid(address, acc_token, device):
    """
    Retrieves the device ID for the given device name.
    """
    response = requests.get(
        url=f"{address}/api/tenant/devices",
        params={"deviceName": device},
        headers={
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'X-Authorization': acc_token
        }
    )
    return response.json()['id']['id']
    


def create_virtual(start_time, end_time,descriptors,vrtl,operation,layer, acc_token, address):
    """
    Creates a virtual DataFrame by aggregating data from multiple devices.
    """
    agg = pd.DataFrame([])
    try:
        for virtual in vrtl:
            devid = get_devid(address, acc_token, virtual) if layer == 1 else virtual
    
            df = read_data(devid, acc_token, address, start_time, end_time,descriptors)
            if not df.empty:
                if not agg.empty:
                    if operation == 1:
                        agg = agg.add(df)#,fill_value=0)
                    else:
                        cols = ['cnrgA','cnrgB','cnrgC','pwrA','pwrB','pwrC','curA','curB','curC']
                        agg = agg.sub(df)#, fill_value=0)
                        #agg[cols] = agg[cols].sub(df[cols])#, fill_value=0)
                        #agg[['vltA','vltB','vltC']] = agg[['vltA','vltB','vltC']].add(df[['vltA','vltB','vltC']])
                        
                else:
                    agg = df
            else:
                return pd.DataFrame([])
            
        # if not agg.empty:
            
        #     for vlt in ['vltA','vltB','vltC']:
        #         agg[vlt] = agg[vlt]/2
    except Exception as e:
        print(f"Unable to retrieve data for all virtuals: {e}")
        
    return agg.dropna()


def postproc(df, label):
    """
    Post-processes the DataFrame.
    """
    if not df.empty:
        print('not empty: ',label)
        df = df.dropna()
        
        # convert cnrg to integer
        for cnrg in ['cnrgA','cnrgB','cnrgC']:
            df[cnrg] = df[cnrg].astype(np.int64)
    else:
        print('empty: ', label)
    return df



def send_data(mydf, device, address):
    """
    Write telemetry data to the API.
    """
    df = mydf.copy()
    print(device)
    for nrg in ['cnrgA','cnrgB','cnrgC']:
        if nrg in df.columns:
            df.loc[df[nrg]<df[nrg].shift(), nrg]=np.nan
    df = df.dropna()
    if df.empty:
        return
    
    try:
        
        r = requests.post(
            f"{address}/api/auth/login",
            json={'username': 'meazon-scripts@meazon.com', 'password': 'scr1pt!'}
        ).json()
        acc_token = 'Bearer ' + r['token']

        # get devid of virtual
        devid = get_devid(address, acc_token, device)
        
        # get dev_token
        r1 = requests.get(
            url=f"{address}/api/device/{devid}/credentials",
            headers={
                'Content-Type': 'application/json',
                'Accept': '*/*',
                'X-Authorization': acc_token
            }
        ).json()
        devtoken = r1['credentialsId']
        
        # transform ts and write telemetry
        df['ts'] = df.index
        df['ts'] = df.apply(lambda row: int(row['ts'].timestamp()) * 1000, axis=1)
        df.set_index('ts', inplace=True, drop=True)

        mydict = df.to_dict('index')

        for key, value in mydict.items():
            my_json = json.dumps({'ts': key, 'values': value})
            requests.post(
                url=f"{address}/api/v1/{devtoken}/telemetry",
                data=my_json,
                headers={
                    'Content-Type': 'application/json',
                    'Accept': '*/*',
                    'X-Authorization': acc_token
                }
            )
    except Exception as e:
        print(f"Error sending data for device {device}: {e}")
     
    


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
    descriptors = 'cnrgA,cnrgB,cnrgC,pwrA,pwrB,pwrC,curA,curB,curC'#,vltA,vltB,vltC'
    r = requests.post(address + "/api/auth/login",json={'username': 'meazon-scripts@meazon.com', 'password': 'scr1pt!'}).json()
    acc_token = 'Bearer' + ' ' + r['token']
        
    
    
    #define start - end date
    end_time = datetime.datetime.now()
    end_time = end_time - datetime.timedelta(hours=end_time.hour, minutes=end_time.minute, seconds=end_time.second,
                                                 microseconds=end_time.microsecond)
    
    start_time = end_time +relativedelta(days=-1)
    
    start_time = str(int(start_time.timestamp()) * 1000)
    end_time = str(int(end_time.timestamp()) * 1000)
    #start_time = '1698789600000'
    #end_time = '1701381600000'

    # iterate over meters
    for virtualName,submeters in virtualMeters.items():

        if virtualName=='Κλιματισμός Κινητής':
            operation=0 # sub
        else:
            operation=1 #add
        #print(operation)
        agg = create_virtual(start_time, end_time,descriptors,submeters,operation,1,acc_token, address)

        agg = postproc(agg, virtualName)
        print(virtualName,agg.tail(15))
        send_data(agg,virtualName, address)
        print('Completed ',virtualName)
        
    
    # end of layer 1
    ################################################################
    
    # Virtual meter Synolo Texnologias (Synolo statheris, Kiniti synolo)
    virtualName = 'Σύνολο Τεχνολογίας'
    texnologia = ['Σύνολο Σταθερής', # Synolo statheris
                   '102.402.000031'] # Kinitis synolo
    
    operation=1 # add meters
    tech_df = create_virtual(start_time, end_time,descriptors,texnologia,operation,1,acc_token, address)
    tech_df = postproc(tech_df, virtualName)
    print(virtualName,tech_df.tail(15))
    send_data(tech_df,virtualName, address)
    print('Completed ', virtualName)
    
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
    agg1 = create_virtual(start_time, end_time,descriptors,athr_grafeiwn,operation,1,acc_token, address)
    agg1 = postproc(agg1,'Άθροισμα γραφείων')
    print('Άθροισμα γραφείων',agg1.tail(15))
    
    # Synolo kinitis, statheris, paroxwn, katastimatos  (den grafetai) 
    synolo_kinstath = ['102.402.000039','102.301.000921', # V09 Synolo Paroxwn
                       '102.301.000923','102.301.000916','102.301.000935','102.301.000978', # V10 Synolo statheris
                       '102.402.000031', # M23 Synolo kinitis
                       '102.301.000896'] # Synolo katastimatos cosmote shop
    agg2 = create_virtual(start_time, end_time,descriptors,synolo_kinstath,operation,1,acc_token, address)
    agg2 = postproc(agg2,'Σύνολο κινητής, σταθερής, παρόχων, καταστήματος')
    print('Σύνολο κινητής, σταθερής, παρόχων, καταστήματος',agg2.tail(15))
            
#######################################################################################            
    # Ypoloipa katastimatos = Cosmote shop - (fwtismos katastimatos + klimatismos)

    # Fwtismos katastimatos+klimatismos
    fwtismos_klimatismos = ['102.301.001108', # Φωτισμός - Πρίζες 
                            '102.301.001128', # Φωτισμός - Φώτα Νυκτός 
                            '102.301.001102'] # Klimatismos katastimatos
    operation=1
    fwt_kl = create_virtual(start_time, end_time,descriptors,fwtismos_klimatismos,operation,1,acc_token, address)
    fwt_kl = postproc(fwt_kl,'Φωτισμός καταστήματος, κλιματισμός')
    print('Φωτισμός καταστήματος, κλιματισμός',fwt_kl.tail(15))
    
    # 102.301.000896 -Fwtismos_klimatismos
    virtualName = 'Υπόλοιπα Καταστήματος'
    device = '102.301.000896' # Cosmote shop
    devid = get_devid(address, acc_token, device)
    df = read_data(devid, acc_token, address, start_time, end_time,descriptors)
    df = df.dropna()
    
    if not df.empty:
        for cnrg in ['cnrgA','cnrgB','cnrgC']:
            df[cnrg] = df[cnrg].astype(np.int64)
        if not fwt_kl.empty:
            agg = pd.DataFrame([])
            agg = df.copy()
            cols = ['cnrgA','cnrgB','cnrgC','pwrA','pwrB','pwrC','curA','curB','curC']
            agg = agg.sub(fwt_kl)
            # agg[cols] = agg[cols].sub(fwt_kl[cols])

            # agg[['vltA','vltB','vltC']] = agg[['vltA','vltB','vltC']].add(fwt_kl[['vltA','vltB','vltC']])
            
            # for vlt in ['vltA','vltB','vltC']:
            #         agg[vlt] = agg[vlt]/2
                    
            agg.dropna(inplace=True)
            del fwt_kl
            print(virtualName,agg.tail(15))
            send_data(agg,virtualName, address) # write to Ypoloipa katastimatos (virtual meter)
    
        
#######################################################################################
    
    # Ypoloipa fortia xrisi grafeiwn: Genikos - athroisma grafeiwn - synolo kinitis/statheris/paroxwn/katastimatos
    
    device = '102.216.000651' # Ktirio Athinas
    devid = get_devid(address, acc_token, device)
    df = read_data(devid, acc_token, address, start_time, end_time,descriptors)
    df = df.dropna()
    
    if not df.empty:
        for cnrg in ['cnrgA','cnrgB','cnrgC']:
            df[cnrg] = df[cnrg].astype(np.int64)
    
        if ((not agg1.empty) & (not agg2.empty)):
            agg = pd.DataFrame([])
            agg = df.copy()
            cols = ['cnrgA','cnrgB','cnrgC','pwrA','pwrB','pwrC','curA','curB','curC']
            agg = agg.sub(agg1)
            # agg[cols] = agg[cols].sub(agg1[cols])
            # agg[['vltA','vltB','vltC']] = agg[['vltA','vltB','vltC']].add(agg1[['vltA','vltB','vltC']])

            agg = agg.sub(agg2)

            # agg[cols] = agg[cols].sub(agg2[cols])
            # agg[['vltA','vltB','vltC']] = agg[['vltA','vltB','vltC']].add(agg2[['vltA','vltB','vltC']])
            
            # for vlt in ['vltA','vltB','vltC']:
            #         agg[vlt] = agg[vlt]/2
            
            agg.dropna(inplace=True)

            print('OTHER:',agg)
            
            if not agg.empty:
                virtualName = 'Υπόλοιπα Φορτία Γραφείων'
                send_data(agg,virtualName, address)
    
    # end of layer 2
    ################################################################
    #Synolo grafeiwn = Other + athroisma grafeiwn
    virtualName = 'Σύνολο Γραφείων'
    if not agg.empty:
        agg = agg.add(agg1)
        # for vlt in ['vltA','vltB','vltC']:
        #         agg[vlt] = agg[vlt]/2
        
        
        agg.dropna(inplace=True)
        print(virtualName, agg.tail(15))
        send_data(agg, virtualName, address) 
    
    
    
if __name__ == "__main__":
    sys.exit(main())