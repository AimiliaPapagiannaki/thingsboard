#!/usr/bin/env python3

import sys
import pandas as pd
import datetime
import requests
import numpy as np
from pandas import ExcelWriter
import os
import glob
import pytz
from dateutil.tz import gettz
import timeit
# from datetime import datetime
from datetime import timedelta
import time


def read_data(devid, acc_token, address, start_time, end_time, descriptors):


    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys=" + descriptors + "&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()

    if r2:
        df = pd.DataFrame([])

        for desc in r2.keys():
            df1 = pd.DataFrame(r2[desc])
            df1.set_index('ts', inplace=True)
            df1.columns = [str(desc)]
            df = pd.concat([df, df1], axis=1)


        if df.empty == False:

            df.reset_index(drop=False, inplace=True)
            df = df.sort_values(by=['ts'])
            df.reset_index(drop=True, inplace=True)
            df.set_index('ts', inplace=True, drop=True)
            for col in df.columns:
                df[col] = df[col].astype('float')

            df = df.groupby(df.index).max()

        else:
            df = pd.DataFrame([])
    else:
        df = pd.DataFrame([])
        print('Empty json!')
    return df


def main(argv):
    startt = time.time()
    devid = argv[1]
    devserial = argv[2]
    #descriptors = argv[2]
    start_time = argv[3]
    end_time = argv[4]
   
    address = "http://localhost:8080"
    #address = "http://157.230.210.37:8081"
    
    
    #path = '../csv files'
    #
    #path = path + '/'
    #os.chdir(path)
    
    # filename of csv export file
    filename = str(devserial)+'_'+str(start_time)+'_'+str(end_time)+'.csv'

    

    r = requests.post(address + "/api/auth/login",json={'username': 'a.papagiannaki@meazon.com', 'password': 'eurobank'}).json()

    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']

    # request all descriptors that have ever been assigned to this device
    r1 = requests.get(url = address+"/api/plugins/telemetry/DEVICE/"+devid+"/keys/timeseries",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    descriptors = [x for x in r1]
    descriptors = ','.join(descriptors)
    print(descriptors)
    
    timediff = int(end_time)-int(start_time)
    
    # if difference between end and start time is greater than 5 minutes, split data 
    if timediff<2500000000: # approximately 30 days 
        summary = read_data(devid, acc_token, address, start_time, end_time, descriptors)
    
        if not summary.empty:
            summary.to_csv(filename, index=True)
    else:
        print('time requested larger than 5 minutes...')
        svec = np.arange(int(start_time),int(end_time),2500000000)
        for st in svec:
            en = st+2500000000-1
            
            if int(end_time)-en<=0: en = int(end_time)
            print('slot start: %i, slot end: %i:' % (st,en))
        
            summary = read_data(devid, acc_token, address, str(st), str(en), descriptors)
            if not summary.empty:
                if os.path.isfile(filename)==False:
                    summary.to_csv(filename, index=True)
                else:
                    summary.to_csv(filename, index=True,mode='a', header=False)
           
            

    elapsed = time.time() - startt
    print("---  seconds ---", elapsed)


if __name__ == "__main__":
    sys.exit(main(sys.argv))

