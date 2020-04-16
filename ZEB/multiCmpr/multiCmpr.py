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


#
#
def conv_to_consumption(df, interval, Amin, Bmin, Cmin):
    #     convert cumulative energy to consumed energy
    if 'cnrgA' in df.columns:
        firstdif = df['cnrgA'].iloc[0] - Amin
        df['cnrgA'] = df['cnrgA'] / 1000
        df['diffA'] = np.nan
        df.diffA[((df.cnrgA.isna() == False) & (df.cnrgA.shift().isna() == False))] = df.cnrgA - df.cnrgA.shift()
        # df.diffA.iloc[0] = firstdif / 1000
        df.rename(columns={"diffA": "Consumed energy (kWh) A"}, inplace=True)
        df.drop(['cnrgA'], axis=1, inplace=True)

    if 'cnrgB' in df.columns:
        firstdif = df['cnrgB'].iloc[0] - Bmin
        df['cnrgB'] = df['cnrgB'] / 1000
        df['diffB'] = np.nan
        df.diffB[(df.cnrgB.isna() == False) & (df.cnrgB.shift().isna() == False)] = df.cnrgB - df.cnrgB.shift()
        # df.diffB.iloc[0] = firstdif / 1000
        df.rename(columns={"diffB": "Consumed energy (kWh) B"}, inplace=True)
        df.drop(['cnrgB'], axis=1, inplace=True)

    if 'cnrgC' in df.columns:
        firstdif = df['cnrgC'].iloc[0] - Cmin
        df['cnrgC'] = df['cnrgC'] / 1000
        df['diffC'] = np.nan
        df.diffC[(df.cnrgC.isna() == False) & (df.cnrgC.shift().isna() == False)] = df.cnrgC - df.cnrgC.shift()
        # df.diffC.iloc[0] = firstdif / 1000
        df.rename(columns={"diffC": "Consumed energy (kWh) C"}, inplace=True)
        df.drop(['cnrgC'], axis=1, inplace=True)

    if (('Consumed energy (kWh) A' in df.columns) and ('Consumed energy (kWh) B' in df.columns) and (
            'Consumed energy (kWh) C' in df.columns)):
        df['total'] = np.nan
        df.total[(df['Consumed energy (kWh) A'].isna() == False) & (df['Consumed energy (kWh) B'].isna() == False) & (
                df['Consumed energy (kWh) C'].isna() == False)] = df['Consumed energy (kWh) A'] + df[
            'Consumed energy (kWh) B'] + df['Consumed energy (kWh) C']
        df.rename(columns={"total": "Total Consumed energy (kWh)"}, inplace=True)
        df = df[["Total Consumed energy (kWh)"]]
    df = df.iloc[1:]
    #print(df.head(10))
    return df


def align_resample(df, interval, tmzn):
    # df = df.groupby(df.index).max()
    # df.index = df.index.ceil('5T')
    # df = df.resample('5T', label='right', closed='right').max()
    

    ##########set timezone
    df['ts'] = df.index
    df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
    df.reset_index(drop=True, inplace=True)
    df.set_index('ts', inplace=True, drop=True)

    #resample at given interval to round timestamp
    res = int(interval)/1000
    df = df.resample(str(res)+'S').max()

    Amin=np.nan
    Bmin = np.nan
    Cmin = np.nan
    return df, Amin, Bmin, Cmin


def read_data(devid, acc_token, address, start_time, end_time, interval, descriptors, tmzn):

    if descriptors == 'totalEnergy':
        descriptors = 'cnrgA,cnrgB,cnrgC'
    if descriptors == 'totalAP':
        descriptors = 'pwrA,pwrB,pwrC'
    if descriptors == 'totalRP':
        descriptors = 'rpwrA,rpwrB,rpwrC'

    if ('cnrgA' in descriptors) or ('cnrgB' in descriptors) or ('cnrgC' in descriptors):
        aggfn = 'MAX'
    else:
        aggfn = 'AVG'

    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys=" + descriptors + "&startTs=" + start_time + "&endTs=" + end_time + "&interval="+interval+"&agg="+aggfn+"&limit=1000000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()

    if r2:
        df = pd.DataFrame([])
        if 'pwrA' in r2.keys():
            df1 = pd.DataFrame(r2['pwrA'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Average active power A (kW)']
            df1['Average active power A (kW)'] = df1['Average active power A (kW)'].astype('float')
            df1['Average active power A (kW)'] = df1['Average active power A (kW)'] / 1000
            df = pd.concat([df, df1], axis=1)
            del df1

        if 'pwrB' in r2.keys():
            df2 = pd.DataFrame(r2['pwrB'])
            df2.set_index('ts', inplace=True)
            df2.columns = ['Average active power B (kW)']
            df2['Average active power B (kW)'] = df2['Average active power B (kW)'].astype('float')
            df2['Average active power B (kW)'] = df2['Average active power B (kW)'] / 1000
            df = pd.concat([df, df2], axis=1)
            del df2

        if 'pwrC' in r2.keys():
            df3 = pd.DataFrame(r2['pwrC'])
            df3.set_index('ts', inplace=True)
            df3.columns = ['Average active power C (kW)']
            df3['Average active power C (kW)'] = df3['Average active power C (kW)'].astype('float')
            df3['Average active power C (kW)'] = df3['Average active power C (kW)'] / 1000
            df = pd.concat([df, df3], axis=1)
            del df3

        if 'rpwrA' in r2.keys():
            df4 = pd.DataFrame(r2['rpwrA'])
            df4.set_index('ts', inplace=True)
            df4.columns = ['Reactive Power A (kVAR)']
            df4['Reactive Power A (kVAR)'] = df4['Reactive Power A (kVAR)'].astype('float')
            df4['Reactive Power A (kVAR)'] = df4['Reactive Power A (kVAR)'] / 1000
            df = pd.concat([df, df4], axis=1)
            del df4

        if 'rpwrB' in r2.keys():
            df5 = pd.DataFrame(r2['rpwrB'])
            df5.set_index('ts', inplace=True)
            df5.columns = ['Reactive Power B (kVAR)']
            df5['Reactive Power B (kVAR)'] = df5['Reactive Power B (kVAR)'].astype('float')
            df5['Reactive Power B (kVAR)'] = df5['Reactive Power B (kVAR)'] / 1000
            df = pd.concat([df, df5], axis=1)
            del df5

        if 'rpwrC' in r2.keys():
            df6 = pd.DataFrame(r2['rpwrC'])
            df6.set_index('ts', inplace=True)
            df6.columns = ['Reactive Power C (kVAR)']
            df6['Reactive Power C (kVAR)'] = df6['Reactive Power C (kVAR)'].astype('float')
            df6['Reactive Power C (kVAR)'] = df6['Reactive Power C (kVAR)'] / 1000
            df = pd.concat([df, df6], axis=1)
            del df6


        if 'cnrgA' in r2.keys():
            df13 = pd.DataFrame(r2['cnrgA'])
            df13.set_index('ts', inplace=True)
            df13.columns = ['cnrgA']
            df = pd.concat([df, df13], axis=1)
            del df13

        if 'cnrgB' in r2.keys():
            df14 = pd.DataFrame(r2['cnrgB'])
            df14.set_index('ts', inplace=True)
            df14.columns = ['cnrgB']
            df = pd.concat([df, df14], axis=1)
            del df14

        if 'cnrgC' in r2.keys():
            df15 = pd.DataFrame(r2['cnrgC'])
            df15.set_index('ts', inplace=True)
            df15.columns = ['cnrgC']
            df = pd.concat([df, df15], axis=1)
            del df15


        if df.empty == False:

            df.reset_index(drop=False, inplace=True)
            df['ts'] = pd.to_datetime(df['ts'], unit='ms')
            # df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn).dt.tz_localize(None)

            df = df.sort_values(by=['ts'])
            df.reset_index(drop=True, inplace=True)
            df.set_index('ts', inplace=True, drop=True)
            for col in df.columns:
                df[col] = df[col].astype('float')

            [df, Amin, Bmin, Cmin] = align_resample(df, interval, tmzn)
            df = conv_to_consumption(df, interval, Amin, Bmin, Cmin)
            df['ts'] = df.index
            df.reset_index(drop=True, inplace=True)


            df['Date'] = [d.date() for d in df['ts']]
            df['Time ' + tmzn] = [d.time() for d in df['ts']]
            df = df.drop('ts', axis=1)
            # change order of columns
            cols = df.columns.tolist()
            cols = cols[-2:] + cols[:-2]
            df = df[cols]
        else:
            df = pd.DataFrame([])
    else:
        df = pd.DataFrame([])
        print('Empty json!')
    return df


def main(argv):
    startt = time.time()
    devset = argv[1]
    
    interval = str(argv[2])



    tmzn = 'Europe/Athens'


    path = '../multiCmpr files'

    path = path + '/'
    os.chdir(path)
    filename = 'Multi_Compare.xlsx'

    address = "http://localhost:8080"
    # address =  "https://m3.meazon.com"


    r = requests.post(address + "/api/auth/login",
                      json={'username': 'a.papagiannaki@meazon.com', 'password': 'eurobank'}).json()

    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']


    devices = devset.split(',')
    i=0
    with pd.ExcelWriter(filename) as writer:
        for device in devices:
            i=i+1
            devargs = device.split(';')
            devid = devargs[0]
            devName = devargs[1]
            devName = devName.replace(' ', '')
            devName = devName.replace('[', '')
            devName = devName.replace(']', '')
            devName = devName.replace('-', '')
            devName = devName.replace('"', '')
            devName = devName[:30]
            descriptors = devargs[2]
            start_time = str(int(devargs[3]) - int(interval))
            end_time = devargs[4]
            devName = devName+str(i)
            print('devname:',devName)

            summary = read_data(devid, acc_token, address, start_time, end_time, interval, descriptors, tmzn)

            if summary.empty == False:

                print("Writing device:",devName)
                summary.to_excel(writer, sheet_name=devName, index=False)
            else:
                df = pd.DataFrame({'There are no measurements for the selected period': []})
                df.to_excel(writer, sheet_name=devName, index=False)

    writer.save()
    writer.close()

    elapsed = time.time() - startt
    print("---  seconds ---", elapsed)


if __name__ == "__main__":
    sys.exit(main(sys.argv))





