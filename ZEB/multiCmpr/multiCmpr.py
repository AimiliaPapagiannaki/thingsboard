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


###########
def fill_dropped_nrg(df, nrg,interval):
    df['Timestamp'] = df.index
    print(df.head())
    for cnrg in nrg:
        dfnew = df[np.isfinite(df[cnrg])].copy()
        dropped = dfnew[dfnew[cnrg] < dfnew[cnrg].shift()]
        
        if dropped.empty == False:
            # keep endpoint of range of reseted values
            dropped['endpoint1'] = dfnew.index[dfnew[cnrg] > dfnew[cnrg].shift(-1)]
            
            # shift endpoints to match starting points and set last endpoint as the last instance of df
            dropped['endpoint'] = dropped['endpoint1'].shift(-1)
            dropped['endpoint'].iloc[-1] = df.index[-1]

            dropped.apply((lambda x: correct_dropped(x, cnrg, df,interval)), axis=1)
    
    df.drop('Timestamp',axis=1,inplace=True)

    return df


def correct_dropped(row, cnrg, df,interval):
    #df[cnrg].loc[row.Timestamp:row.endpoint] = np.sum([df[cnrg], df[cnrg].loc[row.endpoint1]])
    if df[cnrg].loc[row.Timestamp]>df[cnrg].loc[row.endpoint1-timedelta(minutes = interval)]:
        df[cnrg].loc[row.endpoint1] = df[cnrg].loc[row.Timestamp]
    else:
        df[cnrg].loc[row.Timestamp:row.endpoint] = np.sum([df[cnrg], np.abs(df[cnrg].loc[row.endpoint1]-df[cnrg].loc[row.Timestamp])])
###########################
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
        
        #df.loc[df["Total Consumed energy (kWh)"]<0,"Total Consumed energy (kWh)"] = np.nan 
    df = df.iloc[1:]
    #print(df.head(10))
    return df


def align_resample(df, interval, tmzn):
    # df = df.groupby(df.index).max()
    # df.index = df.index.ceil('5T')
    # df = df.resample('5T', label='right', closed='right').max()
    
    #############################
    #if 'cnrgA' in df.columns:
     #   df = fill_dropped_nrg(df, ['cnrgA', 'cnrgB', 'cnrgC'],5)	
 ###########################


    ##########set timezone
    df['ts'] = df.index
    df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
    df.reset_index(drop=True, inplace=True)
    df.set_index('ts', inplace=True, drop=True)

    #resample at given interval to round timestamp
    res = int(interval)/1000
    
    if int(interval)==86400000:
        print('daily')
        df = df.resample('D').max()
    else:
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
            
            if (('Average active power A (kW)' in df.columns) and ('Average active power B (kW)' in df.columns) and (
            'Average active power C (kW)' in df.columns)):
                df['total']  = df['Average active power A (kW)'] + df['Average active power B (kW)'] + df['Average active power C (kW)']
                df.rename(columns={"total": "Total Average active power (kW)"}, inplace=True)
                df = df[["Total Average active power (kW)"]]

            if (('Reactive Power A (kVAR)' in df.columns) and ('Reactive Power B (kVAR)' in df.columns) and (
            'Reactive Power C (kVAR)' in df.columns)):
                df['total']  = df['Reactive Power A (kVAR)'] + df['Reactive Power B (kVAR)'] + df['Reactive Power C (kVAR)']
                df.rename(columns={"total": "Total Reactive power (kVAR)"}, inplace=True)
                df = df[["Total Reactive power (kVAR)"]]
            
            
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
    ncol=0
    #summary = pd.DataFrame([])
    with pd.ExcelWriter(filename) as writer:
        for device in devices:
          i=i+1
          devargs = device.split(';')
          devid = devargs[0]
          devName = devargs[1]
                #devName = devName.replace(' ', '')
                #devName = devName.replace('[', '')
                #devName = devName.replace(']', '')
                #devName = devName.replace('-', '')
          devName = devName.replace('"', '')
                #devName = devName[:30]
          descriptors = devargs[2]
          start_time = str(int(devargs[3]) - int(interval))
          end_time = devargs[4]
          #devName = devName+str(i)
          print('devname:',devName)
    
          df = read_data(devid, acc_token, address, start_time, end_time, interval, descriptors, tmzn)
          print('ncol before:',ncol)
          if df.empty == False:
              df.columns=[str(devName)]
              summary = df.copy()
              summary['ts'] = summary.index
              summary.reset_index(drop=True, inplace=True)
              summary['Date'] = [d.date() for d in summary['ts']]
              summary['Time ' + tmzn] = [d.time() for d in summary['ts']]
              summary = summary.drop('ts', axis=1)
        # change order of columns
              cols = summary.columns.tolist()
              cols = cols[-2:] + cols[:-2]
              summary = summary[cols]
              summary.to_excel(writer, index=False, startcol=ncol)
              
              
              
          else:
              df = pd.DataFrame({devName: ['There are no measurements for the selected period']})
              summary = df.copy()
              summary.to_excel(writer, index=False, startcol=ncol)
              #df.to_excel(writer, sheet_name=devName, index=False)
          ncol = ncol+len(summary.columns)+1
          print('ncol after:',ncol)
    
    
    #if summary.empty == False:
     #   with pd.ExcelWriter(filename) as writer:
      #      summary.to_excel(writer, index=False)
    writer.save()
    writer.close()

    elapsed = time.time() - startt
    print("---  seconds ---", elapsed)


if __name__ == "__main__":
    sys.exit(main(sys.argv))





