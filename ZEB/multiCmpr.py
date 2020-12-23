#!/usr/bin/env python3
# This version is a copy of multi_overflow.py where the problem of overflow is solved. Return to stable if anything comes up.
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
import base64


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
def conv_to_consumption(df, interval, Amin, Bmin, Cmin, overA, overB, overC, deltaA, deltaB, deltaC, tsA, tsB, tsC):
    print('DELTAS:',deltaA,deltaB,deltaC)
    #deltaA=0
    #deltaB=0
    #deltaC=0
    #     convert cumulative energy to consumed energy
    if 'cnrgA' in df.columns:
        firstdif = df['cnrgA'].iloc[0] - Amin
        df['cnrgA'] = df['cnrgA'] / 1000
        #df['diffA'] = np.nan
        #df.diffA[((df.cnrgA.isna() == False) & (df.cnrgA.shift().isna() == False))] = df.cnrgA - df.cnrgA.shift()
        
        
        tmp = df[['cnrgA']].copy()
        tmp = tmp.dropna()
        print(tmp)
        tmp['diffA'] = np.nan
        tmp['diffA'] = tmp['cnrgA'] - tmp['cnrgA'].shift()
        
        ################## If year per month
        if ((int(interval)==2628000000) and overA!=0):
            tmp.loc[(tmp.index.month==tsA+1) & (tmp['diffA']>0),'diffA'] = tmp['diffA']+ (overA/1000) - (deltaA/1000)
            
            ndf=tmp.loc[tmp.index.month==tsA+1].copy()
            if ndf.empty:
                tmp.loc[(tmp.index.month==tsA),'diffA'] = tmp['diffA']+(overA/1000)
        ###################
        
        tmp.loc[tmp['diffA'].shift(-1)<0,'diffA'] = tmp.loc[tmp['diffA'].shift(-1)<0,'diffA'] + (deltaA/1000)
        tmp.loc[tmp['diffA']<0,'diffA'] = tmp.loc[tmp['diffA']<0,'diffA'] + (overA/1000) - (deltaA/1000)
        tmp = tmp[['diffA']]
        
        df = pd.concat([df,tmp],axis=1)
        
        # df.diffA.iloc[0] = firstdif / 1000
        df.rename(columns={"diffA": "Consumed energy (kWh) A"}, inplace=True)
        df.drop(['cnrgA'], axis=1, inplace=True)

    if 'cnrgB' in df.columns:
        firstdif = df['cnrgB'].iloc[0] - Bmin
        df['cnrgB'] = df['cnrgB'] / 1000
        #df['diffB'] = np.nan
        #df.diffB[(df.cnrgB.isna() == False) & (df.cnrgB.shift().isna() == False)] = df.cnrgB - df.cnrgB.shift()
        
        tmp = df[['cnrgB']].copy()
        tmp = tmp.dropna()
        print(tmp)
        tmp['diffB'] = np.nan
        tmp['diffB'] = tmp['cnrgB'] - tmp['cnrgB'].shift()
        
        ################## If year per month
        if ((int(interval)==2628000000) and overB!=0):
            tmp.loc[(tmp.index.month==tsB+1) & (tmp['diffB']>0),'diffB'] = tmp['diffB']+ (overB/1000) - (deltaB/1000)
            
            ndf=tmp.loc[tmp.index.month==tsB+1].copy()
            if ndf.empty:
                tmp.loc[(tmp.index.month==tsB),'diffB'] = tmp['diffB']+(overB/1000)
        ###################
        tmp.loc[tmp['diffB'].shift(-1)<0,'diffB'] = tmp.loc[tmp['diffB'].shift(-1)<0,'diffB'] + (deltaB/1000)
        tmp.loc[tmp['diffB']<0,'diffB'] = tmp.loc[tmp['diffB']<0,'diffB'] + (overB/1000) - (deltaB/1000)
        tmp = tmp[['diffB']]
        
        df = pd.concat([df,tmp],axis=1)
        
        # df.diffB.iloc[0] = firstdif / 1000
        df.rename(columns={"diffB": "Consumed energy (kWh) B"}, inplace=True)
        df.drop(['cnrgB'], axis=1, inplace=True)

    if 'cnrgC' in df.columns:
        firstdif = df['cnrgC'].iloc[0] - Cmin
        df['cnrgC'] = df['cnrgC'] / 1000
        #df['diffC'] = np.nan
        #df.diffC[(df.cnrgC.isna() == False) & (df.cnrgC.shift().isna() == False)] = df.cnrgC - df.cnrgC.shift()
        
        tmp = df[['cnrgC']].copy()
        tmp = tmp.dropna()
        tmp['diffC'] = np.nan
        tmp['diffC'] = tmp['cnrgC'] - tmp['cnrgC'].shift()
        ################## If year per month
        if ((int(interval)==2628000000) and overC!=0):
            tmp.loc[(tmp.index.month==tsC+1) & (tmp['diffC']>0),'diffC'] = tmp['diffC']+ (overC/1000) - (deltaC/1000)
            
            ndf=tmp.loc[tmp.index.month==tsC+1].copy()
            if ndf.empty:
                tmp.loc[(tmp.index.month==tsC),'diffC'] = tmp['diffC']+(overC/1000)
        ###################
        tmp.loc[tmp['diffC'].shift(-1)<0,'diffC'] = tmp.loc[tmp['diffC'].shift(-1)<0,'diffC'] + (deltaC/1000)
        tmp.loc[tmp['diffC']<0,'diffC'] = tmp.loc[tmp['diffC']<0,'diffC'] + (overC/1000) - (deltaC/1000)
        tmp = tmp[['diffC']]
        
        df = pd.concat([df,tmp],axis=1)
        
        # df.diffC.iloc[0] = firstdif / 1000
        df.rename(columns={"diffC": "Consumed energy (kWh) C"}, inplace=True)
        df.drop(['cnrgC'], axis=1, inplace=True)

    if (('Consumed energy (kWh) A' in df.columns) and ('Consumed energy (kWh) B' in df.columns) and (
            'Consumed energy (kWh) C' in df.columns)):
        df['total'] = np.nan
        df['total'] = df['Consumed energy (kWh) A'] + df['Consumed energy (kWh) B'] + df['Consumed energy (kWh) C']   
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
    print(int(interval))
    if int(interval)==86400000:
        print('daily')
        df = df.resample('D', label='left').max()
    elif int(interval)==2628000000:
        print('Month')
        df = df.resample('M', label='right',closed='right').max()
    else:
        df = df.resample(str(res)+'S', label='left').max()
   
    if int(interval)==3960000:
        df.index = df.index - timedelta(minutes=12)
    print(df.head())
    #df.to_csv('test_cnrg.csv')
    Amin=np.nan
    Bmin = np.nan
    Cmin = np.nan
    return df, Amin, Bmin, Cmin


def read_data(devid, acc_token, address, start_time, end_time, interval, descriptors, tmzn, yearmode):

    if descriptors == 'totalEnergy':
        descriptors = 'cnrgA,cnrgB,cnrgC'
    if descriptors == 'totalAP':
        descriptors = 'pwrA,pwrB,pwrC'
    if descriptors == 'totalRP':
        descriptors = 'rpwrA,rpwrB,rpwrC'
    
    descriptors=descriptors+',overflA,overflB,overflC,difH_A,difH_B,difH_C,difD_A,difD_B,difD_C,difM_A,difM_B,difM_C'
    print('descriptors:',descriptors)
    msg = 'zeb_yearly_service:m3aZ0n_zeb_yearly'
    msgb = msg.encode('ascii')
    b64 = base64.b64encode(msgb)
    
    
    if yearmode==1:
        r2 = requests.post("http://localhost:9009/getYearEnergy", json={'devices': devid, 'year': start_time, 'keys': descriptors}, headers={"Accept": "application/json","Authorization":"Basic " + str(b64)[1:]}).json()
        print(r2)
        r2=r2[devid]
        
    
    else:
    
        if ('cnrgA' in descriptors) or ('cnrgB' in descriptors) or ('cnrgC' in descriptors):
            aggfn = 'MAX'
        else:
            aggfn = 'AVG'
    
        r2 = requests.get(
            url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys=" + descriptors + "&startTs=" + start_time + "&endTs=" + end_time + "&interval="+interval+"&agg="+aggfn+"&limit=1000000",
            headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
        
    if r2:
        overA = 0
        overB = 0
        overC = 0
        deltaA = 0
        deltaB = 0
        deltaC = 0
        tsA=0
        tsB=0
        tsC=0
        print(r2.keys())
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
        if 'overflA' in r2.keys():
            overA = float(r2['overflA'][0]['value'])
            if interval=='3600000': # if Hour interval
                deltaA = float(r2['difH_A'][0]['value'])
            elif interval=='86400000': # if Day interval
                deltaA = float(r2['difD_A'][0]['value'])
            elif interval=='2628000000': # if Month interval
                deltaA = float(r2['difM_A'][0]['value'])
                tsA = int(r2['difM_A'][0]['ts'])
                tsA = int(datetime.datetime.fromtimestamp(tsA/1000, pytz.timezone(tmzn)).strftime('%m'))
                print('Month with overflow:',tsA)
            print(overA, deltaA)
            
        if 'overflB' in r2.keys():
            overB = float(r2['overflB'][0]['value'])
            if interval=='3600000': # if Hour interval
                deltaB = float(r2['difH_B'][0]['value'])
            elif interval=='86400000': # if Day interval
                deltaB = float(r2['difD_B'][0]['value'])
            elif interval=='2628000000': # if Month interval
                deltaB = float(r2['difM_B'][0]['value'])
                tsB = int(r2['difM_B'][0]['ts'])
                tsB = int(datetime.datetime.fromtimestamp(tsB/1000, pytz.timezone(tmzn)).strftime('%m'))
            print(overB, deltaB)
                
        if 'overflC' in r2.keys():
            overC = float(r2['overflC'][0]['value'])
            if interval=='3600000': # if Hour interval
                deltaC = float(r2['difH_C'][0]['value'])
            elif interval=='86400000': # if Day interval
                deltaC = float(r2['difD_C'][0]['value'])
            elif interval=='2628000000': # if Month interval
                deltaC = float(r2['difM_C'][0]['value'])
                tsC = int(r2['difM_C'][0]['ts'])
                tsC = int(datetime.datetime.fromtimestamp(tsC/1000, pytz.timezone(tmzn)).strftime('%m'))
            print(overC, deltaC)
        print(df)
        if df.empty == False:

            df.reset_index(drop=False, inplace=True)
            df['ts'] = pd.to_datetime(df['ts'], unit='ms')
            # df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn).dt.tz_localize(None)

            df = df.sort_values(by=['ts'])
            df.reset_index(drop=True, inplace=True)
            df.set_index('ts', inplace=True, drop=True)
            for col in df.columns:
                df[col] = df[col].astype('float')
            print('values after reading:',df.head())
            [df, Amin, Bmin, Cmin] = align_resample(df, interval, tmzn)
            df = conv_to_consumption(df, interval, Amin, Bmin, Cmin, overA, overB, overC, deltaA, deltaB, deltaC, tsA, tsB, tsC)
            
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
          
          yearmode=0
          # if year per month
          if len(str(devargs[3]))==4:
              yearmode=1
              start_time = str(devargs[3])
              print('Year:',start_time)
          else:
              start_time = str(int(devargs[3]) - int(interval))
          print('start time is:',start_time)
          end_time = devargs[4]
          #devName = devName+str(i)
          print('devname:',devName)
    
          df = read_data(devid, acc_token, address, start_time, end_time, interval, descriptors, tmzn, yearmode)
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
              if yearmode==0:
                  summary = summary.iloc[:-1]
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





