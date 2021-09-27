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


        
def conv_to_consumption(df, interval,Amin,Bmin,Cmin):
    #     convert cumulative energy to consumed energy
    
    energies = ['Consumed energy (kWh) A','Consumed energy (kWh) B','Consumed energy (kWh) C','Estimated consumed energy (kWh) A','Estimated consumed energy (kWh) B','Estimated consumed energy (kWh) C']
    mins = [Amin,Bmin,Cmin]
    
    for nrg in energies:
        
        if nrg in df.columns:
            #df['diff'] = np.nan
            if nrg=='Consumed energy (kWh) A':
                firstdif = df[nrg].iloc[0]-Amin
            elif nrg=='Consumed energy (kWh) B':
                firstdif = df[nrg].iloc[0]-Bmin
            elif nrg=='Consumed energy (kWh) C':
                firstdif = df[nrg].iloc[0]-Cmin
            else:
                firstdif = 0
            #df.loc[df[nrg]>1000000000,nrg] = np.nan
            tmp = df[[nrg]].copy()
            tmp = tmp.dropna()
            
            
            
            
            tmp['diff'] = np.nan
            tmp['diff'] = tmp[nrg] - tmp[nrg].shift()
            tmp = tmp[['diff',nrg]]
            
            
            #df = pd.concat([df,tmp],axis=1)

            #df.loc[((df[nrg].isna() == False) & (df[nrg].shift().isna() == False)),'diff'] = df[nrg] - df[nrg].shift()
            
            tmp.loc[tmp['diff']>100000000,['diff',nrg]] = np.nan
            tmp.loc[tmp['diff']<(-200), 'diff'] = tmp.loc[tmp['diff']<(-200), 'diff'].values+tmp.loc[tmp['diff'].shift(-1)<(-200),nrg].values
            tmp.loc[np.abs(tmp['diff'])>100000000,['diff',nrg]] = np.nan
			
            #print(df.loc[np.abs(df['diff'])>100000000,['diff',nrg]])
            tmp['diff'].iloc[0] = firstdif
            
            
            df.drop([nrg], axis=1, inplace=True)
            df = pd.concat([df,tmp['diff']],axis=1)
            df.rename(columns={"diff": nrg}, inplace = True)
            
            
            #df.drop([nrg], axis=1, inplace=True)
        
    

    if (('Consumed energy (kWh) A' in df.columns) and ('Consumed energy (kWh) B' in df.columns) and ('Consumed energy (kWh) C' in df.columns)):
        
        df['total'] = np.nan
        # df.loc[((df['Consumed energy (kWh) A'].isna() == False) | (df['Consumed energy (kWh) B'].isna() == False) | (
                # df['Consumed energy (kWh) C'].isna() == False)),'total'] = df['Consumed energy (kWh) A'] + df['Consumed energy (kWh) B'] + df['Consumed energy (kWh) C']
        #df.total = df['Consumed energy (kWh) A'] + df['Consumed energy (kWh) B'] + df['Consumed energy (kWh) C']
        df['total'] = df[['Consumed energy (kWh) A','Consumed energy (kWh) B','Consumed energy (kWh) C']].sum(axis=1)
        df.rename(columns={"total": "Total Consumed energy (kWh)"}, inplace = True)
        

    if (('Estimated consumed energy (kWh) A' in df.columns) and ('Estimated consumed energy (kWh) B' in df.columns) and ('Estimated consumed energy (kWh) C' in df.columns)):
        df['total'] = np.nan
        df.total[(df['Estimated consumed energy (kWh) A'].isna() == False) & (df['Estimated consumed energy (kWh) B'].isna() == False) & (
                df['Estimated consumed energy (kWh) C'].isna() == False)] = df['Estimated consumed energy (kWh) A'] + df['Estimated consumed energy (kWh) B'] + df['Estimated consumed energy (kWh) C']
        df.rename(columns={"total": "Total Estimated Consumed energy (kWh)"}, inplace = True)

    return df

def align_resample(df, interval,tmzn):
   
    # identify report interval and round to closest minute
    
    tmpdf = pd.DataFrame(df[df.columns[0]].dropna())
    tmpdf['minutes'] = tmpdf.index.minute
    
    tmpdf['interv'] = tmpdf['minutes'].shift(-1) - tmpdf['minutes']
    inter = int(tmpdf['interv'].value_counts().idxmax())
    
    if inter<1:
        inter = 1
    elif inter>int(interval): # If the user selects smaller interval than the report interval, then apply the report interval
        interval=str(inter)
        print('interval asked is smaller, new interval is ',interval)
    
    del tmpdf
    
    #df.index = df.index.round(str(inter) + 'T')
    
    
    #############################
    #if 'Consumed energy (kWh) A' in df.columns:
     #   df = fill_dropped_nrg(df, ['Consumed energy (kWh) A', 'Consumed energy (kWh) B', 'Consumed energy (kWh) C'],inter)	
 ###########################
    
    if ('Consumed energy (kWh) A' in df.columns): 
        # print(df.loc[(df['Consumed energy (kWh) A'] - df['Consumed energy (kWh) A'].shift())>100000,'Consumed energy (kWh) A'])
        # df.loc[(df['Consumed energy (kWh) A'] - df['Consumed energy (kWh) A'].shift())>100000,'Consumed energy (kWh) A'] = np.nan
        Amin = df['Consumed energy (kWh) A'].min()
        if df.index[df['Consumed energy (kWh) A']==Amin][0]>df.index[0]:
            Amin = df['Consumed energy (kWh) A'].iloc[0]
    else:
        Amin = np.nan
    if ('Consumed energy (kWh) B' in df.columns):
        # print(df.loc[(df['Consumed energy (kWh) B'] - df['Consumed energy (kWh) B'].shift())>100000,'Consumed energy (kWh) B'])	
        # df.loc[(df['Consumed energy (kWh) B'] - df['Consumed energy (kWh) B'].shift())>100000,'Consumed energy (kWh) B'] = np.nan
        Bmin = df['Consumed energy (kWh) B'].min()
        if df.index[df['Consumed energy (kWh) B']==Bmin][0]>df.index[0]:
            Bmin = df['Consumed energy (kWh) B'].iloc[0]
    else:
        Bmin = np.nan
    if ('Consumed energy (kWh) C' in df.columns):
        # print(df.loc[(df['Consumed energy (kWh) C'] - df['Consumed energy (kWh) C'].shift())>100000,'Consumed energy (kWh) C'])
        # df.loc[(df['Consumed energy (kWh) C'] - df['Consumed energy (kWh) C'].shift())>100000,'Consumed energy (kWh) C'] = np.nan
        Cmin = df['Consumed energy (kWh) C'].min()
        if df.index[df['Consumed energy (kWh) C']==Cmin][0]>df.index[0]:
            Cmin = df['Consumed energy (kWh) C'].iloc[0]
    else:
        Cmin = np.nan
    
        
  
      
    #df.index = df.index.map(lambda x: x.replace(second=0, microsecond=0))
    df = df.groupby(df.index).max()
    df.sort_index(inplace=True)
    
    ##########set timezone
    df['ts'] = df.index
    df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
    df.reset_index(drop=True, inplace=True)
    df.set_index('ts',inplace = True, drop = True)
    
    
    if int(interval)>=1440:
        res = 'D'
        side = 'left'
    else:
        res = interval+'T'
        side = 'left' 
    
    
    # resample df to given interval 
    if (('Consumed energy (kWh) A' in df.columns) or ('Consumed energy (kWh) B' in df.columns) or ('Consumed energy (kWh) C' in df.columns)):
          df = conv_to_consumption(df, interval,Amin,Bmin,Cmin)
          
          # Significant fix so that if consequtive cnrg is nan, empty cells will appear and not 0
          #df_nrg = df.resample(res,label = side,closed = side).sum().copy()
          df_nrg = df.resample(res,label = side,closed = side).agg(pd.Series.sum, min_count=1).copy()
          print('dfnrg',df_nrg)
		  
          if (('Consumed energy (kWh) A' in df.columns) and ('Consumed energy (kWh) B' in df.columns) and ('Consumed energy (kWh) C' in df.columns)):
            df_nrg = df_nrg[['Consumed energy (kWh) A','Consumed energy (kWh) B','Consumed energy (kWh) C','Total Consumed energy (kWh)']]
          else:
            df_nrg = df_nrg[['Consumed energy (kWh) A']]
			
             
    if (('Average active power A (kW)' in df.columns) and ('Average active power C (kW)' in df.columns) and ('Average active power B (kW)' in df.columns)):
      df_demand = df.resample(res,label = side, closed = side).max().copy()
      df_demand = df_demand[['Average active power A (kW)','Average active power B (kW)','Average active power C (kW)']]
      df_demand.rename(columns={"Average active power A (kW)": "Maximum active power A (kW)","Average active power B (kW)": "Maximum active power B (kW)","Average active power C (kW)": "Maximum active power C (kW)"}, inplace = True)
      
      df = df.resample(res,label = side, closed = side).mean()
      df.reset_index(inplace = True, drop = False)
      df.set_index('ts',inplace = True, drop = False)
      df = pd.concat([df,df_demand], axis = 1)
    else:
      df = df.resample(res,label = side, closed = side).mean()
      df.reset_index(inplace = True, drop = False)
      df.set_index('ts',inplace = True, drop = False)
    if (('Consumed energy (kWh) A' in df.columns) and ('Consumed energy (kWh) B' in df.columns) and ('Consumed energy (kWh) C' in df.columns)):
      df = df.drop(['Consumed energy (kWh) A','Consumed energy (kWh) B','Consumed energy (kWh) C','Total Consumed energy (kWh)'],axis = 1)
      df = pd.concat([df,df_nrg], axis = 1)
    elif ('Consumed energy (kWh) A' in df.columns):
      df = df.drop(['Consumed energy (kWh) A'],axis = 1)
      df = pd.concat([df,df_nrg], axis = 1)
    
    
    return df,Amin,Bmin,Cmin,side

def read_data(devid, acc_token, address, start_time, end_time, interval, descriptors, tmzn):

    # request all descriptors that have ever been assigned to this device
    r1 = requests.get(url = address+"/api/plugins/telemetry/DEVICE/"+devid+"/keys/timeseries",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    # keep estimated descriptors in dictionary and add them to list of asked descriptors
    estimated = {}
    estimated['pwrA'] = 'pwrAest'
    estimated['pwrB'] = 'pwrBest'
    estimated['pwrC'] = 'pwrCest'
    estimated['cnrgA'] = 'cnrgAest'
    estimated['cnrgB'] = 'cnrgBest'
    estimated['cnrgC'] = 'cnrgCest'
    
    
    descriptors = descriptors.split(",")
    descriptors = [x for x in descriptors if x in r1]
    print('descriptors:',descriptors)
    
    for est in estimated.keys():
        if est in descriptors:descriptors.append(estimated[est])
        
    descriptors = ','.join(descriptors)
    
    # mapping is a dictionary to map all occurencies of descriptors to descent column names
    mapping = {}
    mapping['pwrA'] = 'Average active power A (kW)'
    mapping['pwrB'] = 'Average active power B (kW)'
    mapping['pwrC'] = 'Average active power C (kW)'
    mapping['pwrAest'] = 'Average estimated active power A (kW)'
    mapping['pwrBest'] = 'Average estimated active power B (kW)'
    mapping['pwrCest'] = 'Average estimated active power C (kW)'
    mapping['rpwrA'] = 'Reactive Power A (kVAR)'
    mapping['rpwrB'] = 'Reactive Power B (kVAR)'
    mapping['rpwrC'] = 'Reactive Power C (kVAR)'
    mapping['cnrgA'] =  'Consumed energy (kWh) A'
    mapping['cnrgB'] =  'Consumed energy (kWh) B'
    mapping['cnrgC'] =  'Consumed energy (kWh) C'
    mapping['pnrgA'] =  'Produced energy (kWh) A'
    mapping['pnrgB'] =  'Produced energy (kWh) B'
    mapping['pnrgC'] =  'Produced energy (kWh) C'
    mapping['cnrgAest'] =  'Estimated consumed energy (kWh) A'
    mapping['cnrgBest'] =  'Estimated consumed energy (kWh) B'
    mapping['cnrgCest'] =  'Estimated consumed energy (kWh) C'
    mapping['nrg'] = 'Total Consumed Energy (kWh)'
    mapping['vltA'] = 'Voltage A'
    mapping['vltB'] = 'Voltage B'
    mapping['vltC'] = 'Voltage C'
    mapping['svltA'] = 'SVoltage A'
    mapping['svltB'] = 'SVoltage B'
    mapping['svltC'] = 'SVoltage C'
    mapping['curA'] = 'Current A'
    mapping['curB'] = 'Current B'
    mapping['curC'] = 'Current C'
    mapping['scurA'] = 'SCurrent A'
    mapping['scurB'] = 'SCurrent B'
    mapping['scurC'] = 'SCurrent C'
    mapping['ecur'] = 'eCurrent A'
    mapping['ecurB'] = 'eCurrent B'
    mapping['ecurC'] = 'eCurrent C'
    mapping['frq'] = 'Frequency'
    mapping['cosA'] = 'Power factor A'
    mapping['cosB'] = 'Power factor B'
    mapping['cosC'] = 'Power factor C'
    mapping['scosA'] = 'SPower factor A'
    mapping['scosB'] = 'SPower factor B'
    mapping['scosC'] = 'SPower factor C'
    mapping['tmp'] = 'Temperature (Celsius)'
    mapping['clhmd'] = 'Humidity \%'
    mapping['hmd'] = 'Humidity \%'
    mapping['bindc'] = 'Motion'
    mapping['ppb'] = 'VOC'
    mapping['batvlt'] = 'Battery voltage'
       
    # watt_div is dictionary with descriptors to be divided by 1000
    watt_div = ['Average active power A (kW)','Average active power B (kW)','Average active power C (kW)','Total active power (kW)','Reactive Power A (kVAR)','Reactive Power B (kVAR)','Reactive Power C (kVAR)','Average estimated active power A (kW)','Average estimated active power B (kW)','Average estimated active power C (kW)','Consumed energy (kWh) A','Consumed energy (kWh) B','Consumed energy (kWh) C','Total Consumed energy (kWh)','Estimated consumed energy (kWh) A','Estimated consumed energy (kWh) B','Estimated consumed energy (kWh) C','Total Estimated Consumed energy (kWh)','Maximum active power A (kW)','Maximum active power B (kW)','Maximum active power C (kW)']
    
    df = pd.DataFrame([])
    
    if descriptors:
        if int(end_time)-int(start_time)>(5*86400000):
            print('More than 5 days requested')
            offset = 5*86400000
        else:
            offset = 86400000
        svec = np.arange(int(start_time), int(end_time), offset) # 1 day
        for st in svec:
            en = st + offset - 1
        
            if int(end_time) - en <= 0: en = int(end_time)
            tmp = pd.DataFrame([])
        
            r2 = requests.get(url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys="+descriptors+"&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",
                headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
            print("keys length ",len(r2.keys()))
            if ((len(r2.keys())>0) & (len(descriptors)>0)):
                
                
        
                # read all descriptors at once
                for desc in r2.keys():
                    try:
                        df1 = pd.DataFrame(r2[desc])
                        df1.set_index('ts', inplace=True)
                        df1.columns = [mapping.get(str(desc))]
                        tmp = pd.concat([tmp,df1], axis = 1)
                    except:
                        continue
                           
        
                if not tmp.empty:
                
                    tmp.reset_index(drop=False, inplace=True)
                    tmp['ts'] = pd.to_datetime(tmp['ts'], unit='ms')
                    # df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn).dt.tz_localize(None)
            
                    # Set timestamp as index, convert all columns to float
                    tmp = tmp.sort_values(by=['ts'])
                    #df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
                    tmp.reset_index(drop=True, inplace=True)
                    tmp.set_index('ts',inplace = True, drop = True)
                    df = pd.concat([df, tmp])
                      
                      
    if not df.empty:
        for col in df.columns:
            df[col] = df[col].astype('float')
                
            
            
        [df,Amin,Bmin,Cmin,side] = align_resample(df, interval,tmzn)
        # df = conv_to_consumption(df, interval,Amin,Bmin,Cmin)
        
        
        for col in watt_div:
            if col in df.columns:
                df[col] = df[col]/1000 #divide by 1000 to convert W/Wh to kW/kWh
          
        
    
        # create additional columns with total value of three phases
        if (('Average active power A (kW)' in df.columns) and ('Average active power C (kW)' in df.columns) and ('Average active power B (kW)' in df.columns)):
            df['Total Average active power (kW)'] = df['Average active power A (kW)'] + df['Average active power B (kW)'] + df['Average active power C (kW)']
            
        if (('Average estimated active power A (kW)' in df.columns) and ('Average estimated active power C (kW)' in df.columns) and ('Average estimated active power B (kW)' in df.columns)):
            df['Total Average estimated active power (kW)'] = df['Average estimated active power A (kW)'] + df['Average estimated active power B (kW)'] + df['Average estimated active power C (kW)']
            
        if (('Reactive Power A (kVAR)' in df.columns) and ('Reactive Power C (kVAR)' in df.columns) and ('Reactive Power B (kVAR)' in df.columns)):
            df['Total Reactive Power (kVAR)'] = df['Reactive Power A (kVAR)'] + df['Reactive Power B (kVAR)'] + df['Reactive Power C (kVAR)']
            
        if (('Maximum active power A (kW)' in df.columns) and ('Maximum active power B (kW)' in df.columns) and ('Maximum active power C (kW)' in df.columns)):
            df['Total Maximum active power (kW)'] = df['Maximum active power A (kW)'] + df['Maximum active power B (kW)'] + df['Maximum active power C (kW)']

        # convert to given timezone and split Date and Time columns
        #df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
        
        # Round to 3 digits
        for col in df.columns:
            if col!='ts':
                df[col] = df[col].round(3)
        
        df['Date'] = [d.date() for d in df['ts']]
        df['Time '+tmzn] = [d.time() for d in df['ts']]
        df = df.drop('ts',axis = 1)
        
        
        
        # change order of columns
        cols = df.columns.tolist()
        cols = cols[-2:]+cols[:-2]
        df = df[cols]
        df = df.iloc[1:]
        
        if int(interval) == 1:
            while df.index[0].hour == 23:
                df = df.iloc[1:]
        if int(interval) < 1440:
            if df.index[-1].day != df.index[-2].day:
                df = df.iloc[:-1]
            
        
    else:
        df = pd.DataFrame([])
        side = ' '
        print('Empty json!')
    return df,side


def get_harmonics(start_time, end_time, devid, acc_token, interval, address,tmzn):
    
    start_time = str(int(start_time) + int(5*60000))
    r2 = requests.get(url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys=ihd03A,ihd05A,ihd07A,ihd03B,ihd05B,ihd07B,ihd03C,ihd05C,ihd07C&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    dfH = pd.DataFrame([])
    if len(r2.keys())>0:
        print(r2.keys())
        # read all descriptors at once
        for desc in r2.keys():
            df1 = pd.DataFrame(r2[desc])
            df1.set_index('ts', inplace=True)
            df1.columns = [str(desc)]
            dfH = pd.concat([dfH,df1], axis = 1)
        
        if dfH.empty == False:
        
            dfH.reset_index(drop=False, inplace=True)
            dfH['ts'] = pd.to_datetime(dfH['ts'], unit='ms')
            
            # Set timestamp as index, convert all columns to float
            dfH = dfH.sort_values(by=['ts'])
            dfH['ts'] = dfH['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
            dfH.reset_index(drop=True, inplace=True)
            dfH.set_index('ts',inplace = True,drop=True)
            for col in dfH.columns:
                dfH[col] = dfH[col].astype('float')
                dfH.rename(columns={col:col+' %'},inplace=True)
        
        dfH['ts'] = dfH.index
        dfH['Date'] = [d.date() for d in dfH['ts']]
        dfH['Time '+tmzn] = [d.time() for d in dfH['ts']]
        dfH = dfH.drop('ts',axis = 1)
        
        # change order of columns
        cols = dfH.columns.tolist()
        cols = cols[-2:]+cols[:-2]
        dfH = dfH[cols]
        #dfH = dfH.iloc[1:]
        
        print(dfH)
    return dfH
    

def fetch_tmp(start_time, end_time, assetid, acc_token, interval, address,tmzn):


    mapping={}
    mapping['tmp'] = 'Temperature (Celsius)'
    mapping['clhmd'] = 'Humidity \%'

    r3 = requests.get(
        url= address + "/api/plugins/telemetry/ASSET/"+ assetid +"/values/timeseries?keys=tmp,clhmd&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    dfT = pd.DataFrame([])
    if len(r3.keys())>0:
        
        # read all descriptors at once
        for desc in r3.keys():
            df1 = pd.DataFrame(r3[desc])
            df1.set_index('ts', inplace=True)
            df1.columns = [mapping.get(str(desc))]
            dfT = pd.concat([dfT,df1], axis = 1)
        
        if dfT.empty == False:
        
            dfT.reset_index(drop=False, inplace=True)
            dfT['ts'] = pd.to_datetime(dfT['ts'], unit='ms')
            
            # Set timestamp as index, convert all columns to float
            dfT = dfT.sort_values(by=['ts'])
            dfT['ts'] = dfT['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
            dfT.reset_index(drop=True, inplace=True)
            dfT.set_index('ts',inplace = True,drop=True)
            for col in dfT.columns:
                dfT[col] = dfT[col].astype('float')
                
                
        if int(interval)>=1440:
            res = 'D'
        else:
            res = str(interval)+'T'
        dfT = dfT.resample(res).mean()
        for col in dfT.columns:
            dfT[col] = dfT[col].round(2)
        
        dfT['ts'] = dfT.index
        dfT['Date'] = [d.date() for d in dfT['ts']]
        dfT['Time '+tmzn] = [d.time() for d in dfT['ts']]
        dfT = dfT.drop('ts',axis = 1)
        
        # change order of columns
        cols = dfT.columns.tolist()
        cols = cols[-2:]+cols[:-2]
        dfT = dfT[cols]
        dfT = dfT.iloc[1:]
        
        print(dfT)
        
    return dfT
        


def postproc(df):
    if ('Voltage A' in df.columns) and ('SVoltage A' in df.columns):
        df['Voltage A'] = df[['Voltage A','SVoltage A']].mean(axis = 1)
        df.drop('SVoltage A',axis = 1,inplace = True)
    if ('Voltage B' in df.columns) and ('SVoltage B' in df.columns):
        df['Voltage B'] = df[['Voltage B','SVoltage B']].mean(axis = 1)
        df.drop('SVoltage B',axis = 1,inplace = True)
    if ('Voltage C' in df.columns) and ('SVoltage C' in df.columns):
        df['Voltage C'] = df[['Voltage C','SVoltage C']].mean(axis = 1)
        df.drop('SVoltage C',axis = 1,inplace = True)
    
    if ('Current A' in df.columns) and ('SCurrent A' in df.columns):
        df['Current A'] = df[['Current A','SCurrent A']].mean(axis = 1)
        df.drop('SCurrent A',axis = 1,inplace = True)
    if ('Current B' in df.columns) and ('SCurrent B' in df.columns):
        df['Current B'] = df[['Current B','SCurrent B']].mean(axis = 1)
        df.drop('SCurrent B',axis = 1,inplace = True)
    if ('Current C' in df.columns) and ('SCurrent C' in df.columns):
        df['Current C'] = df[['Current C','SCurrent C']].mean(axis = 1)
        df.drop('SCurrent C',axis = 1,inplace = True)
    
        
    return df


def main(argv):
    
    
    startt = time.time()
    print(len(argv))
    if len(argv)==9:
        # input arguments
        entityName = str(argv[1])
        entityID = str(argv[2])
        reportID = str(argv[3])
        start_time = str(argv[4])
        end_time = str(argv[5]) 
        interval = str(argv[6])
        descriptors = str(argv[7])
        tmzn = str(argv[8])
    elif len(argv)==8:
        # input arguments
        entityName = "Smart_meter"
        entityID = str(argv[1])
        reportID = str(argv[2])
        start_time = str(argv[3])
        end_time = str(argv[4]) 
        interval = str(argv[5])
        descriptors = str(argv[6])
        tmzn = str(argv[7])
    
    
    path = '/home/iotsm/HttpServer_Andreas/xlsx files/'
        
    
    os.chdir(path)
    filename = entityName+'_'+start_time+'_'+end_time+'.xlsx'
   
    #address = "http://157.230.210.37:8081"
    address = "http://localhost:8080"

    
    r = requests.post(address + "/api/auth/login",json={'username': 'a.andrikopoulos19@meazon.com', 'password': 'andrikopMeazon13'}).json()

    start_time = str(int(start_time)- (5*60000))
    
    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']
    
    dfH52=pd.DataFrame([])
    dfH53=pd.DataFrame([])
    dfH605=pd.DataFrame([])
    if reportID == 'b':
        r1 = requests.get(url=address + '/api/relations/info?fromId=' + entityID + '&fromType=ASSET',
                          headers={'Content-Type': 'application/json', 'Accept': '*/*',
                                   'X-Authorization': acc_token}).json()
        sum_nrg = pd.DataFrame([])
        with pd.ExcelWriter(filename) as writer:
        
            
            
            for device in r1:

                # read ID and name of building's devices
                devid  = str(device['to']['id'])
                devName = str(device['toName'])
                print(devName)

                ##################################################################################
                if entityID == 'ed73a120-f73b-11e9-b4dc-013e65d2f65e': # if building is moxy
                    r2 = requests.get(url=address + '/api/device/'+devid,
                          headers = {'Content-Type': 'application/json', 'Accept': '*/*',
                                                         'X-Authorization': acc_token}).json()                                    
                                                         
                    label = str(r2['label'])
                    label = label.replace('-','_')
                    label = label.replace(' ','_')
                    
                    label=label[7:]
                    label = label.replace('_','')
                    devName=label
                    
                ##################################################################################
                #print('devname:',devName)
                [summary,side] = read_data(devid,acc_token,address, start_time, end_time, interval, descriptors,tmzn)
                
                # if panitsas get harmonics
                if entityID == '10cec210-d129-11e8-9e11-dd47b1d84573':
                    if devid=='902a5580-6938-11ea-9788-2bd444f36b4e': # 052
                        dfH52 = get_harmonics(start_time, end_time, devid, acc_token, interval, address,tmzn)
                        
                    elif devid=='803e6800-6938-11ea-9788-2bd444f36b4e': # 053
                        dfH53 = get_harmonics(start_time, end_time, devid, acc_token, interval, address,tmzn)
                    
                
                if summary.empty==False:
                    summary = postproc(summary)
                    if (int(interval)==1440 and tmzn=='America/Chicago'):
                        summary = summary.iloc[1:]
                
                    if ('Total Consumed energy (kWh)' in summary.columns):
                        new_row = {'Power meter':devName, 'Total consumed energy (kWh)':summary['Total Consumed energy (kWh)'].sum()}
                        sum_nrg = sum_nrg.append(new_row, ignore_index = True)     
                        sum_nrg.to_excel(writer,sheet_name = 'Summary', index = False)
                        
                    
                    elif ('Consumed energy (kWh) A' in summary.columns):
                        new_row = {'Power meter':devName, 'Total consumed energy (kWh)':summary['Consumed energy (kWh) A'].sum()}
                        sum_nrg = sum_nrg.append(new_row, ignore_index = True)  
                        sum_nrg.to_excel(writer,sheet_name = 'Summary', index = False)                
                    
                    
                    #if ((summary.index[0]<=pd.to_datetime(start_time,unit='ms').tz_localize('utc').tz_convert(tmzn)) & (side=='right')):
                    #    print('first row smaller than start time')
                    #    summary = summary.iloc[1:]
                    #elif ((summary.index[-1]>pd.to_datetime(end_time,unit='ms').tz_localize('utc').tz_convert(tmzn)) & (side=='left')):
                    #    print('last row bigger than end time')
                    #    summary = summary.iloc[:-1]
                    
                    if entityID == 'ed73a120-f73b-11e9-b4dc-013e65d2f65e': # if building is moxy, don't write separate A,B,C columns
                        if 'Average active power A (kW)' in summary.columns:
                            summary = summary.drop(['Average active power A (kW)','Maximum active power A (kW)'],axis=1)
                        if 'Average active power B (kW)' in summary.columns:
                            summary = summary.drop(['Average active power B (kW)','Maximum active power B (kW)'],axis=1)
                        if 'Average active power C (kW)' in summary.columns:
                            summary = summary.drop(['Average active power C (kW)','Maximum active power C (kW)'],axis=1)
                            
                        if 'Average estimated active power A (kW)' in summary.columns:
                            summary = summary.drop(['Average estimated active power A (kW)'],axis=1)
                        if 'Average estimated active power B (kW)' in summary.columns:
                            summary = summary.drop(['Average estimated active power B (kW)'],axis=1)
                        if 'Average estimated active power C (kW)' in summary.columns:
                            summary = summary.drop(['Average estimated active power C (kW)'],axis=1)
                            
                        if 'Reactive Power A (kVAR)' in summary.columns:
                            summary = summary.drop(['Reactive Power A (kVAR)'],axis = 1)
                        if 'Reactive Power B (kVAR)' in summary.columns:
                            summary = summary.drop(['Reactive Power B (kVAR)'],axis = 1)
                        if 'Reactive Power C (kVAR)' in summary.columns:
                            summary = summary.drop(['Reactive Power C (kVAR)'],axis = 1)
                        if 'Estimated consumed energy (kWh) A' in summary.columns:
                            summary = summary.drop(['Estimated consumed energy (kWh) A'],axis=1)
                        if 'Estimated consumed energy (kWh) B' in summary.columns:
                            summary = summary.drop(['Estimated consumed energy (kWh) B'],axis=1)
                        if 'Estimated consumed energy (kWh) C' in summary.columns:
                            summary = summary.drop(['Estimated consumed energy (kWh) C'],axis=1)
                    
                    devName = devName.replace(':','')
   
                    summary.to_excel(writer,sheet_name = devName, index = False, engine = 'xlsxwriter')
                    
                else:
                    summary = pd.DataFrame({'There are no measurements for the selected period':[]})
                    devName = devName.replace(':','')

                    summary.to_excel(writer,sheet_name = devName, index = False)

            if entityID == 'ed73a120-f73b-11e9-b4dc-013e65d2f65e':
                assetid = 'ed73a120-f73b-11e9-b4dc-013e65d2f65e'
                dfT = fetch_tmp(start_time, end_time, assetid, acc_token, interval,address,tmzn)
                if not dfT.empty:
                    dfT.to_excel(writer,sheet_name = 'Temperature_Humidity', index = False, engine = 'xlsxwriter')
            if not dfH52.empty:
                dfH52.to_excel(writer,sheet_name = 'Harmonics_102.402.000052', index = False, engine = 'xlsxwriter')
            if not dfH53.empty:
                dfH53.to_excel(writer,sheet_name = 'Harmonics_102.402.000053', index = False, engine = 'xlsxwriter')
            
        writer.save()
        writer.close()
    else:
        # read ID and name of building's devices
        devid = str(entityID)
        devName = str(entityName)
        
        # if Ditiki Makedonia get harmonics
        
        if devid == 'b5977640-0bf8-11ec-ab8f-ef1ea7f487fc':
            dfH605 = get_harmonics(start_time, end_time, devid, acc_token, interval, address,tmzn)
                        
  
        print('devid:', devid)
        sum_nrg = pd.DataFrame([])
        [summary,side] = read_data(devid, acc_token, address, start_time, end_time, interval, descriptors, tmzn)
        
        if summary.empty==False:
            summary = postproc(summary)
            if (int(interval)==1440 and tmzn=='America/Chicago'):
                summary = summary.iloc[1:]
            
            #if ((summary.index[0]<=pd.to_datetime(start_time,unit='ms').tz_localize('utc').tz_convert(tmzn)) & (side=='right')):
            #    print('first row smaller than start time')
            #    summary = summary.iloc[1:]
            #elif ((summary.index[-1]>pd.to_datetime(end_time,unit='ms').tz_localize('utc').tz_convert(tmzn)) & (side=='left')):
            #    print('last row bigger than end time')
            #    summary = summary.iloc[:-1]
            
            with pd.ExcelWriter(filename) as writer:
                if ((summary.index[0]<=pd.to_datetime(start_time,unit='ms').tz_localize('utc').tz_convert(tmzn)) & (side=='right')):
                    print('first row smaller than start time')
                    summary = summary.iloc[1:]
                elif ((summary.index[-1]>pd.to_datetime(end_time,unit='ms').tz_localize('utc').tz_convert(tmzn)) & (side=='left')):
                    print('last row bigger than end time')
                    summary = summary.iloc[:-1]
                    
                if ('Total Consumed energy (kWh)' in summary.columns):
                    new_row = {'Power meter':devName, 'Total consumed energy (kWh)':summary['Total Consumed energy (kWh)'].sum()}
                    sum_nrg = sum_nrg.append(new_row, ignore_index = True)
                    sum_nrg.to_excel(writer,sheet_name = 'Summary', index = False)
                elif ('Consumed energy (kWh) A' in summary.columns):
                    new_row = {'Power meter':devName, 'Total consumed energy (kWh)':summary['Consumed energy (kWh) A'].sum()}
                    sum_nrg = sum_nrg.append(new_row, ignore_index = True)  
                    sum_nrg.to_excel(writer,sheet_name = 'Summary', index = False) 
                if summary.empty==False:
                    devName = devName.replace(':','')
                    summary.to_excel(writer, sheet_name=devName, index=False)
                else:
                    summary = pd.DataFrame({'There are no measurements for the selected period':[]})
                    devName = devName.replace(':','')
                
                if not dfH605.empty:
                    dfH605.to_excel(writer,sheet_name = 'Harmonics_102.402.000605', index = False, engine = 'xlsxwriter')
            writer.save()
            writer.close()
    
    elapsed = time.time() - startt
    print("---  seconds ---" , elapsed)
if __name__ == "__main__":
    sys.exit(main(sys.argv))





