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

def fill_dropped_nrg(df, nrg,interval):
    df['Timestamp'] = df.index
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
        
        
def conv_to_consumption(ndf, interval,Amin,Bmin,Cmin):
    df = ndf.copy()
    df.dropna(inplace=True)
    #     convert cumulative energy to consumed energy
    if 'cnrgA' in df.columns:
        firstdif = df['cnrgA'].iloc[0]-Amin
        df['cnrgA'] = df['cnrgA']/1000
        df['diffA'] = np.nan
        df.diffA[((df.cnrgA.isna() == False) & (df.cnrgA.shift().isna() == False))] = df.cnrgA - df.cnrgA.shift()
        df.diffA.iloc[0] = firstdif/1000  
        df.rename(columns={"diffA": "Consumed energy (kWh) A"}, inplace = True)
        ndf.drop(['cnrgA'], axis=1, inplace=True)
        ndf = pd.concat([ndf,df[['Consumed energy (kWh) A']]],axis = 1)


    if 'cnrgB' in df.columns:
        firstdif = df['cnrgB'].iloc[0]-Bmin
        df['cnrgB'] = df['cnrgB']/1000
        df['diffB'] = np.nan
        df.diffB[(df.cnrgB.isna() == False) & (df.cnrgB.shift().isna() == False)] = df.cnrgB - df.cnrgB.shift()
        df.diffB.iloc[0] = firstdif/1000
        df.rename(columns={"diffB": "Consumed energy (kWh) B"}, inplace=True)
        ndf.drop(['cnrgB'], axis=1, inplace=True)
        ndf = pd.concat([ndf,df[['Consumed energy (kWh) B']]],axis = 1)


    if 'cnrgC' in df.columns:
        firstdif = df['cnrgC'].iloc[0]-Cmin
        df['cnrgC'] = df['cnrgC']/1000
        df['diffC'] = np.nan
        df.diffC[(df.cnrgC.isna() == False) & (df.cnrgC.shift().isna() == False)] = df.cnrgC - df.cnrgC.shift()
        df.diffC.iloc[0] = firstdif/1000
        df.rename(columns={"diffC": "Consumed energy (kWh) C"}, inplace=True)
        ndf.drop(['cnrgC'], axis=1, inplace=True)
        ndf = pd.concat([ndf,df[['Consumed energy (kWh) C']]],axis = 1)

    if (('Consumed energy (kWh) A' in df.columns) and ('Consumed energy (kWh) B' in df.columns) and ('Consumed energy (kWh) C' in df.columns)):
        df['total'] = df['Consumed energy (kWh) A'] + df['Consumed energy (kWh) B'] + df['Consumed energy (kWh) C']
        df.rename(columns={"total": "Total Consumed energy (kWh)"}, inplace = True)
        ndf = pd.concat([ndf,df[['Total Consumed energy (kWh)']]],axis = 1)
    print('df after consumption:',ndf.tail())
    return ndf

def align_resample(df, interval,tmzn):

    #df.index = df.index.map(lambda x: x.replace(second=0, microsecond=0))
    df = df.groupby(df.index).max()
    df.index = df.index.ceil('5T')
    df =  df .resample('5T',label = 'right',closed = 'right').max()
    
  #############################
    df = fill_dropped_nrg(df, ['cnrgA', 'cnrgB', 'cnrgC'],5)	
 ###########################
 
    if ('cnrgA' in df.columns):  
      Amin = df['cnrgA'].min()
    else:
      Amin = np.nan
    if ('cnrgB' in df.columns):  
      Bmin = df['cnrgB'].min()
    else:
      Bmin = np.nan
    if ('cnrgC' in df.columns): 
      Cmin = df['cnrgC'].min()
    else:
      Cmin = np.nan
	
    
   # df = df.groupby(df.index).max()
   # df.sort_index(inplace=True)
    df = df.iloc[1:]
    
    
    ##########set timezone
    df['ts'] = df.index
    df['minut'] = [d.time().minute for d in df['ts']]
    if df['minut'].iloc[-1]==0:
      df = df.iloc[:-1]   
    df.drop('minut',axis=1, inplace=True)
     
    df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
    df.reset_index(drop=True, inplace=True)
    df.set_index('ts',inplace = True, drop = True)
    if int(interval)>=1440:
      res='D'
      side = 'left'
      closed='left'
    else:
      res=interval+'T'
      side = 'left'
      closed='left' 
   
    if (('cnrgA' in df.columns) and ('cnrgB' in df.columns) and ('cnrgC' in df.columns)):
      df_nrg = df.resample(res,label = side,closed = closed).max().copy()
      df_nrg = df_nrg[['cnrgA','cnrgB','cnrgC']]
      
      
	
    if (('Average active power A (kW)' in df.columns) and ('Average active power C (kW)' in df.columns) and ('Average active power B (kW)' in df.columns)):
      df_demand = df.resample(res,label=side, closed=closed).max().copy()
      df_demand = df_demand[['Average active power A (kW)','Average active power B (kW)','Average active power C (kW)']]
      df_demand.rename(columns={"Average active power A (kW)": "Maximum active power A (kW)","Average active power B (kW)": "Maximum active power B (kW)","Average active power C (kW)": "Maximum active power C (kW)"}, inplace = True)
      
      df = df.resample(res,label=side,closed=closed).mean()
      df.reset_index(inplace = True, drop = False)
      df.set_index('ts',inplace = True, drop = False)
      df = pd.concat([df,df_demand], axis = 1)
    else:
      df = df.resample(res,label=side,closed=closed).mean()
      df.reset_index(inplace = True, drop = False)
      df.set_index('ts',inplace = True, drop = False)
    if (('cnrgA' in df.columns) and ('cnrgB' in df.columns) and ('cnrgC' in df.columns)):
      df = df.drop(['cnrgA','cnrgB','cnrgC'],axis = 1)
      df = pd.concat([df,df_nrg], axis = 1)
   
   
    return df,Amin,Bmin,Cmin

def read_data(devid, acc_token, address, start_time, end_time, interval, descriptors, tmzn):

    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys="+descriptors+"&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    if r2:
        df = pd.DataFrame([])

        if 'pwrA' in r2.keys():
            df1 = pd.DataFrame(r2['pwrA'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Average active power A (kW)']
            df1['Average active power A (kW)'] = df1['Average active power A (kW)'].astype('float')
            df1['Average active power A (kW)'] = df1['Average active power A (kW)']/1000
            df = pd.concat([df,df1], axis = 1)
            del df1

        if 'pwrB' in r2.keys():
            df2 = pd.DataFrame(r2['pwrB'])
            df2.set_index('ts', inplace=True)
            df2.columns = ['Average active power B (kW)']
            df2['Average active power B (kW)'] = df2['Average active power B (kW)'].astype('float')
            df2['Average active power B (kW)'] = df2['Average active power B (kW)']/1000
            df = pd.concat([df,df2], axis=1)
            del df2

        if 'pwrC' in r2.keys():
            df3 = pd.DataFrame(r2['pwrC'])
            df3.set_index('ts', inplace=True)
            df3.columns = ['Average active power C (kW)']
            df3['Average active power C (kW)'] = df3['Average active power C (kW)'].astype('float')
            df3['Average active power C (kW)'] = df3['Average active power C (kW)']/1000
            df = pd.concat([df,df3], axis=1)
            del df3
            
        if 'rpwrA' in r2.keys():
            df4 = pd.DataFrame(r2['rpwrA'])
            df4.set_index('ts', inplace=True)
            df4.columns = ['Reactive Power A (kVAR)']
            df4['Reactive Power A (kVAR)'] = df4['Reactive Power A (kVAR)'].astype('float')
            df4['Reactive Power A (kVAR)'] = df4['Reactive Power A (kVAR)']/1000
            df = pd.concat([df,df4], axis=1)
            del df4

        if 'rpwrB' in r2.keys():
            df5 = pd.DataFrame(r2['rpwrB'])
            df5.set_index('ts', inplace=True)
            df5.columns = ['Reactive Power B (kVAR)']
            df5['Reactive Power B (kVAR)'] = df5['Reactive Power B (kVAR)'].astype('float')
            df5['Reactive Power B (kVAR)'] = df5['Reactive Power B (kVAR)']/1000
            df = pd.concat([df,df5], axis=1)
            del df5

        if 'rpwrC' in r2.keys():
            df6 = pd.DataFrame(r2['rpwrC'])
            df6.set_index('ts', inplace=True)
            df6.columns = ['Reactive Power C (kVAR)']
            df6['Reactive Power C (kVAR)'] = df6['Reactive Power C (kVAR)'].astype('float')
            df6['Reactive Power C (kVAR)'] = df6['Reactive Power C (kVAR)']/1000
            df = pd.concat([df,df6], axis=1)
            del df6

        if 'vltA' in r2.keys():
            df7 = pd.DataFrame(r2['vltA'])
            df7.set_index('ts', inplace=True)
            df7.columns = ['Voltage A']
            df = pd.concat([df,df7], axis=1)
            del df7

        if 'vltB' in r2.keys():
            df8 = pd.DataFrame(r2['vltB'])
            df8.set_index('ts', inplace=True)
            df8.columns = ['Voltage B']
            df = pd.concat([df,df8], axis=1)
            del df8

        if 'vltC' in r2.keys():
            df9 = pd.DataFrame(r2['vltC'])
            df9.set_index('ts', inplace=True)
            df9.columns = ['Voltage C']
            df = pd.concat([df,df9], axis=1)
            del df9

        if 'curA' in r2.keys():
            df10 = pd.DataFrame(r2['curA'])
            df10.set_index('ts', inplace=True)
            df10.columns = ['Current A']
            df = pd.concat([df,df10], axis=1)
            del df10

        if 'curB' in r2.keys():
            df11 = pd.DataFrame(r2['curB'])
            df11.set_index('ts', inplace=True)
            df11.columns = ['Current B']
            df = pd.concat([df,df11], axis=1)
            del df11

        if 'curC' in r2.keys():
            df12 = pd.DataFrame(r2['curC'])
            df12.set_index('ts', inplace=True)
            df12.columns = ['Current C']
            df = pd.concat([df,df12], axis=1)
            del df12

        if 'cnrgA' in r2.keys():
            df13 = pd.DataFrame(r2['cnrgA'])
            df13.set_index('ts', inplace=True)
            df13.columns = ['cnrgA']
            df = pd.concat([df,df13], axis=1)
            del df13

        if 'cnrgB' in r2.keys():
            df14 = pd.DataFrame(r2['cnrgB'])
            df14.set_index('ts', inplace=True)
            df14.columns = ['cnrgB']
            df = pd.concat([df,df14], axis=1)
            del df14

        if 'cnrgC' in r2.keys():
            df15 = pd.DataFrame(r2['cnrgC'])
            df15.set_index('ts', inplace=True)
            df15.columns = ['cnrgC']
            df = pd.concat([df,df15], axis=1)
            del df15

        if 'frq' in r2.keys():
            df16 = pd.DataFrame(r2['frq'])
            df16.set_index('ts', inplace=True)
            df16.columns = ['Frequency']
            df = pd.concat([df,df16], axis=1)
            del df16
            
        if 'cosA' in r2.keys():
            df17 = pd.DataFrame(r2['cosA'])
            df17.set_index('ts', inplace=True)
            df17.columns = ['Power factor A']
            df = pd.concat([df,df17], axis=1)
            del df17
            
        if 'cosB' in r2.keys():
            df18 = pd.DataFrame(r2['cosB'])
            df18.set_index('ts', inplace=True)
            df18.columns = ['Power factor B']
            df = pd.concat([df,df18], axis=1)
            del df18
            
        if 'cosC' in r2.keys():
            df19 = pd.DataFrame(r2['cosC'])
            df19.set_index('ts', inplace=True)
            df19.columns = ['Power factor C']
            df = pd.concat([df,df19], axis=1)
            del df19

        if df.empty == False:
        
            df.reset_index(drop=False, inplace=True)
            df['ts'] = pd.to_datetime(df['ts'], unit='ms')
            # df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn).dt.tz_localize(None)
    
            df = df.sort_values(by=['ts'])
            df.reset_index(drop=True, inplace=True)
            df.set_index('ts',inplace = True, drop = True)
            for col in df.columns:
                df[col] = df[col].astype('float')
    
            
            [df,Amin,Bmin,Cmin] = align_resample(df, interval,tmzn)
            
            df = conv_to_consumption(df, interval,Amin,Bmin,Cmin)
    
    
            if (('Average active power A (kW)' in df.columns) and ('Average active power C (kW)' in df.columns) and ('Average active power B (kW)' in df.columns)):
                df['Total Average active power (kW)'] = df['Average active power A (kW)'] + df['Average active power B (kW)'] + df['Average active power C (kW)']
            if (('Reactive Power A (kVAR)' in df.columns) and ('Reactive Power C (kVAR)' in df.columns) and ('Reactive Power B (kVAR)' in df.columns)):
                df['Total Reactive Power (kVAR)'] = df['Reactive Power A (kVAR)'] + df['Reactive Power B (kVAR)'] + df['Reactive Power C (kVAR)']
            if (('Maximum active power A (kW)' in df.columns) and ('Maximum active power B (kW)' in df.columns) and ('Maximum active power C (kW)' in df.columns)):
                df['Total Maximum active power (kW)'] = df['Maximum active power A (kW)'] + df['Maximum active power B (kW)'] + df['Maximum active power C (kW)']
    
            #df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
            df['Date'] = [d.date() for d in df['ts']]
            df['Time '+tmzn] = [d.time() for d in df['ts']]
            df = df.drop('ts',axis = 1)
            # change order of columns
            cols = df.columns.tolist()
            cols = cols[-2:]+cols[:-2]
            df = df[cols]
        else:
            df = pd.DataFrame([])
    else:
        df = pd.DataFrame([])
        print('Empty json!')
    return df

def main(argv):
    
    startt = time.time()
    
    entityName = str(argv[1])
    entityID = str(argv[2])
    reportID = str(argv[3])
    stime = str(argv[4])
    start_time = str(int(argv[4])-301000)
    end_time = str(argv[5]) 
    interval = str(argv[6])
    descriptors = str(argv[7])
    tmzn = str(argv[8])
    
    path = '../xlsx files'
        
    path = path+'/'
    os.chdir(path)
    filename = entityName+'_'+stime+'_'+end_time+'.xlsx'

    
    
    

    #address = "http://157.230.210.37:8081"
    address = "http://localhost:8080"

    
    r = requests.post(address + "/api/auth/login",json={'username': 'a.papagiannaki@meazon.com', 'password': 'eurobank'}).json()


    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']

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

                #print('devname:',devName)
                summary = read_data(devid,acc_token,address, start_time, end_time, interval, descriptors,tmzn)
                
                
                if summary.empty==False:
                    
                    #if int(interval)==1440:
                        #summary = summary.iloc[:-1]
                    if ('Total Consumed energy (kWh)' in summary.columns):
                        new_row = {'Power meter':devName, 'Total consumed energy (kWh)':summary['Total Consumed energy (kWh)'].sum()}
                        sum_nrg = sum_nrg.append(new_row, ignore_index = True)     
                        sum_nrg.to_excel(writer,sheet_name = 'Summary', index = False)
                    elif ('Consumed energy (kWh) A' in summary.columns):
                        new_row = {'Power meter':devName, 'Total consumed energy':summary['Consumed energy (kWh) A'].sum()}
                        sum_nrg = sum_nrg.append(new_row, ignore_index = True)  
                        sum_nrg.to_excel(writer,sheet_name = 'Summary', index = False)
                    
                    devName = devName.replace(':','_')
                    summary.to_excel(writer,sheet_name = devName, index = False)
                else:
                    df = pd.DataFrame({'There are no measurements for the selected period':[]})
                    df.to_excel(writer,sheet_name = devName, index = False)
                
        writer.save()
        writer.close()
    else:
        

        # read ID and name of building's devices
        devid = str(entityID)
        devName = str(entityName)
  
        print('devid:', devid)
        sum_nrg = pd.DataFrame([])
        summary = read_data(devid, acc_token, address, start_time, end_time, interval, descriptors, tmzn)
        
        if summary.empty==False:
            #if int(interval)==1440:
                #summary = summary.iloc[:-1]
            with pd.ExcelWriter(filename) as writer:
                if ('Total Consumed energy (kWh)' in summary.columns):
                    new_row = {'Power meter':devName, 'Total consumed energy (kWh)':summary['Total Consumed energy (kWh)'].sum()}
                    sum_nrg = sum_nrg.append(new_row, ignore_index = True)
                    sum_nrg.to_excel(writer,sheet_name = 'Summary', index = False)
                elif ('Consumed energy (kWh) A' in summary.columns):
                    new_row = {'Power meter':devName, 'Total consumed energy':summary['Consumed energy (kWh) A'].sum()}
                    sum_nrg = sum_nrg.append(new_row, ignore_index = True)  
                    sum_nrg.to_excel(writer,sheet_name = 'Summary', index = False) 
                if summary.empty==False:
                    devName = devName.replace(':','')
                    summary.to_excel(writer, sheet_name=devName, index=False)
        
            writer.save()
            writer.close()
    elapsed = time.time() - startt
    print("---  seconds ---" , elapsed)
if __name__ == "__main__":
    sys.exit(main(sys.argv))





