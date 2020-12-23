#!/usr/bin/env python3

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

import smtplib
from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
# from datetime import datetime

#address = "https://m3.meazon.com"
address = "http://localhost:8080"

def send_email(email_recipient, email_subject, email_message):
    email_sender = 'a.papagiannaki@meazon.com'
    try:
        msg = MIMEMultipart()
        msg['From'] = email_sender
        msg['To'] = email_recipient[0]
        msg['Subject'] = email_subject
    
        msg.attach(MIMEText(email_message, 'plain'))
    
        
    
        server = smtplib.SMTP('smtp-mail.outlook.com', 587)
        server.ehlo()
        server.starttls()
        server.login('a.papagiannaki@meazon.com', '2817emily!')
        text = msg.as_string()
        server.sendmail(email_sender, email_recipient, text)
        print('email sent')
        server.quit()
    except:
        print("SMPT server connection error")
    return True
    

def align_resample(df, cnrg,pwr,rpwr,frun,pvind,pvser):

  
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y/%m/%d %H:%M:%S.%f')
    #df['Timestamp'] = df['Timestamp'].dt.tz_localize('utc')
    df = df.sort_values(by="Timestamp")

    df['Timestamp'] = df['Timestamp'].astype('datetime64[s]')
    df = df.set_index('Timestamp', drop=True)


    for col in df.columns:
        df[col] = df[col].astype(float)
    
    
    # reverse pwr sign to matvh other PV
    if pvind==1:
        if pvser=='102.402.000110':
            for pr in pwr:
                if pr in df.columns:
                    df[pr] = -df[pr]
            for rpr in rpwr:
                if rpr in df.columns:
                    df[rpr] = -df[rpr]
            
    # identify report interval and round to closest minute
    #tmpdf = pd.DataFrame(df[df.columns[0]].dropna())
    #tmpdf['minutes'] = tmpdf.index.minute
    #tmpdf['interv'] = tmpdf['minutes'].shift(-1) - tmpdf['minutes']
    #interval = int(tmpdf['interv'].value_counts().idxmax())
    #del tmpdf
    df.index = df.index.map(lambda x: x.replace(second=0))

    df = df.groupby(df.index).max()
    df.sort_index(inplace=True)
    
    overfl={}
    

    
    
    if pvind==0:
        for nrg in cnrg:
            
            tmp = df[[nrg]].copy()
            tmp = tmp.dropna()
            # check if there has been overflow and add values
            if not frun:
                # if there has been an overflow, add values
                if path.exists("/home/iotsm/Analytics/jsonfiles/"+str(pvser.replace('.','').replace(':',''))+".json"):
                    
                    with open('/home/iotsm/Analytics/jsonfiles/'+str(pvser.replace('.','').replace(':',''))+'.json') as f:
                        tmpdict = json.load(f)
                    print('PAST OVERFLOW',tmpdict[nrg])
                    tmp[nrg] = tmp[nrg]+tmpdict[nrg]
                    
            
            #df.loc[df['diff']<0, 'diff'] = df.loc[df['diff']<0, 'diff'].values+df.loc[df['diff'].shift(-1)<0,nrg].values
            
            # if next value is smaller, but next-previous is still cumulative, consider it a spike and make it nan -->positive spike
            print(tmp.loc[(((tmp[nrg].shift(-1)-tmp[nrg])<(0)) & ((tmp[nrg].shift(-1)-tmp[nrg].shift())>=0)),nrg])
            tmp.loc[(((tmp[nrg].shift(-1)-tmp[nrg])<(0)) & ((tmp[nrg].shift(-1)-tmp[nrg].shift())>=0)),nrg] = tmp[nrg].shift()
            
            # if next value is smaller, but second next-current is still cumulative, consider it a spike and make it nan -->negative spike
            tmp.loc[(((tmp[nrg]-tmp[nrg].shift())<(0)) & ((tmp[nrg].shift(-1)-tmp[nrg].shift())>0)),nrg] = tmp[nrg].shift()
            
            if not tmp.loc[(tmp[nrg].shift(-1)-tmp[nrg])<(-200)].empty:
                print('Found negative dE')
                ind = tmp.index[(tmp[nrg].shift(-1)-tmp[nrg])<(-200)].values[0]
                val = (tmp.loc[(tmp[nrg].shift(-1)-tmp[nrg])<(-200),nrg].values[0] - tmp.loc[(tmp[nrg]-tmp[nrg].shift())<(-100),nrg].values[0]) 
                print('Index and val of overflow:',ind,val)
                
                tmp.loc[tmp.index>ind,nrg] += val
                
                overfl[nrg] = val
                
            df = df.drop(nrg,axis=1)
            df = pd.concat([df,tmp],axis=1)
            del tmp
                
        if overfl:
            jfile = pvser.replace('.','').replace(':','')
            with open('/home/iotsm/Analytics/jsonfiles/'+str(jfile)+'.json', 'w') as f:
                json.dump(overfl, f)
                
            
                    
            
            
            
    # sum energies into one variable        
    
    df['totalMeanpwr'] = np.zeros(df.shape[0])
    df['totalRpwr'] = np.zeros(df.shape[0])
    
    for pr in pwr:
            df['totalMeanpwr'] = df['totalMeanpwr'] + df[pr]
    
    for rpr in rpwr:
        if rpr in df.columns:
            df['totalRpwr'] = df['totalRpwr'] + df[rpr]
        else:
            df['totalRpwr'] = np.nan
    
    df = df.resample('5T',label = 'left').mean().copy()
    df1 = df.resample('5T',label = 'left').max().copy()
    
    
    df1['totalCnrg'] = np.zeros(df.shape[0])
    for nrg in cnrg:
        df1['totalCnrg'] = df1['totalCnrg'] + df1[nrg]    
        
    df = pd.concat([df,df1['totalCnrg']],axis=1)
    del df1
            
    ######### check if there are nan values in cnrg
    if frun==1:
    
        for i in range(1,df.shape[0]):
            if np.isnan(df['totalCnrg'].iloc[i]):
                df['totalCnrg'].iloc[i] = df['totalCnrg'].iloc[i-1]
                #print('Found nans, new value is:',df_demand['totalCnrg'].iloc[i])

    # df.reset_index(inplace=True, drop=False)
    # df.set_index('ts', inplace=True, drop=False)
    
    df = df[['totalCnrg', 'totalMeanpwr','totalRpwr']]
######    print('df after concat:',df.head())

    return df


def download_nrg(start_date, end_date, devid,acc_token,pvind,pvser):
    global address

    start_time = str(start_date)
    end_time = str(end_date)

    # make request to detect connection type of device
    r1 = requests.get(url=address + '/api/plugins/telemetry/DEVICE/'+devid+'/values/attributes/SERVER_SCOPE',
                      headers = {'Content-Type': 'application/json', 'Accept': '*/*',
                                                     'X-Authorization': acc_token}).json()
    # define regex to read at which Lines is the total consumption set

    tmp = next(item for item in r1 if item["key"] == "connectionType")
   
    conn_info = json.loads(tmp['value'])
    
    descr = []
    cnrg = []
    pwr = []
    rpwr=[]
    
    
        
        
    if len(conn_info)>0:
       
        if conn_info['type']=='TPMSP':
    
            if 'A' in conn_info['mainLine']:
                descr = descr + ['cnrgA','pwrA','rpwrA']
                cnrg =  cnrg + ['cnrgA']
                pwr =  pwr + ['pwrA']
                rpwr =  rpwr + ['rpwrA']
            if 'B' in conn_info['mainLine']:
                descr = descr + ['cnrgB', 'pwrB','rpwrB']
                cnrg = cnrg + ['cnrgB']
                pwr = pwr + ['pwrB']
                rpwr =  rpwr + ['rpwrB']
            if 'C' in conn_info['mainLine']:
                descr = descr + ['cnrgC', 'pwrC','rpwrC']
                cnrg = cnrg + ['cnrgC']
                pwr = pwr + ['pwrC']
                rpwr =  rpwr + ['rpwrC']
    
        elif conn_info['type'] == 'TWPM':
            if 'A' in conn_info['mainLine']:
                descr = descr + ['cnrgA','pwrA','rpwrA']
                cnrg =  cnrg + ['cnrgA']
                pwr =  pwr + ['pwrA']
                rpwr =  rpwr + ['rpwrA']
            if 'B' in conn_info['mainLine']:
                descr = descr + ['cnrgB', 'pwrB','rpwrB']
                cnrg = cnrg + ['cnrgB']
                pwr = pwr + ['pwrB']
                rpwr =  rpwr + ['rpwrB']
            if 'C' in conn_info['mainLine']:
                descr = descr + ['cnrgC', 'pwrC','rpwrC']
                cnrg = cnrg + ['cnrgC']
                pwr = pwr + ['pwrC']
                rpwr =  rpwr + ['rpwrC']
    
        elif conn_info['type'] == 'SPM':
            descr = descr + ['cnrgA', 'pwrA','rpwrA']
            cnrg = cnrg + ['cnrgA']
            pwr = pwr + ['pwrA']
            rpwr = rpwr + ['rpwrA']
            
    
        elif conn_info['type'] == 'TPMSPP' :
            if pvind==1:
                if pvser=='102.402.000110':
                    descr = descr + ['pnrgA','pnrgB','pnrgC', 'pwrA','pwrB','pwrC','rpwrA','rpwrB','rpwrC']
                    cnrg = cnrg + ['pnrgA','pnrgB','pnrgC']
                    
                else:
                    descr = descr + ['cnrgA','cnrgB','cnrgC', 'pwrA','pwrB','pwrC','rpwrA','rpwrB','rpwrC']
                    cnrg = cnrg + ['cnrgA','cnrgB','cnrgC']
                pwr = pwr + ['pwrA','pwrB','pwrC']
                rpwr = rpwr + ['rpwrA','rpwrB','rpwrC']
            else:
                descr = descr + ['cnrgA','cnrgB','cnrgC', 'pwrA','pwrB','pwrC','rpwrA','rpwrB','rpwrC']
                cnrg = cnrg + ['cnrgA','cnrgB','cnrgC']
                pwr = pwr + ['pwrA','pwrB','pwrC']
                rpwr = rpwr + ['rpwrA','rpwrB','rpwrC']
            
        
            
        descr = ','.join(descr)
       

    #r = requests.post(address + "/api/auth/login",
    #                  json={'username': 'meazon@thingsboard.org', 'password': 'meazon'}).json()

    # acc_token is the token to be used in the next request
    #acc_token = 'Bearer' + ' ' + r['token']

    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + str(devid) + "/values/timeseries?keys="+str(descr)+"&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()

    if len(descr)>0 and r2:
        df = pd.DataFrame([])

        if 'pwrA' in r2.keys():
            df1 = pd.DataFrame(r2['pwrA'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['pwrA']
            df1 = df1[~df1.index.duplicated(keep='first')]
            df = pd.concat([df, df1], axis=1, sort=True)

        if 'pwrB' in r2.keys():
            df2 = pd.DataFrame(r2['pwrB'])
            df2.set_index('ts', inplace=True)
            df2 = df2[~df2.index.duplicated(keep='first')]
            df2.columns = ['pwrB']
            df = pd.concat([df, df2], axis=1, sort=True)

        if 'pwrC' in r2.keys():
            df3 = pd.DataFrame(r2['pwrC'])
            df3.set_index('ts', inplace=True)
            df3 = df3[~df3.index.duplicated(keep='first')]
            df3.columns = ['pwrC']
            df = pd.concat([df, df3], axis=1, sort=True)
        if 'rpwrA' in r2.keys():
            df1 = pd.DataFrame(r2['rpwrA'])
            df1.set_index('ts', inplace=True)
            df1 = df1[~df1.index.duplicated(keep='first')]
            df1.columns = ['rpwrA']
            df = pd.concat([df, df1], axis=1, sort=True)

        if 'rpwrB' in r2.keys():
            df2 = pd.DataFrame(r2['rpwrB'])
            df2.set_index('ts', inplace=True)
            df2 = df2[~df2.index.duplicated(keep='first')]
            df2.columns = ['rpwrB']
            df = pd.concat([df, df2], axis=1, sort=True)

        if 'rpwrC' in r2.keys():
            df3 = pd.DataFrame(r2['rpwrC'])
            df3.set_index('ts', inplace=True)
            df3 = df3[~df3.index.duplicated(keep='first')]
            df3.columns = ['rpwrC']
            df = pd.concat([df, df3], axis=1, sort=True)

        if 'cnrgA' in r2.keys():
            df4 = pd.DataFrame(r2['cnrgA'])
            df4.set_index('ts', inplace=True)
            df4 = df4[~df4.index.duplicated(keep='first')]
            df4.columns = ['cnrgA']
            df = pd.concat([df, df4], axis=1, sort=True)

        if 'cnrgB' in r2.keys():
            df5 = pd.DataFrame(r2['cnrgB'])
            df5.set_index('ts', inplace=True)
            df5 = df5[~df5.index.duplicated(keep='first')]
            df5.columns = ['cnrgB']
            df = pd.concat([df, df5], axis=1, sort=True)

        if 'cnrgC' in r2.keys():
            df6 = pd.DataFrame(r2['cnrgC'])
            df6.set_index('ts', inplace=True)
            df6 = df6[~df6.index.duplicated(keep='first')]
            df6.columns = ['cnrgC']
            df = pd.concat([df, df6], axis=1, sort=True)
        if 'pnrgA' in r2.keys():
            df4 = pd.DataFrame(r2['pnrgA'])
            df4.set_index('ts', inplace=True)
            df4 = df4[~df4.index.duplicated(keep='first')]
            df4.columns = ['cnrgA']
            df = pd.concat([df, df4], axis=1, sort=True)

        if 'pnrgB' in r2.keys():
            df5 = pd.DataFrame(r2['pnrgB'])
            df5.set_index('ts', inplace=True)
            df5 = df5[~df5.index.duplicated(keep='first')]
            df5.columns = ['cnrgB']
            df = pd.concat([df, df5], axis=1, sort=True)

        if 'pnrgC' in r2.keys():
            df6 = pd.DataFrame(r2['pnrgC'])
            df6.set_index('ts', inplace=True)
            df6 = df6[~df6.index.duplicated(keep='first')]
            df6.columns = ['cnrgC']
            df = pd.concat([df, df6], axis=1, sort=True)

        df.reset_index(drop=False, inplace=True)
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')

        df = df.sort_values(by=['ts'])
        df.reset_index(drop=True, inplace=True)
        df = df.rename(columns={"ts": "Timestamp"})
        
        
        
        
                
        if pvser=='102.402.000110':
            
            cnrg=['cnrgA','cnrgB','cnrgC']
        

    else:
        df = pd.DataFrame([])
        print('Empty json!')

    return [df,cnrg,pwr,rpwr]



def get_energy_data(start_date, end_date, devid,acc_token,frun,pvind,pvser):
    
    if frun==1:
        energy = pd.DataFrame([])
        svec = np.arange(int(start_date),int(end_date),2592000000)
        for st in svec:
            en = st+2592000000-1
            
            if int(end_date)-en<=0: en = int(end_date)
            #print('start and end of iteration:',st,en)
            [tmp,cnrg,pwr,rpwr] = download_nrg(str(st), str(en),devid,acc_token,pvind,pvser)
            energy = pd.concat([energy,tmp],axis=0)
    else:
        [energy,cnrg,pwr,rpwr] = download_nrg(start_date, end_date, devid,acc_token,pvind,pvser)

    #print(energy)
    #if energy.empty==True:
    if cnrg[0] not in energy.columns:
        energy = pd.DataFrame([])
        return energy
    else:
        energy = align_resample(energy,cnrg,pwr,rpwr,frun,pvind,pvser)
        

        # energy['Timestamp'] = energy['Timestamp'].dt.tz_localize('utc')
        # energy.set_index('Timestamp', inplace=True, drop=False)

        print('Energy df has successfully been created')
        return energy

def transform_df(df):
    import datetime

    df['totalCnrg'] = df['totalCnrg'].apply(str)
    df['totalMaxpwr'] = df['totalMaxpwr'].apply(str)
    df['totalMeanpwr'] = df['totalMeanpwr'].apply(str)
    df['totalRpwr'] = df['totalRpwr'].apply(str)

    df['ts'] = df.index.values.astype(np.int64) // 10 ** 6

    # df['ts'] = df['ts'].astype(int)
    df.columns = ['totalCnrg','totalMaxpwr','totalMeanpwr','totalRpwr','ts']
    df.set_index('ts',inplace = True, drop = True)
    mydict = df.to_dict('index')

    return mydict
    
def transform_pv(df):
   
    
    df['totalPnrg'] = df['totalPnrg'].apply(str)
    df['totalMaxPVpwr'] = df['totalMaxPVpwr'].apply(str)
    df['totalPVpwr'] = df['totalPVpwr'].apply(str)
    if df['totalPVrpwr'].isnull().all(): 
        df.drop('totalPVrpwr',axis=1,inplace=True)
    else:
        df['totalPVrpwr'] = df['totalPVrpwr'].apply(str)

    df['ts'] = df.index.values.astype(np.int64) // 10 ** 6

    # df['ts'] = df['ts'].astype(int)
    if 'totalPVrpwr' in df.columns:
        df.columns = ['totalPnrg','totalMaxPVpwr','totalPVpwr','totalPVrpwr','ts']
    else:
        df.columns = ['totalPnrg','totalMaxPVpwr','totalPVpwr','ts']
    df.set_index('ts',inplace = True, drop = True)
    mydict = df.to_dict('index')

    return mydict


def thingsboard_http(acc_token, dev_token, mydict):
    # address = "https://m3.meazon.com"

    global address


    for key, value in mydict.items():
        # time.sleep(0.015)
        my_json = json.dumps({'ts': key, 'values': value})
        #print(my_json)
        r = requests.post(url=address + "/api/v1/" + dev_token + "/telemetry",
                           data=my_json, headers={'Content-Type': 'application/json', 'Accept': '*/*',
                                                  'X-Authorization': acc_token})
        #print('finished descriptor..')

    print('Finished writing descriptors')

    return

def main():
    startt = time.time()

    # input arguments -->DEH assetid
    entityID =  str('a457a6e0-676f-11ea-9788-2bd444f36b4e') #Aggregated meter
    DevToken = 'cuCNBRaiJ04PQtsKWVNv'

    if path.exists("/home/iotsm/Analytics/testfiles/test_dei.txt") == False:
        # First run. Create test file
        print('First run')
        frun=1
        os.mknod("/home/iotsm/Analytics/testfiles/test_dei.txt")
        # end_ = (datetime.datetime.now())
        end_ = datetime.datetime.utcnow()
        
        end_ = end_ - datetime.timedelta(seconds=end_.second%60,
                                          microseconds=end_.microsecond)
        end_ = end_ - datetime.timedelta(minutes=end_.minute,seconds=end_.second%60,microseconds=end_.microsecond)
        end_ = end_ - relativedelta(seconds=1)
        print('Running script at time:', end_)

        # start from 1/4/2020 12:00 AM
        start_time = int(1585688400000)
        end_time = int(end_.replace(tzinfo=pytz.utc).timestamp()) * 1000


    else:
        print('\n\n\n')
        print('No first run')
        frun = 0
        end_ = (datetime.datetime.utcnow())
        end_ = end_ - datetime.timedelta(minutes=end_.minute,seconds=end_.second%60,microseconds=end_.microsecond)
        start_ = end_ + relativedelta(minutes=-60)
        end_ = end_ - relativedelta(seconds=1)
        print('Running script at time:', end_)
        

        end_time = int(end_.replace(tzinfo=pytz.utc).timestamp()) * 1000
        start_time = int(start_.replace(tzinfo=pytz.utc).timestamp()) * 1000



    global address
    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazon@thingsboard.org', 'password': 'meazon'}).json()

    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']


    r1 = requests.get(url=address + '/api/relations/info?fromId=' + entityID + '&fromType=DEVICE',
                      headers={'Content-Type': 'application/json', 'Accept': '*/*',
                               'X-Authorization': acc_token}).json()


    dfpv = pd.DataFrame([])
    dfglob = pd.DataFrame([])
    
    cnrgdict = dict()
    for device in r1:

        # read ID and name of building's devices
        devid = str(device['to']['id'])
        devName = str(device['toName'])
        print(devName)
        pvind=0
        pvser=devName
        
        if ((devName=='102.402.000110') or (devName=='102.402.000124')):
            pvind=1
            print('PV FOUND')
            if ((devName=='102.402.000110') and (frun==1)):
                start_time=int(1601672400000)
            elif ((devName=='102.402.000124') and (frun==1)):
                start_time=int(1596834000000)
            
        print('TIME:',start_time, end_time)
        # Fetch cnrg and cnrgest data
        energy = get_energy_data(start_time, end_time, devid,acc_token,frun,pvind,pvser)
        
        # Store last cnrg value to json
        if frun==1:
            cnrgdict[devName] = energy['totalCnrg'].iloc[-1]
        else:
            if energy.empty:
                with open('/home/iotsm/Analytics/jsonfiles/cnrgjson.json') as f:
                    tmpdict = json.load(f)
                    print('tmpdict:',tmpdict)
                cnrgdict[devName] = tmpdict[devName]
            else:
                cnrgdict[devName] = energy['totalCnrg'].max()
                    
            
        print(energy.tail())
        
        if pvind==1:
            if energy.empty==False:
                dfpv= pd.concat([dfpv,energy],axis=1).fillna(method='ffill')
                dfpv = dfpv.groupby(by=dfpv.columns, axis=1).sum()
                #dfpv = pd.concat([dfpv, energy]).groupby(level=0).sum()
                
            else:
                dfpv['totalCnrg'].iloc[-1] = dfpv['totalCnrg'].iloc[-1] + tmpdict[devName]
            print('dfpv contat tail:',dfpv.tail())
            
            
        else:
            if energy.empty==False:
                dfglob= pd.concat([dfglob,energy],axis=1).fillna(method='ffill')
                dfglob = dfglob.groupby(by=dfglob.columns, axis=1).sum()
                #dfglob = pd.concat([dfglob, energy]).groupby(level=0).sum()
                
            else:
                if not dfglob.empty:
                    print('Empty series!Add previously stored cnrg:',dfglob['totalCnrg'].iloc[-1] + tmpdict[devName])
                    dfglob['totalCnrg'].iloc[-1] = dfglob['totalCnrg'].iloc[-1] + tmpdict[devName]
                else:
                    dfglob['totalCnrg'] = tmpdict[devName]
            print('dfglob contat tail:',dfglob.tail())
            
    
    # write latest cnrg of each device to json
    with open('/home/iotsm/Analytics/jsonfiles/cnrgjson.json', 'w') as f:
        json.dump(cnrgdict, f)
    #cnrgjson = json.dumps(cnrgdict)
    
    # resample on 1hour
    tmp = dfglob.resample('1H',label = 'left').mean().copy()
    dfglob = dfglob.resample('1H',label = 'left').max().copy()
    dfglob.drop('totalRpwr',axis=1,inplace=True)
    dfglob.rename(columns = {"totalMeanpwr":"totalMaxpwr"},inplace=True)

    dfglob = pd.concat([dfglob,tmp[['totalMeanpwr','totalRpwr']]],axis=1)
    del tmp
    
    tmp = dfpv.resample('1H',label = 'left').mean().copy()
    dfpv = dfpv.resample('1H',label = 'left').max().copy()
    dfpv.drop('totalRpwr',axis=1,inplace=True)
    dfpv.rename(columns = {"totalMeanpwr":"totalMaxpwr"},inplace=True)
    
    dfpv = pd.concat([dfpv,tmp[['totalMeanpwr','totalRpwr']]],axis=1)
    del tmp
      
    dfpv.rename(columns={"totalCnrg":"totalPnrg","totalMeanpwr":"totalPVpwr","totalMaxpwr":"totalMaxPVpwr","totalRpwr":"totalPVrpwr"},inplace=True)    
    
    print("###############################")
    print('dfglob:',dfglob.tail())
    print('dfpv:',dfpv.tail())
    
    if frun==0:
        oldt =  end_ + relativedelta(days=-9)
        oldtime= int(oldt.timestamp()) * 1000
        r2 = requests.get(url=address + "/api/plugins/telemetry/DEVICE/" + str(entityID) + "/values/timeseries?keys=totalPnrg,totalPVpwr,totalPVrpwr,totalCnrg,totalMeanpwr,totalMaxpwr,totalRpwr&startTs=" + str(oldtime) + "&endTs=" + str(end_time) + "&agg=NONE&limit=1000000",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
        
        df = pd.DataFrame([])
            
        # read all descriptors at once
        for desc in r2.keys():
            df1 = pd.DataFrame(r2[desc])
            df1.set_index('ts', inplace=True)
            df1.columns = [str(desc)]
            df = pd.concat([df,df1], axis = 1)
        for col in df.columns:
            df[col] = df[col].astype('float64')
        df.sort_index(inplace = True)
        df.dropna(inplace=True)
        print('OLD DF:',df)
        df = df.iloc[-1:]
        
           
    if dfglob.empty == False:
        
        if frun==0:
            print('old cnrg was:',df['totalCnrg'])
            print('new total cnrg:',dfglob['totalCnrg'])
            
            # If new energy is smaller send alarm via email
            if (dfglob['totalCnrg'].values<df['totalCnrg'].values):
                ## send email to recipient
                sbj =  'Negative energy alarm'
                msg = 'Negative energy in DEH. \n'
                recipients = ['a.papagiannaki@meazon.com']
                send_email(recipients,sbj,msg)
        # transform dfs
        print()
        mydict = transform_df(dfglob)
        
    else:
        print('empty df! old variables will be stored')
        mydict = {np.int64(end_time):{'totalCnrg':str(df['totalCnrg'].values[-1]),'totalMeanpwr':str(df['totalMeanpwr'].values[-1]),'totalMaxpwr':str(df['totalMaxpwr'].values[-1]),'totalRpwr':str(df['totalRpwr'].values[-1])}}
        
        
    if dfpv.empty == False:

        if frun==0:
            print('old Pnrg was:',df['totalPnrg'])
            print('new total Pnrg:',dfpv['totalPnrg'])
        pvdict = transform_pv(dfpv)
    else:
        pvdict = {np.int64(end_time):{'totalPnrg':str(df['totalPnrg'].values[-1]),'totalPVpwr':str(df['totalPVpwr'].values[-1]),'totalPVrpwr':str(df['totalPVrpwr'].values[-1])}}
        
    # write values to thingsboard
    thingsboard_http(acc_token, DevToken, mydict)
    thingsboard_http(acc_token, DevToken, pvdict)


    elapsed = time.time() - startt
    print("---  seconds ---", elapsed)


if __name__ == "__main__":
    sys.exit(main())





