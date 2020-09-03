#!/usr/bin/env python3 

import sys
import pandas as pd
import datetime
import requests
import numpy as np
from pandas import ExcelWriter
import os
import glob
import timezonefinder,pytz
from dateutil.tz import gettz
import timeit
# from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import time


import smtplib
from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path
address = "http://localhost:8080"
#
def conv_to_consumption(df, interval,Amin,Bmin,Cmin):
    #     convert cumulative energy to consumed energy
    
    energies = ['Consumed energy (kWh) A','Consumed energy (kWh) B','Consumed energy (kWh) C','Estimated consumed energy (kWh) A','Estimated consumed energy (kWh) B','Estimated consumed energy (kWh) C']
    mins = [Amin,Bmin,Cmin]
   
    for nrg in energies:
        
        if nrg in df.columns:
            df['diff'] = np.nan
            
            
            #df.loc[df[nrg]==4294967295,nrg] = np.nan
            if nrg=='Consumed energy (kWh) A':
                firstdif = df[nrg].iloc[0]-Amin
                
            elif nrg=='Consumed energy (kWh) B':
                firstdif = df[nrg].iloc[0]-Bmin
            elif nrg=='Consumed energy (kWh) C':
                firstdif = df[nrg].iloc[0]-Cmin
            else:
                firstdif = 0
            

            df.loc[((df[nrg].isna() == False) & (df[nrg].shift().isna() == False)),'diff'] = df[nrg] - df[nrg].shift()
            
            df.loc[df['diff']<0, 'diff'] = df.loc[df['diff']<0, 'diff'].values+df.loc[df['diff'].shift(-1)<0,nrg].values
            df['diff'].iloc[0] = firstdif
            
            df.drop([nrg], axis=1, inplace=True)
            df.rename(columns={"diff": nrg}, inplace = True)
            
            # calculate quantiles to remove outliers
            #print(df.loc[df[nrg]>(df[nrg].mean()+4*df[nrg].std()),nrg])
            df.loc[df[nrg]>100000000,nrg] = np.nan
            #df.drop([nrg], axis=1, inplace=True)
        
    

    if (('Consumed energy (kWh) A' in df.columns) and ('Consumed energy (kWh) B' in df.columns) and ('Consumed energy (kWh) C' in df.columns)):
        
        df['total'] = np.nan
        df.loc[((df['Consumed energy (kWh) A'].isna() == False) & (df['Consumed energy (kWh) B'].isna() == False) & (
                df['Consumed energy (kWh) C'].isna() == False)),'total'] = df['Consumed energy (kWh) A'] + df['Consumed energy (kWh) B'] + df['Consumed energy (kWh) C']
        #df.total = df['Consumed energy (kWh) A'] + df['Consumed energy (kWh) B'] + df['Consumed energy (kWh) C']
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
    if inter<1:inter = 1
    del tmpdf
    df.index = df.index.round(str(inter) + 'T')
    
    if ('Consumed energy (kWh) A' in df.columns):  
        Amin = df['Consumed energy (kWh) A'].min()        
    else:
        Amin = np.nan
    if ('Consumed energy (kWh) B' in df.columns):  
        Bmin = df['Consumed energy (kWh) B'].min() 
    else:
        Bmin = np.nan
    if ('Consumed energy (kWh) C' in df.columns): 
        Cmin = df['Consumed energy (kWh) C'].min()       
    else:
        Cmin = np.nan
    
        
  
      
    #df.index = df.index.map(lambda x: x.replace(second=0, microsecond=0))
    df = df.groupby(df.index).max()
    df.sort_index(inplace=True)
    
    if int(interval)>=1440:
        side = 'left'
    else:
        side = 'right' 
        
    ##########set timezone
    df['ts'] = df.index
    df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
    df.reset_index(drop=True, inplace=True)
    df.set_index('ts',inplace = True, drop = True)
    
    
    
    # resample df to given interval
    if (('Consumed energy (kWh) A' in df.columns) and ('Consumed energy (kWh) B' in df.columns) and ('Consumed energy (kWh) C' in df.columns)):
          df_nrg = df.resample(interval+'T',label = side,closed = side).max().copy()
          df_nrg = df_nrg[['Consumed energy (kWh) A','Consumed energy (kWh) B','Consumed energy (kWh) C']]
          
          if df.index[df['Consumed energy (kWh) A']==Amin][0]>df.index[0]:
              Amin = df['Consumed energy (kWh) A'].iloc[0]
          if df.index[df['Consumed energy (kWh) B']==Bmin][0]>df.index[0]:
              Bmin = df['Consumed energy (kWh) B'].iloc[0]
          if df.index[df['Consumed energy (kWh) C']==Cmin][0]>df.index[0]:
              Cmin = df['Consumed energy (kWh) C'].iloc[0]
              
    if (('Average active power A (kW)' in df.columns) and ('Average active power C (kW)' in df.columns) and ('Average active power B (kW)' in df.columns)):
      df_demand = df.resample(interval+'T',label = side,closed = side).max().copy()
      df_demand = df_demand[['Average active power A (kW)','Average active power B (kW)','Average active power C (kW)']]
      df_demand.rename(columns={"Average active power A (kW)": "Maximum active power A (kW)","Average active power B (kW)": "Maximum active power B (kW)","Average active power C (kW)": "Maximum active power C (kW)"}, inplace = True)
      
      
      df = df.resample(interval+'T',label = side, closed = side).mean()
      df.reset_index(inplace = True, drop = False)
      df.set_index('ts',inplace = True, drop = False)
      df = pd.concat([df,df_demand], axis = 1)
    else:
      df = df.resample(interval+'T',label = side, closed = side).mean()
      df.reset_index(inplace = True, drop = False)
      df.set_index('ts',inplace = True, drop = False)
    
    if (('Consumed energy (kWh) A' in df.columns) and ('Consumed energy (kWh) B' in df.columns) and ('Consumed energy (kWh) C' in df.columns)):
      df = df.drop(['Consumed energy (kWh) A','Consumed energy (kWh) B','Consumed energy (kWh) C'],axis = 1)
      df = pd.concat([df,df_nrg], axis = 1)
      
      
    
      
    
    return df,Amin,Bmin,Cmin

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
    mapping['cnrgAest'] =  'Estimated consumed energy (kWh) A'
    mapping['cnrgBest'] =  'Estimated consumed energy (kWh) B'
    mapping['cnrgCest'] =  'Estimated consumed energy (kWh) C'
    mapping['nrg'] = 'Total Consumed Energy (kWh)'
    mapping['vltA'] = 'Voltage A'
    mapping['vltB'] = 'Voltage B'
    mapping['vltC'] = 'Voltage C'
    mapping['svltA'] = 'Voltage A'
    mapping['svltB'] = 'Voltage B'
    mapping['svltC'] = 'Voltage C'
    mapping['curA'] = 'Current A'
    mapping['curB'] = 'Current B'
    mapping['curC'] = 'Current C'
    mapping['scurA'] = 'Current A'
    mapping['scurB'] = 'Current B'
    mapping['scurC'] = 'Current C'
    mapping['ecur'] = 'Current A'
    mapping['ecurB'] = 'Current B'
    mapping['ecurC'] = 'Current C'
    mapping['frq'] = 'Frequency'
    mapping['cosA'] = 'Power factor A'
    mapping['cosB'] = 'Power factor B'
    mapping['cosC'] = 'Power factor C'
    mapping['scosA'] = 'Power factor A'
    mapping['scosB'] = 'Power factor B'
    mapping['scosC'] = 'Power factor C'
       
    # watt_div is dictionary with descriptors to be divided by 1000
    watt_div = ['Average active power A (kW)','Average active power B (kW)','Average active power C (kW)','Total active power (kW)','Reactive Power A (kVAR)','Reactive Power B (kVAR)','Reactive Power C (kVAR)','Average estimated active power A (kW)','Average estimated active power B (kW)','Average estimated active power C (kW)','Consumed energy (kWh) A','Consumed energy (kWh) B','Consumed energy (kWh) C','Total Consumed energy (kWh)','Estimated consumed energy (kWh) A','Estimated consumed energy (kWh) B','Estimated consumed energy (kWh) C','Total Estimated Consumed energy (kWh)','Maximum active power A (kW)','Maximum active power B (kW)','Maximum active power C (kW)']
    
    r2 = requests.get(url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys="+descriptors+"&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    if r2:
        df = pd.DataFrame([])
        

        # read all descriptors at once
        for desc in r2.keys():
            df1 = pd.DataFrame(r2[desc])
            df1.set_index('ts', inplace=True)
            df1.columns = [mapping.get(str(desc))]
            df = pd.concat([df,df1], axis = 1)
                   
        

        if df.empty == False:
        
            df.reset_index(drop=False, inplace=True)
            df['ts'] = pd.to_datetime(df['ts'], unit='ms')
            # df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn).dt.tz_localize(None)
    
            # Set timestamp as index, convert all columns to float
            df = df.sort_values(by=['ts'])
            #df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
            df.reset_index(drop=True, inplace=True)
            df.set_index('ts',inplace = True, drop = True)
            for col in df.columns:
                df[col] = df[col].astype('float')
                
            
            
            [df,Amin,Bmin,Cmin] = align_resample(df, interval,tmzn)
            df = conv_to_consumption(df, interval,Amin,Bmin,Cmin)
            
            
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

            df['Date'] = [d.date() for d in df['ts']]
            df['Time '+tmzn] = [d.time() for d in df['ts']]
            
            df = df.drop('ts',axis = 1)
            
            # change order of columns
            cols = df.columns.tolist()
            cols = cols[-2:]+cols[:-2]
            df = df[cols]
            df['month'] = pd.DatetimeIndex(df['Date']).month
        else:
            df = pd.DataFrame([])
    else:
        df = pd.DataFrame([])
        print('Empty json!')
    return df



def find_timezone(acc_token,entityID):
    global address
    # Find location and timezone
    r1 = requests.get(url=address+'/api/plugins/telemetry/ASSET/'+entityID+'/values/attributes?latitude,longitude',headers={'Content-Type': 'application/json', 'Accept': '*/*','X-Authorization': acc_token}).json()
    
    lon = np.float(next(item for item in r1 if item["key"] == "longitude")['value'])
    lat = np.float(next(item for item in r1 if item["key"] == "latitude")['value'])
    
    tf = timezonefinder.TimezoneFinder()
    timezone_str = tf.certain_timezone_at(lat=lat, lng=lon)
    #print("Detected timezone str is: %s" % timezone_str)
    tmzn = pytz.timezone(timezone_str)
    #print("Converted to timezone: %s" % timezone)
    utc_timezone = pytz.timezone("UTC")

        
    dt = datetime.datetime.utcnow()
    
    
    # get 1st day of previous month
    endm = datetime.datetime(year = dt.year, month=dt.month, day=1, tzinfo=tmzn)
    
    # get last day of current month
    startm = endm - relativedelta(months=1)
    endm = endm - timedelta(seconds = 1) # 1
    
    
    
    print('start: %s, end: %s' % ( startm, endm ))
#    print('Treat TS as local:: start: %s, end: %s' % tuple(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(x.timestamp())) for x in (startm - utcoff, endm - utcoff)))
 #   print('Treat TS as GMT::  start: %s, end: %s' % tuple(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(x.timestamp())) for x in (startm - utcoff, endm - utcoff)))
    end_time = str(int((endm ).timestamp() * 1000))
    start_time = str(int((startm ).timestamp() * 1000))
    
    #print('Start and end of previous month {}, {} for timezone {}:'.format(start_time,end_time,timezone))
    return end_time,start_time,str(tmzn)

def send_email(email_recipient, email_subject, email_message, attachment_location = ''):
    email_sender = 'support@meazon.com'

    msg = MIMEMultipart()
    msg['From'] = email_sender
    msg['To'] = ", ".join(email_recipient)
    msg['Subject'] = email_subject

    msg.attach(MIMEText(email_message, 'plain'))
    
    if attachment_location != '':
        filename = os.path.basename(attachment_location)
        attachment = open(attachment_location, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
        msg.attach(part)
        
    try:
        server = smtplib.SMTP('smtp-mail.outlook.com', 587)
        server.ehlo()
        server.starttls()
        server.login('support@meazon.com', 'm3az0n2!')
        text = msg.as_string()
        server.sendmail(email_sender, email_recipient, text)
        print('email sent')
        server.quit()
    except:
        print("SMPT server connection error")
    return True
    


def main(argv):
    
    
    startt = time.time()
    
    # input arguments
    entityName = 'st-joseph-school'
    entityID = '78fd5420-4d72-11ea-8762-6bf954fc5af1'
    interval = '1440'
    descriptors = 'cnrgA,cnrgB,cnrgC'
    
    # request token
    global address
    #address = "http://localhost:8080"
    r = requests.post(address + "/api/auth/login",json={'username': 'meazon@thingsboard.org', 'password': 'meazon'}).json()
    acc_token = 'Bearer' + ' ' + r['token']
    
    # identify timezone, define start, end of month
    end_time,start_time,tmzn = find_timezone(acc_token,entityID)
    
    path = '/home/iotsm/Analytics/xlsx_files/'    
    os.chdir(path)
    filename = entityName+'_'+start_time+'_'+end_time+'.xlsx'
   
    

    
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

            r2 = requests.get(url=address + '/api/device/'+devid,
                      headers = {'Content-Type': 'application/json', 'Accept': '*/*',
                                                     'X-Authorization': acc_token}).json()
                                                     
                                                     
            label = str(r2['label'])
            devName = devName.replace(':','')
            label = label.replace('-','_')
            label = label.replace(' ','_')
            dev = label+'_'+devName
                        
            #print('devname:',devName)
            summary = read_data(devid,acc_token,address, start_time, end_time, interval, descriptors,tmzn)
            if (summary['month'].iloc[0] != summary['month'].iloc[1]):
                    summary = summary.iloc[1:]
            if (summary['month'].iloc[-1] != summary['month'].iloc[-2]):
                    summary = summary.iloc[:-1]
            summary.drop(['month'],axis=1, inplace=True)
            
            if ('Total Consumed energy (kWh)' in summary.columns):
                new_row = {'Power meter':dev, 'Total consumed energy (kWh)':summary['Total Consumed energy (kWh)'].sum()}
                sum_nrg = sum_nrg.append(new_row, ignore_index = True)     
                sum_nrg.to_excel(writer,sheet_name = 'Summary', index = False)
            elif ('Consumed energy (kWh) A' in summary.columns):
                new_row = {'Power meter':dev, 'Total consumed energy':summary['Consumed energy (kWh) A'].sum()}
                sum_nrg = sum_nrg.append(new_row, ignore_index = True)  
                sum_nrg.to_excel(writer,sheet_name = 'Summary', index = False)
            if summary.empty==False:
                summary.to_excel(writer,sheet_name = dev, index = False)
            else:
                df = pd.DataFrame({'There are no measurements for the selected period':[]})
                df.to_excel(writer,sheet_name = 'Sheet1', index = False)
            
    writer.save()
    writer.close()


    ## send email to recipient
    sbj =  'Monthly data export'
    msg = 'Attached you can find the previous month\'s energy consumption measurements. \n\n Respectfully,\n Meazon team'
    #recipients = ['emily.pijay@gmail.com','a.papagiannaki@meazon.com']
    recipients = ['peterd@hardtelectric.com','a.papagiannaki@meazon.com,n.betsos@meazon.com']
    send_email(recipients,sbj,msg,filename)
    ##
    elapsed = time.time() - startt
    print("---  seconds ---" , elapsed)
if __name__ == "__main__":
    sys.exit(main(sys.argv))



