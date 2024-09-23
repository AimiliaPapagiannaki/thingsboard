#!/usr/bin/env python3 
import sys
import requests
import json
import csv
import datetime
import os
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import smtplib
from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import logging
BASEPATH = '/home/azureuser/HEDNOKPIs/voltageFailDetector/'

def send_email(device, label):
    """
    Send informative email
    """
    email_subject =  'Ειδοποίηση για πτώση τάσης/καμμένη ασφάλεια'
    email_message = 'Alarm στον μετασχηματιστή '+label+' με serial number '+device+'. \n\n Πιθανώς καμμένη ασφάλεια στη μέση τάση, ενεργήστε άμεσα για την αποκατάσταση του προβλήματος.'
    #email_recipient = ['a.papagiannaki@meazon.com','s.koutroubinas@meazon.com','s.kleftogiannis@meazon.com','g.siapalidis@deddie.gr','k.agavanakis@meazon.com']
    email_recipient = ['a.papagiannaki@meazon.com']
    email_bcc = ['s.koutroubinas@meazon.com','s.kleftogiannis@meazon.com','k.agavanakis@meazon.com']
    
    # Setup logging
    email_sender = 'support@meazon.com'
    rcpt = email_recipient
    # Logging recipient information
    logging.debug(f'Recipients: {rcpt}')
    
    msg = MIMEMultipart()
    msg['From'] = email_sender
    msg['To'] = ", ".join(email_recipient)
    msg['Subject'] = email_subject

    msg.attach(MIMEText(email_message, 'plain'))
            
    try:
        server = smtplib.SMTP('smtp-mail.outlook.com', 587)
        server.ehlo()
        server.starttls()
        #server.login('support@meazon.com', 'sup4m3aZ0n!')
        server.login('support@meazon.com', 'ege$#^$#jrhrtjYTKJTY54745')
        
        text = msg.as_string()
        server.sendmail(email_sender, rcpt, text)
        logging.info('Email sent successfully')
        print('email sent')
    except smtplib.SMTPException as e:
        logging.error(f'Failed to send email. SMTP error: {e}')
    except Exception as e:
        logging.error(f'Failed to send email. Error: {e}')
    finally:
        server.quit()
    
    #except:
        #print("SMPT server connection error")
    return True


def write_df(df, address, acc_token, devtoken):
    """
    Send telemetry to TB
    """
    for col in df.columns:
        tmp = df[[col]].copy().dropna()
        if not tmp.empty:
            tmp[col] = np.round(tmp[col],2)
            mydict = tmp.to_dict('index')
            l=[]
            for i, (key, value) in enumerate(mydict.items(), 1):
                newdict={}
                newdict['ts'] = key
                # newdict['values'] = value
                newdict['values'] = { k: v for k, v in value.items() if v == v }
                l.append(newdict)
            # write to json and send telemetry to TB
            my_json = json.dumps(l)          
            r = requests.post(url=address+"/api/v1/" + devtoken + "/telemetry",data=my_json, headers={'Content-Type': 'application/json', 'Accept': '*/*',
                                                        'X-Authorization': acc_token})


        
def get_dev_info(device, address):
    """
    Get device information
    """
    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()
    
    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']
    
    # get devid by serial name
    r1 = requests.get(
        url=address + "/api/tenant/devices?deviceName=" + device,
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    label = r1['label']
    devid = r1['id']['id']
    r1 = requests.get(
        url=address + "/api/device/" + devid + "/credentials",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    devtoken = r1['credentialsId']

    return devid,acc_token,label, devtoken

def read_latest(acc_token, devid, address, end_time, descriptors):
    """
    Retrieve raw data from TB
    """
    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys=" + descriptors + "&startTs=0&endTs=" + end_time + "&agg=NONE&limit=1",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    df = pd.DataFrame([])
    if r2:
        for desc in r2.keys():
            df = pd.DataFrame(r2[desc])
            df.set_index('ts', inplace=True)
            df.columns = [str(desc)]
    return df


def read_data(acc_token, devid, address, start_time, end_time, descriptors):
    """
    Retrieve raw data from TB
    """
    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys=" + descriptors + "&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    if r2:
        df = pd.DataFrame([])
        
        for desc in r2.keys():
            df1 = pd.DataFrame(r2[desc])
            df1.set_index('ts', inplace=True)
            df1.columns = [str(desc)]
                      
            if not df1.empty:
                df = pd.concat([df, df1], axis=1)

        if not df.empty:
            for col in df.columns:
                df[col] = df[col].astype('float64')
    else:
        df = pd.DataFrame([])
        # print('Empty json!')
    return df

def legacy_info(df, device, legadict, val):
    """
    Write incident info to json
    """
    if val=='1':
        ind = df.index[-1]
    else:
        ind = df
    legadict[device] = {'ts':str(ind),'status':val}
    print(legadict)
    # Write the data to the file
    with open(BASEPATH+'legacy_info.json', 'w', encoding='utf-8') as file:
        json.dump(legadict, file, ensure_ascii=False, indent=4)

    return legadict

# def check_phase_deviation(df):
#     """
#     Check if the each phase angle exceeds 120 +- 2 degrees
#     """
#     tmp = df.copy()
#     df1 = tmp.loc[(np.abs(tmp['angleAB'])>122) | (np.abs(tmp['angleAB'])<118)].copy()
#     df2 = tmp.loc[(np.abs(tmp['angleAC'])>122) | (np.abs(tmp['angleAC'])<118)].copy()
#     df3 = tmp.loc[(np.abs(tmp['angleBC'])>122) | (np.abs(tmp['angleBC'])<118)].copy()
    
    
#     if (((not df1.empty) & (len(df1)>2)) | ((not df2.empty) & (len(df2)>2)) | ((not df3.empty) & (len(df3)>2))):
#         failure = True
#     else:
#         failure = False
#     return tmp, failure


# def check_sum_phases(df):
#     """
#     Check if the sum of absulote V-V angles exceeds the threshold of 360+-0.5 degrees
#     """
#     df['sumAngles'] = np.abs(df['angleAB'])+np.abs(df['angleBC'])+np.abs(df['angleAC'])

#     df = df.loc[((df['sumAngles']>360.5) | (df['sumAngles']<359.5))]
#     df = df[['sumAngles']]
#     if ((not df.empty) & (len(df)>2)):
#         # Raise alarm
#         sum_failure = True
#     else:
#         sum_failure = False
#     return df,sum_failure

def check_undervoltage(df):
    """
    Check if two phases are below 220V for at least 3 consecutive values
    """
    undervoltages = {'A':0,'B':0,'C':0}
    threshold = 220
    under_threshold = df < threshold
    
    rolling_under_threshold = under_threshold.rolling(window=3).sum() == 3
    df['alarm'] = (rolling_under_threshold.sum(axis=1) >= 2)
    vlt_failure = True if df['alarm'].any() else False
    
    
    return df,vlt_failure

    

def add_event(device, legadict, start_time):
    """
    Write finished event in csv file
    """
    new_event = [device, legadict[device]['ts'], start_time]
    csv_file = BASEPATH+'events.csv'
    with open(csv_file, mode='a', newline='') as file:
        writer = csv.writer(file)
        # Add the new row
        writer.writerow(new_event)

def detect_alarms(df, address, acc_token, device, devtoken,label, legadict, start_time):
    """
    Check if V-V rules
    """
    
    df.sort_index(inplace=True)
    #[singledf,phase_failure] = check_phase_deviation(df.copy())
    #[df, sum_failure] = check_sum_phases(df.copy())
    [df,vlt_failure] = check_undervoltage(df)
    # if (phase_filure or sum_failure):
    if vlt_failure:
        print(device)
        if device in legadict.keys():
            if legadict[device]['status']=='0':
                legadict = legacy_info(df, device, legadict, '1')
                write_df(df, address, acc_token, devtoken)
                print('Alarm for device ', device, label)
                send_email(device, label)
        else:
            legadict = legacy_info(df, device, legadict, '1')
            write_df(df, address, acc_token, devtoken)
            print('Alarm for device ', device, label)
            send_email(device, label)
    else:
        if device in legadict.keys():
            if legadict[device]['status']=='1':
                print('Alarm finished for device ', device, label)
                add_event(device, legadict, start_time)
                legadict = legacy_info(start_time, device, legadict,'0')
        
        
def main():
    filename = BASEPATH+'legacy_info.json'
    if os.path.isfile(filename):
        with open(filename, 'r', encoding='utf-8') as file:
            legadict = json.load(file)
    else:
        legadict = {}
    print('legacy existing:',legadict)
    #define start - end date
    end_time = datetime.datetime.now()
    end_time = end_time - datetime.timedelta(seconds=end_time.second,
                                                microseconds=end_time.microsecond)
    
    start_time = end_time +relativedelta(minutes=-10)
    
    # print('Time running script:',start_time, end_time)
    start_time = str(int(start_time.timestamp()) * 1000)
    end_time = str(int(end_time.timestamp()) * 1000)

    #start_time = '1726653000000'
    #end_time = '1726653600000'
    
    address = 'http://localhost:8080'
    # address = 'https://mi6.meazon.com'
    r = requests.post(address + "/api/auth/login",
                    json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()
    
    acc_token = 'Bearer' + ' ' + r['token']
    
    
    entityId = '47545f30-5b7f-11ee-b2c9-653b42f73605' # DEDDHE ATHINAS
    r1 = requests.get(url=address + "/api/entityGroup/"+entityId+"/entities?pageSize=1000&page=0",headers={'Content-Type': 'application/json', 
'Accept': '*/*', 'X-Authorization': acc_token}).json()

    for i in range(0,len(r1['data'])):
        assetid = r1['data'][i]['id']['id']
        assetname = r1['data'][i]['name']
        
    
        if assetname[0]!='0':
        
            r2 = requests.get(url=address + "/api/relations/info?fromId="+assetid+"&fromType=ASSET",headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
            
            for j in range(0, len(r2)):
                device = r2[j]['toName']
                if device[:3]=='102':
                
                    #try:
                    [devid, acc_token, label, devtoken] = get_dev_info(device, address)                   
                    #descriptors = 'angleAB,angleAC,angleBC'
                    descriptors = 'vltA,vltB,vltC'    
                    # latest_status = read_latest(acc_token, devid, address, end_time, 'alarm_status')
                    df = read_data(acc_token, devid, address,  start_time, end_time, descriptors)
                    if not df.empty:
                        detect_alarms(df, address, acc_token, device, devtoken, label, legadict, start_time)
                    #except Exception as e:
                    #    print(f"Error reading data for device {device}: {e}")
                    #    continue

if __name__ == '__main__':
    sys.exit(main())