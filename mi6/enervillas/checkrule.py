#!/usr/bin/env python3
import glob, os, sys
import pandas as pd
import datetime
import time
import requests
import json
import csv
import pytz
from astral import LocationInfo
from astral.sun import sun

def get_dev_info(devname):
    address = "http://localhost:8080"
    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()

    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']
    # get devid by serial name
    r1 = requests.get(
        url=address + "/api/tenant/devices?deviceName=" + devname,
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()

    devid = r1['id']['id']
    r1 = requests.get(
        url=address + "/api/device/" + devid + "/credentials",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    devtoken = r1['credentialsId']

    return devid,devtoken,acc_token,address


def send_data(mydict,devtoken,address,acc_token):
 
    for key, value in mydict.items():
        my_json = json.dumps({'ts': key, 'values': value})
        print(my_json)
        r = requests.post(url=address + "/api/v1/" + devtoken + "/telemetry",
                          data=my_json, headers={'Content-Type': 'application/json', 'Accept': '*/*','X-Authorization': acc_token})
    
    return


def send_att_val(mydict,address, devid, acc_token):


    my_json = json.dumps(mydict)
    print(my_json)
    r = requests.post(url=address+"/api/plugins/telemetry/DEVICE/"+devid+"/SERVER_SCOPE",
      data=my_json, headers={'Content-Type': 'application/json', 'Accept': '*/*','X-Authorization': acc_token})
  
    return


def read_attr(devid, address, acc_token, start_time, end_time, descriptors):
    end_time = str(datetime.datetime.utcnow().timestamp()*1e3)
    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/attributes?keys=" + descriptors+ "&startTs=0&endTs=" + end_time + "&agg=NONE&limit=1",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    att = r2[0]['value']
    ts = int(r2[0]['lastUpdateTs'])
    
    
    return att,ts
    

def read_data(devid, address, acc_token, start_time, end_time, descriptors, limit):
       
    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys="+descriptors+"&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit="+limit,
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    
    if r2:
        df = pd.DataFrame([])
        
        for desc in descriptors.split(','):
            df1 = pd.DataFrame(r2[desc])
            df1.set_index('ts', inplace=True)
            df1.columns = [str(desc)]
            
            df1.reset_index(drop=False, inplace=True)
            df1['ts'] = pd.to_datetime(df1['ts'], unit='ms')
            df1['ts'] = df1['ts'].dt.tz_localize('utc').dt.tz_convert('Europe/Athens')
            df1 = df1.sort_values(by=['ts'])
            df1.reset_index(drop=True, inplace=True)
            df1.set_index('ts', inplace=True, drop=True)            
            
            df = pd.concat([df, df1], axis=1)

        if df.empty:
            df = pd.DataFrame([])
        else:
            for col in df.columns:
                df[col] = df[col].astype('float64')
    else:
        df = pd.DataFrame([])
        #print('Empty json!')
    
    return df
    
    
    
def pool_time(devid, address, acc_token, start_time, end_time, pool_id):
    descriptors = 'pwrA,pwrB,pwrC'
    
    start_time = datetime.datetime.utcnow()
    start_time = start_time - datetime.timedelta(hours=start_time.hour, minutes=start_time.minute, seconds=start_time.second,
                                                 microseconds=start_time.microsecond)
    tz = pytz.timezone('Europe/Athens')
    start_time = tz.localize(start_time)
    start_time = str(int(start_time.timestamp()*1000))
    
    df = read_data(devid, address, acc_token, start_time, end_time, descriptors, '1000000')
    df['dif'] = df.index.to_series().diff().astype('timedelta64[s]')
    
    if pool_id==1:
        df.loc[df['pwrB']<100, 'dif']=0
    elif pool_id==2:
        df.loc[df['pwrC']<100, 'dif']=0
    
    duration = df['dif'].sum() # operating time in seconds
    
    return duration
    
    
def check_states(evserial, pool1, pool2, address, acc_token, start_time, end_time):

    # check if EV is operating
    [devid,_,_,_] = get_dev_info(evserial)
    descriptors = 'pwrA,pwrB,pwrC'
    df = read_data(devid, address, acc_token, start_time, end_time, descriptors,'1')
    
    pwrsum = df['pwrA'].iloc[0]+df['pwrB'].iloc[0]+df['pwrC'].iloc[0]
    if pwrsum>100:
        ev_op = 1
    else:
        ev_op = 0
    
    
    # check if pool1 is operating
    [devid,_,_,_] = get_dev_info(pool1)
    descriptors = 'pwrA,pwrB,pwrC'
    df = read_data(devid, address, acc_token, start_time, end_time, descriptors,'1')
    pwrsum = df['pwrA'].iloc[0]+df['pwrB'].iloc[0]+df['pwrC'].iloc[0]
    if pwrsum>100:
        pool_op1 = 1
    else:
        pool_op1 = 0
    
    # check if pool1 is operating
    [devid,_,_,_] = get_dev_info(pool2)
    descriptors = 'pwrA,pwrB,pwrC'
    df = read_data(devid, address, acc_token, start_time, end_time, descriptors,'1')
    pwrsum = df['pwrA'].iloc[0]+df['pwrB'].iloc[0]+df['pwrC'].iloc[0]
    if pwrsum>100:
        pool_op2 = 1
    else:
        pool_op2 = 0
    
    return ev_op, pool_op1, pool_op2
        
        
def EV_checks(curr_state, EVthres):
    change = 0
    EV_mz_cmd=0
    
    if curr_state['pwrsum']<=EVthres:
        if (curr_state['ecomode'] and curr_state['evstat'] and curr_state['ev_op']!=1):
            EV_mz_cmd = 1
            change = 1
            curr_state['pwrsum'] = curr_state['pwrsum']-EVthres # subtract EV threshold from power sum to check residual for pools
            print('EV may charge')
    elif ((curr_state['EV_eco'] == 1) and (curr_state['pwrsum']>=1000)):
            print('EV should turn off')
            EV_mz_cmd = 0
    
    return EV_mz_cmd, change, curr_state
        
    
def pool1_checks(curr_state, pool1thres, pool1_duration_threshold, pool_min_thres, deltasunset, deltasunrise): 
    change = 0
    pool1_mz_cmd = 0
    
    # IF DAYLIGHT
    if (deltasunset>=(3*3600) and deltasunrise>=0):
        # if pool is off and duration hasnt been reached, check power to turn it on
        if (curr_state['pool_op1']==0 and curr_state['pool1_dur']<pool1_duration_threshold):
            if curr_state['pwrsum']<=pool1thres:
                pool1_mz_cmd = 1
                change = 1
                curr_state['pwrsum'] = curr_state['pwrsum']-pool1thres
                
        # CHECK OFFS
        
        if ((curr_state['pool_op1'] == 1) and (curr_state['pool1_dur']>pool1_duration_threshold)):
            print('Pool 1 should turn off, pool duration has been reached')
            pool1_mz_cmd = 0
            change = 1
        elif (((curr_state['pool_op1'] == 1) or (curr_state['pool1_eco']==1)) and curr_state['pwrsum']>1000 and (curr_state['pool1_eco_time']>=pool_min_thres)):
            print('Pool 1 should turn off, too much consumption in residence')
            pool1_mz_cmd = 0
            change = 1
        
    # IF NIGHTFALL
    else:
        if (curr_state['pool1_dur']<pool1_duration_threshold and curr_state['pool_op1']==0):
            print('Pool 1 should turn on due to duration constraint')
            pool1_mz_cmd = 1
            change = 1
            
        elif (curr_state['pool1_dur']>=pool1_duration_threshold and curr_state['pool_op1']==1):
            print('Pool 1 should turn off due to duration constraint')
            pool1_mz_cmd = 0
            change = 1
            
    
    
    return pool1_mz_cmd, change, curr_state
    

def pool2_checks(curr_state, pool2thres, pool2_duration_threshold, pool_min_thres, deltasunset, deltasunrise): 
    change = 0
    pool2_mz_cmd = 0
    
    # IF DAYLIGHT
    if (deltasunset>=(3*3600) and deltasunrise>=0):
        # if pool is off and duration hasnt been reached, check power to turn it on
        if (curr_state['pool_op2']==0 and curr_state['pool2_dur']<pool2_duration_threshold):
            if curr_state['pwrsum']<=pool2thres:
                pool2_mz_cmd = 1
                change = 1
                curr_state['pwrsum'] = curr_state['pwrsum']-pool2thres
                
        # CHECK OFFS
        
        if ((curr_state['pool_op2'] == 1) and (curr_state['pool2_dur']>pool2_duration_threshold)):
            print('Pool 2 should turn off, pool duration has been reached')
            pool1_mz_cmd = 0
            change = 1
        if (((curr_state['pool_op2'] == 1) or (curr_state['pool2_eco']==1)) and curr_state['pwrsum']>1000 and (curr_state['pool2_eco_time']>=pool_min_thres)):
            print('Pool 2 should turn off, too much consumption in residence')
            pool2_mz_cmd = 0
            change = 1
            
    # IF NIGHTFALL
    else:
        if (curr_state['pool2_dur']<pool2_duration_threshold and curr_state['pool_op2']==0):
            print('Pool 2 should turn on due to duration constraint')
            pool2_mz_cmd = 1
            change = 1
            
        elif (curr_state['pool2_dur']>=pool2_duration_threshold and curr_state['pool_op2']==1):
            print('Pool 2 should turn off due to duration constraint')
            pool2_mz_cmd = 0
            change = 1
        
    
    return pool2_mz_cmd, change, curr_state
    
        

    
    
if __name__ == '__main__':
    
    evserial = '106.101.000006' # serial of EV charger 
    devname = '102.402.000118' # central meter on Enervilla
    pvname = '102.402.000110' # PV
    pool1 = '102.402.000927' # pool Nefeli
    pool2 = '102.402.000109' # pool Anna
    
    end_time = str(int(datetime.datetime.utcnow().timestamp()*1e3)) # current datetime
    start_time = str(int(datetime.datetime.utcnow().timestamp()*1e3)-86400000) # previous day datetime, but fetch only last 5 values aka 5minutes
    
    # get tokens and ids of devices
    [evid, evtoken, acc_token, address] =  get_dev_info(evserial)
    [pool1_id, pool1_token, _, _] =  get_dev_info(pool1)
    [pool2_id, pool2_token, _, _] =  get_dev_info(pool2)
    [devid, devtoken, _, _] =  get_dev_info(devname)
    
    
    local_tz = pytz.timezone('Europe/Athens')
    EVthres = -3000 # threshold to charge, 3000+2000 
    pool1thres = -600 # threshold to operate pool2, equal to half the operating power
    pool2thres = -350 # threshold to operate pool2, equal to half the operating power
    #pool_duration_threshold = 29000 # approx. 8 hours of operation
    pool_min_thres = 3600 # minimum operation time for pool to complete a cycle -> 1.5 hour
    
    [pool1_duration_threshold, _] = read_attr(pool1_id, address, acc_token, start_time, end_time, 'operationTime')
    [pool2_duration_threshold, _] = read_attr(pool2_id, address, acc_token, start_time, end_time, 'operationTime')
    pool1_duration_threshold = pool1_duration_threshold*3600
    pool2_duration_threshold = pool2_duration_threshold*3600
    #print('pool1 & pool2 duration:',pool1_duration_threshold,pool2_duration_threshold)
    
    # get sunset time
    city = LocationInfo('Patras', 'Greece', 'Europe/Athens',38.24671738448589, 21.733760692975086)
    now = datetime.datetime.utcnow()
    s = sun(city.observer, date=now)
    sunset = s['sunset']
    sunrise = s['sunrise']
    
    
    now = now.replace(tzinfo=pytz.UTC)
    deltasunset = int((sunset-now).total_seconds()) # remaining time until sunset, in seconds
    deltasunrise = int((now-sunrise).total_seconds()) #  time after sunrise, in seconds    
    
    
    
    print('***************************')
    print('Running script at UTC time:',datetime.datetime.utcnow())
    #print('Sunset/sunrise:',sunset,sunrise)
    # initialize dictionary with current state of residence
    curr_state = {}
    
    # read EV charger attribute to ensure eco mode is ON
    descriptors = 'limitViaPV'   
    [curr_state['ecomode'], _] = read_attr(evid, address, acc_token, start_time, end_time, descriptors)
    
    #if curr_state['ecomode']:
    #    print('ECO mode ON')
    #else:
    #    print('ECO mode OFF')
        
    # read EV charger status to check if car is connected
    descriptors = 'evsestat'
    df = read_data(evid, address, acc_token, start_time, end_time, descriptors,'1')
    
    if df['evsestat'].iloc[0]>0:
        curr_state['evstat'] = True
        #print('Car connected')
    else:
        curr_state['evstat'] = False
        #print('Car disconnected')
        
        
    # read central meter active power, to check if there is returning load
    descriptors = 'pwrA,pwrB,pwrC'
    df = read_data(devid, address, acc_token, start_time, end_time, descriptors,'1')
    
    pwrA = df['pwrA'].iloc[0] #if df['pwrA'].iloc[0]<=0 else 0
    pwrB = df['pwrB'].iloc[0] #if df['pwrB'].iloc[0]<=0 else 0
    pwrC = df['pwrC'].iloc[0] #if df['pwrC'].iloc[0]<=0 else 0 
    
    if (pwrA<0 or pwrB<0 or pwrC<0):
        pwrA = pwrA if pwrA<=0 else 0
        pwrB = pwrB if pwrB<=0 else 0
        pwrC = pwrC if pwrC<=0 else 0
         
        
    curr_state['pwrsum'] = pwrA+pwrB+pwrC
    
    #print('Current returning power:', curr_state['pwrsum'])
    
    # read operating state of ev and pools
    [curr_state['ev_op'], curr_state['pool_op1'], curr_state['pool_op2']] = check_states(evserial, pool1, pool2, address, acc_token, start_time, end_time)
    #print('Operation status of EV, pool1, pool2:',curr_state['ev_op'], curr_state['pool_op1'], curr_state['pool_op2'])
    
    # calculate operating time of pools
    curr_state['pool1_dur'] = pool_time(pool1_id, address, acc_token, start_time, end_time, 1)
    curr_state['pool2_dur'] = pool_time(pool2_id, address, acc_token, start_time, end_time, 2)
    #print('duration of pools 1 & 2:',curr_state['pool1_dur'],curr_state['pool2_dur'])
    
    # check eco-operate of EV, pool1, pool2
    try:
        df = read_data(evid, address, acc_token, '0', end_time, 'eco_operate','1')
        curr_state['EV_eco'] = df['eco_operate'].iloc[0]
    except:
        curr_state['EV_eco'] = 0
        print('Variable has no value for EV')
    
    dtnow = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC).astimezone(local_tz)
    df = read_data(pool1_id, address, acc_token, '0', end_time, 'eco_operate','1')
    curr_state['pool1_eco'] = df['eco_operate'].iloc[0]
    curr_state['pool1_eco_time'] = (dtnow-df.index[0]).total_seconds()
    
    
    df = read_data(pool2_id, address, acc_token, '0', end_time, 'eco_operate','1')
    curr_state['pool2_eco'] = df['eco_operate'].iloc[0]
    curr_state['pool2_eco_time'] = (dtnow-df.index[0]).total_seconds()
    
    # read command-by-user attribute to check if user has taken control of pools
    descriptors = 'command_by_user'
    [curr_state['pool1_command'], ts1] = read_attr(pool1_id, address, acc_token, start_time, end_time, descriptors)
    [curr_state['pool2_command'], ts2] = read_attr(pool2_id, address, acc_token, start_time, end_time, descriptors)

    print('Current state:', curr_state)
    
    
    # Check EV
    [EV_mz_cmd, EV_change, curr_state] = EV_checks(curr_state, EVthres)
    if EV_change==1:
        mydict = {int(end_time) : {'eco_operate':str(EV_mz_cmd)}}
        print('EV',mydict)
        send_data(mydict,ev_token,address,acc_token)
    
    
    # if user has not activated pools, run pool chceck process
    if not curr_state['pool1_command']:
        [pool1_mz_cmd, pool1_change, curr_state] = pool1_checks(curr_state, pool1thres, pool1_duration_threshold, pool_min_thres, deltasunset, deltasunrise)
        if pool1_change==1:
            mydict = {int(end_time) : {'eco_operate':str(pool1_mz_cmd)}}
            print('pool1',mydict)
            send_data(mydict,pool1_token,address,acc_token)
    else:
        delta_op = int(round(time.time() * 1000)) - ts1
        if delta_op>=pool_min_thres:
            print('User command, pool1 has reached time limit:',delta_op/1000)
            mydict = {'command_by_user' : False}
            #send_att_val(mydict,address, pool1_id, acc_token)
         
    if not curr_state['pool2_command']:
        [pool2_mz_cmd, pool2_change, curr_state] = pool2_checks(curr_state, pool2thres, pool2_duration_threshold, pool_min_thres, deltasunset, deltasunrise)
        if pool2_change==1:
            mydict = {int(end_time) : {'eco_operate':str(pool2_mz_cmd)}}
            print('pool2',mydict)
            send_data(mydict,pool2_token,address,acc_token)        
    else:
        delta_op = int(round(time.time() * 1000)) - ts2
        if delta_op>=pool_min_thres:
            print('User command, pool2 has reached time limit:',delta_op/1000)
            mydict = {'command_by_user' : False}
            #send_att_val(mydict,address, pool2_id, acc_token)
    
    
    
    #print('mz commands:',EV_mz_cmd, pool1_mz_cmd, pool2_mz_cmd)
    print('********************************************************')
    #print('time from sunrise and sunset in seconds:',deltasunrise,deltasunset)
########################################################################################        