import requests
import time
import json
import pandas as pd
import numpy as np
import pytz
import datetime
from dateutil.tz import gettz
from dateutil.relativedelta import relativedelta


def get_access_token(address):

    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()
    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']
    return acc_token


def get_devid(address, device,entity):
    """
    Retrieves the device ID for the given device name.
    """
    acc_token = get_access_token(address)

    response = requests.get(
        url=f"{address}/api/tenant/{entity}s",
        params={entity+"Name": device},
        headers={
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'X-Authorization': acc_token
        }
    )
    return response.json()['id']['id']
    

def get_devtoken(address, device):
    """
    Retrieves the device token for the given device name.
    """
    acc_token = get_access_token(address)
    devid = get_devid(address, device)

    response = requests.get(
        url=f"{address}/api/device/{devid}/credentials",
        headers={
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'X-Authorization': acc_token
        }
    )
    return response.json()['credentialsId']


def send_telemetry(address, device, my_json, entity):
    acc_token = get_access_token(address)
    devid = get_devid(address, device, entity.lower())
    
    requests.post(
        url=f"{address}/api/plugins/telemetry/{entity}/{devid}/timeseries/SERVER_SCOPE",
        data=my_json,
        headers={
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'X-Authorization': acc_token
        }
    )

    
def send_attr(address, devid, mydict):

    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()

    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']
    for key, value in mydict.items():
        mydict[key] = str(mydict[key])
        
    myjson  = json.dumps(mydict)
  
    r = requests.post(address+"/api/plugins/telemetry/DEVICE/"+devid+"/attributes/SERVER_SCOPE", 
        json=mydict, headers={'Content-Type': 'application/json', 'Accept': '*/*','X-Authorization':acc_token})
    return
        
            