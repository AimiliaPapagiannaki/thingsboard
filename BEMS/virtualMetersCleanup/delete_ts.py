#!/usr/bin/env python

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
import pytz
import json
from dateutil.relativedelta import relativedelta
from dateutil.tz import gettz
import thingsAPI

ADDRESS = "http://localhost:8080" 



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


def delete_ts(devid, entity, acc_token):
    """
    Delete telemetry 
    """
    start_time = '1706738400000'
    end_time = '1709200800000'
    descriptors = 'clean_nrgA,clean_nrgB,clean_nrgC,totalCleanNrg'
    entity = entity.upper()

    response = requests.delete(
        url=f"{ADDRESS}/api/plugins/telemetry/{entity}/{devid}/timeseries/delete",
        params={
            "keys": descriptors,
            "startTs": start_time,
            "endTs": end_time
        },
        headers={
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'X-Authorization': acc_token
        }
    )
    # print('Telemetry sent for device {}'.format(device))


     
    
    
def main():

    # define descriptors and access token
    
    acc_token = thingsAPI.get_access_token(ADDRESS)
    
    
    # load meter info
    filepath = '/home/azureuser/BEMS/'
    os.chdir(filepath)
    filename = 'meters_info.json'
    with open(filename, 'r', encoding='utf-8') as file:
        loaded_data = json.load(file)

    # Access the loaded data
    physical_meters = loaded_data["physical_meters"] # physical meters
    virtualMeters = loaded_data["virtualMeters"] # virtual meters aggregations with physical meters
    unwriteable = loaded_data["unwriteable"] # intermediate virtual meters, no need to write data 
    assetdevs = loaded_data["assetdevs"] # Virtual rooms, represented as assets, not devices
    
    # Create list of meters (physical+virtual) whose telemetry will be stored in TB
    lst = [value for value in list(virtualMeters.keys()) if value not in unwriteable]
    totalist = physical_meters + lst
    
    for meter in totalist:
        print(meter)
        # if meter in ['VIRTUAL METER 41 - ΛΟΙΠΑ ΦΟΡΤΙΑ ΕΓΚΑΤΑΣΤΑΣΗΣ','Λοιπά Φορτία Eγκατάστασης','Υποσύνολο']:
        
            
        if meter in assetdevs:
            entity = 'asset'
        else:
            entity = 'device'
        devid = thingsAPI.get_devid(ADDRESS, meter, entity)
        #delete_ts(devid, entity, acc_token)
        
   
    
if __name__ == "__main__":
    sys.exit(main())