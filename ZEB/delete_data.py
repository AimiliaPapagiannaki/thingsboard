#!/usr/bin/env python3
import sys
import pandas as pd
import datetime
import requests
import json
import numpy as np
import os


def main():

    list_ind=[]
    
    did ={'102.107.000082':'e6914c80-070e-11ea-a66e-7bc859fa1c1b','102.201.000652':'c4e27d70-07a4-11ea-a66e-7bc859fa1c1b','102.201.000659':'bd2e4aa0-07a4-11ea-a66e-7bc859fa1c1b','102.201.000660':'c02d80e0-07a4-11ea-a66e-7bc859fa1c1b','102.201.000661':'c00addb0-07a4-11ea-a66e-7bc859fa1c1b','102.105.000231':'ddd387c0-07a4-11ea-a66e-7bc859fa1c1b','102.105.000232':'de935550-07a4-11ea-a66e-7bc859fa1c1b'}
    
    entid = did['102.105.000232']
    with open('janus_restore/listind_232.txt', 'r') as filehandle:
        for line in filehandle:
        # remove linebreak which is the last character of the string
            currentPlace = line[:-1]


        # add item to the list
            list_ind.append(currentPlace)
            
          
    address = "http://localhost:8080"
    
    r = requests.post(address + "/api/auth/login",json={'username': 'a.papagiannaki@meazon.com', 'password': 'eurobank'}).json()
    acc_token = 'Bearer' + ' ' + r['token']
    
    for i in range(0,len(list_ind)):
        starts=str(int(list_ind[i])-10)
        endts=str(int(list_ind[i])+10)
        r = requests.delete(address + "/api/plugins/telemetry/DEVICE/"+entid+"/timeseries/delete?keys=cnrgA,cnrgB,cnrgC&deleteAllDataForKeys=False&startTs="+starts+"&endTs="+endts+"&rewriteLatestIfDeleted=False",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token})
    print('Finished descriptors')


if __name__ == "__main__":
    sys.exit(main())
