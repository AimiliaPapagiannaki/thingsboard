#!/usr/bin/env python3 

import sys
import pandas as pd
import datetime
import requests
import numpy as np
import os
import json
  

def main(argv):
    
    dev_token = str(argv[1])
    ts = str(argv[2])
    flowA = str(argv[3])
    flowB = str(argv[4])
    flowC = str(argv[5])
    difHA = str(argv[6])
    difHB = str(argv[7])
    difHC = str(argv[8])
    difDA = str(argv[9])
    difDB = str(argv[10])
    difDC = str(argv[11])
    difMA = str(argv[12])
    difMB = str(argv[13])
    difMC = str(argv[14])
    
    #dev_token='MnRaSBzopu2sznNv16Jj' # 102.116.000125 Korai
    #ts=1575374400000 #3/12/2019 14:00)
    #flowA='668430'
    #flowB='603263'
    #flowC='570112'
    
    address = "http://localhost:8080"
    r = requests.post(address + "/api/auth/login",json={'username': 'a.papagiannaki@meazon.com', 'password': 'eurobank'}).json()

    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']
    
    
    
    my_json = json.dumps({'ts': ts, 'values': {'overflA':flowA,'overflB':flowB,'overflC':flowC,'difH_A':difHA, 'difH_B':difHB,'difH_C':difHC, 'difD_A':difDA, 'difD_B':difDB, 'difD_C':difDC, 'difM_A':difMA, 'difM_B':difMB, 'difM_C':difMC}})
    print(my_json)
    r = requests.post(url=address + "/api/v1/" + dev_token + "/telemetry",
                       data=my_json, headers={'Content-Type': 'application/json', 'Accept': '*/*',
                                              'X-Authorization': acc_token})
   
    print('finished descriptor..')


if __name__ == "__main__":
    sys.exit(main(sys.argv))
