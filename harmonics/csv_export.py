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

def read_data(devid, acc_token, address, start_time, end_time, descriptors):
    # mapping is a dictionary to map all occurencies of descriptors to descent column names
    # mapping = {}
    # mapping['pwrA'] = 'Αctive power A (kW)'
    # mapping['pwrB'] = 'Active power B (kW)'
    # mapping['pwrC'] = 'Active power C (kW)'
    # mapping['apwrA'] = 'Apparent power A (VAR)'
    # mapping['apwrB'] = 'Apparent power B (VAR)'
    # mapping['apwrC'] = 'Apparent power C (VAR)'
    # mapping['scre'] = 'Crest factor'
    # mapping['curA'] = 'Current A (A)'
    # mapping['curB'] = 'Current A (B)'
    # mapping['curC'] = 'Current A (C)'


    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys=" + descriptors + "&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()

    if r2:
        df = pd.DataFrame([])

        for desc in r2.keys():
            df1 = pd.DataFrame(r2[desc])
            df1.set_index('ts', inplace=True)
            df1.columns = [str(desc)]
            df = pd.concat([df, df1], axis=1)


        if df.empty == False:

            df.reset_index(drop=False, inplace=True)
            df = df.sort_values(by=['ts'])
            df.reset_index(drop=True, inplace=True)
            df.set_index('ts', inplace=True, drop=True)
            for col in df.columns:
                df[col] = df[col].astype('float')

            df = df.groupby(df.index).max()

        else:
            df = pd.DataFrame([])
    else:
        df = pd.DataFrame([])
        print('Empty json!')
    return df


def main(argv):
    startt = time.time()
    devid = argv[1]
    descriptors = argv[2]
    start_time = argv[3]
    end_time = argv[4]

    # path = '../csv files'
    #
    # path = path + '/'
    # os.chdir(path)
    filename = 'csv_export.csv'

    # address = "http://localhost:8080"
    address =  "http://52.77.235.183:8080"

    r = requests.post(address + "/api/auth/login",
                      json={'username': 'tenant@thingsboard.org', 'password': 'tenant'}).json()

    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']

    summary = read_data(devid, acc_token, address, start_time, end_time, descriptors)

    if not summary.empty:
        summary.to_csv(filename, index=True)

    elapsed = time.time() - startt
    print("---  seconds ---", elapsed)


if __name__ == "__main__":
    sys.exit(main(sys.argv))

