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
def conv_to_consumption(df, interval):
    #     convert cumulative energy to consumed energy

    energies = ['cnrgA', 'cnrgB', 'cnrgC', 'cnrgAest', 'cnrgBest', 'cnrgCest']
    for nrg in energies:
        if nrg in df.columns:
            df['diff'] = np.nan
            df.diff[((df.nrg.isna() == False) & (df.nrg.shift().isna() == False))] = df.nrg - df.nrg.shift()
            df.diff.iloc[0] = 0
            df.rename(columns={"diff": mapping.get(nrg)}, inplace=True)
            df.drop([nrg], axis=1, inplace=True)

    if (('Consumed energy (kWh) A' in df.columns) and ('Consumed energy (kWh) B' in df.columns) and (
            'Consumed energy (kWh) C' in df.columns)):
        df['total'] = np.nan
        df.total[(df['Consumed energy (kWh) A'].isna() == False) & (df['Consumed energy (kWh) B'].isna() == False) & (
                df['Consumed energy (kWh) C'].isna() == False)] = df['Consumed energy (kWh) A'] + df[
            'Consumed energy (kWh) B'] + df['Consumed energy (kWh) C']
        df.rename(columns={"total": "Total Consumed energy (kWh)"}, inplace=True)

    if (('Estimated consumed energy (kWh) A' in df.columns) and (
            'Estimated consumed energy (kWh) B' in df.columns) and ('Estimated consumed energy (kWh) C' in df.columns)):
        df['total'] = np.nan
        df.total[(df['Estimated consumed energy (kWh) A'].isna() == False) & (
                    df['Estimated consumed energy (kWh) B'].isna() == False) & (
                         df['Estimated consumed energy (kWh) C'].isna() == False)] = df[
                                                                                         'Estimated consumed energy (kWh) A'] + \
                                                                                     df[
                                                                                         'Estimated consumed energy (kWh) B'] + \
                                                                                     df[
                                                                                         'Estimated consumed energy (kWh) C']
        df.rename(columns={"total": "Total Estimated Consumed energy (kWh)"}, inplace=True)

    return df


def align_resample(df, interval):
    # identify report interval and round to closest minute
    tmpdf = pd.DataFrame(df[df.columns[0]].dropna())
    tmpdf['minutes'] = tmpdf.index.minute
    tmpdf['interv'] = tmpdf['minutes'].shift(-1) - tmpdf['minutes']
    inter = int(tmpdf['interv'].value_counts().idxmax())
    del tmpdf
    df.index = df.index.round(str(interval) + 'T')

    # df.index = df.index.map(lambda x: x.replace(second=0, microsecond=0))
    df = df.groupby(df.index).max()
    df.sort_index(inplace=True)

    # resample df to given interval
    if (('Average active power A (kW)' in df.columns) and ('Average active power C (kW)' in df.columns) and (
            'Average active power B (kW)' in df.columns)):
        df_demand = df.resample(interval + 'T').max().copy()
        df_demand = df_demand[
            ['Average active power A (kW)', 'Average active power B (kW)', 'Average active power C (kW)']]
        df_demand.rename(columns={"Average active power A (kW)": "Maximum active power A (kW)",
                                  "Average active power B (kW)": "Maximum active power B (kW)",
                                  "Average active power C (kW)": "Maximum active power C (kW)"}, inplace=True)

        df = df.resample(interval + 'T').mean()
        df.reset_index(inplace=True, drop=False)
        df.set_index('ts', inplace=True, drop=False)
        df = pd.concat([df, df_demand], axis=1)
    else:
        df = df.resample(interval + 'T').mean()
        df.reset_index(inplace=True, drop=False)
        df.set_index('ts', inplace=True, drop=False)

    return df


def read_data(devid, acc_token, address, start_time, end_time, interval, descriptors, tmzn):
    # request all descriptors that have ever been assigned to this device
    r1 = requests.get(url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/keys/timeseries",
                      headers={'Content-Type': 'application/json', 'Accept': '*/*',
                               'X-Authorization': acc_token}).json()

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
        if est in descriptors: descriptors.append(estimated[est])

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
    mapping['cnrgA'] = 'Consumed energy (kWh) A'
    mapping['cnrgB'] = 'Consumed energy (kWh) B'
    mapping['cnrgC'] = 'Consumed energy (kWh) C'
    mapping['cnrgAest'] = 'Estimated consumed energy (kWh) A'
    mapping['cnrgBest'] = 'Estimated consumed energy (kWh) B'
    mapping['cnrgCest'] = 'Estimated consumed energy (kWh) C'
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
    watt_div = ['Average active power A (kW)', 'Average active power B (kW)', 'Average active power C (kW)',
                'Total active power (kW)', 'Reactive Power A (kVAR)', 'Reactive Power B (kVAR)',
                'Reactive Power C (kVAR)', 'Average estimated active power A (kW)',
                'Average estimated active power B (kW)', 'Average estimated active power C (kW)',
                'Consumed energy (kWh) A', 'Consumed energy (kWh) B', 'Consumed energy (kWh) C',
                'Total Consumed Energy (kWh)', 'Estimated consumed energy (kWh) A', 'Estimated consumed energy (kWh) B',
                'Estimated consumed energy (kWh) C', 'Total Estimated Consumed energy (kWh)',
                'Maximum active power A (kW)', 'Maximum active power B (kW)', 'Maximum active power C (kW)']

    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys=" + descriptors + "&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=1000000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    if r2:
        df = pd.DataFrame([])

        # read all descriptors at once
        for desc in r2.keys():
            df1 = pd.DataFrame(r2[desc])
            df1.set_index('ts', inplace=True)
            df1.columns = [mapping.get(str(desc))]
            df = pd.concat([df, df1], axis=1)

        if df.empty == False:

            df.reset_index(drop=False, inplace=True)
            df['ts'] = pd.to_datetime(df['ts'], unit='ms')
            # df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn).dt.tz_localize(None)

            # Set timestamp as index, convert all columns to float
            df = df.sort_values(by=['ts'])
            df.reset_index(drop=True, inplace=True)
            df.set_index('ts', inplace=True, drop=True)
            for col in df.columns:
                df[col] = df[col].astype('float')

            df = align_resample(df, interval)
            df = conv_to_consumption(df, interval)
            for col in watt_div:
                if col in df.columns:
                    df[col] = df[col] / 1000  # divide by 1000 to convert W/Wh to kW/kWh

            # create additional columns with total value of three phases
            if (('Average active power A (kW)' in df.columns) and ('Average active power C (kW)' in df.columns) and (
                    'Average active power B (kW)' in df.columns)):
                df['Total Average active power (kW)'] = df['Average active power A (kW)'] + df[
                    'Average active power B (kW)'] + df['Average active power C (kW)']

            if (('Average estimated active power A (kW)' in df.columns) and (
                    'Average estimated active power C (kW)' in df.columns) and (
                    'Average estimated active power B (kW)' in df.columns)):
                df['Total Average estimated active power (kW)'] = df['Average estimated active power A (kW)'] + df[
                    'Average estimated active power B (kW)'] + df['Average estimated active power C (kW)']

            if (('Reactive Power A (kVAR)' in df.columns) and ('Reactive Power C (kVAR)' in df.columns) and (
                    'Reactive Power B (kVAR)' in df.columns)):
                df['Total Reactive Power (kVAR)'] = df['Reactive Power A (kVAR)'] + df['Reactive Power B (kVAR)'] + df[
                    'Reactive Power C (kVAR)']

            if (('Maximum active power A (kW)' in df.columns) and ('Maximum active power B (kW)' in df.columns) and (
                    'Maximum active power C (kW)' in df.columns)):
                df['Total Maximum active power (kW)'] = df['Maximum active power A (kW)'] + df[
                    'Maximum active power B (kW)'] + df['Maximum active power C (kW)']

            # convert to given timezone and split Date and Time columns
            df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
            df['Date'] = [d.date() for d in df['ts']]
            df['Time ' + tmzn] = [d.time() for d in df['ts']]
            df = df.drop('ts', axis=1)

            # change order of columns
            cols = df.columns.tolist()
            cols = cols[-2:] + cols[:-2]
            df = df[cols]
        else:
            df = pd.DataFrame([])
    else:
        df = pd.DataFrame([])
        print('Empty json!')
    return df


def main(argv):
    startt = time.time()

    # input arguments
    entityName = str(argv[1])
    entityID = str(argv[2])
    reportID = str(argv[3])
    start_time = str(argv[4])
    end_time = str(argv[5])
    interval = str(argv[6])
    descriptors = str(argv[7])
    tmzn = str(argv[8])

    path = '../xlsx files'

    path = path + '/'
    os.chdir(path)
    filename = entityName + '_' + start_time + '_' + end_time + '.xlsx'

    # address = "http://157.230.210.37:8081"
    address = "http://localhost:8080"

    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazon@thingsboard.org', 'password': 'meazon'}).json()

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
                devid = str(device['to']['id'])
                devName = str(device['toName'])
                print(devName)

                # print('devname:',devName)
                summary = read_data(devid, acc_token, address, start_time, end_time, interval, descriptors, tmzn)

                if ('Total Consumed energy (kWh)' in summary.columns):
                    new_row = {'Power meter': devName,
                               'Total consumed energy (kWh)': summary['Total Consumed energy (kWh)'].sum()}
                    sum_nrg = sum_nrg.append(new_row, ignore_index=True)
                    sum_nrg.to_excel(writer, sheet_name='Summary', index=False)
                elif ('Consumed energy (kWh) A' in summary.columns):
                    new_row = {'Power meter': devName,
                               'Total consumed energy': summary['Consumed energy (kWh) A'].sum()}
                    sum_nrg = sum_nrg.append(new_row, ignore_index=True)
                    sum_nrg.to_excel(writer, sheet_name='Summary', index=False)
                if summary.empty == False:
                    if entityID == 'ed73a120-f73b-11e9-b4dc-013e65d2f65e':  # if building is moxy, don't write separate A,B,C columns
                        if 'Average active power A (kW)' in summary.columns:
                            summary = summary.drop(['Average active power A (kW)', 'Maximum active power A (kW)'],
                                                   axis=1)
                        if 'Average active power B (kW)' in summary.columns:
                            summary = summary.drop(['Average active power B (kW)', 'Maximum active power B (kW)'],
                                                   axis=1)
                        if 'Average active power C (kW)' in summary.columns:
                            summary = summary.drop(['Average active power C (kW)', 'Maximum active power C (kW)'],
                                                   axis=1)

                        if 'Average estimated active power A (kW)' in summary.columns:
                            summary = summary.drop(['Average estimated active power A (kW)'], axis=1)
                        if 'Average estimated active power B (kW)' in summary.columns:
                            summary = summary.drop(['Average estimated active power B (kW)'], axis=1)
                        if 'Average estimated active power C (kW)' in summary.columns:
                            summary = summary.drop(['Average estimated active power C (kW)'], axis=1)

                        if 'Reactive Power A (kVAR)' in summary.columns:
                            summary = summary.drop(['Reactive Power A (kVAR)'], axis=1)
                        if 'Reactive Power B (kVAR)' in summary.columns:
                            summary = summary.drop(['Reactive Power B (kVAR)'], axis=1)
                        if 'Reactive Power C (kVAR)' in summary.columns:
                            summary = summary.drop(['Reactive Power C (kVAR)'], axis=1)

                    devName = devName.replace(':', '')
                    summary.to_excel(writer, sheet_name=devName, index=False)
                else:
                    df = pd.DataFrame({'There are no measurements for the selected period': []})
                    df.to_excel(writer, sheet_name='Sheet1', index=False)

        writer.save()
        writer.close()
    else:
        # read ID and name of building's devices
        devid = str(entityID)
        devName = str(entityName)

        print('devid:', devid)
        sum_nrg = pd.DataFrame([])
        summary = read_data(devid, acc_token, address, start_time, end_time, interval, descriptors, tmzn)

        if summary.empty == False:
            with pd.ExcelWriter(filename) as writer:
                if ('Total Consumed energy (kWh)' in summary.columns):
                    new_row = {'Power meter': devName,
                               'Total consumed energy (kWh)': summary['Total Consumed energy (kWh)'].sum()}
                    sum_nrg = sum_nrg.append(new_row, ignore_index=True)
                    sum_nrg.to_excel(writer, sheet_name='Summary', index=False)
                elif ('Consumed energy (kWh) A' in summary.columns):
                    new_row = {'Power meter': devName,
                               'Total consumed energy': summary['Consumed energy (kWh) A'].sum()}
                    sum_nrg = sum_nrg.append(new_row, ignore_index=True)
                    sum_nrg.to_excel(writer, sheet_name='Summary', index=False)
                if summary.empty == False:
                    devName = devName.replace(':', '')
                    summary.to_excel(writer, sheet_name=devName, index=False)

            writer.save()
            writer.close()

    elapsed = time.time() - startt
    print("---  seconds ---", elapsed)


if __name__ == "__main__":
    sys.exit(main(sys.argv))





