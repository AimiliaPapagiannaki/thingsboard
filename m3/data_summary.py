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


def conv_to_consumption(df, interval):
    #     convert cumulative energy to consumed energy
    if 'cnrgA' in df.columns:
        df['cnrgA'] = df['cnrgA'] / 1000
        df['diffA'] = np.nan
        df.diffA[((df.cnrgA.isna() == False) & (df.cnrgA.shift().isna() == False))] = df.cnrgA - df.cnrgA.shift()
        df.diffA.iloc[0] = 0
        df.rename(columns={"diffA": "Consumed energy (kWh) A"}, inplace=True)
        df.drop(['cnrgA'], axis=1, inplace=True)

    if 'cnrgB' in df.columns:
        df['cnrgB'] = df['cnrgB'] / 1000
        df['diffB'] = np.nan
        df.diffB[(df.cnrgB.isna() == False) & (df.cnrgB.shift().isna() == False)] = df.cnrgB - df.cnrgB.shift()
        df.diffB.iloc[0] = 0
        df.rename(columns={"diffB": "Consumed energy (kWh) B"}, inplace=True)
        df.drop(['cnrgB'], axis=1, inplace=True)

    if 'cnrgC' in df.columns:
        df['cnrgC'] = df['cnrgC'] / 1000
        df['diffC'] = np.nan
        df.diffC[(df.cnrgC.isna() == False) & (df.cnrgC.shift().isna() == False)] = df.cnrgC - df.cnrgC.shift()
        df.diffC.iloc[0] = 0
        df.rename(columns={"diffC": "Consumed energy (kWh) C"}, inplace=True)
        df.drop(['cnrgC'], axis=1, inplace=True)

    if (('Consumed energy (kWh) A' in df.columns) and ('Consumed energy (kWh) B' in df.columns) and (
            'Consumed energy (kWh) C' in df.columns)):
        df['total'] = np.nan
        df.total[(df['Consumed energy (kWh) A'].isna() == False) & (df['Consumed energy (kWh) B'].isna() == False) & (
                df['Consumed energy (kWh) C'].isna() == False)] = df['Consumed energy (kWh) A'] + df[
            'Consumed energy (kWh) B'] + df['Consumed energy (kWh) C']
        df.rename(columns={"total": "Total Consumed energy (kWh)"}, inplace=True)

    return df


def align_resample(df, interval):
    df.index = df.index.map(lambda x: x.replace(second=0, microsecond=0))
    df = df.groupby(df.index).max()
    df.sort_index(inplace=True)

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
    r1 = requests.get(url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/keys/timeseries",
                      headers={'Content-Type': 'application/json', 'Accept': '*/*',
                               'X-Authorization': acc_token}).json()

    descriptors = descriptors.split(",")
    descriptors = [x for x in descriptors if x in r1]
    descriptors = ','.join(descriptors)

    mapping = {}
    mapping['pwrA'] = 'Average active power A (kW)'
    mapping['pwrB'] = 'Average active power B (kW)'
    mapping['pwrC'] = 'Average active power C (kW)'
    mapping['rpwrA'] = 'Reactive Power A (kVAR)'
    mapping['rpwrB'] = 'Reactive Power B (kVAR)'
    mapping['rpwrC'] = 'Reactive Power C (kVAR)'
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
    mapping['ecurA'] = 'Current A'
    mapping['ecurB'] = 'Current B'
    mapping['ecurC'] = 'Current C'
    mapping['frq'] = 'Frequency'
    mapping['cosA'] = 'Power factor A'
    mapping['cosB'] = 'Power factor B'
    mapping['cosC'] = 'Power factor C'
    mapping['scosA'] = 'Power factor A'
    mapping['scosB'] = 'Power factor B'
    mapping['scosC'] = 'Power factor C'

    r2 = requests.get(
        url=address + "/api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys=" + descriptors + "&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=250000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    if r2:
        df = pd.DataFrame([])

        if 'pwrA' in r2.keys():
            df1 = pd.DataFrame(r2['pwrA'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Average active power A (kW)']
            df1['Average active power A (kW)'] = df1['Average active power A (kW)'].astype('float')
            df1['Average active power A (kW)'] = df1['Average active power A (kW)'] / 1000

            df = pd.concat([df, df1], axis=1)

        if 'pwrB' in r2.keys():
            df1 = pd.DataFrame(r2['pwrB'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Average active power B (kW)']
            df1['Average active power B (kW)'] = df1['Average active power B (kW)'].astype('float')
            df1['Average active power B (kW)'] = df1['Average active power B (kW)'] / 1000
            df = pd.concat([df, df1], axis=1)

        if 'pwrC' in r2.keys():
            df1 = pd.DataFrame(r2['pwrC'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Average active power C (kW)']
            df1['Average active power C (kW)'] = df1['Average active power C (kW)'].astype('float')
            df1['Average active power C (kW)'] = df1['Average active power C (kW)'] / 1000
            df = pd.concat([df, df1], axis=1)

        if 'pwr' in r2.keys():
            df1 = pd.DataFrame(r2['pwr'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Total active power (kW)']
            df1['Total active power (kW)'] = df1['Total active power (kW)'].astype('float')
            df1['Total active power (kW)'] = df1['Total active power (kW)'] / 1000
            df = pd.concat([df, df1], axis=1)

        if 'rpwrA' in r2.keys():
            df1 = pd.DataFrame(r2['rpwrA'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Reactive Power A (kVAR)']
            df1['Reactive Power A (kVAR)'] = df1['Reactive Power A (kVAR)'].astype('float')
            df1['Reactive Power A (kVAR)'] = df1['Reactive Power A (kVAR)'] / 1000
            df = pd.concat([df, df1], axis=1)

        if 'rpwrB' in r2.keys():
            df1 = pd.DataFrame(r2['rpwrB'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Reactive Power B (kVAR)']
            df1['Reactive Power B (kVAR)'] = df1['Reactive Power B (kVAR)'].astype('float')
            df1['Reactive Power B (kVAR)'] = df1['Reactive Power B (kVAR)'] / 1000
            df = pd.concat([df, df1], axis=1)

        if 'rpwrC' in r2.keys():
            df1 = pd.DataFrame(r2['rpwrC'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Reactive Power C (kVAR)']
            df1['Reactive Power C (kVAR)'] = df1['Reactive Power C (kVAR)'].astype('float')
            df1['Reactive Power C (kVAR)'] = df1['Reactive Power C (kVAR)'] / 1000
            df = pd.concat([df, df1], axis=1)

        if 'vltA' in r2.keys():
            df1 = pd.DataFrame(r2['vltA'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Voltage A']
            df = pd.concat([df, df1], axis=1)

        if 'svltA' in r2.keys():
            df1 = pd.DataFrame(r2['svltA'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Voltage A']
            df = pd.concat([df, df1], axis=1)

        if 'vltB' in r2.keys():
            df1 = pd.DataFrame(r2['vltB'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Voltage B']
            df = pd.concat([df, df1], axis=1)

        if 'svltB' in r2.keys():
            df1 = pd.DataFrame(r2['svltB'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Voltage B']
            df = pd.concat([df, df1], axis=1)

        if 'vltC' in r2.keys():
            df1 = pd.DataFrame(r2['vltC'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Voltage C']
            df = pd.concat([df, df1], axis=1)

        if 'svltC' in r2.keys():
            df1 = pd.DataFrame(r2['svltC'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Voltage C']
            df = pd.concat([df, df1], axis=1)

        if 'curA' in r2.keys():
            df1 = pd.DataFrame(r2['curA'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Current A']
            df = pd.concat([df, df1], axis=1)

        if 'curB' in r2.keys():
            df1 = pd.DataFrame(r2['curB'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Current B']
            df = pd.concat([df, df1], axis=1)

        if 'curC' in r2.keys():
            df1 = pd.DataFrame(r2['curC'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Current C']
            df = pd.concat([df, df1], axis=1)

        if 'scurA' in r2.keys():
            df1 = pd.DataFrame(r2['scurA'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Current A']
            df = pd.concat([df, df1], axis=1)

        if 'scurB' in r2.keys():
            df1 = pd.DataFrame(r2['scurB'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Current B']
            df = pd.concat([df, df1], axis=1)

        if 'scurC' in r2.keys():
            df1 = pd.DataFrame(r2['scurC'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Current C']
            df = pd.concat([df, df1], axis=1)

        if 'ecur' in r2.keys():
            df1 = pd.DataFrame(r2['ecur'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Current A']
            df = pd.concat([df, df1], axis=1)

        if 'ecurB' in r2.keys():
            df1 = pd.DataFrame(r2['ecurB'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Current B']
            df = pd.concat([df, df1], axis=1)

        if 'ecurC' in r2.keys():
            df1 = pd.DataFrame(r2['ecurC'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Current C']
            df = pd.concat([df, df1], axis=1)

        if 'cnrgA' in r2.keys():
            df1 = pd.DataFrame(r2['cnrgA'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['cnrgA']
            df = pd.concat([df, df1], axis=1)

        if 'cnrgB' in r2.keys():
            df1 = pd.DataFrame(r2['cnrgB'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['cnrgB']
            df = pd.concat([df, df1], axis=1)

        if 'cnrgC' in r2.keys():
            df1 = pd.DataFrame(r2['cnrgC'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['cnrgC']
            df = pd.concat([df, df1], axis=1)

        if 'nrg' in r2.keys():
            df1 = pd.DataFrame(r2['nrg'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Total Consumed energy (kWh)']
            df1['Total Consumed energy (kWh)'] = df1['Total Consumed energy (kWh)'].astype(float)
            df1['Total Consumed energy (kWh)'] = df1['Total Consumed energy (kWh)'] / 1000
            df = pd.concat([df, df1], axis=1)

        if 'frq' in r2.keys():
            df1 = pd.DataFrame(r2['frq'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Frequency']
            df = pd.concat([df, df1], axis=1)

        if 'cosA' in r2.keys():
            df1 = pd.DataFrame(r2['cosA'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Power factor A']
            df = pd.concat([df, df1], axis=1)

        if 'cosB' in r2.keys():
            df1 = pd.DataFrame(r2['cosB'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Power factor B']
            df = pd.concat([df, df1], axis=1)

        if 'cosC' in r2.keys():
            df1 = pd.DataFrame(r2['cosC'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Power factor C']
            df = pd.concat([df, df1], axis=1)

        if 'scosA' in r2.keys():
            df1 = pd.DataFrame(r2['scosA'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Power factor A']
            df = pd.concat([df, df1], axis=1)

        if 'scosB' in r2.keys():
            df1 = pd.DataFrame(r2['scosB'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Power factor B']
            df = pd.concat([df, df1], axis=1)

        if 'scosC' in r2.keys():
            df1 = pd.DataFrame(r2['scosC'])
            df1.set_index('ts', inplace=True)
            df1.columns = ['Power factor C']
            df = pd.concat([df, df1], axis=1)

        if df.empty == False:

            df.reset_index(drop=False, inplace=True)
            df['ts'] = pd.to_datetime(df['ts'], unit='ms')
            # df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn).dt.tz_localize(None)

            df = df.sort_values(by=['ts'])
            df.reset_index(drop=True, inplace=True)
            df.set_index('ts', inplace=True, drop=True)
            for col in df.columns:
                df[col] = df[col].astype('float')

            df = align_resample(df, interval)
            df = conv_to_consumption(df, interval)

            if (('Average active power A (kW)' in df.columns) and ('Average active power C (kW)' in df.columns) and (
                    'Average active power B (kW)' in df.columns)):
                df['Total Average active power (kW)'] = df['Average active power A (kW)'] + df[
                    'Average active power B (kW)'] + df['Average active power C (kW)']
            if (('Reactive Power A (kVAR)' in df.columns) and ('Reactive Power C (kVAR)' in df.columns) and (
                    'Reactive Power B (kVAR)' in df.columns)):
                df['Total Reactive Power (kVAR)'] = df['Reactive Power A (kVAR)'] + df['Reactive Power B (kVAR)'] + df[
                    'Reactive Power C (kVAR)']
            if (('Maximum active power A (kW)' in df.columns) and ('Maximum active power B (kW)' in df.columns) and (
                    'Maximum active power C (kW)' in df.columns)):
                df['Total Maximum active power (kW)'] = df['Maximum active power A (kW)'] + df[
                    'Maximum active power B (kW)'] + df['Maximum active power C (kW)']

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
                    if entityID == 'ed73a120-f73b-11e9-b4dc-013e65d2f65e':
                        if 'Average active power A (kW)' in summary.columns:
                            summary = summary.drop(['Average active power A (kW)', 'Maximum active power A (kW)'],
                                                   axis=1)
                        if 'Average active power B (kW)' in summary.columns:
                            summary = summary.drop(['Average active power B (kW)', 'Maximum active power B (kW)'],
                                                   axis=1)
                        if 'Average active power C (kW)' in summary.columns:
                            summary = summary.drop(['Average active power C (kW)', 'Maximum active power C (kW)'],
                                                   axis=1)
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