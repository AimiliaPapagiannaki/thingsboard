import sys
import pandas as pd
import datetime
import time
import requests
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import calendar
import os
import pytz
from fpdf import FPDF
from dateutil.tz import gettz
import timeit
# from datetime import datetime
from datetime import timedelta


### Definitions of functions to download and manipulate energy data
def download_nrg(start_date, end_date, devid, tmzn):
    address = "http://localhost:8080/"
    # address =  "http://157.230.210.37:8081/"

    r = requests.post(address + "api/auth/login",
                      json={'username': 'esd@esd-global.com', 'password': 'meazon'}).json()

    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']

    start_time = str(start_date)
    end_time = str(end_date)

    r2 = requests.get(
        url=address + "api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys=cnrgA,cnrgB,cnrgC,pwrA,pwrB,pwrC&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=300000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()

    if r2:
        dfA = pd.DataFrame(r2['cnrgA'])
        dfA.set_index('ts', inplace=True)
        dfA.columns = ['cnrgA']

        dfB = pd.DataFrame(r2['cnrgB'])
        dfB.set_index('ts', inplace=True)
        dfB.columns = ['cnrgB']

        dfC = pd.DataFrame(r2['cnrgC'])
        dfC.set_index('ts', inplace=True)
        dfC.columns = ['cnrgC']

        df = pd.concat([dfA, dfB, dfC])

        df.reset_index(drop=False, inplace=True)
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        # df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn).dt.tz_localize(None)

        df = df.sort_values(by=['ts'])
        df.reset_index(drop=True, inplace=True)
        df.columns = ['Timestamp', 'cnrgA', 'cnrgB', 'cnrgC']
    else:
        df = pd.DataFrame([])
        print('Empty json!')

    return df, r2


def fill_missing_values(df, interval):
    # merge rows with same datetime to exclude nans
    df = df.groupby(df.index).max()
    df.sort_index(inplace=True)

    # create datetime series to import missing dates and reindex
    start_date = df.index[0]
    end_date = df.index[-1]
    idx = pd.date_range(start_date, end_date, freq=str(interval) + 'T')
    df = df.reindex(idx)

    return df


def create_nrg_table(df):
    df['cnrgA'] = df['cnrgA'].astype(float)
    df['cnrgB'] = df['cnrgB'].astype(float)
    df['cnrgC'] = df['cnrgC'].astype(float)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y/%m/%d %H:%M:%S.%f')
    df = df.sort_values(by="Timestamp")

    df['Timestamp'] = df['Timestamp'].astype('datetime64[s]')
    df = df.set_index('Timestamp', drop=True)
    df.index = df.index.map(lambda x: x.replace(second=0))

    ####################
    tmpdf = pd.DataFrame(df['cnrgA'].dropna())
    tmpdf['minutes'] = tmpdf.index.minute
    tmpdf['interv'] = tmpdf['minutes'].shift(-1) - tmpdf['minutes']
    interval = int(tmpdf['interv'].value_counts().argmax())
    del tmpdf

    df.index = df.index.round(str(interval) + 'T')
    df = fill_missing_values(df, interval)
    df['Timestamp'] = df.index
    df['total'] = df['cnrgA'] + df['cnrgB'] + df['cnrgC']

    return [df, interval]


def conv_to_consumption(df):
    #     convert cumulative energy to consumed energy

    df['diffA'] = np.nan
    df['diffB'] = np.nan
    df['diffC'] = np.nan
    df.diffA[((df.cnrgA.isna() == False) & (df.cnrgA.shift().isna() == False))] = df.cnrgA - df.cnrgA.shift()
    df.diffB[(df.cnrgB.isna() == False) & (df.cnrgB.shift().isna() == False)] = df.cnrgB - df.cnrgB.shift()
    df.diffC[(df.cnrgC.isna() == False) & (df.cnrgC.shift().isna() == False)] = df.cnrgC - df.cnrgC.shift()

    df.diffA.iloc[0] = 0
    df.diffB.iloc[0] = 0
    df.diffC.iloc[0] = 0

    df['total'] = np.nan
    df.total[(df.diffA.isna() == False) & (df.diffB.isna() == False) & (
            df.diffC.isna() == False)] = df.diffA + df.diffB + df.diffC

    return df


def find_nans(cnrg, energy, interval):
    # store starting points of NaNs

    df_start = energy[((energy[cnrg].isnull()) & (energy[cnrg].shift().isnull() == False))]
    if df_start.empty == False:

        if (np.isnan(energy[cnrg].iloc[0]) == True):

            df2 = pd.DataFrame([])
            df2['endpoint'] = energy.index[(energy[cnrg].isnull()) & (energy[cnrg].shift(-1).isnull() == False)].copy()
            df2 = df2.iloc[1:]

            df_start['endpoint'] = df2['endpoint']
        else:
            df_start['endpoint'] = energy.index[(energy[cnrg].isnull()) & (energy[cnrg].shift(-1).isnull() == False)]
        df_start = df_start.drop(['cnrgA', 'cnrgB', 'cnrgC', 'total'], axis=1)

        df_start['previous_dt'] = df_start.index - timedelta(minutes=interval)
        df_start['next_dt'] = df_start.endpoint + timedelta(minutes=interval)
        df_start['previous_week_start'] = df_start.previous_dt - timedelta(days=7)
        df_start['previous_week_end'] = df_start.next_dt - timedelta(days=7)

        df_start['next_week_start'] = df_start.previous_dt + timedelta(days=7)
        df_start['next_week_end'] = df_start.next_dt + timedelta(days=7)

    return df_start


def backfill(row, cnrg, energy, interval):
    tmp = pd.DataFrame(energy.loc[row.previous_week_start:row.previous_week_end])
    if tmp.shape[0] == 0:
        tmp = pd.DataFrame(
            energy.loc[row.previous_week_start + timedelta(days=7):row.previous_week_end + timedelta(days=7)])
    if tmp.shape[0] > 0:

        start1 = tmp[cnrg].iloc[0]  # starting point of previous week
        start2 = energy[cnrg].loc[row.previous_dt]  # starting point of current week

        diff1 = tmp[cnrg].iloc[-1] - start1  # diff of previous week
        diff2 = energy[cnrg].loc[row.next_dt] - start2  # diff of current week

        for i in range(1, tmp.shape[0] - 1):
            if ((tmp[cnrg].iloc[i] - start1) != diff1 and (diff1 != 0)):
                perc = (tmp[cnrg].iloc[i] - start1) / diff1
                energy[cnrg].loc[row.previous_dt + timedelta(minutes=i * interval)] = perc * diff2 + start2

            else:
                perc = 0
                energy[cnrg].loc[row.previous_dt + timedelta(minutes=i * interval)] = energy[cnrg].loc[
                    row.previous_dt + timedelta(minutes=i * interval - interval)]


def forwardfill(row, cnrg, energy, interval):
    tmp = energy.loc[row.next_week_start:row.next_week_end]
    if tmp.shape[0] > 0:
        k = 1
        # while all intermediate values are nan
        while ((tmp[cnrg].iloc[0] + tmp[cnrg].iloc[-1] == tmp[cnrg].sum()) & (
                row.next_week_start - timedelta(days=k) >= energy.Timestamp.iloc[
            0])):
            tmp = energy.loc[row.next_week_start - timedelta(days=k):row.next_week_end - timedelta(days=k)]
            k = k + 1

        if tmp.shape[0] > 0:
            start1 = tmp[cnrg].iloc[0]  # starting point of next week
            diff1 = tmp[cnrg].iloc[-1] - start1  # diff of next week

            start2 = energy[cnrg].loc[row.previous_dt]  # starting point of current week
            diff2 = energy[cnrg].loc[row.next_dt] - start2  # diff of current week

            for i in range(1, tmp.shape[0] - 1):

                # if ((tmp[cnrg].iloc[i] - start1) != diff1 and (diff1 != 0) and (np.isnan(start1)==False)):
                if ((tmp[cnrg].iloc[i] - start1) != diff1 and (diff1 != 0)):

                    perc = (tmp[cnrg].iloc[i] - start1) / diff1
                    energy[cnrg].loc[row.previous_dt + timedelta(minutes=i * interval)] = perc * diff2 + start2

                else:
                    perc = 0
                    energy[cnrg].loc[row.previous_dt + timedelta(minutes=i * interval)] = energy[cnrg].loc[
                        row.previous_dt + timedelta(minutes=i * interval - interval)]

        else:
            energy[cnrg] = energy[cnrg].interpolate(method='linear')


    else:
        k = 1
        while ((tmp.shape[0] == 0) & ((row.next_dt + timedelta(hours=24 * k)) <= energy.Timestamp.iloc[-1])):
            tmp = energy.loc[row.previous_dt + timedelta(hours=24 * k):row.next_dt + timedelta(hours=24 * k)]

            k = k + 1

        if tmp.shape[0] > 0:
            start1 = tmp[cnrg].iloc[0]  # starting point of next week
            diff1 = tmp[cnrg].iloc[-1] - start1  # diff of next week

            start2 = energy[cnrg].loc[row.previous_dt]  # starting point of current week
            diff2 = energy[cnrg].loc[row.next_dt] - start2  # diff of current week

            for i in range(1, tmp.shape[0] - 1):

                # if ((tmp[cnrg].iloc[i] - start1) != diff1 and (diff1 != 0) and (np.isnan(start1)==False)):
                if ((tmp[cnrg].iloc[i] - start1) != diff1 and (diff1 != 0)):
                    perc = (tmp[cnrg].iloc[i] - start1) / diff1
                    energy[cnrg].loc[row.previous_dt + timedelta(minutes=i * interval)] = perc * diff2 + start2
                else:
                    perc = 0
                    energy[cnrg].loc[row.previous_dt + timedelta(minutes=i * interval)] = energy[cnrg].loc[
                        row.previous_dt + timedelta(minutes=i * interval - interval)]
        else:
            energy[cnrg] = energy[cnrg].interpolate(method='linear')


def fill_nans(nrg, energy, interval):
    for cnrg in nrg:

        df_start = find_nans(cnrg, energy, interval)
        prev_status = df_start.shape[0]
        df_start.apply((lambda x: backfill(x, cnrg, energy, interval)), axis=1)
        print('backfill ended')

        while energy[cnrg].isna().sum() > 0:

            if ((energy[cnrg].isna().sum() == 1) & np.isnan(energy[cnrg].iloc[0])):
                energy[cnrg].iloc[0] = energy[cnrg].iloc[1]

            else:
                df_start = find_nans(cnrg, energy, interval)
                cur_status = df_start.shape[0]
                if ((df_start.empty == False) and (cur_status < prev_status)):
                    df_start.apply((lambda x: forwardfill(x, cnrg, energy, interval)), axis=1)

                else:
                    energy[cnrg] = energy[cnrg].interpolate(method='linear')
                prev_status = cur_status
        print('forward fill ended')


def fill_dropped_nrg(df, nrg, interval):
    for cnrg in nrg:
        dfnew = df[np.isfinite(df[cnrg])].copy()
        dropped = dfnew[dfnew[cnrg] < dfnew[cnrg].shift()]

        if dropped.empty == False:
            # keep endpoint of range of reseted values
            dropped['endpoint1'] = dfnew.index[dfnew[cnrg] > dfnew[cnrg].shift(-1)]

            # shift endpoints to match starting points and set last endpoint as the last instance of df
            dropped['endpoint'] = dropped['endpoint1'].shift(-1)
            dropped['endpoint'].iloc[-1] = df.index[-1]

            dropped.apply((lambda x: correct_dropped(x, cnrg, df, interval)), axis=1)

    return df


def correct_dropped(row, cnrg, df, interval):
    # df[cnrg].loc[row.Timestamp:row.endpoint] = np.sum([df[cnrg], df[cnrg].loc[row.endpoint1]])
    if df[cnrg].loc[row.Timestamp] > df[cnrg].loc[row.endpoint1 - timedelta(minutes=interval)]:
        df[cnrg].loc[row.endpoint1] = df[cnrg].loc[row.Timestamp]
    else:
        df[cnrg].loc[row.Timestamp:row.endpoint] = np.sum(
            [df[cnrg], np.abs(df[cnrg].loc[row.endpoint1] - df[cnrg].loc[row.Timestamp])])


def get_energy_data(start_date, end_date, devid, tmzn):
    print('Downloading cumulative energy...')

    [dfcnrg, r2] = download_nrg(start_date, end_date, devid, tmzn)

    if dfcnrg.empty == True:
        energy = pd.DataFrame([])
        interval = np.nan
        return energy, r2, interval
    else:

        nrg = ['cnrgA', 'cnrgB', 'cnrgC']
        [energy, interval] = create_nrg_table(dfcnrg)

        if ((energy.cnrgA.isna().sum() > 0.6 * energy.shape[0]) | (energy.shape[0] < 7 * 24 * 60 / interval)):
            print('Very few values!')
            energy = pd.DataFrame([])
            interval = np.nan()
            return energy, r2, interval
        else:

            print('Correcting energy dropdowns')
            energy = fill_dropped_nrg(energy, nrg, interval)

            print('Filling missing values')
            energy = conv_to_consumption(energy)
            thresA = energy.diffA.mean() + 3 * energy.diffA.std()
            thresB = energy.diffB.mean() + 3 * energy.diffB.std()
            thresC = energy.diffC.mean() + 3 * energy.diffC.std()

            energy.cnrgA[energy.diffA.shift(-1) > thresA] = np.nan
            energy.cnrgB[energy.diffB.shift(-1) > thresB] = np.nan
            energy.cnrgC[energy.diffC.shift(-1) > thresC] = np.nan

            energy = conv_to_consumption(energy)
            fill_nans(nrg, energy, interval)
            energy = conv_to_consumption(energy)

            energy['Timestamp'] = energy['Timestamp'].dt.tz_localize('utc').dt.tz_convert(tmzn)
            energy.set_index('Timestamp', inplace=True, drop=False)

            energy.drop(['cnrgA', 'cnrgB', 'cnrgC', 'Timestamp'], axis=1, inplace=True)
            energy.columns = ['totalnrg', 'nrgA', 'nrgB', 'nrgC']
            energy.nrgA = energy.nrgA / 1000
            energy.nrgB = energy.nrgB / 1000
            energy.nrgC = energy.nrgC / 1000
            energy.totalnrg = energy.totalnrg / 1000

            print('Energy df has successfully been created')
            return energy, r2, interval


def set_labels(df):
    df['working_day'] = df['Timestamp'].apply(lambda x: x.weekday())
    df['working_day'] = df['working_day'].apply(lambda x: 1 if (x < 5) else 0)
    df['day'] = df.Timestamp.dt.day
    df['hour'] = df.Timestamp.dt.hour
    dfnew = df.set_index('Timestamp')
    dfnew = dfnew.resample('1D').sum()

    if (dfnew.shape[0] >= 8):
        dfnew.reset_index(drop=False, inplace=True)
        dfnew['working_day'] = dfnew['Timestamp'].apply(lambda x: x.weekday())
        dfnew['working_day'] = dfnew['working_day'].apply(lambda x: 1 if (x < 5) else 0)
        dfnew['hour'] = dfnew.Timestamp.dt.hour
        # max and min consumption for working days
        var1 = max(dfnew.loc[dfnew.working_day == 1].totalnrg)
        var2 = min(dfnew.loc[dfnew.working_day == 1].totalnrg)

        # max and min consumption for weekends
        var3 = max(dfnew.loc[dfnew.working_day == 0].totalnrg)
        var4 = min(dfnew.loc[dfnew.working_day == 0].totalnrg)

        df['label'] = 0  # initialize

        # label 1 for working day with max consumpiton
        # label 2 for working day with min consumption
        # label 3 for weekend day with max consumption
        # label 4 for weekend day with min consumption
        for j in range(0, dfnew.shape[0]):
            if (dfnew.totalnrg[j] == var1 and dfnew.working_day[j] == 1):
                date1 = dfnew.Timestamp[j]

                df['label'].loc[df.day == date1.day] = 1
                # for k in range(0, df.shape[0] - 1):
                #   if (df.Timestamp[k].day == date1.day):
                #      df['label'][k] = 1

            elif (dfnew.totalnrg[j] == var2 and dfnew.working_day[j] == 1):
                date2 = dfnew.Timestamp[j]

                df['label'].loc[df.day == date2.day] = 2
                # for k in range(0, df.shape[0] - 1):
                #   if (df.Timestamp[k].day == date2.day):
                #      df['label'][k] = 2


            elif (dfnew.totalnrg[j] == var3 and dfnew.working_day[j] == 0):
                date3 = dfnew.Timestamp[j]
                df['label'].loc[df.day == date3.day] = 3
                # for k in range(0, df.shape[0] - 1):
                #   if (df.Timestamp[k].day == date3.day):
                #      df['label'][k] = 3

            elif (dfnew.totalnrg[j] == var4 and dfnew.working_day[j] == 0):
                date4 = dfnew.Timestamp[j]
                df['label'].loc[df.day == date4.day] = 4
                # for k in range(0, df.shape[0] - 1):
                #   if (df.Timestamp[k].day == date4.day):
                #      df['label'][k] = 4

    return df


def impute_weather_data(df, interval):
    df.tmp = df.tmp.fillna(method='bfill', limit=int(60 / interval))
    # fill nans with previous day weather data
    df['prev_tmp'] = np.roll(df.tmp, 24 * int(60 / interval))

    df.loc[df['tmp'].isnull(), 'tmp'] = df['prev_tmp']

    del df['prev_tmp']

    return df


def create_power_table(dfpwr, interval):
    dfpwr['pwrA'] = dfpwr['pwrA'].astype(float)
    dfpwr['pwrB'] = dfpwr['pwrB'].astype(float)
    dfpwr['pwrC'] = dfpwr['pwrC'].astype(float)

    dfpwr['Timestamp'] = pd.to_datetime(dfpwr['Timestamp'], format='%Y/%m/%d %H:%M:%S.%f')
    dfpwr = dfpwr.sort_values(by="Timestamp")
    dfpwr = dfpwr.set_index('Timestamp', drop=True)
    dfpwr = dfpwr.abs()

    dfpwr = dfpwr.replace(0.0, 0.000001)  # in order not to lose data with 0.0 value

    dfpwr = dfpwr.resample(str(interval) + 'T').mean()
    dfpwr = dfpwr.reset_index(drop=False)
    dfpwr = dfpwr.loc[(dfpwr.pwrA != 0.0) | (dfpwr.pwrB != 0.0) | (dfpwr.pwrC != 0.0)]
    dfpwr = dfpwr.reset_index(drop=True)

    dfpwr = dfpwr.replace(0.0, np.nan)
    dfpwr = dfpwr.interpolate(method='linear', axis=0)

    dfpwr = dfpwr.replace(0.000001, 0.0)
    dfpwr['total'] = dfpwr['pwrA'] + dfpwr['pwrB'] + dfpwr['pwrC']

    return dfpwr


def impute_consumption_data(df, interval):
    df.pwrA = df.pwrA.fillna(method='bfill', limit=1)
    df.pwrB = df.pwrB.fillna(method='bfill', limit=1)
    df.pwrC = df.pwrC.fillna(method='bfill', limit=1)

    # fill nans with previous week's consumption
    df['prev_pwrA'] = np.roll(df.pwrA, 24 * 7 * int(60 / interval))
    df['prev_pwrB'] = np.roll(df.pwrB, 24 * 7 * int(60 / interval))
    df['prev_pwrC'] = np.roll(df.pwrC, 24 * 7 * int(60 / interval))

    df.loc[df['pwrA'].isnull(), 'pwrA'] = df['prev_pwrA']
    df.loc[df['pwrB'].isnull(), 'pwrB'] = df['prev_pwrB']
    df.loc[df['pwrC'].isnull(), 'pwrC'] = df['prev_pwrC']

    # interpolate values that are maybe still missing
    df.pwrA = df.pwrA.interpolate(method='linear', axis=0)
    df.pwrB = df.pwrB.interpolate(method='linear', axis=0)
    df.pwrC = df.pwrC.interpolate(method='linear', axis=0)

    df['total'] = df['pwrA'] + df['pwrB'] + df['pwrC']

    del df['prev_pwrA']
    del df['prev_pwrB']
    del df['prev_pwrC']

    return df


def download_data(start_date, end_date, devid, assetid, r2, tmzn, interval):
    start_time = str(start_date)
    end_time = str(end_date)

    address = "http://localhost:8080/"
    # address =  "http://157.230.210.37:8081/"

    r = requests.post(address + "api/auth/login",
                      json={'username': 'esd@esd-global.com', 'password': 'meazon'}).json()
    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']

    # http://localhost:8080
    # https://m3.meazon.com
    #    r2 = requests.get(
    #       url= address + "api/plugins/telemetry/DEVICE/" + devid + "/values/timeseries?keys=pwrA,pwrB,pwrC,rpwrA,rpwrB,rpwrC&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=45000",
    #      headers={"Content-Type": "application/json", "Accept": "*/*", "X-Authorization": acc_token}).json()

    r3 = requests.get(
        url=address + "api/plugins/telemetry/ASSET/" + assetid + "/values/timeseries?keys=tmp&startTs=" + start_time + "&endTs=" + end_time + "&agg=NONE&limit=300000",
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()

    if len(r2) == 0:
        df = pd.DataFrame([])
        return df

    dfA = pd.DataFrame(r2['pwrA'])
    dfA.set_index('ts', inplace=True)
    dfA.columns = ['pwrA']

    dfB = pd.DataFrame(r2['pwrB'])
    dfB.set_index('ts', inplace=True)
    dfB.columns = ['pwrB']

    dfC = pd.DataFrame(r2['pwrC'])
    dfC.set_index('ts', inplace=True)
    dfC.columns = ['pwrC']

    df = pd.concat([dfA, dfB, dfC], sort=True)
    df.reset_index(drop=False, inplace=True)
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    # df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn).dt.tz_localize(None)
    df = df.sort_values(by=['ts'])
    df.reset_index(drop=True, inplace=True)
    df.columns = ['Timestamp', 'pwrA', 'pwrB', 'pwrC']

    df = create_power_table(df, interval)
    df.reset_index(inplace=True, drop=True)
    start_date = df.Timestamp[0]
    end_date = df.Timestamp[df.shape[0] - 1]
    date_indices = pd.date_range(start=start_date, end=end_date, freq=str(interval) + 'T')
    df.set_index('Timestamp', inplace=True)
    df = df.reindex(date_indices)
    df.reset_index(drop=False, inplace=True)
    df.rename(columns={"index": "Timestamp"}, inplace=True)
    df = impute_consumption_data(df, interval)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y/%m/%d %H:%M:%S.%f')
    df = df.sort_values(by="Timestamp")

    if len(r3) != 0:
        print('Temperature not empty')
        dfT = pd.DataFrame(r3['tmp'])
        dfT.set_index('ts', inplace=True)
        dfT.columns = ['tmp']

        dfT.reset_index(drop=False, inplace=True)
        dfT['ts'] = pd.to_datetime(dfT['ts'], unit='ms')
        # dfT['ts'] = dfT['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn).dt.tz_localize(None)
        dfT = dfT.sort_values(by=['ts'])
        dfT.columns = ['Timestamp', 'tmp']
        dfT['tmp'] = dfT['tmp'].astype(str)
        dfT['tmp'] = dfT['tmp'].str[:5]
        dfT['tmp'] = dfT['tmp'].astype(float)
        dfT.reset_index(drop=True, inplace=True)
        dfT['Timestamp'] = pd.to_datetime(dfT['Timestamp'], format='%Y/%m/%d %H:%M:%S.%f')
        dfT = dfT.sort_values(by="Timestamp")
        dfT = dfT.set_index('Timestamp', drop=True)
        dfT = dfT.resample(str(interval) + 'T').sum()
        dfT = dfT.replace(0.0, np.nan)

        df = pd.merge(df, dfT, how='left', on='Timestamp')
        df = impute_weather_data(df, interval)
        df['tmp'] = df['tmp'].astype(float)

    df['Timestamp'] = df['Timestamp'].dt.tz_localize('utc').dt.tz_convert(tmzn)
    df.set_index('Timestamp', inplace=True, drop=True)
    df.pwrA = df.pwrA / 1000
    df.pwrB = df.pwrB / 1000
    df.pwrC = df.pwrC / 1000
    df.total = df.total / 1000

    return df


def plot_energy_for_each_day(df):
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df.set_index('Timestamp')
    df = df.resample('1D').sum()
    df = df.reset_index(drop=False)
    month = calendar.month_name[df.Timestamp[0].month]

    fig, ax = plt.subplots(figsize=(7.5, 5.0))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=90)
    plt.bar(df.Timestamp.dt.day, df.totalnrg)
    plt.xlabel('Days of month')
    plt.ylabel('Energy [kWh]')
    plt.xticks(df.Timestamp.dt.day)
    plt.title('Daily energy consumption during ' + month)
    fig.tight_layout()
    plt.savefig('plot_energy_for_each_day.png')
    return month


def plot_month_statistics(dataset):
    dataset['Timestamp'] = pd.to_datetime(dataset['Timestamp'])
    dataset = dataset.set_index('Timestamp')
    dataset.resample("1H").agg(
        {'totalnrg': 'sum', 'nrgA': 'sum', 'nrgB': 'sum', 'nrgC': 'sum', 'working_day': 'mean', 'hour': 'mean',
         'label': 'mean'})
    dataset = dataset.reset_index(drop=False)
    month = calendar.month_name[dataset.Timestamp[0].month]
    arrays = [['Day with maximum energy', 'Day with minimum energy',
               'Hour with maximum energy', 'Hour with minimum energy']]
    index = pd.MultiIndex.from_arrays(arrays, names=[' '])
    df_table = pd.DataFrame({'Value [kWh]': [np.nan, np.nan, np.nan, np.nan],
                             'Date': [np.nan, np.nan, np.nan, np.nan]},
                            index=index)
    if (dataset.loc[dataset.label == 1].totalnrg.sum() > dataset.loc[dataset.label == 3].totalnrg.sum()):
        df_table.iloc[0]['Value [kWh]'] = round(dataset.loc[dataset.label == 1].totalnrg.sum(), 2)
    else:
        df_table.iloc[0]['Value [kWh]'] = round(dataset.loc[dataset.label == 3].totalnrg.sum(), 2)

    if (dataset.loc[dataset.label == 2].totalnrg.sum() > dataset.loc[dataset.label == 4].totalnrg.sum()):
        df_table.iloc[1]['Value [kWh]'] = round(dataset.loc[dataset.label == 4].totalnrg.sum(), 2)
    else:
        df_table.iloc[1]['Value [kWh]'] = round(dataset.loc[dataset.label == 2].totalnrg.sum(), 2)

    df_table.iloc[2]['Value [kWh]'] = round(max(dataset.totalnrg), 2)
    dataset_for_min = dataset.loc[dataset.index != dataset['totalnrg'].idxmin()]
    df_table.iloc[3]['Value [kWh]'] = round(min(dataset_for_min.totalnrg), 2)

    if (dataset.loc[dataset.label == 1].totalnrg.sum() > dataset.loc[dataset.label == 3].totalnrg.sum()):
        df_table = df_table.set_value('Day with maximum energy', 'Date',
                                      dataset.loc[dataset.label == 1].Timestamp.reset_index()['Timestamp'][0].strftime(
                                          "%a %d %b %Y"))
    else:
        df_table = df_table.set_value('Day with maximum energy', 'Date',
                                      dataset.loc[dataset.label == 3].Timestamp.reset_index()['Timestamp'][0].strftime(
                                          "%a %d %b %Y"))
    if (dataset.loc[dataset.label == 2].totalnrg.sum() > dataset.loc[dataset.label == 4].totalnrg.sum()):
        df_table = df_table.set_value('Day with minimum energy', 'Date',
                                      dataset.loc[dataset.label == 4].Timestamp.reset_index()['Timestamp'][0].strftime(
                                          "%a %d %b %Y"))
    else:
        df_table = df_table.set_value('Day with minimum energy', 'Date',
                                      dataset.loc[dataset.label == 2].Timestamp.reset_index()['Timestamp'][0].strftime(
                                          "%a %d %b %Y"))

    df_table = df_table.set_value('Hour with maximum energy', 'Date',
                                  dataset.loc[dataset['totalnrg'].idxmax()].Timestamp.strftime("%a %d %b %Y %H:00"))
    df_table = df_table.set_value('Hour with minimum energy', 'Date',
                                  dataset_for_min.loc[dataset_for_min['totalnrg'].idxmin()].Timestamp.strftime(
                                      "%a %d %b %Y %H:00"))

    df_table = df_table.reset_index(drop=False)
    fig, ax = plt.subplots(dpi=250)

    # Hide axes
    ax.xaxis.set_visible(False)
    ax.yaxis.set_visible(False)

    # Table from Ed Smith answer
    clust_data = df_table.as_matrix()
    collabel = (" ", "Value [kWh]", "Date")
    the_table = ax.table(cellText=clust_data, colWidths=[0.35, 0.30, 0.35], colLabels=collabel, loc='center')
    the_table.auto_set_font_size(False)
    the_table.set_fontsize(6)
    plt.savefig('plot_month_statistics.png')


def heatmap_with_energy(df):
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df.set_index('Timestamp')
    df = df.resample('1H').sum()
    df.reset_index(drop=False, inplace=True)
    df.hour = df.Timestamp.dt.hour
    month = calendar.month_name[df.Timestamp[0].month]
    df = pd.pivot_table(df, 'totalnrg', df.Timestamp.dt.day, 'hour')
    fig, ax = plt.subplots(figsize=(7.5, 5.0))
    sns.heatmap(df, cmap="Blues")
    plt.title('Heatmap of energy (kWh)')
    plt.xlabel('Hours')
    plt.ylabel('Days of month')
    plt.savefig('heatmap_with_energy.png')


def energy_of_working_day_with_maximum_consumption(df):
    df = df.loc[df.label == 1]
    df = df.reset_index(drop=True)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df.set_index('Timestamp')
    df = df.resample('1H').sum()
    df.reset_index(drop=False, inplace=True)
    fig, ax = plt.subplots(figsize=(7.5, 5.0))
    ax = plt.subplot(111)
    plt.bar(df.Timestamp.dt.hour, df.totalnrg, color='g')
    plt.xlabel('Hours of day')
    plt.ylabel('Energy [kWh]')
    plt.xticks(df.Timestamp.dt.hour)
    plt.title('Working day with maximum consumption')
    plt.savefig('energy_of_working_day_with_maximum_consumption.png')


def energy_of_working_day_with_minimum_consumption(df):
    df = df.loc[df.label == 2]
    df = df.reset_index(drop=True)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df.set_index('Timestamp')
    df = df.resample('1H').sum()
    df.reset_index(drop=False, inplace=True)
    fig, ax = plt.subplots(figsize=(7.5, 5.0))
    ax = plt.subplot(111)
    plt.bar(df.Timestamp.dt.hour, df.totalnrg, color='g')
    plt.xlabel('Hours of day')
    plt.ylabel('Energy [kWh]')
    plt.xticks(df.Timestamp.dt.hour)
    plt.title('Working day with minimum consumption')
    plt.savefig('energy_of_working_day_with_minimum_consumption.png')


def energy_of_weekend_with_maximum_consumption(df):
    df = df.loc[df.label == 3]
    df = df.reset_index(drop=True)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df.set_index('Timestamp')
    df = df.resample('1H').sum()
    df.reset_index(drop=False, inplace=True)
    fig, ax = plt.subplots(figsize=(7.5, 5.0))
    ax = plt.subplot(111)
    plt.bar(df.Timestamp.dt.hour, df.totalnrg, color='purple')
    plt.xlabel('Hours of day')
    plt.ylabel('Energy [kWh]')
    plt.xticks(df.Timestamp.dt.hour)
    plt.title('Weekend day with maximum consumption')
    plt.savefig('energy_of_weekend_with_maximum_consumption.png')


def energy_of_weekend_with_minimum_consumption(df):
    df = df.loc[df.label == 4]
    df = df.reset_index(drop=True)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df.set_index('Timestamp')
    df = df.resample('1H').sum()
    df.reset_index(drop=False, inplace=True)
    fig, ax = plt.subplots(figsize=(7.5, 5.0))
    ax = plt.subplot(111)
    plt.bar(df.Timestamp.dt.hour, df.totalnrg, color='purple')
    plt.xlabel('Hours of day')
    plt.ylabel('Energy [kWh]')
    plt.xticks(df.Timestamp.dt.hour)
    plt.title('Weekend day with minimum consumption')
    plt.savefig('energy_of_weekend_with_minimum_consumption.png')


def plot_energy_for_the_whole_month(df):
    fig, ax = plt.subplots(figsize=(7.5, 5.0))
    x = df.Timestamp
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    plt.plot(x, df.totalnrg, 'c')

    plt.xlabel('Date')
    plt.ylabel('Energy [kWh]')
    plt.title('Per-minute energy consumption of month')
    fig.tight_layout()
    plt.savefig('plot_energy_for_the_whole_month.png')


def plot_energy_and_temperature(df, tmzn):
    df2 = df.copy()
    df2 = df2.set_index(df.Timestamp)
    df2 = df2.resample('1D').mean()

    df = df.set_index(df.Timestamp)
    df = df.resample('1D').sum()
    fig, ax1 = plt.subplots(figsize=(7.5, 5.0))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

    color = 'tab:purple'
    ax1.set_xlabel('Days')
    ax1.set_ylabel('Energy [kWh]', color=color)
    ax1.bar(df.index, df['totalnrg'], color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.legend(['Energy'], loc=2)

    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

    color = 'tab:cyan'
    print('timezone find:', tmzn.find('US/'))
    if tmzn.find('US/') >= 0:
        print('timezone find:', tmzn.find('US/'))
        df2['tmp'] = df2['tmp'] * 1.8 + 32
        ax2.set_ylabel('Temperature (Fahrenheit)', color=color)  # we already handled the x-label with ax1
    else:
        ax2.set_ylabel('Temperature (Celsius)', color=color)  # we already handled the x-label with ax1
    ax2.plot(df2.index, df2['tmp'], '--*', color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    ax2.legend(['Temperature'], loc=1)
    plt.title('Daily energy consumption & mean daily temperature')
    fig.tight_layout()
    plt.savefig('plot_energy_and_temperature.png')


class FPDF(FPDF):
    # Page footer
    def footer(self):
        # Position at 1.5 cm from bottom
        self.set_y(-15)
        # Arial italic 8
        self.set_font('Arial', 'I', 8)
        # Page number
        self.cell(0, 10, str(self.page_no()), 0, 0, 'C')
        # self.set_y(-10)

    #      self.image('picturemessage_vinnf3lu.tl2.png', x=180, y=None, w=25, h=8)
    def header(self):
        self.set_y(10)
        self.image('meazon.png', x=10, y=None, w=30, h=10)


def create_pdf(path, filename, max_power, total_energy, month_Name, building_name):
    pdf = FPDF()

    pdf.add_page()
    pdf.set_xy(20, 20)
    pdf.set_font('arial', 'B', 16)
    pdf.cell(0, 10, "Summarized activity of month: " + month_Name, 0, 1, 'C')

    pdf.set_font('arial', '', 12)
    # pdf.cell(0,10, "("+building_name+")", 0, 1, 'C')
    # pdf.cell(0,10, "(Nosokomeio_Rio)", 0, 1, 'C')
    str9 = " "
    pdf.write(5, str9)
    try:
        pdf.image("epri_photos/" + building_name + ".png", x=10, y=None, w=180, h=120, type='', link='')
        pdf.cell(0, 10, "\n", 0, 1, 'C')
        pdf.write(5, str9)
        pdf.set_xy(10, 160)
        str10 = "\nMaximum active power (kW) : " + str(
            round(max_power, 2)) + "\nTotal energy consumption (kWh) : " + str(round(total_energy, 2))
        pdf.write(5, str10)

        pdf.add_page()
        pdf.set_xy(20, 20)
        pdf.set_font('arial', 'B', 16)
        pdf.cell(0, 10, "Heatmap", 0, 1, 'C')
        pdf.set_font('arial', '', 12)
        str3 = "The following graph illustrates the energy to distinguish activity levels. Light spots correspond to low activity while dark spots correspond to high activity."
        pdf.write(5, str3)
        pdf.cell(75, 10, " ", 0, 2, 'C')
        pdf.image('heatmap_with_energy.png', x=10, y=None, w=200, h=120, type='', link='')
    except:
        print('No such image')
        pdf.cell(0, 10, "\n", 0, 1, 'C')
        pdf.write(5, str9)
        pdf.set_xy(10, 40)
        str10 = "\nMaximum active power (kW) : " + str(
            round(max_power, 2)) + "\nTotal energy consumption (kWh) : " + str(round(total_energy, 2))
        pdf.write(5, str10)

        pdf.set_xy(20, 80)
        pdf.set_font('arial', 'B', 16)
        pdf.cell(0, 10, "Heatmap", 0, 1, 'C')
        pdf.set_font('arial', '', 12)
        str3 = "The following graph illustrates the energy to distinguish activity levels. Light spots correspond to low activity while dark spots correspond to high activity."
        pdf.write(5, str3)
        pdf.cell(75, 10, " ", 0, 2, 'C')
        pdf.image('heatmap_with_energy.png', x=10, y=None, w=200, h=120, type='', link='')

    # pdf.image("nosokomeio_rio.png", x=10, y=None, w=180, h=120, type='', link='')

    pdf.add_page()
    pdf.set_xy(20, 20)
    pdf.set_font('arial', 'B', 16)
    pdf.cell(0, 10, "Month synopsis\n", 0, 1, 'C')
    # pdf.cell(0, 10, "\n", 0, 1, 'C')
    pdf.set_font('arial', '', 12)
    str1 = " The daily energy energy consumption across the reporting month is illustrated in the following chart."
    pdf.write(5, str1)
    pdf.cell(75, 10, " ", 0, 2, 'C')
    pdf.image('plot_energy_for_each_day.png', x=10, y=None, w=180, h=100, type='', link='')

    pdf.set_xy(10, 150)
    pdf.set_font('arial', 'B', 16)
    pdf.cell(0, 10, "Energy plot for the entire month\n", 0, 1, 'C')
    pdf.set_font('arial', '', 12)
    str8 = "The following chart depicts energy consumption as a function of time."
    pdf.write(5, str8)
    pdf.cell(75, 10, " ", 0, 2, 'C')
    pdf.image('plot_energy_for_the_whole_month.png', x=10, y=None, w=180, h=100, type='', link='')

    pdf.add_page()
    pdf.set_xy(20, 20)
    pdf.set_font('arial', 'B', 16)
    pdf.cell(0, 10, "Month statistics", 0, 1, 'C')
    pdf.cell(0, 10, "\n", 0, 1, 'C')
    pdf.set_font('arial', '', 12)
    str2 = "The following table contains meaningful information for the reference period about the days and hours with maximun and minimun energy."
    pdf.write(5, str2)
    pdf.cell(75, 10, " ", 0, 2, 'C')
    pdf.image('plot_month_statistics.png', x=10, y=None, w=200, h=120, type='', link='')

    if os.path.isfile('plot_energy_and_temperature.png'):
        pdf.add_page()
        pdf.set_xy(20, 20)
        pdf.set_font('arial', 'B', 16)
        pdf.cell(0, 10, "Energy and temperature", 0, 1, 'C')
        pdf.cell(0, 10, "\n", 0, 1, 'C')
        pdf.set_font('arial', '', 12)
        str12 = "The graph below illustrates the mean temperature for each day compared to daily energy consumption."
        pdf.write(5, str12)
        pdf.cell(75, 10, " ", 0, 2, 'C')
        pdf.image('plot_energy_and_temperature.png', x=10, y=None, w=200, h=120, type='', link='')

    pdf.add_page()
    pdf.set_xy(20, 20)
    pdf.set_font('arial', 'B', 16)
    pdf.cell(0, 10, "Maximum energy on working days\n", 0, 1, 'C')
    pdf.set_font('arial', '', 12)
    str4 = "The following chart depicts the hourly energy consumption of the working day (Monday - Friday) with maximum energy."
    pdf.write(5, str4)
    pdf.cell(75, 10, " ", 0, 2, 'C')
    pdf.cell(-75)
    pdf.image('energy_of_working_day_with_maximum_consumption.png', x=10, y=None, w=180, h=100, type='', link='')

    pdf.set_xy(10, 150)
    pdf.set_font('arial', 'B', 16)
    pdf.cell(0, 10, "Minimum energy on working days\n", 0, 1, 'C')
    pdf.set_font('arial', '', 12)
    str5 = "The following chart depicts the hourly energy consumption of the working day (Monday - Friday) with minimum energy."
    pdf.write(5, str5)
    pdf.cell(75, 10, " ", 0, 2, 'C')
    pdf.image('energy_of_working_day_with_minimum_consumption.png', x=10, y=None, w=180, h=100, type='', link='')

    pdf.add_page()
    pdf.set_xy(20, 20)
    pdf.set_font('arial', 'B', 16)
    pdf.cell(0, 10, "Maximum energy on weekends\n", 0, 1, 'C')
    pdf.set_font('arial', '', 12)
    str6 = "The following chart depicts the hourly energy consumption of the day within the weekend (Saturday - Sunday) with maximum energy."
    pdf.write(5, str6)
    pdf.cell(75, 10, " ", 0, 2, 'C')
    pdf.cell(-75)
    pdf.image('energy_of_weekend_with_maximum_consumption.png', x=10, y=None, w=180, h=100, type='', link='')

    pdf.set_xy(10, 150)
    pdf.set_font('arial', 'B', 16)
    pdf.cell(0, 10, "Minimum energy on weekends\n", 0, 1, 'C')
    pdf.set_font('arial', '', 12)
    str7 = "The following chart depicts the hourly energy consumption of the day within the weekend (Saturday - Sunday) with minimum energy."
    pdf.write(5, str7)
    pdf.cell(75, 10, " ", 0, 2, 'C')
    pdf.image('energy_of_weekend_with_minimum_consumption.png', x=10, y=None, w=180, h=100, type='', link='')

    os.chdir(path)

    pdf.output(filename + ".pdf", 'F')


def main(argv):
    print(argv)

    month = int(argv[1])
    year = int(argv[2])
    device_id = str(argv[3])
    device_name = str(argv[4])
    asset_id = str(argv[5])
    building_name = str(argv[6])
    tmzn = str(argv[7])

    path = '../PDF Files/'

    filename = str(device_name) + '_' + str(month) + '_' + str(year)

    start_of_last_month = datetime.datetime(year, month, 1, tzinfo=gettz(tmzn))
    next_month = month + 1
    if next_month == 13:
        next_month = 1
        year = year + 1
    end_of_last_month = datetime.datetime(year, next_month, 1, 23, 59, 59, tzinfo=gettz(tmzn)) - datetime.timedelta(
        days=1)

    # UTC_OFFSET_TIMEDELTA = start_of_last_month.astimezone(utc).replace(tzinfo=None) - start_of_last_month

    # end_of_last_month = end_of_last_month - UTC_OFFSET_TIMEDELTA
    # # end_of_last_month = end_of_last_month.strftime("%Y-%m-%d %H:%M:%S")
    # start_of_last_month = start_of_last_month - UTC_OFFSET_TIMEDELTA
    # # start_of_last_month = start_of_last_month.strftime("%Y-%m-%d %H:%M:%S")

    start_epoch = int(start_of_last_month.timestamp()) * 1000
    end_epoch = int(end_of_last_month.timestamp()) * 1000

    [energy, r2, interval] = get_energy_data(start_epoch, end_epoch, device_id, tmzn)

    if (energy.shape[0] == 0):
        # pdf = FPDF()
        # pdf.output("empty.pdf", 'F')
        return "empty"
    print('download power data...')
    power = download_data(start_epoch, end_epoch, device_id, asset_id, r2, tmzn, interval)
    print('power data download completed')

    max_power = max(power.total)
    total_energy = sum(energy.totalnrg)

    energy = energy.reset_index(drop=False)
    energy.columns = ['Timestamp', 'totalnrg', 'nrgA', 'nrgB', 'nrgC']
    # energy['Timestamp'] = energy['Timestamp'].dt.tz_localize('utc').dt.tz_convert('Europe/Athens')
    energy = set_labels(energy)
    print('start creating plots...')
    try:
        month_Name = plot_energy_for_each_day(energy)
        plot_energy_for_the_whole_month(energy)

        plot_month_statistics(energy)

        heatmap_with_energy(energy)
        energy_of_working_day_with_maximum_consumption(energy)
        energy_of_working_day_with_minimum_consumption(energy)
        energy_of_weekend_with_maximum_consumption(energy)
        energy_of_weekend_with_minimum_consumption(energy)

        df_merged = pd.merge(power, energy, how='inner', on='Timestamp')
        print('merged energy and power')
        print(df_merged.head())

        try:
            plot_energy_and_temperature(df_merged, tmzn)
        except:
            print('No temperature')
        print('create pdf')
        create_pdf(path, filename, max_power, total_energy, month_Name, building_name)
        os.chdir('../serverFiles')
        # os.chdir('../autoreports')
        if os.path.isfile('plot_energy_for_each_day.png'):
            os.remove("plot_energy_for_each_day.png")
        if os.path.isfile('plot_energy_for_the_whole_month.png'):
            os.remove("plot_energy_for_the_whole_month.png")
        if os.path.isfile('plot_month_statistics.png'):
            os.remove("plot_month_statistics.png")
        if os.path.isfile('heatmap_with_energy.png'):
            os.remove("heatmap_with_energy.png")
        if os.path.isfile('energy_of_working_day_with_maximum_consumption.png'):
            os.remove("energy_of_working_day_with_maximum_consumption.png")
        if os.path.isfile('energy_of_working_day_with_minimum_consumption.png'):
            os.remove("energy_of_working_day_with_minimum_consumption.png")
        if os.path.isfile('energy_of_weekend_with_maximum_consumption.png'):
            os.remove("energy_of_weekend_with_maximum_consumption.png")
        if os.path.isfile('energy_of_weekend_with_minimum_consumption.png'):
            os.remove("energy_of_weekend_with_minimum_consumption.png")
        if os.path.isfile('plot_energy_and_temperature.png'):
            os.remove("plot_energy_and_temperature.png")
    except:
        if os.path.isfile('plot_energy_for_each_day.png'):
            os.remove("plot_energy_for_each_day.png")
        if os.path.isfile('plot_energy_for_the_whole_month.png'):
            os.remove("plot_energy_for_the_whole_month.png")
        if os.path.isfile('plot_month_statistics.png'):
            os.remove("plot_month_statistics.png")
        if os.path.isfile('heatmap_with_energy.png'):
            os.remove("heatmap_with_energy.png")
        if os.path.isfile('energy_of_working_day_with_maximum_consumption.png'):
            os.remove("energy_of_working_day_with_maximum_consumption.png")
        if os.path.isfile('energy_of_working_day_with_minimum_consumption.png'):
            os.remove("energy_of_working_day_with_minimum_consumption.png")
        if os.path.isfile('energy_of_weekend_with_maximum_consumption.png'):
            os.remove("energy_of_weekend_with_maximum_consumption.png")
        if os.path.isfile('energy_of_weekend_with_minimum_consumption.png'):
            os.remove("energy_of_weekend_with_minimum_consumption.png")
        if os.path.isfile('plot_energy_and_temperature.png'):
            os.remove("plot_energy_and_temperature.png")
        print('fail')

    return filename


if __name__ == "__main__":
    sys.exit(main(sys.argv))
