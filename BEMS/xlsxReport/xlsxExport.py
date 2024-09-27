import sys
import requests
import pandas as pd
import numpy as np
import config
import argparse
import datetime
import pytz
import re

def get_access_token():
    """
    Get the access token from TB
    """

    r = requests.post(config.DATA_URL + "/api/auth/login",
                      json={'username': config.USER, 'password': config.PASS}).json()
    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']
    return acc_token   


def get_devName(acc_token):
    """
    Collect all devices' and assets' info
    """
    response = requests.get(url=f"{config.DATA_URL}/api/relations/info?fromId={config.EVGENIDIO_ID}&fromType=ASSET",
    headers={'Content-Type': 'application/json','Accept': '*/*','X-Authorization': acc_token}).json()
    
    ent_dict = {}
    for i in range(0,len(response)):
        ent_dict[response[i]['to']['id']]={'device':response[i]['toName'],'entityType':response[i]['to']['entityType']}

    return ent_dict
        
def get_dev_info(acc_token, device, entity):
    """
    Retrieve device information, such as device id and label
    """
    # get devid by serial name
    url=config.DATA_URL + "/api/tenant/"+entity+"s?"+entity+"Name=" + device
    
    r1 = requests.get(
        url=f"{config.DATA_URL}/api/tenant/{entity}s",
        params={entity+"Name": device},
        headers={
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'X-Authorization': acc_token
        }
    ).json()
    
    label = r1['label'] 
    devid = r1['id']['id']   
    
    return devid, label


def read_data(acc_token, devid, start_time, end_time, descriptors, entity):
    """
    Fetch raw telemetry
    """

    url = f"{config.DATA_URL}/api/plugins/telemetry/{entity}/{devid}/values/timeseries"

    df = pd.DataFrame([])
    offset = 30 * 86400000 if int(end_time) - int(start_time) > 30 * 86400000 else 86400000
    svec = np.arange(int(start_time), int(end_time), offset)
    
    for st in svec:
        en = st + offset - 1
        en = int(end_time) if int(end_time) - en <= 0 else en

        response = requests.get(
                url=url,
                params={
                    "keys": descriptors,
                    "startTs": st,
                    "endTs": en,
                    "agg": "NONE",
                    "limit": 1000000
                },
                headers={
                    'Content-Type': 'application/json',
                    'Accept': '*/*',
                    'X-Authorization': acc_token
                }
            )
        r2 = response.json()
        
        if r2:
            tmp = pd.concat(
                [pd.DataFrame(r2[desc]).rename(columns={'value': desc}).set_index('ts') for desc in r2],
                axis=1
            )

            tmp.reset_index(drop=False, inplace=True)
            tmp['ts'] = pd.to_datetime(tmp['ts'], unit='ms')
            tmp['ts'] = tmp['ts'].dt.tz_localize('utc').dt.tz_convert('Europe/Athens')
            tmp.sort_values(by=['ts'], inplace=True)
            tmp.set_index('ts', inplace=True, drop=True)
            df = pd.concat([df, tmp])

            if df.empty:
                df = pd.DataFrame([])
            else:
                df = df.apply(pd.to_numeric, errors='coerce')
                df = df.resample('5T').max()
        else:
            df = pd.DataFrame([])
        # print('Empty json!')
    return df


def correct_cumulative_column(df, column_name):
    # Extract the column to a list for easier manipulation
    values = df[column_name].tolist()

    # Iterate through the list and correct the values
    for i in range(1, len(values)):
        if values[i] < values[i-1]:
            diff = values[i-1] - values[i]
            for j in range(i, len(values)):
                values[j] += diff
    
    # Assign the corrected values back to the DataFrame
    df[column_name] = values
    return df

def preprocess_nrg(df):
    for ph in ['A','B','C']:
        # check if there are incidents of negative delta nrg
        if not df.loc[df['cnrg'+ph]<df['cnrg'+ph].shift()].empty:
            print('negative')
            df = correct_cumulative_column(df, 'cnrg'+ph)
    
    return df

def process_cnrg(df):
    df = df.dropna()
    df = preprocess_nrg(df)
    for ph in ['A','B','C']:
        df['cnrg'+ph] = df['cnrg'+ph]-df['cnrg'+ph].shift()
        
    df['totalCleanNrg'] = df['cnrgA']+df['cnrgB']+df['cnrgC']
    df = df.resample('1H', label='left').sum()
    df = df.dropna()
    df = df.iloc[1:]
    return df  


def parse_args():
    """
    Parse input arguments and assign them to relative variables
    """
    parser = argparse.ArgumentParser(description="Process device telemetry data.")
    parser.add_argument("entityId", help="Id of the entity")
    parser.add_argument("start_time", help="Start time in epoch milliseconds")
    parser.add_argument("end_time", help="End time in epoch milliseconds")
    parser.add_argument("interval", help="Resampling interval in days")
    return parser.parse_args()


def main(argv):
    args = parse_args()
    # input arguments
    
    # Convert Unix timestamp to a datetime object
    athens_tz = pytz.timezone('Europe/Athens')
    st_date = datetime.datetime.fromtimestamp(int(args.start_time)/1000, tz=athens_tz)
    st_date = st_date.strftime('%d_%b_%Y')
    en_date = datetime.datetime.fromtimestamp(int(args.end_time)/1000, tz=athens_tz)
    en_date = en_date.strftime('%d_%b_%Y')
    
    if args.interval=='D':
        interval_name = 'Daily'
    elif args.interval=='M':
        interval_name = 'Monthly'
    elif args.interval=='H':
        interval_name = 'Hourly'
    
    filename = 'Evgenidio_'+st_date+'_'+en_date+'_'+interval_name+'.xlsx'
    excel_file_path = config.XLSX_DIR+filename
    
    mapcols = {'clean_nrgA':'Energy A (kWh)',
               'clean_nrgB':'Energy B (kWh)',
               'clean_nrgC':'Energy C (kWh)',
               'totalCleanNrg': 'Total Energy (kWh)',
               'cnrgA':'Energy A (kWh)',
               'cnrgB':'Energy B (kWh)',
               'cnrgC':'Energy C (kWh)',
               }
    acc_token = get_access_token()
    
    if args.interval=='H':
        descriptors = 'cnrgA,cnrgB,cnrgC'
        args.start_time = str(int(args.start_time)-int(3e5))
    else:
        descriptors = 'clean_nrgA,clean_nrgB,clean_nrgC,totalCleanNrg'
    labels = []
    ids = []
    entities = []
    entities_info = get_devName(acc_token)

    for devid in args.entityId.split(","):
        device = entities_info[devid]['device']
        entity = entities_info[devid]['entityType']
        print(device, entity)
        entities.append(entity)
        [_, label] = get_dev_info(acc_token, device, entity.lower())

        if device == '102.402.002072':
            label = 'Σύνολο φορτίων' # rename central meter
        labels.append(label)
        ids.append(devid)

    with pd.ExcelWriter(excel_file_path) as writer:
        for i in range(0,len(labels)):
            df = read_data(acc_token, ids[i],  args.start_time, args.end_time, descriptors, entities[i])

            if args.interval == 'H':
                df = process_cnrg(df)
            else:
                df = df.resample('1D').max()
                # df['Average hourly active power (kW)'] = df['totalCleanNrg']/(1000*24 )
                # df = df.resample(args.interval).agg({'clean_nrgA':'sum','clean_nrgB':'sum','clean_nrgC':'sum','totalCleanNrg':'sum','Average hourly active power (kW)':'mean'})
                df = df.resample(args.interval).sum()
            for col in df.columns:
                if col in mapcols.keys():
                    df[col] = np.round(df[col]/1000,2)
                    df = df.rename(columns={col:mapcols[col]})

            df.index = df.index.tz_localize(None)
            if args.interval=='M':
                df.index = df.index.strftime('%b-%Y')
            clean_sheet_name = re.sub(r'[\/:*?"<>|]', '-', labels[i])
            clean_sheet_name = clean_sheet_name[:31]
            df.to_excel(writer, sheet_name=clean_sheet_name)    
                
if __name__ == "__main__":
    sys.exit(main(sys.argv))