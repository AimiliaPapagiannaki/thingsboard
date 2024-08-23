import sys
import requests
import pandas as pd
import numpy as np
import config
import argparse


def get_access_token():
    """
    Get the access token from TB
    """

    r = requests.post(config.DATA_URL + "/api/auth/login",
                      json={'username': config.USER, 'password': config.PASS}).json()
    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']
    return acc_token   


def get_dev_info(acc_token, device, entity):
    """
    Retrieve device information, such as device id and label
    """
    # get devid by serial name
    r1 = requests.get(
        url=config.DATA_URL + "/api/tenant/"+entity+"s?"+entity+"Name=" + device,
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
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
    """
    Check for negative delta energy in consecutive values
    """
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
    """
    Transform cumulative energy to delta energy
    """
    for ph in ['A','B','C']:
        # check if there are incidents of negative delta nrg
        if not df.loc[df['cnrg'+ph]<df['cnrg'+ph].shift()].empty:
            df = correct_cumulative_column(df, 'cnrg'+ph)
        df['nrg'+ph] = df['cnrg'+ph]-df['cnrg'+ph].shift()
    df = df.drop(['cnrgA','cnrgB','cnrgC'], axis=1)
    
    return df

def parse_args():
    """
    Parse input arguments and assign them to relative variables
    """
    parser = argparse.ArgumentParser(description="Process device telemetry data.")
    parser.add_argument("entityName", help="Name of the entity")
    parser.add_argument("start_time", help="Start time in epoch milliseconds")
    parser.add_argument("end_time", help="End time in epoch milliseconds")
    parser.add_argument("interval", help="Resampling interval in days")
    return parser.parse_args()


def main(argv):
    args = parse_args()
    # input arguments
    

    excel_file_path = 'test.xlsx'
    mapcols = {'clean_nrgA':'Energy A (kWh)',
               'clean_nrgB':'Energy B (kWh)',
               'clean_nrgC':'Energy C (kWh)',
               'totalCleanNrg': 'Total Energy (kWh)'
               }
    acc_token = get_access_token()

    # descriptors = 'cnrgA,cnrgB,cnrgC,pwrA,pwrB,pwrC,vltA,vltB,vltC,curA,curB,curC,cosA,cosB,cosC'
    descriptors = 'clean_nrgA,clean_nrgB,clean_nrgC,totalCleanNrg'
    # start_time = '1722459600000'
    # end_time = '1722546000000'
    # interval = '60'
    
    labels = []
    ids = []

    for device in args.entityName.split(","):
    # if args.entityName=='building': # if the entire building is selected
    #     # entityId = '889379a0-2e37-11ef-9186-d723be8e1872' # Power meters Eugenideio - deviceGroup
    #     entityId = 'cc6c3fe0-2e37-11ef-9186-d723be8e1872' # Rooms asset group id, to get virtual Rooms

    #     r1 = requests.get(url=config.DATA_URL + "/api/entityGroup/"+entityId+"/entities?pageSize=1000&page=0",headers={'Content-Type': 'application/json', 
    #     'Accept': '*/*', 'X-Authorization': acc_token}).json()
    #     for i in range(0,len(r1['data'])):
    #         if r1['data'][i]['name'] != 'Test Room':
    #             devices.append(r1['data'][i]['name'])
    #             ids.append(r1['data'][i]['id']['id'])
        if device == '102.402.002072':
            entity = 'device'
        else:
            entity = 'asset'
        
        [devid, label] = get_dev_info(acc_token, device, entity)
        print(label)
        labels.append(label)
        ids.append(devid)

    with pd.ExcelWriter(excel_file_path) as writer:
        for i in range(0,len(labels)):
            df = read_data(acc_token, ids[i],  args.start_time, args.end_time, descriptors, entity.upper())
            df = df.resample(args.interval).sum()
            print(df)
            for col in df.columns:
                if col in mapcols.keys():
                    df[col] = np.round(df[col]/1000,2)
                    df = df.rename(columns={col:mapcols[col]})
                    
            df.index = df.index.tz_localize(None)
            df.to_excel(writer, sheet_name=labels[i])    
                
if __name__ == "__main__":
    sys.exit(main(sys.argv))