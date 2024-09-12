import requests
import pandas as pd
import numpy as np
import json
import datetime

def get_access_token(address):

    r = requests.post(address + "/api/auth/login",
                      json={'username': 'meazonpro@meazon.com', 'password': 'meazonpro1'}).json()
    # acc_token is the token to be used in the next request
    acc_token = 'Bearer' + ' ' + r['token']
    return acc_token




def get_devid(address, device, entity):
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

def get_attr(address, acc_token, devid, month, start_time, end_time, tmzn):
    
    year = datetime.datetime.now().year
    attrib = {}
    # get sqmt
    attr = 'unitsSquareMeters'
    r2 = requests.get(
        url=address + "/api/plugins/telemetry/ASSET/" + devid + "/values/attributes?keys="+attr,
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    attrib['sqmt'] = r2[0]['value']['Eugenides Foundation']

    # get monthly cost
    attr = 'budgetInfo'
    r2 = requests.get(
        url=address + "/api/plugins/telemetry/ASSET/" + devid + "/values/attributes?keys="+attr,
        headers={'Content-Type': 'application/json', 'Accept': '*/*', 'X-Authorization': acc_token}).json()
    # attrib['budget'] = json.loads(r2[0]['value'])['kiloWattCost'][month-1]  
    attrib['budget'] = r2[0]['value'][str(year)]['kiloWattCost'][month-1]
    


    # get occupancy telemetry
    descriptors = 'occupancy'
    url = f"{address}/api/plugins/telemetry/ASSET/{devid}/values/timeseries"
    # try:
    response = requests.get(
        url=url,
        params={
            "keys": descriptors,
            "startTs": start_time,
            "endTs": end_time,
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
    occ_raw = r2['occupancy']
    
    occ_processed = []
    for entry in occ_raw:
        ts = entry['ts']
        values = json.loads(entry['value'])
        values['ts'] = ts
        occ_processed.append(values)
    # Create DataFrame
    df_occ = pd.DataFrame(occ_processed)
    df_occ['ts'] = pd.to_datetime(df_occ['ts'], unit='ms')
    df_occ['ts'] = df_occ['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
    df_occ.sort_values(by=['ts'], inplace=True)
    df_occ.set_index('ts', inplace=True, drop=True)

    for col in df_occ.columns:
        df_occ[col] = pd.to_numeric(df_occ[col], errors='coerce')
    df_occ.rename(columns={'Αμφιθέατρο':'Χρήση Αμφιθέατρο','Πλανητάριο':'Χρήση Πλανητάριο'}, inplace=True)
    df_occ = df_occ.resample('MS').first()
    

    return attrib, df_occ


def read_data(address, devid, acc_token, start_time, end_time, descriptors, entity, tmzn):
    """
    Reads data from the API and returns a DataFrame.
    """

    url = f"{address}/api/plugins/telemetry/{entity.upper()}/{devid}/values/timeseries"
    # try:
    response = requests.get(
        url=url,
        params={
            "keys": descriptors,
            "startTs": start_time,
            "endTs": end_time,
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
        df = pd.concat(
            [pd.DataFrame(r2[desc]).rename(columns={'value': desc}).set_index('ts') for desc in r2],
            axis=1
        )
        df.reset_index(drop=False, inplace=True)
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        df['ts'] = df['ts'].dt.tz_localize('utc').dt.tz_convert(tmzn)
        df.sort_values(by=['ts'], inplace=True)
        df.set_index('ts', inplace=True, drop=True)
        for col in df.columns:
            df[col] = df[col].astype('float')
            
            

    # except Exception as e:
    #     print(f"Error reading data for device {devid}: {e}")
    return df
        


def retrieve_raw(url, start_time, end_time, tmzn, start_time2, end_time2, month):
    
    zero_start_time = '1709244000000' # March 1st 2024
    acc_token = get_access_token(url)

    # Retrieve building info (sqmt, occ, budget)
    buildingid = get_devid(url, 'Eugenides Foundation', 'asset')
    [attrib, df_occ] = get_attr(url, acc_token, buildingid, month, zero_start_time, end_time, tmzn)


    listdevs = ['102.402.002072','ΚΛΙΜΑΤΙΣΜΟΣ','ΦΩΤΙΣΜΟΣ','ΨΥΚΤΕΣ',
                'Κέντρο Ερευνας & Τεχνολογίας (ΚΕΤ)',
                'Αμφιθέατρο',
                'Πλανητάριο',
                'Γραφεία Διοίκησης',
                'Αίθουσα Διαλέξεων',
                'U - TECH LAB',
                'Βιβλιοθήκη',
                'Λεβητοστάσιο',
                'Data Room',
                'Κυλικείο',
                'Αποθήκη Ε',
                'Εξωτερικός Φωτισμός',
                'Λοιπά Φορτία Eγκατάστασης']
    devices_subset = listdevs[:4]
    raw_dfs = {}

    
    
    for device in listdevs:
        entity = 'device' if (device in devices_subset) else 'asset'
        devid = get_devid(url, device, entity)

        descriptors = 'pwrA,pwrB,pwrC,cnrgA,cnrgB,cnrgC,totalCleanNrg' if device=='102.402.002072' else 'totalCleanNrg'
        df = read_data(url, devid, acc_token, start_time, end_time, descriptors, entity, tmzn)
        if not df.empty:
            df['totalCleanNrg'] = np.round(df['totalCleanNrg'],2)
            raw_dfs[device] = df

    # Fetch previous month's data for central meter
    devid = get_devid(url, '102.402.002072', 'device')
    df = read_data(url, devid, acc_token, start_time2, end_time2, 'totalCleanNrg', 'device', tmzn)
    if not df.empty:
        df['totalCleanNrg'] = np.round(df['totalCleanNrg'],2)

    
    # Fetch data for 3 devices from March 1st 2024
    
    monthly_dfs = pd.DataFrame([])
    for device in ['102.402.002072','Πλανητάριο','Αμφιθέατρο']:
        entity = 'device' if (device == '102.402.002072') else 'asset'
        devid = get_devid(url, device, entity)

        descriptors = 'totalCleanNrg'
        tmp = read_data(url, devid, acc_token, zero_start_time, end_time, descriptors, entity, tmzn)
        if not tmp.empty:
            tmp['totalCleanNrg'] = np.round(tmp['totalCleanNrg']/1000,2) # convert to kWh
            tmp.rename(columns={'totalCleanNrg':device}, inplace=True)
            monthly_dfs = pd.concat([monthly_dfs,tmp],axis=1)
            
    
    monthly_dfs = monthly_dfs.resample('MS').sum()

    return raw_dfs, df, monthly_dfs, attrib, df_occ


    