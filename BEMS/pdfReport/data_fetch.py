import requests
import json

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


def read_data(address, devid, acc_token, start_time, end_time, descriptors,  entity):
    """
    Reads data from the API and returns a DataFrame.
    """
        
    df = pd.DataFrame([])
    
    url = f"{address}/api/plugins/telemetry/DEVICE/{devid}/values/timeseries"
        
    try:
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
            tmp = pd.concat(
                [pd.DataFrame(r2[desc]).rename(columns={'value': desc}).set_index('ts') for desc in r2],
                axis=1
            )
            tmp.reset_index(drop=False, inplace=True)
            tmp['ts'] = pd.to_datetime(tmp['ts'], unit='ms')
            tmp.sort_values(by=['ts'], inplace=True)
            tmp.set_index('ts', inplace=True, drop=True)

            df = pd.concat([df, tmp])

    except Exception as e:
        print(f"Error reading data for device {devid}: {e}")
        


def retrieve_raw(url):

    get_access_token(url)
    listdevs = ["102.402.002072","ΚΛΙΜΑΤΙΣΜΟΣ","ΦΩΤΙΣΜΟΣ",
                "Κέντρο Ερευνας & Τεχνολογίας (ΚΕΤ)",
                "Αμφιθέατρο",
                "Πλανητάριο",
                "Γραφεία Διοίκησης",
                "Αίθουσα Διαλέξεων",
                "U - TECH LAB",
                "Βιβλιοθήκη",
                "Λεβητοστάσιο",
                "Data Room",
                "Κυλικείο",
                "Αποθήκη Ε",
                "Εξωτερικός Φωτισμός",
                "Λοιπά Φορτία Eγκατάστασης"]
    devices_subset = listdevs[:3]

    for device in listdevs:
        entity = 'asset' if (device in devices_subset) else 'device'
        devid = get_devid(url, device, entity)


    