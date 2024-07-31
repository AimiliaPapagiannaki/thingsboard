import pandas as pd

def preprocess_data(raw):
    # isolate active power data for central meter
    pwr_data = raw['102.402.002072'].copy()
    pwr_data = pwr_data[['pwrA','pwrB','pwrC']].dropna()
    pwr_data['totalpwr'] = pwr_data['pwrA']+pwr_data['pwrB']+pwr_data['pwrC']
    pwr_data = pwr_data[['totalpwr']]
    print('Power df:',pwr_data)
    raw['102.402.002072'] = raw['102.402.002072'][['totalCleanNrg']].dropna()
    
    energy_data = pd.concat(
    {key: df['totalCleanNrg'] for key, df in raw.items()},
    axis=1
    )
    # Rename the columns to the respective device names
    energy_data.columns = energy_data.columns.get_level_values(0)
    energy_data.rename(columns={'102.402.002072':'Γενικός διακόπτης'}, inplace=True)
    energy_data = energy_data/1000

    # daily_loads = energy_data[['ΚΛΙΜΑΤΙΣΜΟΣ','ΦΩΤΙΣΜΟΣ']].copy()
    daily_rooms = energy_data.drop(['ΚΛΙΜΑΤΙΣΜΟΣ','ΦΩΤΙΣΜΟΣ'],axis=1).copy()

    monthly_rooms = daily_rooms.copy().resample('1M').sum()
    monthly_rooms = monthly_rooms.T
    monthly_rooms.columns = ['Energy consumption (kWh)']

    print('monthly sum of rooms:', monthly_rooms)

    return pwr_data, energy_data, monthly_rooms
