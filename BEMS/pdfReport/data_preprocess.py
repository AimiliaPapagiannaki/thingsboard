import pandas as pd

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
            df = correct_cumulative_column(df, 'cnrg'+ph)
    
    for ph in ['A','B','C']:
        df['cnrg'+ph] = df['cnrg'+ph]-df['cnrg'+ph].shift()
        
    df = df.resample('1H', label='left').sum()
    
    return df

def preprocess_data(raw):
    # isolate active power data for central meter
    cnrg_data = raw['102.402.002072'].copy()
    cnrg_data = cnrg_data[['cnrgA','cnrgB','cnrgC']].dropna()

    cnrg_data = preprocess_nrg(cnrg_data)
    cnrg_data['totalcnrg'] = cnrg_data['cnrgA']+cnrg_data['cnrgB']+cnrg_data['cnrgC']
    cnrg_data = cnrg_data[['totalcnrg']]
    
    
    pwr_data = raw['102.402.002072'].copy()
    pwr_data = pwr_data[['pwrA','pwrB','pwrC']].dropna()
    pwr_data['totalpwr'] = pwr_data['pwrA']+pwr_data['pwrB']+pwr_data['pwrC']
    pwr_data = pwr_data[['totalpwr']]
    raw['102.402.002072'] = raw['102.402.002072'][['totalCleanNrg']].dropna()
    
    energy_data = pd.concat(
    {key: df['totalCleanNrg'] for key, df in raw.items()},
    axis=1
    )
    # Rename the columns to the respective device names
    energy_data.columns = energy_data.columns.get_level_values(0)
    energy_data.rename(columns={'102.402.002072':'Γενικός διακόπτης'}, inplace=True)
    energy_data = energy_data/1000 # kWh
    pwr_data = pwr_data/1000 # kW

    # daily_loads = energy_data[['ΚΛΙΜΑΤΙΣΜΟΣ','ΦΩΤΙΣΜΟΣ']].copy()
    daily_rooms = energy_data.drop(['ΚΛΙΜΑΤΙΣΜΟΣ','ΦΩΤΙΣΜΟΣ'],axis=1).copy()

    monthly_rooms = daily_rooms.copy().resample('1M').sum()
    monthly_rooms = monthly_rooms.T
    monthly_rooms.columns = ['Energy consumption (kWh)']


    return cnrg_data, pwr_data, energy_data, monthly_rooms
