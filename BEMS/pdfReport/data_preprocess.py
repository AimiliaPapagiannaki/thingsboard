import pandas as pd

def preprocess_data(raw):
    # isolate active power data for central meter
    pwr_data = raw['102.402.002072'].copy()
    pwr_data = pwr_data[['pwrA','pwrB','pwrC']].dropna()
    pwr_data['totalpwr'] = pwr_data['pwrA']+pwr_data['pwrB']+pwr_data['pwrC']
    pwr_data = pwr_data[['totalpwr']]
    print('Power df:',pwr_data)
    raw['102.402.002072'] = raw['102.402.002072'][['totalCleanNrg']].dropna()
    
    merged_df = pd.concat(
    {key: df['totalCleanNrg'] for key, df in raw.items()},
    axis=1
    )

    # Rename the columns to the respective device names
    merged_df.columns = merged_df.columns.get_level_values(0)

    print('Merged:',merged_df)

    return pwr_data, merged_df