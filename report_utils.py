import time
import pandas as pd

# The dataset that is used to combine regions together
dudetail_data = pd.read_csv('data/fuel_mix/PUBLIC_ARCHIVE#DUDETAILSUMMARY#FILE01#202507010000.CSV', header = 1)
dudetail_generator_data = dudetail_data[dudetail_data['DISPATCHTYPE'] == 'GENERATOR']

def read_file(file_path, select_cols):
    monthly_file = pd.read_csv(
    file_path,
        header=1,
        usecols = select_cols
    )
    return monthly_file

def log_time(objective, task, *args, **kwargs):
    start_time = time.time()
    result = task(*args, **kwargs)
    end_time = time.time()

    print(f'{objective} in {round(end_time - start_time, 2)} seconds')

    return result

def format_dataframe(df, col, date_column, duid):
    if date_column:
        df[col] = pd.to_datetime(df[col], format = '%Y/%m/%d %H:%M:%S')
        df['MONTH'] = df[col].dt.to_period('M')

    if duid:
        df['DUID'] = df['DUID'].astype('category')
    return df

def merge_duids(df):
    duid_regions = dudetail_data.set_index('DUID')['REGIONID'].dropna().to_dict()

    df['REGIONID'] = df['DUID'].map(duid_regions)
    return df

def split_region(state_data, region_data, region):

    nem_data = pd.concat([state_data, region_data[region_data['REGIONID'] == region]])
    return nem_data