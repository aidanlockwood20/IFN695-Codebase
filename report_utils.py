import time
import pandas as pd
from tqdm.notebook import tqdm
import os

current_dir = os.getcwd()

# The dataset that is used to combine regions together
dudetail_data = pd.read_csv('data/fuel_mix/PUBLIC_ARCHIVE#DUDETAILSUMMARY#FILE01#202507010000.CSV', header = 1)
dudetail_generator_data = dudetail_data[dudetail_data['DISPATCHTYPE'] == 'GENERATOR']

def read_file(file_path, select_cols):
    try:
        monthly_file = pd.read_csv(
            file_path,
            header = 1,
            usecols = select_cols
        )
        return monthly_file
    except Exception as e:
        print('Error reading file: ', file_path, ' ', e)
        
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

def generate_renewable_datasets(agg_data):
    renewables = []
    non_renewables = []

    for fuel in agg_data['Fuel Type'].unique().tolist():
        if 'Fossil' in fuel:
            non_renewables.append(fuel)
        else:
            renewables.append(fuel)

    renewables_df = agg_data[agg_data['Fuel Type'].isin(renewables)].reset_index(drop = True)
    non_renewables_df = agg_data[agg_data['Fuel Type'].isin(non_renewables)].reset_index(drop = True)
    
    

    renewables_df = renewables_df.reset_index(drop = True)

    return renewables_df, non_renewables_df

def produce_cagr_dataframe(data, start_date, end_date, num_years = 7):

    fuel_sources = data['Fuel Type'].unique().tolist()

    fuel_data = {}

    print(fuel_sources)

    for fuel in fuel_sources:
        start_mean_supply = data[(data['Fuel Type'] == fuel) &(data['Month'] == start_date)]['Mean Supply (MW)'].values[0]
        end_mean_supply = data[(data['Fuel Type'] == fuel) & (data['Month'] == end_date)]['Mean Supply (MW)'].values[0]

        cagr = (((end_mean_supply / start_mean_supply) ** (1 / num_years)) - 1) * 100

        fuel_data[fuel] = [cagr, start_mean_supply, end_mean_supply]

    return pd.DataFrame(fuel_data, index = ['CAGR (%)', f'Start Mean Supply (MW)', f'End Mean Supply (MW)']).T

def print_lowest_scada_values(state_data, state):

    print('State Data: ', state)
    for fuel_type in state_data['Fuel Type'].unique():
        lowest_val = state_data[state_data['Fuel Type'] == fuel_type]['SCADAVALUE'].min()
        if lowest_val < 0:
            print(f'The lowest values for {fuel_type} in {state} was found to be {lowest_val}')

    print('\n\n')
    return 

def format_dataframe_report(df, col, date_column, duid):
    start_formatting = time.time()
    if date_column:
        df[col] = pd.to_datetime(df[col], format = '%Y/%m/%d %H:%M:%S')
        df['MONTH'] = df[col].dt.to_period('M')

    if duid:
        df['DUID'] = df['DUID'].astype('category')

    end_formatting = time.time()
    print(f"Formatting the {col} column took {end_formatting - start_formatting} seconds.")
    return df

def merge_price_data_fixed(state_data):
    state_data = state_data.copy()
    
    # Convert state data datetime
    print("Converting state data SETTLEMENTDATE...")
    state_data['SETTLEMENTDATE'] = pd.to_datetime(state_data['SETTLEMENTDATE'])
    
    region_id = state_data['REGIONID'].unique()
    print(f'Processing Region: {region_id[0]}')
    
    pricing_dict = {}
    
    pricing_folders = [
        os.path.join(current_dir, 'data', 'price', 'public'),
        os.path.join(current_dir, 'data', 'price', 'archived')
    ]
    
    total_files_processed = 0
    total_files_failed = 0

    for dir in pricing_folders:
        print(f'\nProcessing directory: {dir}')
        
        try:
            monthly_price_files = sorted(os.listdir(dir))
        except FileNotFoundError:
            print(f"Directory not found: {dir}")
            continue
        
        # Filter files
        monthly_price_files = [f for f in monthly_price_files if not f.startswith('.')]
        
        for file in tqdm(monthly_price_files, desc=f'Processing {dir}', total = len(monthly_price_files)):
            file_path = os.path.join(dir, file)
            
            try:
                # Read pricing file with error handling
                monthly_prices = read_file(file_path, ['SETTLEMENTDATE', 'REGIONID', 'RRP'])
                
                if monthly_prices is None or monthly_prices.empty:
                    print(f"  -> No data in file: {file}")
                    total_files_failed += 1
                    continue
                
                # Convert datetime with proper error handling
                try:
                    monthly_prices['SETTLEMENTDATE'] = pd.to_datetime(monthly_prices['SETTLEMENTDATE'])
                except Exception as dt_error:
                    print(f"  -> DateTime conversion failed for {file}: {dt_error}")
                    total_files_failed += 1
                    continue
                
                # Filter for the specific region
                state_monthly_prices = monthly_prices[monthly_prices['REGIONID'] == region_id[0]]
                
                if state_monthly_prices.empty:
                    print(f"  -> No data for region {region_id[0]} in {file}")
                    continue
                
                # Add to pricing dictionary
                state_rrps = state_monthly_prices.set_index('SETTLEMENTDATE')['RRP'].dropna().to_dict()
                pricing_dict.update(state_rrps)
                
                total_files_processed += 1
                
            except Exception as e:
                print(f"  -> Error processing file {file}: {e}")
                total_files_failed += 1
                continue
    
    state_data['RRP'] = state_data['SETTLEMENTDATE'].map(pricing_dict)
    
    state_data['SETTLEMENTDATE'] = state_data['SETTLEMENTDATE'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return state_data, pricing_dict

def populate_state_level_datasets(generator_dirs):
    # Main, Monthly DataFrames
    nsw_data = pd.DataFrame()
    qld_data = pd.DataFrame()
    sa_data = pd.DataFrame()
    tas_data = pd.DataFrame()
    vic_data = pd.DataFrame()

    state_data_columns = ['SCADAVALUE', 'SETTLEMENTDATE', 'DUID']

    for dir in generator_dirs:
        print('Inside directory: ', dir)

        dispatch_folder = sorted(os.listdir(dir))
        try:
            dispatch_folder.remove('.DS_Store')
        except:
            pass
        
        for file in tqdm(sorted(os.listdir(dir)), desc = f'Processing files in {dir}', total = len(os.listdir(dir))):
            if file[0:6] == 'PUBLIC':
                file_path = os.path.join(dir, file)
                monthly_data = pd.read_csv(file_path, header = 1, usecols = state_data_columns)
                monthly_data = format_dataframe(monthly_data, "SETTLEMENTDATE", True, True)

                region_data = merge_duids(monthly_data)

                # The main dataset
                nsw_data = split_region(nsw_data, region_data, 'NSW1')
                qld_data = split_region(qld_data, region_data, 'QLD1')
                sa_data = split_region(sa_data, region_data, 'SA1')
                tas_data = split_region(tas_data, region_data, 'TAS1')
                vic_data = split_region(vic_data, region_data, 'VIC1')

    return nsw_data, qld_data, sa_data, tas_data, vic_data