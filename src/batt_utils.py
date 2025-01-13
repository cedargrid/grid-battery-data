import pandas as pd
#from pathlib import Path
import glob 
from collections import defaultdict
import pandas as pd
import re
import os

# optimized data types for battery/bess columns
batt_dtypes =  {
                'P_AC_Set': pd.Int16Dtype(),
                'Q_AC_Set': pd.Int16Dtype(),
                'P_AC': pd.Int16Dtype(),
                'Q_AC': pd.Int16Dtype(),
                'SOC': pd.Int16Dtype(),
                'I_DC_Batt': pd.Int16Dtype(),
                'U_DC_Batt': pd.Int16Dtype(),
                'Mode_PQ': pd.BooleanDtype(),
                'Mode_Stop': pd.BooleanDtype(),
                'Mode_Silent': pd.BooleanDtype(),
                'Mode_Wait': pd.BooleanDtype(),
                'interpolated': pd.BooleanDtype()
                }

bess_dtypes = {
             'M5BAT_P': pd.Int16Dtype(), 
             'M5BAT_Q': pd.Int16Dtype(), 
             'Grid_frequency':pd.Int32Dtype(),
             'Temperature': pd.Int16Dtype(),
             'FCR_activated': pd.BooleanDtype(),
             'FCR_P': pd.Int16Dtype(),
             'FCR_control': pd.CategoricalDtype(),
             'SPA_ask_P': pd.Int16Dtype(),
             'SPA_exec_P': pd.Int16Dtype(),
             'SOC': pd.Int8Dtype(),
             'interpolated': pd.BooleanDtype(),
            }

def get_metadata():
    """Function returns dataframe with all battery metadata"""
    batt_meta_cols = ['batt_chem', 'batt_power_kw', 'batt_capacity_kwh', 'nom_cycles', 'batt_wiring', 'batt_cell_volts']
    batt_list = ['Batt01','Batt02','Batt03','Batt04','Batt05','Batt06','Batt07','Batt08','Batt09','Batt10',]
    return (pd.DataFrame(
                [['Lead-acid (OCSM)', 630, 1066, 1500, '300s1p', 2],
                ['Lead-acid (OCSM)', 630, 1066, 1500, '300s1p', 2],
                ['Lead-acid Gel (OPzV)', 630, 843, 2400, '308s2p', 2],
                ['Lead-acid Gel (OPzV)', 522, 740, 2400, '306s1p', 2],
                ['Lithium-Manganese-Oxide (LMO)', 630, 774, 6000, '192s16p', 3.7],
                ['Lithium-Manganese-Oxide (LMO)', 630, 774, 6000, '192s16p', 3.7],
                ['Lithium-Manganese-Oxide (LMO)', 630, 774, 6000, '192s16p', 3.7],
                ['Lithium-Manganese-Oxide (LMO)', 630, 774, 6000, '192s16p', 3.7],
                ['Lithium-Iron-Phosphate (LFP)', 630, 738, 5000, '240s10p', 3.2],  # some kind of boost energy of 923kwh
                ['Lithium-Titanate-Oxide (LTO)', 630, 230, 12, '312s32p', 2.3]],
                columns= batt_meta_cols, index= batt_list)
            )

def to_compact(long_df, batt_name):
    """convert single battery long-form timeseries file to compact format
    args:
        long_df (DataFrame): battery csv already converted to dateframe with datetime in index
        batt_name (str): format 'Batt01'
    returns:
        Single row dataframe with timeseries columns compressed into cells as lists
    """
    batt_name = batt_name
    start_timestamp = long_df.index[0]
    freq = '1s'                             # not sure why freq isn't populating via long_df.index.freq...
    periods = len(long_df)

    static_col_dict = {'batt_name':batt_name, 'start_timestamp':start_timestamp, 'freq':freq, 'periods':periods}
    static_cols = list(static_col_dict.keys())
    timeseries_cols = list(batt_dtypes.keys())

    compact_df = pd.DataFrame(columns= static_cols + timeseries_cols)    # initialize compact df format
    
    for col in timeseries_cols:
        compact_df[col] = [long_df[col].to_numpy()]
    for col in static_cols:
        compact_df[col] = static_col_dict[col]

    return compact_df.set_index('batt_name')

def load_csvs_to_compact(batt_path='M5BAT_04-2023_RAW'):
    """convert directory of battery csvs to compact format with metadata
    args:
        batt_path (str): path location for battery csvs
    returns:
        compressed parquet file saved to same location
    """
    batt_file = os.path.join(batt_path, "all_batts_compact_with_metadata.parquet")
    if os.path.exists(batt_file):
        return pd.read_parquet(batt_file)
    else:
        # ensure batt file names have a zero in front of integer
        for filename in os.listdir(batt_path):
            # Check if the file is a CSV file
            if filename.endswith('.csv'):
                # Use regex to find single integers in the filename
                new_filename = re.sub(r'(?<!\d)(\d)(?!\d)', r'0\1', filename)            
                # If the new filename is different, rename the file
                if new_filename != filename:
                    os.rename(os.path.join(batt_path, filename), os.path.join(batt_path, new_filename))  # generates 'Batt01' as opposed to 'Batt1'
    
        batt_file_path = os.path.join(batt_path, "Batt*.csv")
        batt_dfs = []
        for f in glob.glob(batt_file_path):
            batt_num = f[-10:-4]
            df = (pd.read_csv(f,
                            sep=';',
                            parse_dates=['DateAndTime'],
                            index_col='DateAndTime',
                            )
                      .astype(batt_dtypes)          # optimize data types
                 )
            # df.index = df.index.tz_localize('utc').tz_convert('Europe/Berlin')  # adjust to Berlin time zone
            # df.index = df.index.tz_localize(None)
            
            batt_entry = to_compact(df, batt_num)   # create compact dataframe for each battery
            batt_dfs.append(batt_entry)
    
        all_batts_compact = pd.concat(batt_dfs)     # consolidated all compacted battery data
        
        batt_meta_data = get_metadata()             # add battery metadata
        all_batts_compact_with_metadata = batt_meta_data.join(all_batts_compact)
        
        all_batts_compact_with_metadata.to_parquet(batt_file)                   # save to parquet
        
        return pd.read_parquet(batt_file)

def compact_to_long(df_compact):
    """convert compact df to long timeseriees df
    args:
        df_compact (DataFrame): df in compact format
    returns:
        expanded df with metadata dropped
    """
    int_cols = [
                'P_AC_Set',
                'Q_AC_Set',
                'P_AC',
                'Q_AC',
                'SOC',
                'I_DC_Batt',
                'U_DC_Batt',
                ]
    bool_cols = [
                'Mode_PQ',
                'Mode_Stop',
                'Mode_Silent',
                'Mode_Wait',
                'interpolated',
                ]
    timeseries_cols = int_cols + bool_cols
    
    def preprocess_expanded(row):
        batt_name= row.name
        dr = pd.date_range(
                        start=row["start_timestamp"],
                        periods=row["periods"],
                        freq=row["freq"],
                        )                 

        df_columns = defaultdict(list)
        for col in timeseries_cols:
            df_columns[col] = row[col]                                                    # this is where the compact_to_long unpacking magic occurs
        return (pd.DataFrame(df_columns, index=dr)
                  .astype({col: pd.Int16Dtype() for col in int_cols})                     # optimize data types
                  .astype({col: pd.BooleanDtype() for col in bool_cols})                  
                  .rename(columns={col: batt_name + "_" + col for col in timeseries_cols}) # append the battery name to the standard column names
                  )

    all_df_longs = []
    
    for i in range(len(df_compact)):        
        expanded_row = preprocess_expanded(df_compact.iloc[i])
        all_df_longs.append(expanded_row)
    df_long = pd.concat(all_df_longs, axis=1)
    del all_df_longs
    return df_long

def load_bess_data(bess_path='M5BAT_04-2023_RAW'):
    """load bess data from csv, store as parquet file, return dataframe"""
    bess_file = os.path.join(bess_path, "bess_raw.parquet")
    if os.path.exists(bess_file):
        return pd.read_parquet(bess_file)
    else:
        bess_file_csv = os.path.join(bess_path, "BESS.csv")
        df = (pd.read_csv(bess_file_csv, 
                        parse_dates=['DateAndTime'],
                        index_col='DateAndTime',
                        sep=';',
                      )
                .astype(bess_dtypes)
            )
        # df.index = df.index.tz_localize('utc').tz_convert('Europe/Berlin')  # adjust to Berlin time zone
        # df.index = df.index.tz_localize(None)
        
        df.to_parquet(bess_file)
        return pd.read_parquet(bess_file)