
import argparse, logging
from config import build_cfg
import importlib
import os
import pandas as pd
from pathlib import Path
from scripts.db_connect import connect_postgres
import zipfile
from dotenv import load_dotenv
from ret_utils.io_helper import load_cell_list, generate_where_clause, suggestion, tuning_band_logic
from ret_utils.ret_finding import lte_cell_normalized, eric_air, hwret, eric_non_air
from scripts.query_db import fetch_data_lte, fetch_data_nr, fetch_data_air, fetch_data_non_air, fetch_data_hw, fetch_data_hw_no_map, fetch_data_air_no_map, fetch_data_nonair_no_map, fetch_data_bfant_tilt, fetch_data_nr_tilt, fetch_data_split_tilt

env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

cluster_name = os.getenv("CLUSTER_NAME")
week_name = os.getenv("WEEK_NUM")
folder_name = cluster_name.split('_')[0]

INPUT_FILE_PATH = f'D:/D&T Project/CR Preparing/{folder_name}/Tuning_cell_list_{cluster_name}.csv'
OUTPUT_BASE_DIR = f'D:/D&T Project/CR Preparing/'


HOST = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
DATABASE = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")

sql_lte = f'lte_{week_name}'
sql_nr = f'nr_{week_name}'


df_cell = load_cell_list(os.getenv(INPUT_FILE_PATH))
site_ids = df_cell['site_name_1'].unique()
where_clause, where_clause_1, where_clause_2 = generate_where_clause(site_ids)

conn = connect_postgres(HOST, PORT, DATABASE, USER, PASSWORD)


# Setup project paths
output_dir = os.path.join(OUTPUT_BASE_DIR, folder_name)
os.makedirs(output_dir, exist_ok=True)

# Load and process input


df_lte = fetch_data_lte(sql_lte, where_clause,conn)
df_nr = fetch_data_nr(sql_nr,where_clause, conn)

df_air_1 = fetch_data_air(where_clause_1, conn)
df_air = df_air_1.pivot(index=['site', 'nodeid', 'sectorcarrierid'], columns='date', values='digitaltilt')
df_air.reset_index(inplace=True)

df_non_air_1 = fetch_data_non_air(where_clause_1, conn)
df_non_air = df_non_air_1.pivot(index=['site', 'nodeid', 'userlabel','antennaunitgroupid','antennanearunitid','retsubunitid'
                                    ,'antennamodelnumber','mintilt','maxtilt'], columns='date', values='electricalantennatilt')
df_non_air.reset_index(inplace=True)


df_hw_1 = fetch_data_hw(where_clause_2, conn)
df_hw = df_hw_1.pivot(index=['site_name', 'name', 'device_name', 'device_no','subunit_no','max_tilt','min_tilt'], columns='date', values='actual_tilt')
df_hw.reset_index(inplace=True)

#LTE CELL Normalized
df_lte_cell = lte_cell_normalized(df_lte)

#ERIC_AIR Normalized
df_eric_air = eric_air(df_air, sectorcarrierid_col='sectorcarrierid', nodeid_col='nodeid')
#HWRET Normalized
df_hwret = hwret(df_hw)
df_hwret.rename(columns={'site_name': 'site'}, inplace=True)
#ERIC_NON_AIR Normalized
df_eric_non_air = eric_non_air(df_non_air)

#ERIC_AIR MAP
eric_air_map = pd.merge(
    df_lte_cell,
    df_eric_air,
    on=['site', 'tuning_band', 'sector', 'carrier'],
    how='inner'
    )


eric_air_map['Parameter MO'] = 'SectorCarrier=' + eric_air_map['sectorcarrierid']
eric_air_map['Parameter Name'] = 'digitalTilt'
eric_air_map.sort_values(['site_id', 'tuning_band','sector','carrier'])
eric_air_map.drop_duplicates(inplace=True)

#HWRET MAP
hwret_map = pd.merge(
    df_lte_cell,
    df_hwret,
    on=['site', 'tuning_band', 'sector'],
    how='inner'
)
hwret_map.drop_duplicates(inplace=True)


#ERIC_NON_AIR MAP
eric_non_air_map = pd.merge(
    df_lte_cell,
    df_eric_non_air,
    on=['site', 'tuning_band', 'sector'],
    how='inner'
)
eric_non_air_map.drop_duplicates(inplace=True)

columns_to_include= ['cell_name', 'site_id','system', 'sector_name','rat']
df_MD_LTE_1 = df_lte[columns_to_include]
df_MD_NR_1 = df_nr[columns_to_include]
combined_df = pd.concat([df_MD_LTE_1, df_MD_NR_1], ignore_index=True)

df_cell = df_cell.merge(combined_df, left_on='cell name', right_on='cell_name', how='left')






df_lte['Tuning_Band'] = df_lte['system'].apply(tuning_band_logic)
df_nr['Tuning_Band'] = df_nr['system'].apply(tuning_band_logic)
df_cell['Tuning_Band'] = df_cell['system'].apply(tuning_band_logic)
df_cell_LTE = df_cell[df_cell['rat'].isin(['LTE']) | pd.isna(df_cell['rat']) | ((df_cell['rat'] == 'NR') & (df_cell['system'] == 'NR2600'))]
df_cell_NR = df_cell[df_cell['rat'] == 'NR']

df_lte['seach']= df_lte['site_id']+ df_lte['Tuning_Band']+df_lte['sector_name']
df_nr['seach']= df_nr['site_id']+ df_nr['system']+df_nr['sector_name']
df_cell_LTE['seach']= df_cell_LTE['site_id']+ df_cell_LTE['Tuning_Band']+df_cell_LTE['sector_name']
df_cell_NR['seach']= df_cell_NR['site_id']+ df_cell_NR['system']+df_cell_NR['sector_name']

# Merge df_cell_LTE and df_lte on 'seach', and Cell Name
merged_df_LTE = df_cell_LTE[['seach', 'cell name']].merge(
    df_lte,
    on='seach',
    how='left',  # Use left join to retain all rows from df_cell_LTE
    indicator=True  # Adds a column to show if the match was found
)

# Add a column to indicate if the value was found or not
merged_df_LTE['status'] = merged_df_LTE['_merge'].apply(
    lambda x: 'cannot find in database' if x == 'left_only' else 'found'
)


# Drop the '_merge' and 'seach' columns
merged_df_LTE = merged_df_LTE.drop(columns=['_merge', 'seach'])

# Reset the index
merged_df_LTE = merged_df_LTE.reset_index(drop=True)

# NR
# Merge df_cell_NR and df_nr on 'seach', and Cell Name
merged_df_NR = df_cell_NR[['seach', 'cell name']].merge(
    df_nr,
    on='seach',
    how='left',  # Use left join to retain all rows from df_cell_NR
    indicator=True  # Adds a column to show if the match was found
)

# Add a column to indicate if the value was found or not
merged_df_NR['status'] = merged_df_NR['_merge'].apply(
    lambda x: 'cannot find in database' if x == 'left_only' else 'found'
)

# Drop the '_merge' and 'seach' columns
merged_df_NR = merged_df_NR.drop(columns=['_merge', 'seach'])

# Reset the index
merged_df_NR = merged_df_NR.reset_index(drop=True)



# Apply the compacted function
merged_df_LTE['suggestion'] = merged_df_LTE.apply(lambda row: suggestion(row['xtxr'],row['vendor'], row['antenna_type'], is_lte=True), axis=1)
merged_df_NR['suggestion'] = merged_df_NR.apply(lambda row: suggestion(row['xtxr'],row['vendor'], row['antenna_type'], is_lte=False), axis=1)


merged_df_LTE = merged_df_LTE.drop_duplicates()
merged_df_NR = merged_df_NR.drop_duplicates()
merged_df_LTE.rename(columns={'cell name': 'cell_name_remove'}, inplace=True)
merged_df_NR.rename(columns={'cell name': 'cell_name_remove'}, inplace=True)


df_hw_no_map = fetch_data_hw_no_map(where_clause_2, os.getenv("START_DATE"), os.getenv("END_DATE"), conn)
df_hw_no_map.rename(columns={'antenna_type': 'file_type'}, inplace=True)
df_hw_no_map['MO'] = 'RETSUBUNIT'
df_hw_no_map['Parameter'] = 'Tilt'
df_hw_no_map = df_hw_no_map.drop_duplicates()
df_hw_no_map = df_hw_no_map.pivot(index=['file_type', 'site_name','name','device_name','device_no','subunit_no','MO','Parameter','max_tilt','min_tilt'], columns='date', values='actual_tilt')
df_hw_no_map.reset_index(inplace=True)


df_air_no_map = fetch_data_air_no_map(where_clause_1, os.getenv("START_DATE"), os.getenv("END_DATE"), conn)
df_air_no_map.rename(columns={'antenna_type': 'file_type'}, inplace=True)
df_air_no_map['MO'] = 'SectorCarrier=' + df_air_no_map['sectorcarrierid'].astype(str)
df_air_no_map['Parameter'] = 'digitalTilt'
df_air_no_map = df_air_no_map.pivot(index=['file_type', 'site_name','nodeid','sectorcarrierid','MO','Parameter'], columns='date', values='digitaltilt')
df_air_no_map.reset_index(inplace=True)

df_non_air_no_map = fetch_data_nonair_no_map(where_clause_1, os.getenv("START_DATE"), os.getenv("END_DATE"), conn)
df_non_air_no_map.rename(columns={'antenna_type': 'file_type'}, inplace=True)
# Columns to change to int
change_to_int = [ 'antennanearunitid', 'retsubunitid']

# Convert to numeric (float), then to integer
df_non_air_no_map[change_to_int] = df_non_air_no_map[change_to_int].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
df_non_air_no_map['MO'] = 'AntennaUnitGroup='+ df_non_air_no_map['normalizedantennaunitgroupid'].astype(str) +',AntennaNearUnit=' + df_non_air_no_map['antennanearunitid'].astype(str) +', RetSubUnit='+ df_non_air_no_map['retsubunitid'].astype(str)
df_non_air_no_map['Parameter'] = 'electricalAntennaTilt'
df_non_air_no_map = df_non_air_no_map.pivot(index=[ 'file_type', 'site_name','nodeid','normalizedantennaunitgroupid','antennanearunitid','retsubunitid'
                             ,'userlabel','antennamodelnumber','mintilt','maxtilt','MO','Parameter'], columns='date', values='electricalantennatilt')
df_non_air_no_map.reset_index(inplace=True)


df_bfant_tilt = fetch_data_bfant_tilt(sql_lte,where_clause,os.getenv("START_DATE"), os.getenv("END_DATE"), conn)
df_bfant_tilt = df_bfant_tilt.pivot(index=['cell_name', 'system', 'local_cell_id','bfant_name','device_no',
                                           'connect_rru_subrack_no','local_cell_id_cellphy'], columns='date', values='tilt')
df_bfant_tilt.reset_index(inplace=True)


df_nr_tilt = fetch_data_nr_tilt(sql_nr,where_clause,os.getenv("START_DATE"), os.getenv("END_DATE"), conn)
df_nr_tilt = df_nr_tilt.pivot(index=['nr_cell_name', 'system', 'nr_du_cell_id','nrducelltrpbeam_name','nr_du_cell_trp_id'
                                           ], columns='date', values='tilt')
df_nr_tilt.reset_index(inplace=True)

df_split_tilt = fetch_data_split_tilt(sql_lte,where_clause,os.getenv("START_DATE"), os.getenv("END_DATE"), conn)
df_split_tilt = df_split_tilt.pivot(index=['cell_name', 'system', 'local_cell_id','splitcell_name','splitcell_local_cell_id'
                                           ], columns='date', values='cell_beam_tilt')
df_split_tilt.reset_index(inplace=True)




hwret_map.to_csv(os.path.join(output_dir, f'{cluster_name}_hwret_map.csv'), index=False)
eric_air_map.to_csv(os.path.join(output_dir, f'{cluster_name}_eric_air_map.csv'), index=False)
eric_non_air_map.to_csv(os.path.join(output_dir, f'{cluster_name}_eric_non_air_map.csv'), index=False)
csv_files = [
    f'{cluster_name}_hwret_map.csv',
    f'{cluster_name}_eric_air_map.csv',
    f'{cluster_name}_eric_non_air_map.csv'
    ]
zip_file_path = os.path.join(output_dir, f'{cluster_name}_files_map.zip')

# Create the list of file paths
file_paths = [os.path.join(output_dir, file) for file in csv_files]

# Create the .zip archive
with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for file in file_paths:
        if os.path.exists(file):
            zipf.write(file, os.path.basename(file))  # Add file to ZIP with its base name
        else:
            print(f"File {file} does not exist.")

print(f"ZIP archive created at: {zip_file_path}")

# Clean up (delete) the CSV files after zipping them
for file in file_paths:
    if os.path.exists(file):
        os.remove(file)
        print(f"Deleted: {file}")
    else:
        print(f"File {file} does not exist for deletion.")



merged_df_LTE.to_csv(os.path.join(output_dir, f'Cell_LTE_result_{cluster_name}.csv'), index=False)
merged_df_NR.to_csv(os.path.join(output_dir, f'Cell_NR_result_{cluster_name}.csv'), index=False)
df_hw_no_map.to_csv(os.path.join(output_dir, f'{cluster_name}_hw.csv'), index=False)
df_air_no_map.to_csv(os.path.join(output_dir, f'{cluster_name}_air.csv'), index=False)
df_non_air_no_map.to_csv(os.path.join(output_dir, f'{cluster_name}_non_air.csv'), index=False)
df_bfant_tilt.to_csv(os.path.join(output_dir, f'{cluster_name}_bfant_tilt.csv'), index=False)
df_nr_tilt.to_csv(os.path.join(output_dir, f'{cluster_name}_nr_tilt.csv'), index=False)
df_split_tilt.to_csv(os.path.join(output_dir, f'{cluster_name}_split_tilt.csv'), index=False)
#df_RETSUBUNIT.to_csv(os.path.join(output_dir, f'{cluster_name}_RETSUBUNIT_map.csv'), index=False)
# List of CSV files
csv_files = [
    f'Cell_LTE_result_{cluster_name}.csv',
    f'Cell_NR_result_{cluster_name}.csv',
    f'{cluster_name}_hw.csv',
    f'{cluster_name}_air.csv',
    f'{cluster_name}_non_air.csv',
    f'{cluster_name}_bfant_tilt.csv',
    f'{cluster_name}_nr_tilt.csv',
    f'{cluster_name}_split_tilt.csv'
]

# Path for the zip file
zip_file_path = os.path.join(output_dir, f'{cluster_name}_files_1.zip')

# Create the list of file paths
file_paths = [os.path.join(output_dir, file) for file in csv_files]

# Create the .zip archive
with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for file in file_paths:
        if os.path.exists(file):
            zipf.write(file, os.path.basename(file))  # Add file to ZIP with its base name
        else:
            print(f"File {file} does not exist.")




print(f"ZIP archive created at: {zip_file_path}")

# Clean up (delete) the CSV files after zipping them
for file in file_paths:
    if os.path.exists(file):
        os.remove(file)
        print(f"Deleted: {file}")
    else:
        print(f"File {file} does not exist for deletion.")


