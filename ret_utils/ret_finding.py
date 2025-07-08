import pandas as pd
import re
from pathlib import Path

def lte_cell_normalized(df):
    """
    Processes the cell_name and system columns in the given DataFrame and adds new columns:
    - carrier: Extracted carrier (int).
    - sector: Extracted sector (int), A-Z converted to 1-26 for len(cell_name)==14 or sector digits for len(cell_name)!=14.
    - sector_type: Extracted sector type (character, e.g., S, M, C, E) for len(cell_name)!=14.
    - tuning_band: Mapped tuning band from the system column.
    
    Args:
        df (pd.DataFrame): Input DataFrame with columns 'cell_name' and 'system'.
    
    Returns:
        pd.DataFrame: Updated DataFrame with the new columns added.
    """
    system_mapping = {
        'L700': 'LB',
        'L900': 'LB',
        'L1800': 'MB',
        'L2100': 'MB',
        'L2600' : '2600'
    }

    def process_cell_name(cell_name):
        result = {'carrier': None, 'sector': None, 'sector_type': None}
        if len(cell_name) == 14:
            parts = cell_name.split('-')
            if len(parts) == 3:
                last_group = parts[-1]
                carrier = ''.join(filter(str.isdigit, last_group))
                sector_char = ''.join(filter(str.isalpha, last_group))
                if carrier.isdigit() and len(sector_char) == 1:
                    result['carrier'] = int(carrier)
                    result['sector'] = ord(sector_char.upper()) - 64
        else:
            parts = cell_name.split('_')
            if len(parts) > 1:
                result['carrier'] = 1
                last_part = parts[-1]
                match = re.match(r'([A-Z])(\d+)', last_part)
                if match:
                    sector_type, sector_digits = match.groups()
                    result['sector'] = int(sector_digits)
                    result['sector_type'] = sector_type
        return result

    processed_data = df['cell_name'].apply(process_cell_name)
    df['carrier'] = processed_data.apply(lambda x: x['carrier'])
    df['sector'] = processed_data.apply(lambda x: x['sector'])
    df['sector_type'] = processed_data.apply(lambda x: x['sector_type'])
    df['sector'] = df['sector'].astype(pd.Int64Dtype())
    df['tuning_band'] = df['system'].apply(lambda x: system_mapping.get(x, x))
    return df
 



def eric_air(df, sectorcarrierid_col, nodeid_col):
    """
    Processes a DataFrame to extract sector, carrier, and system information 
    from the specified sectorcarrierid and nodeid columns. Additionally, adds 
    'score' and 'advice' based on the length of sectorcarrierid.

    Args:
        df (pd.DataFrame): Input DataFrame.
        sectorcarrierid_col (str): Name of the column containing sectorcarrierid data.
        nodeid_col (str): Name of the column containing nodeid data.

    Returns:
        pd.DataFrame: DataFrame with additional columns: 'sector', 'carrier', 'system', 
                      'score', and 'advice'.
    """
    def process_sectorcarrierid(value, nodeid_value):
        result = {'sector': None, 'carrier': None, 'tuning_band': None}
        
        # Ensure value is treated as a string
        value = str(value).strip()
        
        # Check if value is a 2-digit number
        if value.isnumeric() and len(value) == 2:
            result['sector'] = int(value[0])  # First digit as 'sector' (int)
            result['carrier'] = int(value[1])  # Second digit as 'carrier' (int)
            
            # Determine 'system' based on nodeid_value
            system_mapping = {
                'L23': 'L2300',
                'L21': 'MB'
            }
            system_key = str(nodeid_value)[-3:]  # Extract last 3 characters of nodeid
            result['tuning_band'] = system_mapping.get(system_key, 'manual check')
        elif '-' in value:
            # Check for 'L23-S03C2' kind of format
            parts = value.split('-')
            if len(parts) == 2 and 'S' in parts[1] and 'C' in parts[1]:
                system_part, detail_part = parts[0], parts[1]
                
                # Mapping 'system' values
                system_mapping = {
                    'L23': 'L2300', 'L33': 'L2300', 
                    'L18': 'MB', 'L21': 'MB', 
                    'L07': 'LB', 'L09': 'LB'
                }
                result['tuning_band'] = system_mapping.get(system_part, 'manual check')
                
                # Extract 'sector' and 'carrier'
                try:
                    sector = detail_part.split('S')[1].split('C')[0]
                    carrier = detail_part.split('C')[1]
                    result['sector'] = int(sector)  # Convert to int
                    result['carrier'] = int(carrier)  # Convert to int
                except (IndexError, ValueError):
                    return {'sector': None, 'carrier': None, 'tuning_band': 'manual check'}
            else:
                return {'sector': None, 'carrier': None, 'tuning_band': 'manual check'}
        else:
            return {'sector': None, 'carrier': None, 'tuning_band': 'manual check'}
        
        return result

    # Ensure the sectorcarrierid column is string
    df[sectorcarrierid_col] = df[sectorcarrierid_col].astype(str)
    
    # Process each row
    processed_data = df.apply(
        lambda row: process_sectorcarrierid(row[sectorcarrierid_col], row[nodeid_col]), axis=1
    )
    
    # Ensure processed_data is consistent
    processed_data = processed_data.apply(
        lambda x: x if isinstance(x, dict) else {'sector': None, 'carrier': None, 'tuning_band': 'manual check'}
    )
    
    # Unpack the dictionary into separate columns
    processed_df = pd.DataFrame(processed_data.tolist())
    
    # Convert 'sector' and 'carrier' columns to integers
    processed_df['sector'] = processed_df['sector'].astype(pd.Int64Dtype())
    processed_df['carrier'] = processed_df['carrier'].astype(pd.Int64Dtype())
    
    # Add 'score' column based on the length of sectorcarrierid
    df['score'] = df[sectorcarrierid_col].apply(lambda x: 0 if len(x) in (2, 8, 9) else 1)
    
    # Add 'site' column (first 7 characters of nodeid)
    df['site'] = df[nodeid_col].apply(lambda x: x[:7])
    
    # Group by site and calculate advice
    advice_df = (
        df.groupby('site')['score']
        .sum()
        .reset_index()
        .rename(columns={'score': 'total_score'})
    )
    advice_df['advice'] = advice_df['total_score'].apply(lambda x: 'OK' if x == 0 else 'manual check')
    
    # Merge advice back into the original DataFrame
    df = pd.merge(df, advice_df[['site', 'advice']], on='site', how='left')
    
    # Combine with the processed data
    result_df = pd.concat([df, processed_df], axis=1)
    return result_df

def get_site_name(cell_name):
    match_device = re.search(r'[A-Z]{3,4}\d{3,4}', cell_name)
    if match_device:
        return match_device.group(0)
    else:
        return "No Site Name"


def load_cell_list(csv_path: str) -> pd.DataFrame:
    """Read a tuning‑list CSV, normalise headers, strip cell names, add `site_name_1`."""
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"Tuning list not found: {csv_path}")

    df = pd.read_csv(csv_path)
    df.columns = [c.lower() for c in df.columns]
    if "cell name" not in df.columns:
        raise ValueError("Expected column 'cell name' in tuning list")

    df["cell name"] = df["cell name"].str.strip()
    df["site_name_1"] = df["cell name"].apply(get_site_name)  # get_site_name already exists
    return df




def hwret(df_hw):
    """
    Processes the input DataFrame to classify tuning bands, numeric sectors (S[digit]),
    and character-based sectors (_S[A-Z]), ensuring they are handled separately.

    Args:
        df_hw (pd.DataFrame): Input DataFrame with at least a 'device_name' column.

    Returns:
        pd.DataFrame: Expanded DataFrame with tuning bands, sectors, usage, and classification.
    """
    # Define tuning band regex patterns
    tuning_band_rules = [
        (r'(?<!\b[a-zA-Z]{3})(?<!\b[a-zA-Z]{4})(850)', '850'),
        (r'(?<!\b[a-zA-Z]{3})(?<!\b[a-zA-Z]{4})(700|900|LB)', 'LB'),
        (r'(?<!\b[a-zA-Z]{3})(?<!\b[a-zA-Z]{4})(1800|2100|HB)', 'MB'),
        (r'(?<!\b[a-zA-Z]{3})(?<!\b[a-zA-Z]{4})(2300)', 'L2300'),
        (r'(?<!\b[a-zA-Z]{3})(?<!\b[a-zA-Z]{4})(2600)', 'L2600'),
    ]

    # Regex pattern for valid device_name rows
    pattern = r'^(HB|LB|2300|2600|2100|850|1800)_SET[1-4]_S\d{1,3}$'

    # Fill missing values in 'device_name'
    df_hw['device_name'] = df_hw['device_name'].fillna('')

    # Create the usage words list
    usage_words = df_hw[df_hw['device_name'].str.match(pattern)]['device_name'].tolist()

    # Function to classify tuning bands and sectors for a single device
    def classify_device(device_name):
        # Extract tuning bands
        tuning_bands = [result for pattern, result in tuning_band_rules if re.search(pattern, device_name)]
        if not tuning_bands:
            tuning_bands = ['Other']  # Default if no match

        # Extract numeric sectors (S[digit])
        numeric_sectors = re.findall(r'[Ss](\d{1,3})', device_name)
        numeric_sectors = [int(s) for s in numeric_sectors] if numeric_sectors else []

        # Extract character-based sectors (_S[A-Z])
        char_sectors = re.findall(r'_S([A-Z])(?![A-Z0-9])', device_name)  # Match _S followed by a single letter
        char_sectors_as_numbers = [ord(char) - 64 for char in char_sectors] if char_sectors else []

        # Combine numeric and character-based sectors
        all_sectors = numeric_sectors + char_sectors_as_numbers

        # Ensure sectors align with tuning bands
        mapped_sectors = []
        for i, tuning_band in enumerate(tuning_bands):
            if i < len(all_sectors):
                mapped_sectors.append((tuning_band, all_sectors[i]))
            else:
                mapped_sectors.append((tuning_band, None))  # No sector available for this tuning band

        # Check if device_name is in usage_words
        usage = 0 if device_name in usage_words else 1

        return mapped_sectors, usage

    # Expand rows with tuning bands and sectors
    def expand_rows(row):
        mapped_sectors, usage = classify_device(row['device_name'])
        expanded_rows = []
        for tuning_band, sector in mapped_sectors:
            new_row = row.copy()  # Copy the original row
            new_row['tuning_band'] = tuning_band
            new_row['sector'] = sector
            new_row['usage'] = usage
            expanded_rows.append(new_row)
        return expanded_rows

    # Process each row and flatten the expanded rows
    expanded_data = []
    for _, row in df_hw.iterrows():
        expanded_data.extend(expand_rows(row))

    # Create the expanded DataFrame
    df_expanded = pd.DataFrame(expanded_data)

    # Group by 'site_name' and sum 'usage' to classify as OK or Care
    usage_summary = df_expanded.groupby('site_name')['usage'].sum()
    classification = usage_summary.apply(lambda x: 'OK' if x == 0 else 'manual check')
    df_expanded['advice'] = df_expanded['site_name'].map(classification)

    return df_expanded



def eric_non_air(df):
    """Enhanced version supporting multiple tuning bands in userlabel."""

    # Early exit for empty DataFrame
    if df.empty:
        print("Eric_non_air DataFrame is empty. Returning an empty DataFrame.")
        return pd.DataFrame(columns=[
            'site', 'userlabel', 'antennanearunitid', 'retsubunitid', 'antennaunitgroupid',
            'tuning_band', 'sector', 'Parameter MO', 'usage', 'advice', 'Parameter Name'
        ])
    
    def letter_to_number(letter):
        """Convert single letter to number (A=1, B=2, etc)"""
        return ord(letter.upper()) - ord('A') + 1

    def extract_tuning_band_and_sector(userlabel):
        results = []
        if not userlabel or not isinstance(userlabel, str):
            return [{'tuning_band': None, 'sector': None}]

        # Handle multiple tuning band cases
        parts = re.split(r'\+|_By_|_by_', userlabel)
        
        for part in parts:
            # Skip empty parts or known suffixes
            if not part or part.strip() in ['Triplexer', 'Diplexer']:
                continue

            tuning_band = None
            sector = None

            # Extract tuning bands
            band_match = re.search(r'(?<![A-Z]{2})L(07|7|09|9|18|21|23)', part)
            if band_match:
                band = band_match.group(1)
                tuning_band_mapping = {
                    '07': 'LB', '7': 'LB',
                    '09': 'LB', '9': 'LB',
                    '18': 'MB',
                    '21': 'MB',
                    '23': 'L2300'
                }
                tuning_band = tuning_band_mapping.get(band)

            # First check for alpha sectors
            alpha_sector = re.search(r'S([A-Z])', part)
            if alpha_sector:
                sector = letter_to_number(alpha_sector.group(1))
            else:
                # Then check for numeric sectors
                numeric_sector = re.search(r'S(\d{1,2})', part)
                if numeric_sector:
                    sector = int(numeric_sector.group(1))

            if tuning_band or sector:
                results.append({'tuning_band': tuning_band, 'sector': sector})

        return results if results else [{'tuning_band': None, 'sector': None}]

    def expand_rows(row):
        userlabel = row['userlabel']
        extracted = extract_tuning_band_and_sector(userlabel)
        expanded_rows = []
        for result in extracted:
            new_row = row.copy()
            new_row['tuning_band'] = result['tuning_band']
            new_row['sector'] = result['sector']
            expanded_rows.append(new_row)
        return expanded_rows

    def convert_antennaunitgroupid(value):
        try:
            return int(float(value)) if str(value).replace('.', '', 1).isdigit() else value
        except:
            return value

    def check_pattern(userlabel):
        if not isinstance(userlabel, str):
            return 1
            
        parts = re.split(r'\+|_By_|_by_', userlabel)
        parts = [p for p in parts if p and p.strip() not in ['Triplexer', 'Diplexer']]
        
        patterns = [
            r'^L\d{2}_S\d{1,2}$',
            r'^UL\d{2}_S\d{1,2}$',
            r'^U09/L07_S\d{1,2}$',
            r'^L\d{2}_S[A-Z]$',
            r'^G\d{2}_S\d{1,2}$',
            r'^U\d{2}_S\d{1,2}$'
        ]
        
        for part in parts:
            if not any(re.match(pattern, part.strip()) for pattern in patterns):
                return 1
        return 0

    # Apply the function to expand rows
    expanded_data = []
    for _, row in df.iterrows():
        expanded_data.extend(expand_rows(row))

    # Create the expanded DataFrame
    df_expanded = pd.DataFrame(expanded_data)

    # Convert columns to integers
    columns_to_convert = ['antennanearunitid', 'retsubunitid', 'sector']
    for column in columns_to_convert:
        df_expanded[column] = pd.to_numeric(df_expanded[column], errors='coerce').astype(pd.Int64Dtype())

    # Convert antennaunitgroupid
    df_expanded['antennaunitgroupid'] = df_expanded['antennaunitgroupid'].apply(convert_antennaunitgroupid)

    # Create Parameter MO column
    df_expanded['Parameter MO'] = (
        "AntennaUnitGroup=" 
        + df_expanded['antennaunitgroupid'].astype(str) 
        + ",AntennaNearUnit=" 
        + df_expanded['antennanearunitid'].astype(str) 
        + ",RetSubUnit=" 
        + df_expanded['retsubunitid'].astype(str)
    )

    # Add Pattern Match column (advice)
    df_expanded['usage'] = df_expanded['userlabel'].apply(check_pattern)
    
    # Add site-based advice (advice_1)
    site_advice = df_expanded.groupby('site')['usage'].sum().reset_index()
    site_advice['advice'] = site_advice['usage'].apply(lambda x: 'manual check' if x > 0 else 'OK')
    df_expanded = df_expanded.merge(site_advice[['site', 'advice']], on='site', how='left')

    df_expanded['Parameter Name'] = "electricalAntennaTilt"
    
    return df_expanded



