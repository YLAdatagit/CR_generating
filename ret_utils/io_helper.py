import pandas as pd
import re
from pathlib import Path

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




#  Query-helper & mapping utilities (NEW):
# -------------------------------------------------------------------
def generate_where_clause(site_ids):
    site_ids_str = "', '".join(site_ids)
    clause_site   = f"site IN ('{site_ids_str}')"
    clause_nodeid = f"left(nodeid,7) IN ('{site_ids_str}')"
    clause_site2  = f"site_name IN ('{site_ids_str}')"
    return clause_site, clause_nodeid, clause_site2


def fetch_data(sql: str, conn):
    """Run a raw SQL query via an open psycopg2/SQLAlchemy connection."""
    return pd.read_sql_query(sql, conn)


def tuning_band_logic(system: str) -> str:
    if system in ("L1800", "L2100"):
        return "MB"
    if system in ("L700", "L900", "NR700"):
        return "LB"
    if system in ("L2600", "NR2600"):
        return "2600"
    if system == "L2300":
        return "L2300"
    return "Unknown"

def suggestion(xtxr,vendor, antenna_type, is_lte=True):
    bfant = ['AAU5639', 'AAU5614', 'AAU5636']
    sectorsplitcell = ['AAU5711a', 'AAU5726']
    if vendor == 'Huawei':
        if is_lte: 
            if any(item in antenna_type for item in bfant):
                return 'BFANT'
            elif any(item in antenna_type for item in sectorsplitcell):
                return 'SECTORSPLITCELL'
            else:
                return 'RETSUBUNIT'
        else:
            if xtxr.upper() != '64T64R':
                return 'RETSUBUNIT'
            else:
                return 'NRDUCELLTRPBEAM'
    elif vendor == 'Ericsson':
        if 'AIR' in antenna_type:
            return 'AIR (SectorCarrier)'
        else:
            return 'NON_AIR (ElectricalTilt)'
    else:
        return 'TBD'

