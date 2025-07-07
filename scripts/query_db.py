from dotenv import load_dotenv
import os
import pandas as pd
from pathlib import Path
from ret_utils.io_helper import load_cell_list, generate_where_clause

# Load environment from the repository root
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

cluster_name = os.getenv("CLUSTER_NAME")
week_name = os.getenv("WEEK_NUM")
start_date = os.getenv("START_DATE")
end_date = os.getenv("END_DATE")
folder_name = cluster_name.split('_')[0]

INPUT_FILE_PATH = f"D:/D&T Project/CR Preparing/{folder_name}/Tuning_cell_list_{cluster_name}.csv"
OUTPUT_BASE_DIR = "D:/D&T Project/CR Preparing/"

sql_lte = f"lte_{week_name}"
sql_nr = f"nr_{week_name}"

df_cell = load_cell_list(INPUT_FILE_PATH)
site_ids = df_cell['site_name_1'].unique()
where_clause, where_clause_1, where_clause_2 = generate_where_clause(site_ids)

# ======== COMMON SQL QUERIES ========

query_lte = f"""
SELECT site, site_id, cell_name, system, sector_name, antenna_type, vendor, mtilt, height, xtxr,local_cell_id,'LTE' as RAT
FROM 
    {sql_lte} a
WHERE
    {where_clause} 

"""
query_nr = f"""
SELECT vendor, site_id, gnodeb_name, sector_name,nr_cell_name as cell_name,nr_du_cell_id as local_cell_id,system,xtxr,ant_type as antenna_type,
'NR' as RAT
FROM {sql_nr} a
WHERE {where_clause}
"""


# ======== MAPPED SQL QUERIES ========

query_air = f"""
WITH RankedData AS (
    SELECT 
        LEFT(nodeid, 7) AS site, 
        nodeid, 
        sectorcarrierid, 
        date, 
        digitaltilt,
        ROW_NUMBER() OVER (
            PARTITION BY nodeid, sectorcarrierid
            ORDER BY date DESC
        ) AS RowNum
    FROM eric_air_data
)
SELECT 
    site, 
    nodeid, 
    sectorcarrierid, 
    date, 
    digitaltilt
FROM RankedData 
WHERE {where_clause_1} and RowNum = 1;

"""

query_non_air = f"""
WITH RankedData AS (
    SELECT 
        LEFT(nodeid, 7) AS site, 
        nodeid, 
        userlabel,
        antennaunitgroupid,
        antennanearunitid,
        retsubunitid,
        antennamodelnumber,
        maxtilt,
        mintilt,
        date,
        electricalAntennaTilt,
        ROW_NUMBER() OVER (
            PARTITION BY nodeid, userlabel, antennaunitgroupid, antennanearunitid, retsubunitid, antennamodelnumber, maxtilt, mintilt
            ORDER BY date DESC
        ) AS RowNum
    FROM eric_non_air_data
)
SELECT 
    site, 
    nodeid, 
    userlabel,
    antennaunitgroupid,
    antennanearunitid,
    retsubunitid,
    antennamodelnumber,
    maxtilt,
    mintilt,
    date,
    electricalAntennaTilt
FROM RankedData
WHERE {where_clause_1} and RowNum = 1;


"""

query_hw = f"""
WITH RankedData AS (
    SELECT
        site_name,
        name,
        device_name,
        device_no,
        subunit_no,
        date,
        Actual_tilt,
        ROW_NUMBER() OVER (
            PARTITION BY name, device_name, device_no,subunit_no
            ORDER BY date DESC
        ) AS RowNum
    FROM hwret_data
    WHERE {where_clause_2}
)
SELECT
    site_name,
    a.name,
    a.device_name,
    a.device_no,
    a.subunit_no,
    c.max_tilt,
    c.min_tilt,
    a.date,
    Actual_tilt
FROM RankedData a
LEFT JOIN
retdevicedata_1 c ON concat(a.date,a.NAME,a.Device_Name,a.Device_No,a.subunit_no) = concat(c.date,c.NAME,c.Device_Name,c.Device_No,c.subunit_no)
WHERE {where_clause_2} and RowNum = 1;

"""


# ======== NO MAPPED SQL QUERIES ========

query_hw_no_map = f"""
SELECT  
    'huawei' AS antenna_type, 
    site_name, 
    a.NAME, 
    a.Device_Name,
    a.Device_No,
    a.subunit_no,
    c.max_tilt,
    c.min_tilt,
    a.date,
    Actual_tilt
FROM hwret_data a
LEFT JOIN
    retdevicedata_1 c ON concat(a.NAME, a.Device_Name, a.Device_No, a.subunit_no) = concat(c.NAME, c.Device_Name, c.Device_No, c.subunit_no)
WHERE a.date BETWEEN '{start_date}' AND '{end_date}'
  AND {where_clause_2}
GROUP BY 
     
    antenna_type, 
    site_name, 
    a.NAME, 
    a.Device_Name,
    a.Device_No,
    a.subunit_no,
    a.date,
    c.max_tilt,
    c.min_tilt,
    Actual_tilt
"""

query_air_no_map = f"""
SELECT 
     
    'eric_air' AS antenna_type, 
    LEFT(NodeId, 7) AS site_name, 
    NodeId, 
    SectorCarrierId,
    date,
    digitalTilt
    
FROM 
    eric_air_data a

WHERE a.date BETWEEN '{start_date}' AND '{end_date}' AND {where_clause_1}
GROUP BY 
     
    antenna_type, 
    site_name, 
    NodeId, 
    SectorCarrierId,
    date,
    digitalTilt
"""

query_non_air_no_map = f"""
SELECT 
     
    'eric_non_air' AS antenna_type, 
    LEFT(NodeId, 7) AS site_name, 
    NodeId, 
    CASE 
        WHEN AntennaUnitGroupId ~ '^[0-9]+(\.[0-9]+)?$' THEN
            CASE 
                WHEN POSITION('.' IN AntennaUnitGroupId) > 0 THEN
                    TRIM(TRAILING '.0' FROM AntennaUnitGroupId)
                ELSE 
                    AntennaUnitGroupId
            END
        ELSE 
            AntennaUnitGroupId
    END AS NormalizedAntennaUnitGroupId,  -- Normalizing the AntennaUnitGroupId
    AntennaNearUnitId, 
    RetSubUnitId,
    userLabel,
    AntennaModelNumber,
    maxTilt,
    minTilt,
    date,
    electricalAntennaTilt
FROM
    eric_non_air_data a
WHERE a.date BETWEEN '{start_date}' AND '{end_date}' AND {where_clause_1}
GROUP BY 
     
    antenna_type, 
    site_name,
    NodeId, 
    -- Apply the same normalization in the GROUP BY clause
    CASE 
        WHEN AntennaUnitGroupId ~ '^[0-9]+(\.[0-9]+)?$' THEN
            CASE 
                WHEN POSITION('.' IN AntennaUnitGroupId) > 0 THEN
                    TRIM(TRAILING '.0' FROM AntennaUnitGroupId)
                ELSE 
                    AntennaUnitGroupId
            END
        ELSE 
            AntennaUnitGroupId
    END,
    AntennaNearUnitId, 
    RetSubUnitId,
    userLabel,
    AntennaModelNumber,
    maxTilt,
    minTilt,
    date,
    electricalAntennaTilt
"""

query_bfant_tilt = f"""
SELECT 
    a.cell_name,
	a.system,
    a.local_cell_id,
    b.name AS bfant_name,
	b.device_no,
    b.connect_rru_subrack_no,
    c.local_cell_id AS local_cell_id_cellphy,
    b.date,
    b.tilt
FROM {sql_lte} a 
LEFT JOIN cellphytopo c 
    ON CONCAT(a.enodeb_name, a.local_cell_id) = CONCAT(c.name, c.local_cell_id)
LEFT JOIN bfant b 
    ON CONCAT(b.name, b.connect_rru_subrack_no) = CONCAT(c.name, split_part(c.rf_module_information, '-', 2))
WHERE b.date BETWEEN '{start_date}' AND '{end_date}' AND {where_clause}
GROUP BY a.cell_name,a.system, a.local_cell_id, b.name,b.device_no, b.connect_rru_subrack_no, c.local_cell_id,b.date, b.tilt
"""

query_nr_tilt = f"""
SELECT
	a.nr_cell_name,
	a.system,
	a.nr_du_cell_id,
	b.name AS NRDUCELLTRPBEAM_name,
	b.nr_du_cell_trp_id,
	b.date, b.tilt
FROM {sql_nr} a 
JOIN NRDUCELLTRPBEAM b
    ON CONCAT(a.gnodeb_name, a.nr_du_cell_id) = CONCAT(b.name, b.nr_du_cell_trp_id)
WHERE b.date BETWEEN '{start_date}' AND '{end_date}' AND {where_clause}
GROUP BY a.nr_cell_name,a.system, a.nr_du_cell_id, b.name,b.nr_du_cell_trp_id,b.date, b.tilt
"""

query_split_tilt = f"""
SELECT
	a.cell_name,
	a.system,
	a.local_cell_id,
	b.name AS SPLITCELL_name,
	b.local_cell_id as SPLITCELL_local_cell_id,
	b.date, cell_beam_tilt
FROM {sql_lte} a 
JOIN SECTORSPLITCELL b
    ON CONCAT(a.enodeb_name, a.local_cell_id) = CONCAT(b.name, b.local_cell_id)
WHERE b.date BETWEEN '{start_date}' AND '{end_date}' AND {where_clause}
GROUP BY a.cell_name,a.system, a.local_cell_id, b.name,b.local_cell_id,b.date, cell_beam_tilt
"""
