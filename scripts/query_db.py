import pandas as pd

# ======== COMMON SQL QUERIES ========
def fetch_data_lte(sql_lte,where_clause, conn):
    """Run a raw SQL query via an open psycopg2/SQLAlchemy connection."""
    query_lte = f"""
    SELECT site, site_id, cell_name, system, sector_name, antenna_type, vendor, mtilt, height, xtxr,local_cell_id,'LTE' as RAT
    FROM 
    {sql_lte} a
    WHERE
    {where_clause} 

    """
    return pd.read_sql_query(query_lte, conn)


def fetch_data_nr(sql_nr,where_clause, conn):
    query_nr = f"""
    SELECT vendor, site_id, gnodeb_name, sector_name,nr_cell_name as cell_name,nr_du_cell_id as local_cell_id,system,xtxr,ant_type as antenna_type,
    'NR' as RAT
    FROM {sql_nr} a
    WHERE {where_clause}
    """
    return pd.read_sql_query(query_nr, conn)

# ======== MAPPED SQL QUERIES ========

def fetch_data_air(where_clause_1, conn):
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
    return pd.read_sql_query(query_air, conn)

def fetch_data_non_air(where_clause_1, conn):
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
    return pd.read_sql_query(query_non_air, conn)



def fetch_data_hw(where_clause_2, conn):
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
    return pd.read_sql_query(query_hw, conn)

# ======== NO MAPPED SQL QUERIES ========

def fetch_data_hw_no_map(where_clause_2, start_date, end_date, conn):
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
    return pd.read_sql_query(query_hw_no_map, conn)

def fetch_data_air_no_map(where_clause_1, start_date, end_date, conn):
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
    return pd.read_sql_query(query_air_no_map, conn)



def fetch_data_nonair_no_map(where_clause_1, start_date, end_date, conn):
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

    return pd.read_sql_query(query_non_air_no_map, conn)


def fetch_data_bfant_tilt(sql_lte, where_clause, start_date, end_date, conn):
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
    return pd.read_sql_query(query_bfant_tilt, conn)


def fetch_data_nr_tilt(sql_nr, where_clause, start_date, end_date, conn):
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
    return pd.read_sql_query(query_nr_tilt, conn)

def fetch_data_split_tilt(sql_lte, where_clause, start_date, end_date, conn):
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
    return pd.read_sql_query(query_split_tilt, conn)


