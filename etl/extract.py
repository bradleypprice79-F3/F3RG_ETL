# etl/extract.py
import os
import glob
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from config import DB_CONFIG
import re

def clean_backblast(text_string):
    if not isinstance(text_string, str):
        return text_string
    
    # 1. Remove "Backblast! " prefix (case-sensitive)
    text_string = re.sub(r"^Backblast!\s*", "", text)
    
    # 2. Remove all newlines
    text_string = text_string.replace("\n", " ")
    
    # 3. Cut off at "DATE:" (case-insensitive)
    text_string = re.split(r"date:", text_string, flags=re.IGNORECASE)[0].strip()
    
    return text_string

def get_raw_posts(DB_CONFIG, start_date, end_date):

    # Build connection string
    connection_string = (
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    # create engine
    engine = create_engine(connection_string)

    # Query your data
    # run query to get post data durring the date range.
    raw_post_data_query = text('''SELECT 
        `date`,
        'f3crossroads' AS region,
        ao_id,
        q_user_id,
        user_id,
        1 AS `Current Post Count`
    FROM f3crossroads.bd_attendance
        WHERE `date` >= :start_date
          AND `date` <= :end_date '''
    )
    post_df = pd.read_sql(raw_post_data_query, engine, params={"start_date": start_date, "end_date": end_date})
    return(post_df)

def get_raw_dimension_data(DB_CONFIG):

    # Build connection string
    connection_string = (
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    # create engine
    engine = create_engine(connection_string)

    # Query your data
    AOs_raw_query = text('''SELECT 
            a.channel_id AS ao_id,
            a.ao,
            999 AS post_count,
            
            CASE 
                WHEN a.ao = 'ao-black-diamond' THEN 4
                WHEN a.ao = '3rd-f-qsource'   THEN 4
                WHEN a.ao = '3rd-f'           THEN 5
                WHEN a.ao = 'rg_ec3'          THEN 3
                WHEN a.ao = 'rg_ec2'          THEN 2
                WHEN a.ao = 'rg_ec1'          THEN 1
                WHEN a.ao = 'rg_challenge_flag' THEN 1
                WHEN a.ao = 'rg_csaup'          THEN 40
                WHEN a.ao = '2nd-f-coffeteria'  THEN 0                
                ELSE 3
            END AS points,
            
            CASE
                WHEN a.ao LIKE 'ao-%%'     THEN '1stf'
                WHEN a.ao LIKE 'downrange%%'     THEN '1stf'
                WHEN a.ao LIKE '2nd-f%%'   THEN '2ndf'
                WHEN a.ao LIKE '3rd-f%%'   THEN '3rdf'
                WHEN a.ao LIKE '%%qsource' THEN 'qs'
                WHEN a.ao LIKE 'rg_ec%%'   THEN 'ec'
                WHEN a.ao LIKE '%%challenge_flag'  THEN 'challenge_flag'
                WHEN a.ao LIKE 'rg_csaup%%'   THEN 'csaup'
                ELSE 'none'
            END AS type
        FROM f3crossroads.aos a '''
    )
    AOs_raw = pd.read_sql(AOs_raw_query, engine)

    PAXcurrent_raw_query = text('''Select user_id, user_name from f3crossroads.users ''')
    PAXcurrent_raw = pd.read_sql(PAXcurrent_raw_query, engine)

    backblast_raw_query = text('''Select bd_date, ao_id, q_user_id, backblast from f3crossroads.beatdowns ''' )
    backblast_raw = pd.read_sql(backblast_raw_query, engine)



    return AOs_raw, PAXcurrent_raw, backblast_raw



def posts_from_csv_folder(folder_path, file_pattern="*.csv"):
    """
    Reads all CSV files in the given folder matching the file pattern.
    Returns a single concatenated DataFrame.
    """
    all_files = glob.glob(os.path.join(folder_path, file_pattern))
    if not all_files:
        print(f"No files found in {folder_path} with pattern {file_pattern}")
        return pd.DataFrame()  # empty df

    df_list = [pd.read_csv(f) for f in all_files]
    df = pd.concat(df_list, ignore_index=True)
    print(f"Loaded {len(all_files)} files, {len(df)} total rows")
    return df

def extract_dimension_tables(base_path):
    """
    Extracts 4 dimension tables (CSVs) into pandas DataFrames.

    Parameters:
        base_path (str): Path to the folder containing the CSVs.

    Returns:
        tuple: AOs, date_table, PAXcurrent, PAXdraft as pandas DataFrames
    """
    base = Path(base_path)

    AOs = pd.read_csv(base / "AOs.csv")
    date_table = pd.read_csv(base / "date_table.csv")
    PAXcurrent = pd.read_csv(base / "PAXcurrent.csv")
    PAXdraft = pd.read_csv(base / "PAXdraft.csv")
    backblast = pd.read_csv(base / "backblast.csv")

    # remove \n newline characters from backblast.
    backblast["parsed_backblast"] = backblast["parsed_backblast"].str.replace("\n", " ", regex=False)
    # Drop "DATE: ..." and everything after
    backblast["parsed_backblast"] = backblast["parsed_backblast"].str.replace(r"DATE:.*", "", regex=True).str.strip()


    return AOs, date_table, PAXcurrent, PAXdraft, backblast